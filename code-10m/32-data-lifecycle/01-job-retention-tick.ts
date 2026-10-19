//  JOB RetentionJob.runOnce (mỗi RETENTION_INTERVAL_MS, một pod duy nhất)
//  Chạy dưới khoá cluster-singleton và ngân sách đồng thời, lấy quy tắc của module, xoá theo lô bằng câu lệnh ngắn tự commit, ghi metric.
//  Step 1: Một tick retention; không chạy chồng nhau trong cùng process
//  Step 2: Lấy các quy tắc của module (module sở hữu bảng tự khai báo SQL của nó)
//  Step 3: Quy tắc của module: điều kiện, thứ tự xoá và tham số (SQL tĩnh, không nội suy input)
//  Step 4: Xoay vòng quy tắc đầu tiên để một backlog lớn không bỏ đói các quy tắc sau
//  Step 5: Xoá theo lô cho một quy tắc, có trần thời gian và dừng khi shutdown
//  Step 6: Chốt chặn cấu trúc: không bao giờ xoá bảng sổ cái, ví, chứng từ
//  Step 7: Chọn lô rồi xoá trong MỘT câu lệnh (FOR UPDATE SKIP LOCKED)
//  Step 8: Ghi metric: dry-run chỉ ghi gauge số hàng đủ điều kiện, delete ghi counter số hàng đã xoá
//  Step 9: Ngân sách đồng thời dùng chung: hết chỗ thì BỎ tick trước khi lấy kết nối pool
//  Step 10: Chỉ một pod chạy mỗi tick: advisory lock theo transaction, tự nhả khi kết nối đóng kể cả pod chết

// [WORKER] RetentionJob.runOnce · common/retention/retention-job.ts
export abstract class RetentionJob
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly job = new ManagedInterval();
  private stopping = false;
  private rotation = 0;
  protected abstract readonly logger: Logger;
  // ManagedInterval
  private scheduledRunInFlight = false;
  private clusterRetryAfter = 0;
  // ConcurrencyBudget
  private inFlight = 0;
  constructor(
    protected readonly dataSource: DataSource,
    protected readonly config: ConfigService<CoreApiEnv, true>,
    private readonly meter: Meter,
    // AuthRetentionJob
    @InjectDataSource() dataSource: DataSource,
    config: ConfigService<CoreApiEnv, true>,
    scheduler: SchedulerRegistry,
    @Inject(METRICS_METER) meter: Meter,
    // ConcurrencyBudget
    private readonly limit: number,
  ) {
    // AuthRetentionJob
    super(dataSource, config, scheduler, meter);
    // ConcurrencyBudget
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(`ConcurrencyBudget limit không hợp lệ: ${limit}`);
    }
  }

  // Step 1: Một tick retention; không chạy chồng nhau trong cùng process
  async runOnce(): Promise<PurgeResult[]> {
    return this.job.runExclusive(async () => {
      const mode = this.config.getOrThrow('RETENTION_MODE', { infer: true });
      if (mode === 'off') return [];
      const options = {
        mode,
        batchSize: this.config.getOrThrow('RETENTION_BATCH_SIZE', {
          infer: true,
        }),
        maxBatches: this.config.getOrThrow('RETENTION_MAX_BATCHES_PER_TICK', {
          infer: true,
        }),
        pauseMs: this.config.getOrThrow('RETENTION_BATCH_PAUSE_MS', {
          infer: true,
        }),
      };
      const intervalMs = this.config.getOrThrow('RETENTION_INTERVAL_MS', {
        infer: true,
      });
      // Mỗi tick bị chặn ở tối đa 30 giây (và không quá chu kỳ): cluster-singleton giữ một
      // transaction + một kết nối pool suốt tick nên tick không được kéo dài vô hạn. Hết ngân
      // sách thì dừng ở ranh giới lô, backlog còn lại để tick sau.
      const deadlineAt =
        Date.now() + Math.min(RETENTION_TICK_BUDGET_MS, intervalMs);

      // Step 2: Lấy các quy tắc của module (module sở hữu bảng tự khai báo SQL của nó)
      const steps = this.steps();

      // [WORKER] AuthRetentionJob.steps · auth/jobs/auth-retention.job.ts
      const graceDays = this.config.getOrThrow(
        'AUTH_REFRESH_RETENTION_GRACE_DAYS',
        { infer: true },
      );
      const otpDays = this.config.getOrThrow('AUTH_OTP_RETENTION_DAYS', {
        infer: true,
      });
      const steps: PurgeStep[] = [];
      if (graceDays > 0) {
        steps.push({
          label: 'refresh_tokens:expired',
          table: 'refresh_tokens',
          primaryKey: 'id',

          // Step 3: Quy tắc của module: điều kiện, thứ tự xoá và tham số (SQL tĩnh, không nội suy
          // input)
          // Theo expires_at (không theo created_at): đổi AUTH_REFRESH_TTL_DAYS không làm xoá nhầm
          // token còn hạn. ORDER BY cùng cột để mỗi lô lấy hàng cũ nhất, khớp index B-tree
          // idx_refresh_tokens_expires_at (B-tree thay BRIN: BRIN mất chọn lọc khi bảng bị xoá
          // rồi chèn lại, ≈ 430–540 ms so với ≈ 0,6 ms mỗi lô sau 4 vòng xoá-rồi-chèn-lại, đo
          // lab).
          predicate: 'expires_at < now() - make_interval(days => $2)',
          params: [graceDays],
          orderBy: 'expires_at',
        });
      }
      if (otpDays > 0) {
        steps.push({
          label: 'phone_otps:old',
          table: 'phone_otps',
          primaryKey: 'id',
          predicate: 'created_at < now() - make_interval(days => $2)',
          params: [otpDays],
          orderBy: 'created_at',
        });
      }
      return steps;

      // Step 4: Xoay vòng quy tắc đầu tiên để một backlog lớn không bỏ đói các quy tắc sau
      // Xoay vòng điểm bắt đầu: nếu quy tắc đầu luôn có backlog lớn thì các quy tắc sau không bao
      // giờ tới lượt.
      const start = steps.length === 0 ? 0 : this.rotation % steps.length;
      this.rotation += 1;
      const results: PurgeResult[] = [];
      for (let i = 0; i < steps.length; i += 1) {
        if (this.stopping || (i > 0 && Date.now() >= deadlineAt)) break;
        const step = steps[(start + i) % steps.length] as PurgeStep;

        // Step 5: Xoá theo lô cho một quy tắc, có trần thời gian và dừng khi shutdown
        const result = await purgeInBatches(this.dataSource, step, {
          ...options,
          deadlineAt,
          shouldStop: () => this.stopping,
        });

        // [POSTGRES] purgeInBatches · common/retention/batched-purge.ts
        const dataSource = this.dataSource;
        // options = { ...options, deadlineAt, shouldStop: () => this.stopping, }
        if (options.mode === 'dry-run') {
          const cap = options.batchSize * options.maxBatches;
          const [row] = (await dataSource.query(buildCountSql(step), [
            cap,
            ...step.params,
          ])) as Array<{ n: number }>;
          const rows = row?.n ?? 0;
          return {
            label: step.label,
            mode: 'dry-run',
            rows,
            batches: 0,
            capped: rows >= cap,
          };
        }

        const sql = buildDeleteSql(step);

        // [POSTGRES] buildDeleteSql
        assertIdentifier(step.table, 'table');
        assertIdentifier(step.primaryKey, 'primary key');

        // Step 6: Chốt chặn cấu trúc: không bao giờ xoá bảng sổ cái, ví, chứng từ
        assertDeletable(step.table);

        // [POSTGRES] assertDeletable
        const table = step.table;
        // Khớp cả bảng con phân vùng (transactions_2026_01): ADR 0014 dự kiến phân vùng theo
        // tháng cho chính các bảng này. Tiền tố `<tên>_` có thể chặn nhầm một bảng khác tên
        // giống: hướng lỗi an toàn (từ chối), thấy ngay lúc dựng SQL.
        const protectedName = [...PROTECTED_TABLES].find(
          (name) => table === name || table.startsWith(`${name}_`),
        );
        if (protectedName !== undefined) {
          throw new Error(`Retention không được phép xoá bảng được bảo vệ: ${table}`);
        }

        // ctid chỉ định danh hàng trong MỘT bảng con: với bảng phân vùng phải kèm tableoid, nếu
        // không `t.ctid = d.ctid` khớp cả hàng cùng ctid ở partition khác và xoá nhầm.
        const keyColumns =
          step.primaryKey === 'ctid' ? ['ctid', 'tableoid'] : [step.primaryKey];
        const order = step.orderBy === undefined ? '' : `ORDER BY ${step.orderBy}`;
        return `
          WITH doomed AS MATERIALIZED (
            SELECT ${keyColumns.join(', ')} FROM ${step.table}
             WHERE ${step.predicate}
             ${order}
             LIMIT $1
             FOR UPDATE SKIP LOCKED
          ), removed AS (
            DELETE FROM ${step.table} t
             USING doomed d
             WHERE ${keyColumns.map((column) => `t.${column} = d.${column}`).join(' AND ')}
            RETURNING 1
          )
          SELECT count(*)::int AS n FROM removed`;

        let rows = 0;
        let batches = 0;
        let lastFull = false;
        while (batches < options.maxBatches) {

          // Step 7: Chọn lô rồi xoá trong MỘT câu lệnh (FOR UPDATE SKIP LOCKED)
          // Mỗi lô là một câu lệnh ngắn tự commit: không giữ transaction dài và không giữ khoá
          // hàng lâu. Dừng khi hết hàng hoặc chạm maxBatches; phần còn lại để tick sau.
          const [row] = (await dataSource.query(sql, [
            options.batchSize,
            ...step.params,
          ])) as Array<{ n: number }>;
          const removed = row?.n ?? 0;
          batches += 1;
          rows += removed;
          lastFull = removed >= options.batchSize;
          if (!lastFull) break;
          if (options.shouldStop?.() === true) break;
          // Nghỉ giữa các lô để vacuum và replica theo kịp.
          if (options.pauseMs > 0 && batches < options.maxBatches) {
            await sleep(options.pauseMs);
          }
          if (options.deadlineAt !== undefined && Date.now() >= options.deadlineAt) {
            break;
          }
        }
        return {
          label: step.label,
          mode: 'delete',
          rows,
          batches,
          capped: lastFull,
        };

        // Step 8: Ghi metric: dry-run chỉ ghi gauge số hàng đủ điều kiện, delete ghi counter số
        // hàng đã xoá
        recordRetention(this.meter, result);

        // [WORKER] recordRetention · common/retention/retention-metrics.ts
        const meter = this.meter;
        try {
          const { deleted, eligible } = instrumentsOf(meter);
          if (result.mode === 'delete') {
            deleted.add(result.rows, { rule: result.label });
          } else {
            eligible.record(result.rows, { rule: result.label });
          }
        } catch {
        }

        if (result.rows > 0) {
          this.logger.log(
            `retention ${result.label} (${result.mode}): ${result.rows} hàng / ${result.batches} lô${
              result.capped ? ', còn backlog' : ''
            }`,
          );
        }
        results.push(result);
      }
      return results;
    }, []);
  }
}

// [WORKER] ManagedInterval.runScheduled · common/scheduling/managed-interval.ts
export class ManagedInterval {
  private async runScheduled(options: {
    jobName: string;
    intervalMs: number;
    task: () => Promise<unknown>;
    clusterSingleton?: ClusterSingletonOptions;
    skipWhenIdle?: boolean;
  }): Promise<void> {
    if (this.scheduledRunInFlight) return;
    if (options.skipWhenIdle && !isRuntimeActive()) {
      recordRuntimeBackgroundSkip(options.jobName);
      return;
    }
    if (
      options.clusterSingleton !== undefined &&
      Date.now() < this.clusterRetryAfter
    ) {
      return;
    }

    this.scheduledRunInFlight = true;
    try {
      const clusterSingleton = options.clusterSingleton;
      if (clusterSingleton === undefined) {
        await options.task();
        return;
      }

      // Step 9: Ngân sách đồng thời dùng chung: hết chỗ thì BỎ tick trước khi lấy kết nối pool
      const release = clusterSingleton.budget?.tryAcquire();

      // [WORKER] ConcurrencyBudget.tryAcquire · common/scheduling/concurrency-budget.ts
      // Không chờ và không xếp hàng: hết ngân sách thì tick bị bỏ, tick sau thử lại. Hàng đợi
      // luôn có trần bằng 0 nên không thể phình, và việc chậm lại rơi vào job nền chứ không phải
      // request người dùng. Mỗi tick giữ ≤ 2 kết nối pool (một cho advisory lock, một cho task),
      // nên RETENTION_MAX_CONCURRENT_JOBS=2 giữ retention ở ≤ 4 kết nối.
      if (this.inFlight >= this.limit) return undefined;
      this.inFlight += 1;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        this.inFlight -= 1;
      };

      if (clusterSingleton.budget !== undefined && release === undefined) {
        recordRuntimeBackgroundSkip(options.jobName);
        return;
      }
      try {
        await this.runClusterSingleton({
          ...options,
          clusterSingleton,
        });

        // [WORKER] ManagedInterval.runClusterSingleton
        // options = { ...options, clusterSingleton, }
        const retryMs =
          options.clusterSingleton.followerRetryMs ??
          Math.max(options.intervalMs, CLUSTER_FOLLOWER_RETRY_MIN_MS);
        let runner: QueryRunner | undefined;

        try {
          runner = options.clusterSingleton.dataSource.createQueryRunner();
          await runner.connect();
          await runner.startTransaction();

          // Step 10: Chỉ một pod chạy mỗi tick: advisory lock theo transaction, tự nhả khi kết nối
          // đóng kể cả pod chết
          const rows = (await runner.query(
            `SELECT pg_try_advisory_xact_lock(
                      hashtextextended($1, 0)
                    ) AS acquired`,
            [`litmatch:cluster-job:${options.jobName}`],
          )) as Array<{ acquired: boolean }>;

          if (rows[0]?.acquired !== true) {
            // DB hỏng không được làm mọi replica thử giành khoá ở mỗi tick ngắn: nhớ thời điểm
            // được phép thử lại.
            this.clusterRetryAfter = Date.now() + retryMs;
            await runner.rollbackTransaction();
            return;
          }

          this.clusterRetryAfter = 0;
          await options.task();
          await runner.commitTransaction();
        } catch (error) {
          if (runner?.isTransactionActive) {
            await runner.rollbackTransaction().catch(() => undefined);
          }
          this.clusterRetryAfter = Date.now() + retryMs;
          throw error;
        } finally {
          await runner?.release().catch(() => undefined);
        }
      } finally {
        release?.();
      }
    } finally {
      this.scheduledRunInFlight = false;
    }
  }
}

// [POSTGRES] batched-purge.ts · common/retention/batched-purge.ts
const PROTECTED_TABLES: ReadonlySet<string> = new Set([
  'transactions',
  'ledger_entries',
  'ledger_accounts',
  'wallets',
  'iap_receipts',
  'payos_payment_orders',
  'gift_events',
  'admin_audit_logs',
]);

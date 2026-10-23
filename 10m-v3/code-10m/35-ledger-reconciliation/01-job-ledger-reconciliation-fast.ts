//  JOB ReconciliationService.runFast (mỗi ECONOMY_RECONCILIATION_FAST_INTERVAL_MS)
//  Tiến checkpoint bền theo từng trang transaction (keyset created_at, id) trong ngân sách mỗi tick, đếm receipt đã credit mà thiếu transaction, rồi báo lệch qua log và metric; quét dở không bao giờ báo sạch.
//  Step 1: Tier rẻ chạy dày: kiểm Nợ = Có theo currency và đếm receipt mồ côi
//  Step 2: Tiến checkpoint bền thêm vài trang, rồi lấy các currency còn lệch
//  Step 3: Lặp theo trang tới khi hết trần lô, hết ngân sách hoặc đã bắt kịp
//  Step 4: Mở một transaction cho đúng một trang
//  Step 5: Khoá dòng checkpoint, lấy cutoff = now − LAG từ đồng hồ DB
//  Step 6: Đến kỳ rebase thì đặt lại tổng và cursor
//  Step 7: Đọc trang transaction kế tiếp (keyset) kèm tổng Nợ − Có theo currency
//  Step 8: Xác định đã bắt kịp cutoff hay chưa
//  Step 9: Ghi tổng và cursor mới vào dòng checkpoint
//  Step 10: Đếm receipt đã credit mà chưa có transaction
//  Step 11: Tính ok từ ba điều kiện: đã bắt kịp, không lệch, không receipt mồ côi
//  Step 12: Lệch thì log error (metric đã ghi ở trên); job không tự sửa

// [WORKER] ReconciliationService.runFast · economy/jobs/reconciliation.service.ts
@Injectable()
export class ReconciliationService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(ReconciliationService.name);
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly metrics: EconomyMetrics,
    // LedgerReconciliationCheckpoint
    private readonly dataSource: DataSource,
  ) {}

  // Step 1: Tier rẻ chạy dày: kiểm Nợ = Có theo currency và đếm receipt mồ côi
  async runFast(): Promise<FastReconciliationReport> {
    const startedAt = Date.now();

    // Step 2: Tiến checkpoint bền thêm vài trang, rồi lấy các currency còn lệch
    const progress = await this.currentImbalances();

    // [WORKER] ReconciliationService.currentImbalances
    return new LedgerReconciliationCheckpoint(this.dataSource).advance({
      // LAG phải lớn hơn giao dịch ghi sổ lâu nhất: created_at là giờ BẮT ĐẦU transaction, nên
      // giao dịch chạy lâu có thể commit sau khi cursor đã đi qua mốc của nó; nó chỉ bị bắt lại ở
      // lần rebase kế tiếp.
      lagMs: this.config.getOrThrow('ECONOMY_RECONCILIATION_LAG_MS', {
        infer: true,
      }),
      fullIntervalMs: this.config.getOrThrow(
        'ECONOMY_RECONCILIATION_FULL_INTERVAL_MS',
        { infer: true },
      ),
      batchSize: this.config.getOrThrow('ECONOMY_RECONCILIATION_BATCH_SIZE', {
        infer: true,
      }),
      maxBatches: this.config.getOrThrow(
        'ECONOMY_RECONCILIATION_MAX_BATCHES_PER_TICK',
        { infer: true },
      ),
      budgetMs: this.config.getOrThrow(
        'ECONOMY_RECONCILIATION_TICK_BUDGET_MS',
        { infer: true },
      ),
    });

    // [WORKER] LedgerReconciliationCheckpoint.advance · economy/jobs/ledger-reconciliation-checkpoint.ts
    // options = đối số 1 của lời gọi ở trên
    const deadline = Date.now() + options.budgetMs;
    let progress: LedgerReconciliationProgress = {
      imbalances: [],
      complete: false,
      completedBefore: null,
    };

    // Step 3: Lặp theo trang tới khi hết trần lô, hết ngân sách hoặc đã bắt kịp
    for (let batch = 0; batch < options.maxBatches; batch++) {
      // Lô đầu luôn chạy nên tick nào cũng tiến được ít nhất một trang. Ngân sách thời gian chỉ
      // được kiểm GIỮA các trang: một trang đang chạy không bị cắt, trần của riêng trang là
      // statement deadline.
      if (batch > 0 && Date.now() >= deadline) break;
      progress = await this.advancePage(options);

      // [POSTGRES] LedgerReconciliationCheckpoint.advancePage
      // Step 4: Mở một transaction cho đúng một trang
      // Tổng cộng dồn và cursor commit CÙNG NHAU hoặc không gì cả: lỗi giữa chừng rollback cả
      // hai, pod thay thế tiếp tục đúng chỗ, không cộng trùng và không bỏ sót trang.
      return this.dataSource.transaction(async (manager) => {
        await manager.query(
          `INSERT INTO runtime_job_checkpoints (job_key, state) VALUES ($1, $2::jsonb)
           ON CONFLICT (job_key) DO NOTHING`,
          [CHECKPOINT_KEY, JSON.stringify(initialState())],
        );

        // Step 5: Khoá dòng checkpoint, lấy cutoff = now − LAG từ đồng hồ DB
        // Tính đúng đắn khi nhiều pod nằm ở khoá dòng này, không ở khoá cluster-singleton: hai
        // pod chạy cùng lúc phải xếp hàng và nhận hai trang KHÁC nhau.
        const [checkpoint] = (await manager.query(
          `SELECT state,
                  (clock_timestamp() - ($2::double precision * interval '1 millisecond'))::text AS cutoff,
                  (extract(epoch FROM clock_timestamp()) * 1000)::bigint::text AS "nowMs"
             FROM runtime_job_checkpoints WHERE job_key = $1 FOR UPDATE`,
          [CHECKPOINT_KEY, options.lagMs],
        )) as Array<{ state: CheckpointState; cutoff: string; nowMs: string }>;
        let state = checkpoint.state;
        const nowMs = Number(checkpoint.nowMs);
        if (
          !state.rebuilding &&
          state.rebasedAt !== null &&
          nowMs - state.rebasedAt >= options.fullIntervalMs
        ) {

          // Step 6: Đến kỳ rebase thì đặt lại tổng và cursor
          // Rebase quét LẠI từ đầu nhưng vẫn theo trang và bị chặn như mọi tick. Mục đích là bắt
          // transaction commit muộn, backdate hoặc entry chèn ngoài write path chuẩn mà lượt tăng
          // dần đã đi qua; thời gian hoàn tất phụ thuộc backlog, không có cam kết 24 giờ.
          state = initialState();
        }
        const parameters: unknown[] = [checkpoint.cutoff, options.batchSize];
        if (state.cursorAt !== null)
          parameters.push(state.cursorAt, state.cursorId);

        // Step 7: Đọc trang transaction kế tiếp (keyset) kèm tổng Nợ − Có theo currency
        // Keyset (created_at, id) trên idx_transactions_reconciliation_cursor: đọc tiếp ngay sau
        // cursor, không sort cả lịch sử. Trang chứa trọn từng transaction (join ledger_entries
        // theo id của lô) nên tổng luôn cân nếu dữ liệu đúng. Cursor lấy dạng text để giữ đủ
        // micro giây.
        const [page] = (await manager.query(
          `WITH batch AS MATERIALIZED (
             SELECT id, created_at FROM transactions
              WHERE created_at < $1::timestamptz
                ${state.cursorAt === null ? '' : 'AND (created_at, id) > ($3::timestamptz, $4::uuid)'}
              ORDER BY created_at, id LIMIT $2
           ), deltas AS (
             SELECT le.currency,
                    SUM(CASE WHEN le.direction = 'debit' THEN le.amount ELSE -le.amount END)::text AS delta
               FROM batch b JOIN ledger_entries le ON le.transaction_id = b.id
              GROUP BY le.currency
           )
           SELECT (SELECT count(*)::int FROM batch) AS count,
                  (SELECT created_at::text FROM batch ORDER BY created_at DESC, id DESC LIMIT 1) AS at,
                  (SELECT id::text FROM batch ORDER BY created_at DESC, id DESC LIMIT 1) AS id,
                  COALESCE((SELECT jsonb_object_agg(currency, delta) FROM deltas), '{}'::jsonb) AS sums`,
          parameters,
        )) as Array<{
          count: number;
          at: string | null;
          id: string | null;
          sums: Record<string, string>;
        }>;
        // amount là bigint (driver trả string) nên cộng bằng BigInt, không dùng number.
        for (const [currency, delta] of Object.entries(page.sums)) {
          state.sums[currency] = (
            BigInt(state.sums[currency] ?? '0') + BigInt(delta)
          ).toString();
        }
        if (page.count > 0) {
          state.cursorAt = page.at;
          state.cursorId = page.id;
        }

        // Step 8: Xác định đã bắt kịp cutoff hay chưa
        // Trang ngắn hơn BATCH_SIZE nghĩa là đã hết transaction cũ hơn cutoff. Chỉ khi đó mới đặt
        // completedBefore (và rebasedAt nếu vừa xong một lượt rebase); trang đầy thì chưa biết
        // còn bao nhiêu nên không báo hoàn tất.
        const complete = page.count < options.batchSize;
        if (complete) {
          state.completedBefore = checkpoint.cutoff;
          if (state.rebuilding) state.rebasedAt = nowMs;
          state.rebuilding = false;
        }

        // Step 9: Ghi tổng và cursor mới vào dòng checkpoint
        // Chỉ metadata chẩn đoán dựng lại được mới được ghi, trong cùng transaction đang giữ
        // khoá; transactions, ledger_entries, wallets và iap_receipts không bao giờ bị chạm.
        await manager.query(
          'UPDATE runtime_job_checkpoints SET state = $2::jsonb, updated_at = now() WHERE job_key = $1',
          [CHECKPOINT_KEY, JSON.stringify(state)],
        );
        return {
          // Báo lệch ngay từ tổng đang có, kể cả khi mới quét dở: mỗi transaction đã cân Nợ = Có
          // lúc ghi và trang chứa trọn transaction, nên tổng riêng phần khác 0 là lệch thật chứ
          // không do cắt ngang.
          imbalances: Object.entries(state.sums)
            .filter(([, amount]) => BigInt(amount) !== 0n)
            .map(([currency, imbalance]) => ({ currency, imbalance })),
          complete,
          completedBefore: complete ? state.completedBefore : null,
        };
      });

      if (progress.complete) break;
    }
    return progress;

    const imbalances = progress.imbalances;

    // Step 10: Đếm receipt đã credit mà chưa có transaction
    // COUNT chạy trên index một phần chỉ chứa receipt đã credit mà chưa gắn transaction (thường
    // rỗng), nên không quét bảng receipt.
    const [{ count: orphanReceipts }]: Array<{ count: string }> =
      await this.dataSource.query(
        `SELECT COUNT(*)::text AS count FROM iap_receipts WHERE status = 'credited' AND transaction_id IS NULL`,
      );

    const report: FastReconciliationReport = {
      currencyImbalances: imbalances,
      receiptsWithoutTransaction: Number(orphanReceipts),

      // Step 11: Tính ok từ ba điều kiện: đã bắt kịp, không lệch, không receipt mồ côi
      // Quét dở không bao giờ báo sạch: complete = false luôn kéo ok xuống false dù tổng đang
      // cân, vì phần lịch sử chưa quét có thể còn lệch.
      ok:
        progress.complete &&
        imbalances.length === 0 &&
        Number(orphanReceipts) === 0,
      complete: progress.complete,
      completedBefore: progress.completedBefore,
    };

    for (const { currency } of imbalances)
      this.metrics.recordReconciliationMismatch('invariant', currency);
    this.metrics.recordReconciliationMismatch(
      'orphan_receipt',
      LedgerCurrency.Diamond,
      report.receiptsWithoutTransaction,
    );
    this.metrics.recordReconciliationRun(
      'fast',
      report.ok,
      (Date.now() - startedAt) / 1000,
    );

    // Đang bắt kịp mà chưa thấy lệch thì chỉ cảnh báo: chưa phải thành công (gauge
    // last_run_status = 0) và cũng chưa phải lệch. Backlog lớn giữ trạng thái này qua nhiều tick.
    if (
      !progress.complete &&
      imbalances.length === 0 &&
      Number(orphanReceipts) === 0
    ) {
      this.logger.warn(
        'Reconciliation is catching up; incomplete history is not a successful audit',
      );
    } else if (!report.ok) {

      // Step 12: Lệch thì log error (metric đã ghi ở trên); job không tự sửa
      // Chỉ log và metric để alert fire: sửa lệch thật phải đi qua reversal entry của
      // LedgerService, không bao giờ từ job chẩn đoán.
      this.logger.error(
        { report },
        'ĐỐI SOÁT LỆCH (fast) — tổng Nợ/Có hoặc receipt không khớp, cần điều tra ngay',
      );
    } else {
      this.logger.debug('Đối soát fast OK — tổng Nợ = tổng Có, receipt khớp');
    }
    return report;
  }
}

// [POSTGRES] runtime_job_checkpoints (bảng checkpoint) · database/migrations/1759100000000-runtime-recovery-checkpoints.ts
// Một dòng cho mỗi job: job_key là khoá chính, vừa là đích ON CONFLICT vừa là dòng bị khoá FOR
// UPDATE; state (jsonb) giữ tiến độ của job, ở đối soát sổ cái là tổng và cursor, ở mẫu ví là
// afterUserId. Chỉ là trạng thái điều phối thêm vào, không viết lại lịch sử tài chính và dựng lại
// được.
await queryRunner.query(`CREATE TABLE runtime_job_checkpoints (
  job_key text PRIMARY KEY,
  state jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
)`);

// [POSTGRES] idx_transactions_reconciliation_cursor (index keyset) · database/migrations/1759200000000-recovery-cursor-indexes.ts
{
  // B-tree (created_at, id) đúng thứ tự keyset nên ORDER BY created_at, id LIMIT n đọc n khoá đầu
  // sau cursor thay vì sort cả lịch sử. Migration tạo CONCURRENTLY, ngoài transaction.
  name: 'idx_transactions_reconciliation_cursor',
  sql: 'ON transactions (created_at, id)',
},

// [POSTGRES] idx_iap_receipts_orphan (index một phần) · database/migrations/1758400000000-reconciliation-indexes.ts
{
  // Index một phần chỉ chứa receipt đã credit mà chưa gắn transaction (thường rỗng): câu COUNT
  // orphan của tier fast gần như không tốn gì.
  name: 'idx_iap_receipts_orphan',
  sql: "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_iap_receipts_orphan ON iap_receipts (id) WHERE status = 'credited' AND transaction_id IS NULL",
},

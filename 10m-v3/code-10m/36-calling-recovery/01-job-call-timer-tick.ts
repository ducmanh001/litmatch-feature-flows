//  JOB CallTickerService.runOnce (mỗi CALLING_TICKER_INTERVAL_MS, trên mọi pod core-api)
//  Mỗi pod claim nguyên tử tối đa 100 call bằng một câu UPDATE ... FOR UPDATE SKIP LOCKED (lease theo đồng hồ DB), chốt call pending quá hạn, quá reconnect grace hoặc vượt giới hạn free trong ngân sách thời gian của tick, chỉ để marker cleanup rồi nhả lease.
//  Step 1: Một tick timer của pod này; tick chồng nhau trong cùng process bị bỏ
//  Step 2: Ngân sách thời gian của tick (CALLING_TICK_BUDGET_MS)
//  Step 3: Nhóm pending: claim tối đa 100 call tạo quá CALLING_PENDING_TIMEOUT_SECONDS
//  Step 4: Một câu SQL vừa chọn (SKIP LOCKED) vừa gắn lease theo giờ DB
//  Step 5: Chốt call pending quá hạn: end pending_timeout, chỉ để marker cleanup
//  Step 6: Kiểm lại điều kiện quá hạn dưới row lock: ứng viên chỉ là ảnh chụp cũ
//  Step 7: Ghi marker cleanup cùng transaction với trạng thái ended
//  Step 8: Nhả lease của các call đã xử lý xong, kể cả nhánh lỗi hoặc bỏ qua
//  Step 9: Nhóm reconnect: claim call có cửa sổ reconnect đã quá CALLING_RECONNECT_WINDOW_SECONDS
//  Step 10: Nhóm free-limit: claim call Voice Match active đã quá CALLING_FREE_CALL_SECONDS
//  Step 11: Vòng lặp của tick: chọn nhóm tới lượt rồi dời con trỏ, dừng khi hết ngân sách

// [WORKER] CallTickerService.runOnce · calling/jobs/call-ticker.service.ts
@Injectable()
export class CallTickerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CallTickerService.name);
  private readonly job = new ManagedInterval();
  private stopping = false;
  private nextSweep = 0;
  // CallingService
  private readonly logger = new Logger(CallingService.name);
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly callingService: CallingService,
    // CallingService
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
    private readonly metrics: CallingMetrics,
  ) {}

  // Step 1: Một tick timer của pod này; tick chồng nhau trong cùng process bị bỏ
  async runOnce(): Promise<void> {
    // Trong MỘT process, tick không chạy chồng nhau (cờ running). Giữa các pod thì KHÔNG có loại
    // trừ nào: nhiều pod cùng chạy timer là chủ đích, an toàn nhờ claim lease và kiểm lại dưới
    // row lock.
    await this.job.runExclusive(async () => {
      await withSpan('litmatch.calling', 'calling.ticker.tick', async () => {

        // Step 2: Ngân sách thời gian của tick (CALLING_TICK_BUDGET_MS)
        const deadline = this.tickDeadline();

        // [WORKER] CallTickerService.tickDeadline
        return (
          // Deadline hợp tác: chỉ chặn việc KHỞI ĐỘNG thêm ứng viên hoặc nhóm mới. SQL và I/O đã
          // bắt đầu vẫn chờ driver kết thúc hoặc timeout riêng, không bị huỷ ngay khi hết ngân
          // sách.
          Date.now() +
          this.config.getOrThrow('CALLING_TICK_BUDGET_MS', { infer: true })
        );

        // Ba nhóm chạy NỐI TIẾP, không song song: timer đi làn critical của admission, được dành
        // riêng hai kết nối pool (một cho claim, một cho tác vụ) và chỉ một tick chạy mỗi lần.
        const sweeps = [
          () => this.sweepPending(deadline),
          () => this.sweepReconnectTimeouts(deadline),
          () => this.processActiveCalls(deadline),
        ];

        // [WORKER] CallTickerService.sweepPending
        const timeoutSeconds = this.config.getOrThrow(
          'CALLING_PENDING_TIMEOUT_SECONDS',
          { infer: true },
        );
        const cutoff = new Date(Date.now() - timeoutSeconds * 1000);

        // Step 3: Nhóm pending: claim tối đa 100 call tạo quá CALLING_PENDING_TIMEOUT_SECONDS
        const stale = await this.selectCandidates({
          select: { id: true },
          where: { status: CallSessionStatus.Pending, createdAt: LessThan(cutoff) },
          order: { createdAt: 'ASC', id: 'ASC' },
          take: TICK_BATCH_SIZE,
        });

        // [POSTGRES] CallTickerService.selectCandidates
        const options = {
          select: { id: true },
          where: { status: CallSessionStatus.Pending, createdAt: LessThan(cutoff) },
          order: { createdAt: 'ASC', id: 'ASC' },
          take: TICK_BATCH_SIZE,
        };
        const claimColumn = 'timer_claim_until';
        return this.claimCandidates(
          this.dataSource
            .getRepository(CallSession)
            .createQueryBuilder('call')
            .setFindOptions(options),
          claimColumn,
        );

        // [POSTGRES] CallTickerService.claimCandidates
        const query = this.dataSource
          .getRepository(CallSession)
          .createQueryBuilder('call')
          .setFindOptions(options);
        // Chọn và claim là MỘT câu lệnh: SELECT ... FOR UPDATE SKIP LOCKED nằm trong CTE rồi
        // UPDATE cột lease ngay trong câu đó. Các pod nhận lô rời nhau kể cả khi khoá hàng của
        // SELECT đã nhả; hàng còn lease chưa hết hạn không được chọn lại.
        const [sql, parameters] = query
          .select('call.id', 'id')
          .andWhere(
            `(call.${claimColumn} IS NULL OR call.${claimColumn} <= clock_timestamp())`,
          )
          .setLock('pessimistic_write')
          .setOnLocked('skip_locked')
          .getQueryAndParameters();
        const leaseMs = this.config.getOrThrow('CALLING_JOB_LEASE_MS', {
          infer: true,
        });

        // Step 4: Một câu SQL vừa chọn (SKIP LOCKED) vừa gắn lease theo giờ DB
        // Lease = clock_timestamp() + CALLING_JOB_LEASE_MS theo đồng hồ DB, không theo đồng hồ
        // pod: pod chết giữa chừng thì lease tự hết hạn và pod khác nhận lại lô. Lease chỉ điều
        // phối at-least-once; trạng thái đúng vẫn do kiểm lại dưới row lock quyết định.
        return this.dataSource.query(
          `WITH candidates AS (${sql}), claims AS (
           UPDATE call_sessions claimed
              SET ${claimColumn} = clock_timestamp() + ($${parameters.length + 1}::double precision * interval '1 millisecond')
             FROM candidates
            WHERE claimed.id = candidates.id
           RETURNING claimed.id, claimed.end_reason AS "endReason", claimed.${claimColumn}::text AS "claimUntil"
           ) SELECT * FROM claims`,
          [...parameters, leaseMs],
        ) as Promise<ClaimedCall[]>;

        let attempted = 0;
        try {
          for (const call of stale) {

            // [WORKER] CallTickerService.canStartCandidate
            // Một SELECT claim chậm có thể đã dùng hết ngân sách, nhưng tick vẫn thử ít nhất MỘT
            // ứng viên để luôn tiến triển (không đói vô hạn); shutdown luôn thắng quy tắc này.
            return !this.stopping && (attempted === 0 || Date.now() < deadline);

            if (!this.canStartCandidate(deadline, attempted)) break;
            attempted++;

            // Step 5: Chốt call pending quá hạn: end pending_timeout, chỉ để marker cleanup
            await this.callingService.endById(
              call.id,
              CallEndReason.PendingTimeout,
              (current) =>
                current.status === CallSessionStatus.Pending &&
                current.createdAt < cutoff,
              // deferCleanup: timer chỉ chuyển call sang ended và để marker cleanup_pending,
              // không chờ SFU hay Matching (một SFU chậm không được giữ vòng timer). skipLocked:
              // hàng đang bị khoá bởi webhook hoặc người dùng thì bỏ qua thay vì chờ, các call
              // sau không bị chặn và lô sau nhặt lại.
              { deferCleanup: true, skipLocked: true },
            );

            // [SERVICE] CallingService.endById · calling/calling.service.ts
            const callId = call.id;
            const reason = CallEndReason.PendingTimeout;
            const shouldEnd = (current) =>
              current.status === CallSessionStatus.Pending &&
              current.createdAt < cutoff;
            // options = { deferCleanup: true, skipLocked: true }
            const result = await this.dataSource.transaction(async (manager) => {
              // Khoá FOR UPDATE là chốt chặn race giữa end tay, timer và webhook (cùng khoá một
              // hàng call_sessions). Với skipLocked (timer) hàng đang bị khoá thì trả call = null
              // ngay thay vì chờ.
              const call = await manager.findOne(CallSession, {
                where: { id: callId },
                lock: {
                  mode: 'pessimistic_write',
                  ...(options?.skipLocked ? { onLocked: 'skip_locked' as const } : {}),
                },
              });
              if (!call) return { call: null, justEnded: false };
              if (call.status === CallSessionStatus.Ended) {
                return { call, justEnded: false };
              }

              // Step 6: Kiểm lại điều kiện quá hạn dưới row lock: ứng viên chỉ là ảnh chụp cũ
              // Ứng viên của timer chỉ là ảnh chụp lúc SELECT: webhook có thể đã kích hoạt hoặc
              // cho call hồi phục trong lúc chờ khoá. Điều kiện quá hạn được kiểm lại ở đây, dưới
              // khoá; không còn đúng thì call giữ nguyên.
              if (shouldEnd && !shouldEnd(call)) return { call, justEnded: false };
              call.status = CallSessionStatus.Ended;
              call.endReason = reason;
              call.endedAt = new Date();

              // Step 7: Ghi marker cleanup cùng transaction với trạng thái ended
              // Marker cleanup được ghi cùng transaction với ended: process chết ngay sau commit
              // cũng không làm mất việc dọn Matching và room, vòng cleanup sẽ nhặt lại.
              call.cleanupPending = true;
              // Thời gian nằm trong cửa sổ reconnect không tính vào thời lượng: dời startedAt qua
              // đoạn gián đoạn trước khi chốt durationSeconds (giờ server core-api là nguồn sự
              // thật).
              if (call.startedAt && call.reconnectStartedAt) {
                const pausedMs = Math.max(
                  0,
                  call.endedAt.getTime() - call.reconnectStartedAt.getTime(),
                );
                call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
              }
              call.durationSeconds = call.startedAt
                ? Math.max(
                    0,
                    Math.round(
                      (call.endedAt.getTime() - call.startedAt.getTime()) / 1000,
                    ),
                  )
                : 0;
              return { call: await manager.save(call), justEnded: true };
            });

            // Chỉ lời gọi thực hiện transition mới ghi metric và publish call.ended: end lặp hoặc
            // hai bên đua nhau không bắn lại.
            if (result.justEnded && result.call) {
              this.metrics.recordEnded(result.call.endReason ?? reason);
              await this.publishCallEnded(result.call);
            }
            // Timer truyền deferCleanup nên bỏ qua nhánh này: vòng timer không chờ SFU hay
            // Matching. Việc dọn do vòng cleanup riêng nhặt qua marker (trang kế); end trực tiếp
            // từ endpoint vẫn dọn ngay tại đây.
            if (result.call?.cleanupPending && !options?.deferCleanup) {
              try {
                await this.cleanupEndedCall(result.call);
                result.call.cleanupPending = false;
              } catch (error) {
                await this.callRepo.update(
                  { id: result.call.id, cleanupPending: true },
                  { cleanupPending: true },
                );
                this.logger.warn(
                  `Cleanup call ${result.call.id} chưa hoàn tất — ticker sẽ thử lại: ${error instanceof Error ? error.message : String(error)}`,
                );
              }
            }
            return result;
          }
        } finally {

          // Step 8: Nhả lease của các call đã xử lý xong, kể cả nhánh lỗi hoặc bỏ qua
          // Nhả lease của CẢ lô khi tick xong, kể cả khi endById ném lỗi hoặc ngân sách cắt lô:
          // call chưa xử lý được thử lại ngay ở tick sau thay vì chờ lease hết hạn. Chỉ pod CHẾT
          // mới phải đợi lease hết.
          await this.releaseUnused(stale, 'timer_claim_until');

          // [POSTGRES] CallTickerService.releaseUnused
          const calls = stale;
          // claimColumn = 'timer_claim_until'
          if (calls.length === 0) return;
          // Nhả bằng so sánh ĐÚNG timestamp đã claim: lần nhả đến muộn, sau khi lease hết hạn và
          // pod khác đã claim lại, sẽ không khớp nên không thu hồi nhầm lease mới. Một câu UPDATE
          // cho cả lô qua jsonb_to_recordset.
          await this.dataSource.query(
            `UPDATE call_sessions c SET ${claimColumn} = NULL
               FROM jsonb_to_recordset($1::jsonb) AS unused(id uuid, "claimUntil" timestamptz)
              WHERE c.id = unused.id AND c.${claimColumn} = unused."claimUntil"`,
            [JSON.stringify(calls.map(({ id, claimUntil }) => ({ id, claimUntil })))],
          );
        }

        // [WORKER] CallTickerService.sweepReconnectTimeouts
        const reconnectSeconds = this.config.getOrThrow(
          'CALLING_RECONNECT_WINDOW_SECONDS',
          { infer: true },
        );
        const cutoff = new Date(Date.now() - reconnectSeconds * 1000);

        // Step 9: Nhóm reconnect: claim call có cửa sổ reconnect đã quá
        // CALLING_RECONNECT_WINDOW_SECONDS
        const stale = await this.selectCandidates({
          select: { id: true },
          where: {
            status: CallSessionStatus.Active,
            // Mốc reconnectStartedAt được đặt MỘT lần khi gián đoạn bắt đầu (poll GET lặp lại
            // không gia hạn), nên cửa sổ tính từ đúng mốc đó; quá hạn thì call được chốt
            // ended(completed). Index một phần (reconnect_started_at, id) phục vụ truy vấn này.
            reconnectStartedAt: LessThan(cutoff),
          },
          order: { reconnectStartedAt: 'ASC', id: 'ASC' },
          take: TICK_BATCH_SIZE,
        });
        let attempted = 0;
        try {
          for (const call of stale) {
            if (!this.canStartCandidate(deadline, attempted)) break;
            attempted++;
            await this.callingService.endById(
              call.id,
              CallEndReason.Completed,
              (current) =>
                current.status === CallSessionStatus.Active &&
                current.reconnectStartedAt !== null &&
                current.reconnectStartedAt < cutoff,
              { deferCleanup: true, skipLocked: true },
            );
          }
        } finally {
          await this.releaseUnused(stale, 'timer_claim_until');
        }

        // [WORKER] CallTickerService.processActiveCalls
        const freeSeconds = this.config.getOrThrow('CALLING_FREE_CALL_SECONDS', {
          infer: true,
        });
        const activeQuery = this.dataSource
          .getRepository(CallSession)
          .createQueryBuilder('call')
          .select(['call.id'])
          .where('call.status = :status', { status: CallSessionStatus.Active })
          // Call đang trong cửa sổ reconnect bị loại khỏi nhóm này: đồng hồ free-limit tạm dừng
          // suốt gián đoạn; khi cả hai quay lại, startedAt được dời qua đoạn tạm dừng.
          .andWhere('call.reconnect_started_at IS NULL')
          .andWhere('call.started_at IS NOT NULL')
          .orderBy('call.started_at', 'ASC')
          .addOrderBy('call.id', 'ASC')
          .take(TICK_BATCH_SIZE);

        activeQuery
          // Chỉ Voice Match có giới hạn free; Friend call không đi qua free timer.
          .andWhere("call.call_kind = 'voice_match'")
          .andWhere(
            // Deadline và thứ tự đều theo started_at nên khớp index một phần (started_at, id):
            // khi chưa có call nào quá hạn, truy vấn dừng ở đầu index thay vì lọc cả tập active
            // còn mới. now() là giờ DB.
            'call.started_at <= now() - make_interval(secs => :freeSeconds)',
            { freeSeconds },
          );

        // Step 10: Nhóm free-limit: claim call Voice Match active đã quá CALLING_FREE_CALL_SECONDS
        const active = await this.claimCandidates(activeQuery, 'timer_claim_until');
        let attempted = 0;
        for (const call of active) {
          if (!this.canStartCandidate(deadline, attempted)) break;
          attempted++;
          await this.callingService
            .endById(
              call.id,
              CallEndReason.FreeLimit,
              (current) =>
                current.status === CallSessionStatus.Active &&
                current.callKind === CallKind.VoiceMatch &&
                current.reconnectStartedAt === null &&
                current.startedAt !== null &&
                current.startedAt.getTime() <= Date.now() - freeSeconds * 1000,
              { deferCleanup: true, skipLocked: true },
            )
            // Lỗi của một call được log rồi đi tiếp call kế: một call hỏng không làm mất cả lô;
            // lease được nhả ở cuối nên tick sau thử lại.
            .catch((err) =>
              this.logger.error(
                { err: `${err}` },
                `Xử lý call active ${call.id} lỗi — thử lại ở tick sau`,
              ),
            );
        }
        await this.releaseUnused(active, 'timer_claim_until');

        for (let i = 0; i < sweeps.length && this.canContinue(deadline); i++) {

          // Step 11: Vòng lặp của tick: chọn nhóm tới lượt rồi dời con trỏ, dừng khi hết ngân sách
          // Luân phiên điểm bắt đầu: một nhóm chậm (backlog pending) có thể dùng hết ngân sách
          // của tick; khi đó tick sau bắt đầu từ nhóm kế tiếp, nên reconnect và free-limit không
          // bị bỏ đói.
          const sweep = sweeps[this.nextSweep];
          this.nextSweep = (this.nextSweep + 1) % sweeps.length;
          await sweep();
        }
      });
    }, undefined);
  }
}

//  JOB CallTickerService.runCleanupOnce (mỗi CALLING_TICKER_INTERVAL_MS, trên mọi pod core-api)
//  Claim các call ended còn cleanup_pending bằng lease cleanup_claim_until, dọn Matching rồi xoá room LiveKit với độ đồng thời có trần; lỗi thì giữ marker để thử lại và cả lượt lỗi thì nghỉ ngẫu nhiên 2,5–5 giây.
//  Step 1: Đăng ký HAI ManagedInterval riêng trong mỗi pod: timer (làn critical) và cleanup (làn thường)
//  Step 2: Một tick cleanup, gate runExclusive riêng nên không chặn timer
//  Step 3: Claim tối đa 100 call ended còn cleanup_pending bằng lease cleanup_claim_until
//  Step 4: Độ đồng thời bị chặn bởi cấu hình, pool/4 và số call đã claim
//  Step 5: Dọn từng call qua endById: đọc lại dưới khoá rồi chạy cleanup
//  Step 6: Chốt Matching, xoá room SFU, rồi mới xoá marker
//  Step 7: Gọi LiveKit xoá room; 404 (đã xoá) coi là xong
//  Step 8: Cleanup lỗi: giữ marker và đẩy call về cuối hàng đợi
//  Step 9: Nhả lease của cả lô để call chưa xong được thử lại
//  Step 10: Cả lượt đều lỗi: nghỉ ngẫu nhiên 2,5–5 giây trước khi thử lại

// [WORKER] CallTickerService.onApplicationBootstrap · calling/jobs/call-ticker.service.ts
@Injectable()
export class CallTickerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CallTickerService.name);
  private readonly job = new ManagedInterval();
  private readonly cleanupJob = new ManagedInterval();
  private stopping = false;
  private cleanupRetryAfter = 0;
  // CallingService
  private readonly logger = new Logger(CallingService.name);
  // LivekitSdkClient
  private readonly apiKey: string;
  private readonly apiSecret: string;
  private readonly roomService: RoomServiceClient;
  private readonly receiver: WebhookReceiver;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly scheduler: SchedulerRegistry,
    private readonly callingService: CallingService,
    // CallingService
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
    private readonly matchingService: MatchingService,
    private readonly livekit: LivekitRoomPort,
    private readonly metrics: CallingMetrics,
    // LivekitRoomAdapter
    private readonly client: LivekitSdkClient,
    // LivekitSdkClient
    config: ConfigService<CoreApiEnv, true>,
  ) {
    // LivekitRoomAdapter
    super();
    // LivekitSdkClient
    this.apiKey = config.getOrThrow('LIVEKIT_API_KEY', { infer: true });
    this.apiSecret = config.getOrThrow('LIVEKIT_API_SECRET', { infer: true });

    const apiUrl = config.getOrThrow('LIVEKIT_API_URL', { infer: true });
    const wsUrl = config.getOrThrow('LIVEKIT_URL', { infer: true });
    this.roomService = new RoomServiceClient(
      apiUrl || wsUrl.replace(/^ws/, 'http'),
      this.apiKey,
      this.apiSecret,
    );
    this.receiver = new WebhookReceiver(this.apiKey, this.apiSecret);
  }

  // Step 1: Đăng ký HAI ManagedInterval riêng trong mỗi pod: timer (làn critical) và cleanup (làn
  // thường)
  onApplicationBootstrap(): void {
    this.stopping = false;
    this.cleanupRetryAfter = 0;
    this.job.start(this.scheduler, {
      jobName: TICKER_JOB,
      intervalMs: this.config.getOrThrow('CALLING_TICKER_INTERVAL_MS', {
        infer: true,
      }),
      task: () => this.runOnce(),
      logger: this.logger,
      errorMessage: 'Call ticker tick lỗi',
      // Timer đi làn critical của admission: một slot riêng và hai kết nối pool dành riêng (claim
      // + tác vụ), nên job nền ở làn thường, kể cả cleanup chậm vì SFU, không chiếm chỗ của
      // timer.
      databaseAdmission: { dataSource: this.dataSource, priority: 'critical' },
    });
    // Cleanup là ManagedInterval RIÊNG (gate runExclusive riêng, làn admission thường): SFU (hoặc
    // Matching) chậm giữ vòng cleanup lâu thì timer ở trên vẫn chạy và vẫn chốt call quá hạn.
    this.cleanupJob.start(this.scheduler, {
      jobName: CLEANUP_JOB,
      intervalMs: this.config.getOrThrow('CALLING_TICKER_INTERVAL_MS', {
        infer: true,
      }),
      task: () => this.runCleanupOnce(),
      logger: this.logger,
      errorMessage: 'Call cleanup tick lỗi',
      databaseAdmission: { dataSource: this.dataSource },
    });

    // [WORKER] CallTickerService.runCleanupOnce
    // Step 2: Một tick cleanup, gate runExclusive riêng nên không chặn timer
    await this.cleanupJob.runExclusive(async () => {
      // Đang shutdown thì không bắt đầu tick. Sau một lượt lỗi toàn bộ, cleanup nghỉ tới
      // cleanupRetryAfter thay vì gõ lại dependency đang hỏng mỗi giây; timer ở vòng riêng vẫn
      // chạy.
      if (this.stopping || Date.now() < this.cleanupRetryAfter) return;
      const deadline = this.tickDeadline();

      // [WORKER] CallTickerService.tickDeadline
      return (
        Date.now() +
        this.config.getOrThrow('CALLING_TICK_BUDGET_MS', { infer: true })
      );

      // Step 3: Claim tối đa 100 call ended còn cleanup_pending bằng lease cleanup_claim_until
      const pending = await this.selectCandidates(
        {
          select: { id: true, endReason: true },
          where: { status: CallSessionStatus.Ended, cleanupPending: true },
          order: { updatedAt: 'ASC', id: 'ASC' },
          take: TICK_BATCH_SIZE,
        },
        'cleanup_claim_until',
      );

      // [POSTGRES] CallTickerService.selectCandidates
      const options = {
        select: { id: true, endReason: true },
        where: { status: CallSessionStatus.Ended, cleanupPending: true },
        order: { updatedAt: 'ASC', id: 'ASC' },
        take: TICK_BATCH_SIZE,
      };
      const claimColumn = 'cleanup_claim_until';
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
      // Chọn và claim là MỘT câu lệnh (SELECT ... FOR UPDATE SKIP LOCKED trong CTE rồi UPDATE
      // lease): hai pod nhận các lô rời nhau. Cleanup dùng cột riêng cleanup_claim_until, tách
      // khỏi lease của timer.
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
      // Lease theo đồng hồ DB: pod chết giữa chừng thì lease tự hết hạn và pod khác nhận lại lô
      // (mặc định CALLING_JOB_LEASE_MS = 30 giây). Cleanup là at-least-once nên chạy lại một call
      // là an toàn.
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

      // Step 4: Độ đồng thời bị chặn bởi cấu hình, pool/4 và số call đã claim
      // Độ đồng thời bị chặn bởi CALLING_CLEANUP_CONCURRENCY (mặc định 2), bởi số call đã claim,
      // và bởi DATABASE_POOL_MAX / 4 (tối thiểu 1): đúng phần pool mà admission đã tính cho
      // worker song song của một slot job nền, nên pool nhỏ không bị cleanup vượt phần của nó.
      const concurrency = Math.min(
        this.config.getOrThrow('CALLING_CLEANUP_CONCURRENCY', { infer: true }),
        Math.max(
          1,
          Math.floor(
            this.config.getOrThrow('DATABASE_POOL_MAX', { infer: true }) / 4,
          ),
        ),
        pending.length,
      );
      let next = 0;
      let failures = 0;
      await Promise.all(
        Array.from({ length: concurrency }, async () => {

          // [WORKER] CallTickerService.canStartCandidate
          const attempted = next;
          // Ngay cả khi SELECT claim chậm đã dùng hết ngân sách, cleanup vẫn thử ít nhất MỘT call
          // (attempted = 0) để luôn tiến triển; sau đó hết ngân sách hoặc shutdown thì không khởi
          // động thêm.
          return !this.stopping && (attempted === 0 || Date.now() < deadline);

          while (
            next < pending.length &&
            this.canStartCandidate(deadline, next)
          ) {
            // Các worker song song lấy chung một con trỏ next; hết ngân sách hoặc shutdown thì
            // không khởi động thêm, còn lệnh đã bắt đầu vẫn chờ driver (deadline hợp tác, không
            // huỷ ngay).
            const call = pending[next++];
            try {

              // Step 5: Dọn từng call qua endById: đọc lại dưới khoá rồi chạy cleanup
              const result = await this.callingService.endById(
                call.id,
                call.endReason ?? CallEndReason.Completed,
                undefined,
                // Không truyền deferCleanup: call đã ended nên endById chỉ khoá, đọc lại rồi chạy
                // cleanup ngay trong lời gọi này. skipLocked bỏ qua call đang bị khoá thay vì
                // chờ.
                { skipLocked: true },
              );

              // [SERVICE] CallingService.endById · calling/calling.service.ts
              const callId = call.id;
              const reason = call.endReason ?? CallEndReason.Completed;
              const shouldEnd = undefined;
              // options = { skipLocked: true }
              const result = await this.dataSource.transaction(async (manager) => {
                const call = await manager.findOne(CallSession, {
                  where: { id: callId },
                  lock: {
                    mode: 'pessimistic_write',
                    ...(options?.skipLocked ? { onLocked: 'skip_locked' as const } : {}),
                  },
                });
                if (!call) return { call: null, justEnded: false };
                // Cleanup đi qua nhánh này: call đã ended thì chỉ trả lại trạng thái với
                // justEnded = false, nên chạy lại cleanup (at-least-once) không ghi metric hay
                // publish call.ended lần nữa.
                if (call.status === CallSessionStatus.Ended) {
                  return { call, justEnded: false };
                }
                if (shouldEnd && !shouldEnd(call)) return { call, justEnded: false };
                call.status = CallSessionStatus.Ended;
                call.endReason = reason;
                call.endedAt = new Date();
                call.cleanupPending = true;
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

              if (result.justEnded && result.call) {
                this.metrics.recordEnded(result.call.endReason ?? reason);
                await this.publishCallEnded(result.call);
              }
              if (result.call?.cleanupPending && !options?.deferCleanup) {
                try {

                  // Step 6: Chốt Matching, xoá room SFU, rồi mới xoá marker
                  await this.cleanupEndedCall(result.call);

                  // [SERVICE] CallingService.cleanupEndedCall
                  // call = result.call
                  // Chốt MatchSession (ended) cho cả cặp TRƯỚC khi xoá room; chỉ khi cả Matching
                  // lẫn SFU thành công marker mới bị xoá. Friend call không có MatchSession nên
                  // bỏ qua bước này.
                  if (call.callKind !== CallKind.Friend && call.matchSessionId)
                    await this.matchingService.endVoiceSessionForCall(
                      call.matchSessionId,
                      call.userAId,
                      call.userBId,
                    );
                  // [EXTERNAL] Xoá room trên SFU chạy SAU khi transaction của endById đã commit:
                  // SFU chậm không giữ khoá hàng call_sessions.
                  await this.livekit.deleteRoom(call.roomName);

                  // [EXTERNAL] LivekitRoomAdapter.deleteRoom · calling/clients/livekit-room.adapter.ts
                  const roomName = call.roomName;
                  return this.client.deleteRoom(roomName);

                  // [EXTERNAL] LivekitSdkClient.deleteRoom · common/livekit/livekit-sdk.client.ts
                  try {

                    // Step 7: Gọi LiveKit xoá room; 404 (đã xoá) coi là xong
                    await this.roomService.deleteRoom(roomName);
                  } catch (error) {
                    // Cleanup là at-least-once: lần thử lại sau khi room đã bị xoá nhận 404 thì
                    // coi là xong. Mọi lỗi khác ném lên để marker còn nguyên và được thử lại.

                    // [EXTERNAL] LivekitSdkClient.isNotFound
                    return (
                      typeof error === 'object' &&
                      error !== null &&
                      'status' in error &&
                      (error as { status: unknown }).status === 404
                    );

                    if (this.isNotFound(error)) return;
                    throw error;
                  }

                  // Marker chỉ bị xoá khi Matching và SFU đều thành công; một trong hai ném lỗi
                  // thì marker còn nguyên và tick cleanup sau thử lại.
                  await this.callRepo.update(call.id, { cleanupPending: false });

                  result.call.cleanupPending = false;
                } catch (error) {

                  // Step 8: Cleanup lỗi: giữ marker và đẩy call về cuối hàng đợi
                  // Cleanup lỗi: giữ marker và đẩy call về CUỐI hàng đợi (hàng đợi sắp theo
                  // updated_at, UPDATE này làm mới cột đó), để một room hỏng vĩnh viễn không đứng
                  // đầu mãi và bỏ đói các call khác.
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

              // endById không ném khi cleanup lỗi mà giữ marker; lỗi nhận ra qua cờ
              // cleanupPending còn bật trên call trả về.
              if (result.call?.cleanupPending) failures++;
            } catch (error) {
              failures++;
              this.logger.error(
                { err: `${error}` },
                `Cleanup call ${call.id} lỗi — thử lại ở tick sau`,
              );
            }
          }
        }),
      );

      // Step 9: Nhả lease của cả lô để call chưa xong được thử lại
      // Nhả lease của cả lô: call lỗi hoặc chưa được thử được claim lại ở tick sau (sau thời gian
      // nghỉ nếu cả lượt lỗi).
      await this.releaseUnused(pending, 'cleanup_claim_until');

      // [POSTGRES] CallTickerService.releaseUnused
      const calls = pending;
      // claimColumn = 'cleanup_claim_until'
      if (calls.length === 0) return;
      // Nhả bằng so sánh ĐÚNG timestamp đã claim: lần nhả đến muộn không thu hồi nhầm lease mới
      // của pod khác.
      await this.dataSource.query(
        `UPDATE call_sessions c SET ${claimColumn} = NULL
           FROM jsonb_to_recordset($1::jsonb) AS unused(id uuid, "claimUntil" timestamptz)
          WHERE c.id = unused.id AND c.${claimColumn} = unused."claimUntil"`,
        [JSON.stringify(calls.map(({ id, claimUntil }) => ({ id, claimUntil })))],
      );

      // Step 10: Cả lượt đều lỗi: nghỉ ngẫu nhiên 2,5–5 giây trước khi thử lại
      // Mọi call đã thử đều lỗi thì nghỉ ngẫu nhiên 2,5–5 giây (5 giây × 0,5–1,0). Đây là backoff
      // theo process, không phải lịch retry bền vững cho từng room: marker trong DB mới giữ việc
      // còn lại.
      if (next > 0 && failures === next) {
        this.cleanupRetryAfter =
          Date.now() + CLEANUP_FAILURE_BACKOFF_MS * (0.5 + Math.random() * 0.5);
      }
    }, undefined);
  }
}

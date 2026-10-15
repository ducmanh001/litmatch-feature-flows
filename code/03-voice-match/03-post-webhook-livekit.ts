// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/calling/webhooks/livekit
//  LiveKit báo ai vào hoặc rời phòng, ticker nền canh giờ (reconnect, tối đa 420 giây) rồi kết thúc call.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [EXTERNAL] → [SERVICE] → [POSTGRES] → [WORKER] → [SERVICE]
//             → [EXTERNAL] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận webhook công khai của LiveKit, lấy nguyên văn body và header
//                 Authorization, kiểm chữ ký rồi mới giao event cho service; sau verify
//                 luôn trả 200.
//    [EXTERNAL]   LiveKit SDK kiểm chữ ký JWT của webhook; khi call kết thúc, core-api
//                 gọi ngược LiveKit để xóa phòng.
//    [SERVICE]    Biến event vào hoặc rời phòng thành trạng thái CallSession (pending,
//                 active, đang reconnect, ended); mọi nhánh kết thúc dùng chung một hàm
//                 end idempotent.
//    [POSTGRES]   CallSession là nguồn sự thật của thời gian: mốc vào phòng, mốc bắt
//                 đầu, cửa sổ reconnect; khóa dòng FOR UPDATE tuần tự hóa webhook,
//                 ticker và nút end.
//    [WORKER]     CallTickerService chạy nền theo chu kỳ ngắn (mặc định 1 giây): đóng
//                 call pending quá hạn, call rớt mạng quá cửa sổ reconnect, và call
//                 Voice Match chạm 420 giây.
//    [REDIS]      Pub/sub báo call.ended cho 2 người để client đóng màn hình gọi.
//
//  Bảng       call_sessions — 1 dòng cho mỗi cuộc gọi: trạng thái, mốc vào phòng và rớt
//               của từng bên, mốc bắt đầu reconnect, lý do và thời lượng khi kết thúc.
//             match_sessions — chỉ được Matching sửa: chuyển sang ended khi call kết
//               thúc để 2 người không bị session cũ giữ lại.
//  Redis      realtime:user:{userId} — kênh pub/sub theo người nhận; core-api phát
//               call.ended, signaling-gateway chuyển xuống socket.
//  Liên quan  Matching — endVoiceSessionForCall: chốt MatchSession sang ended khi call
//               kết thúc.
//             LiveKit — SFU gửi webhook participant_joined, participant_left,
//               room_finished; core-api xóa phòng sau khi kết thúc.
//             signaling-gateway — subscribe kênh realtime của user, đẩy call.ended
//               xuống client.
//             Calling (GET /calling/calls/:id) — đường đối soát dự phòng khi webhook bị
//               mất: hỏi LiveKit ai đang trong phòng (chưa chép).
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] LivekitWebhookController.handle  ·  livekit-webhook.controller.ts L27–28, L34–61
@Controller('calling/webhooks')
export class LivekitWebhookController {
  constructor(
    private readonly callingService: CallingService,
    private readonly livekit: LivekitRoomPort,
  ) {}

  // Step 1: Nhận webhook công khai của LiveKit (@Public, lấy nguyên văn body)
  @Public()
  @Post('livekit')
  @HttpCode(HttpStatus.OK)
  async handle(
    @Req() req: RawBodyRequest<Request>,
    @Headers('authorization') authHeader?: string,
  ): Promise<{ ok: true }> {
    // Chữ ký được kiểm trên NGUYÊN VĂN body (không phải JSON đã parse), TRƯỚC khi đọc bất cứ nội
    // dung nào. Sai chữ ký thì 401.
    const rawBody = req.rawBody?.toString('utf8');
    if (!rawBody || !authHeader) {
      throw new DomainException(
        CallingErrors.WEBHOOK_INVALID,
        'Thiếu body hoặc Authorization header',
        HttpStatus.UNAUTHORIZED,
      );
    }
    let event;
    try {

      // Step 2: Kiểm chữ ký webhook (sai thì 401) rồi mới đọc event
      event = await this.livekit.receiveWebhook(rawBody, authHeader);

        // [EXTERNAL] LivekitSdkClient.receiveWebhook  ·  livekit-sdk.client.ts L126–136
        private readonly receiver: WebhookReceiver;
        constructor(
          // CallingService
          @InjectDataSource() private readonly dataSource: DataSource,
          @InjectRepository(CallSession)
          private readonly callRepo: Repository<CallSession>,
        ) {}

        // WebhookReceiver của LiveKit SDK kiểm JWT trong header Authorization bằng API key và
        // secret; sai thì ném lỗi, controller đổi thành 401.
        const event = await this.receiver.receive(rawBody, authHeader);
        return {
          event: event.event,
          roomName: event.room?.name ?? null,
          participantIdentity: event.participant?.identity ?? null,
        };
    } catch {
      throw new DomainException(
        CallingErrors.WEBHOOK_INVALID,
        'Chữ ký webhook không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }
    await this.callingService.handleWebhookEvent(event);

      // [SERVICE] CallingService.handleWebhookEvent  ·  calling.service.ts L490–515
      // Step 3: Chỉ nhận phòng có prefix call-, tìm CallSession theo roomName
      // Chỉ nhận phòng có prefix call-: đó là prefix cố định để lọc phòng không thuộc calling.
      // Phòng lạ hoặc không có dòng CallSession thì bỏ qua.
      if (!event.roomName?.startsWith('call-')) return;
      const call = await this.callRepo.findOneBy({ roomName: event.roomName });
      if (!call) return;

      switch (event.event) {
        case 'participant_joined':
          if (event.participantIdentity) {
            await this.markJoined(call.id, event.participantIdentity);

              // [SERVICE] CallingService.markJoined  ·  calling.service.ts L600–645
              const callId = call.id;
              const identity = event.participantIdentity;

              // Step 4: Người vào phòng: khóa dòng, ghi mốc; đủ 2 người thì call sang active
              await this.dataSource.transaction(async (manager) => {
                const call = await manager.findOne(CallSession, {
                  where: { id: callId },
                  // [POSTGRES] Khóa dòng CallSession FOR UPDATE rồi mới đọc và ghi: webhook của 2
                  // bên, ticker và nút end cùng chạm một dòng. Có khóa rồi mới kiểm lại status.
                  lock: { mode: 'pessimistic_write' },
                });
                if (!call || call.status === CallSessionStatus.Ended) return;

                const now = new Date();
                if (identity === call.userAId) call.joinedAAt = call.joinedAAt ?? now;
                else if (identity === call.userBId)
                  call.joinedBAt = call.joinedBAt ?? now;
                else return;

                if (
                  call.joinedAAt &&
                  call.joinedBAt &&
                  call.status === CallSessionStatus.Pending
                ) {
                  call.status = CallSessionStatus.Active;
                  // Chỉ khi CẢ 2 đã vào phòng, call mới sang active và đặt startedAt. Đây là mốc
                  // tính giới hạn 7 phút của Voice Match; set-if-null nên webhook gửi lại không
                  // ghi đè.
                  call.startedAt = call.startedAt ?? now;
                }

                if (call.status === CallSessionStatus.Active) {
                  // Bên nào quay lại thì gỡ dấu rớt của bên đó. Chỉ khi cả 2 đã quay lại mới chạy
                  // tiếp đồng hồ: startedAt dời tiến đúng bằng khoảng gián đoạn nên thời gian rớt
                  // mạng không bị tính vào 7 phút.
                  if (identity === call.userAId) call.disconnectedAAt = null;
                  if (identity === call.userBId) call.disconnectedBAt = null;

                  if (
                    call.reconnectStartedAt &&
                    call.disconnectedAAt === null &&
                    call.disconnectedBAt === null &&
                    call.startedAt
                  ) {
                    const pausedMs = Math.max(
                      0,
                      now.getTime() - call.reconnectStartedAt.getTime(),
                    );
                    call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
                    call.reconnectStartedAt = null;
                  }
                }
                await manager.save(call);
              });
          }
          return;
        // participant_left và room_finished không kết thúc call ngay mà chỉ mở cửa sổ reconnect:
        // mất mạng hay connection abort có thể quay lại. Quá hạn mới end.
        case 'participant_left':
        case 'participant_connection_aborted':
          if (event.participantIdentity) {
            await this.markDisconnected(call.id, event.participantIdentity);

              // [SERVICE] CallingService.markDisconnected  ·  calling.service.ts L647–665
              const callId = call.id;
              const identity = event.participantIdentity;

              // Step 5: Người rớt: ghi mốc rớt, mở cửa sổ reconnect, chưa end call
              await this.dataSource.transaction(async (manager) => {
                const call = await manager.findOne(CallSession, {
                  where: { id: callId },
                  lock: { mode: 'pessimistic_write' },
                });
                if (!call || call.status !== CallSessionStatus.Active) return;

                const now = new Date();
                if (identity === call.userAId) call.disconnectedAAt ??= now;
                else if (identity === call.userBId) call.disconnectedBAt ??= now;
                else return;
                // Chỉ ghi mốc rớt đầu tiên (??=): event rớt lặp lại không kéo dài cửa sổ
                // reconnect. Từ mốc này đồng hồ giới hạn tạm dừng.
                call.reconnectStartedAt ??= now;
                await manager.save(call);
              });
          }
          return;
        case 'room_finished':
          await this.markRoomDisconnected(call.id);
          return;
        default:
          return;
      }

    // Step 6: Sau khi chữ ký hợp lệ luôn trả 200 { ok: true }
    return { ok: true };
  }
}

// [WORKER] CallTickerService.onApplicationBootstrap  ·  call-ticker.service.ts L34–37, L48–59
@Injectable()
export class CallTickerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(CallTickerService.name);
  private readonly job = new ManagedInterval();
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly scheduler: SchedulerRegistry,
    private readonly callingService: CallingService,
  ) {}

  onApplicationBootstrap(): void {

    // Step 7: Ticker nền: đăng ký job theo chu kỳ, mỗi tick chỉ 1 pod chạy
    this.job.start(this.scheduler, {
      jobName: TICKER_JOB,
      intervalMs: this.config.getOrThrow('CALLING_TICKER_INTERVAL_MS', {
        infer: true,
      }),
      task: () => this.runOnce(),
      logger: this.logger,
      errorMessage: 'Call ticker tick lỗi',
      // [POSTGRES] Mỗi tick chỉ 1 pod làm nhờ advisory lock của Postgres. Nếu 2 pod vẫn chạy
      // trùng thì vẫn an toàn vì endById khóa dòng và chỉ end khi chưa ended.
      clusterSingleton: { dataSource: this.dataSource },
    });

      // [WORKER] CallTickerService.runOnce  ·  call-ticker.service.ts L66–75
      await this.job.runExclusive(async () => {
        await withSpan('litmatch.calling', 'calling.ticker.tick', async () => {

          // Step 8: Đóng call pending quá hạn (chưa ai vào đủ)
          await this.sweepPending();

            // [WORKER] CallTickerService.sweepPending  ·  call-ticker.service.ts L77–92
            const timeoutSeconds = this.config.getOrThrow(
              'CALLING_PENDING_TIMEOUT_SECONDS',
              { infer: true },
            );
            const cutoff = new Date(Date.now() - timeoutSeconds * 1000);
            const stale = await this.dataSource.getRepository(CallSession).find({
              select: { id: true },
              where: { status: CallSessionStatus.Pending, createdAt: LessThan(cutoff) },
              order: { createdAt: 'ASC', id: 'ASC' },
              // Mỗi tick chỉ lấy id theo lô; phần tồn đọng xử lý dần ở các tick sau. Truy vấn đi
              // theo index (status, created_at).
              take: TICK_BATCH_SIZE,
            });
            for (const call of stale) {
              await this.callingService.endById(call.id, CallEndReason.PendingTimeout);

                // [SERVICE] CallingService.endById  ·  calling.service.ts L442–483
                private readonly logger = new Logger(CallingService.name);
                constructor(
                  private readonly matchingService: MatchingService,
                  private readonly livekit: LivekitRoomPort,
                  @Inject(CALLING_REDIS) private readonly redis: Redis,
                  private readonly metrics: CallingMetrics,
                ) {}

                const callId = call.id;
                const reason = CallEndReason.PendingTimeout;

                // Step 9: End idempotent: khóa dòng, kiểm lại status, chốt thời lượng
                const result = await this.dataSource.transaction(async (manager) => {
                  const call = await manager.findOne(CallSession, {
                    where: { id: callId },
                    // [POSTGRES] Khóa dòng rồi kiểm lại status: chốt chặn race end-vs-tick (nút
                    // end, webhook, ticker cùng gọi). Đã ended thì trả lại như cũ, không end lần
                    // 2.
                    lock: { mode: 'pessimistic_write' },
                  });
                  if (!call) return { call: null, justEnded: false };
                  if (call.status === CallSessionStatus.Ended) {
                    return { call, justEnded: false };
                  }
                  call.status = CallSessionStatus.Ended;
                  call.endReason = reason;
                  call.endedAt = new Date();
                  // Thời gian nằm trong cửa sổ reconnect không tính vào thời lượng: dời startedAt
                  // tiến lên trước khi chốt duration.
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

                // Chỉ lời gọi thực hiện transition mới dọn phòng và phát realtime, nên retry hay
                // ticker chạy trùng không bắn event đôi.
                if (result.justEnded && result.call) {
                  this.metrics.recordEnded(result.call.endReason ?? reason);

                  // Step 10: Dọn sau end: chốt MatchSession, xóa phòng, phát call.ended
                  await this.cleanupEndedCall(result.call);

                    // [SERVICE] CallingService.cleanupEndedCall  ·  calling.service.ts L684–713
                    const call = result.call;
                    // Chốt MatchSession ở Matching TRƯỚC khi phát event, để user nhận call.ended
                    // rồi quay lại tìm ghép không bị session cũ giữ lại. Cả webhook lẫn ticker
                    // đều đi qua đây, không chỉ nút UI.
                    if (call.callKind !== CallKind.Friend && call.matchSessionId)
                      await this.matchingService.endVoiceSessionForCall(
                        call.matchSessionId,
                        call.userAId,
                        call.userBId,
                      );
                    // [EXTERNAL] Xóa phòng LiveKit best-effort để không rò tài nguyên. Phòng có
                    // thể đã tự đóng (room_finished) nên lỗi ở đây không phải lỗi nghiệp vụ.
                    await this.livekit.deleteRoom(call.roomName).catch((err) => {
                      this.logger.debug(
                        `deleteRoom ${call.roomName} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
                      );
                    });
                    const envelope: RealtimeEnvelope<CallEndedEventData> = {
                      event: RealtimeEvents.CallEnded,
                      data: {
                        callId: call.id,
                        matchSessionId: call.matchSessionId,
                        reason: call.endReason ?? CallEndReason.Completed,
                        durationSeconds: call.durationSeconds ?? 0,
                      },
                    };
                    // [REDIS] Phát call.ended cho cả 2 người qua kênh realtime:user:{id}.
                    // Best-effort: client vẫn poll được GET /calling/calls/:id.
                    await Promise.all(
                      [call.userAId, call.userBId].map((uid) =>
                        publishRealtimeEvent(this.redis, this.logger, uid, envelope),
                      ),
                    );
                }
                return result;
            }

          // Step 11: Đóng call rớt mạng quá cửa sổ reconnect
          await this.sweepReconnectTimeouts();

            // [WORKER] CallTickerService.sweepReconnectTimeouts  ·  call-ticker.service.ts L94–112
            const reconnectSeconds = this.config.getOrThrow(
              'CALLING_RECONNECT_WINDOW_SECONDS',
              { infer: true },
            );
            const cutoff = new Date(Date.now() - reconnectSeconds * 1000);
            const stale = await this.dataSource.getRepository(CallSession).find({
              select: { id: true },
              where: {
                status: CallSessionStatus.Active,
                // Rớt mạng ngắn không kết thúc call ngay: có CALLING_RECONNECT_WINDOW_SECONDS để
                // quay lại. Quá hạn mà chưa quay lại mới end completed.
                reconnectStartedAt: LessThan(cutoff),
              },
              order: { reconnectStartedAt: 'ASC', id: 'ASC' },
              take: TICK_BATCH_SIZE,
            });
            for (const call of stale) {
              await this.callingService.endById(call.id, CallEndReason.Completed);
            }

          // Step 12: Đóng call Voice Match chạm 420 giây (tính trong SQL)
          await this.processActiveCalls();

            // [WORKER] CallTickerService.processActiveCalls  ·  call-ticker.service.ts L114–149
            const freeSeconds = this.config.getOrThrow('CALLING_FREE_CALL_SECONDS', {
              infer: true,
            });
            const activeQuery = this.dataSource
              .getRepository(CallSession)
              .createQueryBuilder('call')
              .select(['call.id'])
              .where('call.status = :status', { status: CallSessionStatus.Active })
              // Call đang trong cửa sổ reconnect bị loại khỏi đợt kiểm này: đồng hồ 420 giây tạm
              // dừng cho tới khi cả 2 quay lại hoặc quá hạn.
              .andWhere('call.reconnect_started_at IS NULL')
              .andWhere('call.started_at IS NOT NULL')
              .orderBy('call.updated_at', 'ASC')
              .addOrderBy('call.id', 'ASC')
              .take(TICK_BATCH_SIZE);

            activeQuery
              // Chỉ Voice Match bị giới hạn: call bạn bè gọi lâu dài, không đi qua đồng hồ này.
              // Hạn 420 giây tính ngay trong SQL theo đồng hồ của Postgres, từ startedAt đã bù
              // thời gian reconnect; không tin timer của client.
              .andWhere("call.call_kind = 'voice_match'")
              .andWhere(
                'call.started_at <= now() - make_interval(secs => :freeSeconds)',
                { freeSeconds },
              );

            const active = await activeQuery.getMany();
            for (const call of active) {
              await this.callingService
                .endById(call.id, CallEndReason.FreeLimit)
                .catch((err) =>
                  this.logger.error(
                    { err: `${err}` },
                    `Xử lý call active ${call.id} lỗi — thử lại ở tick sau`,
                  ),
                );
            }
        });
      }, undefined);
  }
}

// [POSTGRES] call-session.entity.ts  ·  call-session.entity.ts L35–43
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })
@Index('idx_call_sessions_status_created', ['status', 'createdAt'])
@Index('idx_call_sessions_active_updated', ['updatedAt', 'id'], {
  where: "status = 'active'",
})
@Index('idx_call_sessions_reconnect_started', ['reconnectStartedAt', 'id'], {
  where: "status = 'active' AND reconnect_started_at IS NOT NULL",
})

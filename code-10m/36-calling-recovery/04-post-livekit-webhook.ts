//  POST /api/v1/calling/webhooks/livekit
//  Endpoint công khai chỉ tin event sau khi verify chữ ký trên nguyên văn body; mỗi event chạy trong một transaction khoá hàng với set-if-null và kiểm status nên webhook gửi lại, đến trễ hoặc sai thứ tự không phá trạng thái; rời phòng chỉ mở reconnect grace, không end call.
//  Step 1: LiveKit gọi vào endpoint công khai: không có JWT user, chữ ký thay thế
//  Step 2: Lấy nguyên văn body để verify chữ ký
//  Step 3: Verify chữ ký JWT rồi rút gọn event; sai chữ ký thì 401
//  Step 4: Xử lý idempotent: biến event thành chuyển trạng thái của call
//  Step 5: Chỉ room của calling; không có call tương ứng thì bỏ qua
//  Step 6: participant_joined: ghi mốc join (set-if-null), đủ hai bên thì active
//  Step 7: Cả hai đã quay lại: dời startedAt qua đoạn gián đoạn và xoá grace
//  Step 8: participant_left hoặc connection aborted: mở reconnect grace, không end call
//  Step 9: Mốc grace chỉ đặt một lần, left lặp lại không kéo dài cửa sổ
//  Step 10: room_finished: coi cả hai đã rời, vẫn chỉ mở grace

// [CONTROLLER] LivekitWebhookController.handle · calling/webhooks/livekit-webhook.controller.ts
@Controller('calling/webhooks')
export class LivekitWebhookController {
  // LivekitSdkClient
  private readonly apiKey: string;
  private readonly apiSecret: string;
  private readonly roomService: RoomServiceClient;
  private readonly receiver: WebhookReceiver;
  constructor(
    private readonly callingService: CallingService,
    private readonly livekit: LivekitRoomPort,
    // LivekitRoomAdapter
    private readonly client: LivekitSdkClient,
    // LivekitSdkClient
    config: ConfigService<CoreApiEnv, true>,
    // CallingService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
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

  // Step 1: LiveKit gọi vào endpoint công khai: không có JWT user, chữ ký thay thế
  @Public()
  @Post('livekit')
  @HttpCode(HttpStatus.OK)
  async handle(
    @Req() req: RawBodyRequest<Request>,
    @Headers('authorization') authHeader?: string,
  ): Promise<{ ok: true }> {

    // Step 2: Lấy nguyên văn body để verify chữ ký
    // LiveKit không có JWT của user nên endpoint là @Public; thay vào đó chữ ký JWT ký bằng API
    // key/secret được verify trên NGUYÊN VĂN body TRƯỚC khi đọc nội dung event. Thiếu body hoặc
    // header, hoặc sai chữ ký, thì 401.
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

      // Step 3: Verify chữ ký JWT rồi rút gọn event; sai chữ ký thì 401
      event = await this.livekit.receiveWebhook(rawBody, authHeader);

      // [EXTERNAL] LivekitRoomAdapter.receiveWebhook · calling/clients/livekit-room.adapter.ts
      const event = await this.client.receiveWebhook(rawBody, authHeader);

      // [EXTERNAL] LivekitSdkClient.receiveWebhook · common/livekit/livekit-sdk.client.ts
      // WebhookReceiver verify chữ ký JWT của header Authorization trên body nguyên văn; sai chữ
      // ký thì ném lỗi và controller đổi thành 401.
      const event = await this.receiver.receive(rawBody, authHeader);
      return {
        event: event.event,
        roomName: event.room?.name ?? null,
        participantIdentity: event.participant?.identity ?? null,
      };

      return {
        event: event.event,
        roomName: event.roomName,
        participantIdentity: event.participantIdentity,
      };
    } catch {
      throw new DomainException(
        CallingErrors.WEBHOOK_INVALID,
        'Chữ ký webhook không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }

    // Step 4: Xử lý idempotent: biến event thành chuyển trạng thái của call
    // Event đã rút gọn chỉ còn loại, room và identity (không có id để dedup), nên webhook gửi lại
    // hoặc đến sai thứ tự an toàn nhờ trạng thái: set-if-null và kiểm status dưới row lock ở
    // service.
    await this.callingService.handleWebhookEvent(event);

    // [SERVICE] CallingService.handleWebhookEvent · calling/calling.service.ts
    // Step 5: Chỉ room của calling; không có call tương ứng thì bỏ qua
    // Tiền tố call- là cố định theo id call: room khác bị bỏ qua trước khi chạm DB, và room
    // call-* không có call tương ứng cũng chỉ bị bỏ qua.
    if (!event.roomName?.startsWith('call-')) return;
    const call = await this.callRepo.findOneBy({ roomName: event.roomName });
    if (!call) return;

    switch (event.event) {
      case 'participant_joined':
        if (event.participantIdentity) {

          // Step 6: participant_joined: ghi mốc join (set-if-null), đủ hai bên thì active
          await this.markJoined(call.id, event.participantIdentity);

          // [SERVICE] CallingService.markJoined
          const callId = call.id;
          const identity = event.participantIdentity;
          await this.dataSource.transaction(async (manager) => {
            // Mỗi webhook là một transaction khoá hàng nên các webhook đến cùng lúc (joined của
            // hai bên, left, lần đối soát GET) chạy lần lượt trên cùng call và không ghi đè nhau.
            // Thứ tự đến không được đảm bảo: tính đúng nằm ở set-if-null và kiểm status.
            const call = await manager.findOne(CallSession, {
              where: { id: callId },
              lock: { mode: 'pessimistic_write' },
            });
            // Ended là terminal: webhook joined đến trễ hoặc gửi lại sau khi call đã kết thúc là
            // no-op, không đảo ngược trạng thái.
            if (!call || call.status === CallSessionStatus.Ended) return;

            const now = new Date();

            // [SERVICE] CallingService.recordParticipantJoined
            // Set-if-null: webhook joined trùng không đổi mốc đã có, nên startedAt chỉ được đặt
            // một lần. Identity lạ (không phải một trong hai member) rơi xuống nhánh trả false và
            // markJoined không save gì; identity hợp lệ luôn do server đặt khi mint token.
            if (identity === call.userAId) call.joinedAAt ??= now;
            else if (identity === call.userBId) call.joinedBAt ??= now;
            else return false;
            if (
              call.status === CallSessionStatus.Pending &&
              call.joinedAAt &&
              call.joinedBAt
            ) {
              call.status = CallSessionStatus.Active;
              call.startedAt ??= now;
            }
            if (call.status === CallSessionStatus.Active) {
              // Member quay lại thì xoá dấu rời phòng của chính họ; timer chỉ chạy tiếp khi cả
              // hai dấu đều null.
              if (identity === call.userAId) call.disconnectedAAt = null;
              if (identity === call.userBId) call.disconnectedBAt = null;
            }
            return true;

            if (!this.recordParticipantJoined(call, identity, now)) return;
            if (call.status === CallSessionStatus.Active) {

              // Step 7: Cả hai đã quay lại: dời startedAt qua đoạn gián đoạn và xoá grace
              // Chỉ resume khi CẢ HAI đã quay lại: joined của bên thứ nhất chỉ xoá dấu của bên đó
              // và grace vẫn còn; joined của bên thứ hai mới dời startedAt và xoá grace.
              this.resumeCallTimer(call, now);

              // [SERVICE] CallingService.resumeCallTimer
              if (
                !call.reconnectStartedAt ||
                !call.startedAt ||
                call.disconnectedAAt !== null ||
                call.disconnectedBAt !== null
              )
                return;
              const pausedMs = Math.max(
                0,
                now.getTime() - call.reconnectStartedAt.getTime(),
              );
              // Dời startedAt đúng bằng khoảng tạm dừng rồi xoá reconnectStartedAt, nên webhook
              // joined gửi lại không cộng khoảng tạm dừng lần hai.
              call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
              call.reconnectStartedAt = null;
            }
            await manager.save(call);
          });
        }
        return;
      // Rời phòng hay mất kết nối KHÔNG end call: chỉ mở cửa sổ reconnect (timer tạm dừng).
      // Ticker mới chốt call khi quá CALLING_RECONNECT_WINDOW_SECONDS.
      case 'participant_left':
      case 'participant_connection_aborted':
        if (event.participantIdentity) {

          // Step 8: participant_left hoặc connection aborted: mở reconnect grace, không end call
          await this.markDisconnected(call.id, event.participantIdentity);

          // [SERVICE] CallingService.markDisconnected
          // callId = call.id
          // identity = event.participantIdentity
          await this.dataSource.transaction(async (manager) => {
            const call = await manager.findOne(CallSession, {
              where: { id: callId },
              lock: { mode: 'pessimistic_write' },
            });
            // Chỉ call đang active mới có grace: left của call pending (chưa đủ hai bên) hoặc đã
            // ended bị bỏ qua.
            if (!call || call.status !== CallSessionStatus.Active) return;

            const now = new Date();
            if (identity === call.userAId) call.disconnectedAAt ??= now;
            else if (identity === call.userBId) call.disconnectedBAt ??= now;
            else return;

            // Step 9: Mốc grace chỉ đặt một lần, left lặp lại không kéo dài cửa sổ
            // Mốc grace chỉ đặt một lần: left lặp lại, hoặc left của bên thứ hai, không kéo dài
            // cửa sổ.
            call.reconnectStartedAt ??= now;
            await manager.save(call);
          });
        }
        return;
      // LiveKit có thể phát participant_left hoặc room_finished vì mất mạng hay connection bị
      // abort, nên room_finished cũng chỉ mở grace: chờ reconnect ngắn trước khi terminal hoá.
      case 'room_finished':

        // Step 10: room_finished: coi cả hai đã rời, vẫn chỉ mở grace
        await this.markRoomDisconnected(call.id);

        // [SERVICE] CallingService.markRoomDisconnected
        // callId = call.id
        await this.dataSource.transaction(async (manager) => {
          const call = await manager.findOne(CallSession, {
            where: { id: callId },
            lock: { mode: 'pessimistic_write' },
          });
          if (!call || call.status !== CallSessionStatus.Active) return;

          const now = new Date();
          call.reconnectStartedAt ??= now;
          // room_finished coi cả hai bên đã rời: đặt cả hai dấu để timer chỉ chạy tiếp khi cả hai
          // thật sự quay lại, nhưng vẫn chỉ mở grace, không end call ngay.
          call.disconnectedAAt ??= now;
          call.disconnectedBAt ??= now;
          await manager.save(call);
        });

        return;
      default:
        return;
    }

    return { ok: true };
  }
}

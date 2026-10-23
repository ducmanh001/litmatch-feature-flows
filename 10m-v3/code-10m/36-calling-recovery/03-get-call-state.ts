//  GET /api/v1/calling/calls/:id
//  Chỉ member đọc được; mỗi lần poll hỏi SFU ai đang có mặt (ngoài row lock), nếu cần ghi thì khoá hàng và kiểm lại status + snapshot trước khi mở hoặc giữ nguyên reconnect grace và phục hồi timer; SFU lỗi thì giữ nguyên call.
//  Step 1: Poll fallback của realtime call.ended: chỉ member của call đọc được
//  Step 2: Đọc call kèm đối soát SFU rồi dựng response
//  Step 3: Người ngoài call nhận cùng 404 như call không tồn tại (không lộ call có tồn tại)
//  Step 4: Đối soát participant với SFU làm backstop khi webhook bị mất
//  Step 5: Hỏi SFU ai đang thật sự trong room (ngoài row lock; lỗi thì giữ nguyên call)
//  Step 6: Chỉ khi có gì cần ghi mới mở transaction và khoá hàng
//  Step 7: Webhook đã đổi call trong lúc chờ SFU: bỏ kết quả cũ, trả call mới nhất
//  Step 8: Ghi member đang có mặt như participant_joined (set-if-null)
//  Step 9: Có member vắng mặt: mở grace hoặc GIỮ NGUYÊN mốc đã lưu
//  Step 10: Cả hai có mặt: phục hồi timer và loại khoảng tạm dừng

// [CONTROLLER] CallingController.getCall · calling/calling.controller.ts
@Controller('calling')
export class CallingController {
  // CallingService
  private readonly logger = new Logger(CallingService.name);
  // LivekitSdkClient
  private readonly apiKey: string;
  private readonly apiSecret: string;
  private readonly roomService: RoomServiceClient;
  private readonly receiver: WebhookReceiver;
  constructor(
    private readonly callingService: CallingService,
    // CallingService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
    private readonly livekit: LivekitRoomPort,
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

  // Step 1: Poll fallback của realtime call.ended: chỉ member của call đọc được
  @Get('calls/:id')
  async getCall(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
  ): Promise<CallDto> {

    // Step 2: Đọc call kèm đối soát SFU rồi dựng response
    const call = await this.callingService.getCall(user, id);

    // [SERVICE] CallingService.getCall · calling/calling.service.ts
    const callId = id;
    let call = await this.callRepo.findOneBy({ id: callId });
    if (
      !call ||
      (call.userAId !== user.userId && call.userBId !== user.userId)
    ) {

      // Step 3: Người ngoài call nhận cùng 404 như call không tồn tại (không lộ call có tồn tại)
      // Call không tồn tại và call của người khác trả CÙNG một 404, để người ngoài không dò được
      // call nào tồn tại.
      throw new DomainException(
        CallingErrors.CALL_NOT_FOUND,
        'Không tìm thấy call',
        HttpStatus.NOT_FOUND,
      );
    }

    // Step 4: Đối soát participant với SFU làm backstop khi webhook bị mất
    // GET không chỉ đọc DB: webhook là kênh chính nhưng có thể mất, nên mỗi lần poll đối soát với
    // SFU làm backstop. Mọi thay đổi chỉ được ghi dưới row lock ở bước sau, không ghi thẳng từ
    // kết quả SFU.
    call = await this.reconcileCallWithLivekit(call);

    // [SERVICE] CallingService.reconcileCallWithLivekit
    let identities: string[];
    try {

      // Step 5: Hỏi SFU ai đang thật sự trong room (ngoài row lock; lỗi thì giữ nguyên call)
      // [EXTERNAL] Gọi SFU nằm NGOÀI mọi row lock và transaction: SFU chậm không giữ khoá
      // call_sessions. Vì vậy kết quả chỉ là ảnh chụp, được kiểm lại dưới khoá ở
      // reconcileParticipants.
      identities = await this.livekit.listParticipantIdentities(call.roomName);

      // [EXTERNAL] LivekitRoomAdapter.listParticipantIdentities · calling/clients/livekit-room.adapter.ts
      const roomName = call.roomName;
      return this.client.listParticipantIdentities(roomName);

      // [EXTERNAL] LivekitSdkClient.listParticipantIdentities · common/livekit/livekit-sdk.client.ts
      const participants = await this.roomService.listParticipants(roomName);
      return participants.map((participant) => participant.identity);
    } catch (err) {
      this.logger.debug(
        `Không đối soát được participant của ${call.roomName}: ${err instanceof Error ? err.message : String(err)}`,
      );
      // SFU lỗi thì giữ nguyên call đã lưu thay vì trả 500: màn cuộc gọi không được mất vì
      // RoomService tạm unavailable; webhook và ticker vẫn là đường dự phòng.
      return call;
    }

    return call.status === CallSessionStatus.Ended
      ? call
      : this.reconcileParticipants(call, identities);

    // [SERVICE] CallingService.reconcileParticipants
    const observed = call;
    // Hai trường hợp không có gì để ghi (pending mà SFU chưa thấy ai; active đang khoẻ: cả hai có
    // mặt và không trong grace) trả ngay, không mở transaction và không khoá hàng.
    if (
      observed.status === CallSessionStatus.Pending &&
      !identities.includes(observed.userAId) &&
      !identities.includes(observed.userBId)
    )
      return observed;
    if (
      observed.status === CallSessionStatus.Active &&
      !observed.reconnectStartedAt &&
      identities.includes(observed.userAId) &&
      identities.includes(observed.userBId)
    )
      return observed;

    // Step 6: Chỉ khi có gì cần ghi mới mở transaction và khoá hàng
    return this.dataSource.transaction(async (manager) => {
      // Khoá hàng rồi MỚI so snapshot: kết quả SFU đã cũ sau lúc chờ I/O, webhook có thể đã
      // commit rejoin hoặc disconnect mới.
      const call = await manager.findOne(CallSession, {
        where: { id: observed.id },
        lock: { mode: 'pessimistic_write' },
      });
      if (!call) return observed;
      const unchanged = (
        [
          'updatedAt',
          'joinedAAt',
          'joinedBAt',
          'startedAt',
          'reconnectStartedAt',
          'disconnectedAAt',
          'disconnectedBAt',
        ] as const
      ).every((field) => call[field]?.getTime() === observed[field]?.getTime());

      // Step 7: Webhook đã đổi call trong lúc chờ SFU: bỏ kết quả cũ, trả call mới nhất
      // Status hoặc một mốc bất kỳ (joined, started, reconnect, disconnected, updatedAt) đã đổi
      // thì BỎ kết quả SFU và trả call mới nhất: snapshot cũ không mở lại grace sau rejoin và
      // không xoá mất disconnect mới đã commit.
      if (call.status !== observed.status || !unchanged) return call;

      const now = new Date();
      // Webhook joined bị mất cũng không sao: member nào thật sự có mặt ở SFU được ghi như
      // participant_joined (set-if-null), đủ hai bên thì pending thành active. Identity chỉ lấy
      // từ hai member của call, client không tự khai.
      for (const identity of [call.userAId, call.userBId]) {
        if (identities.includes(identity))

          // Step 8: Ghi member đang có mặt như participant_joined (set-if-null)
          this.recordParticipantJoined(call, identity, now);

          // [SERVICE] CallingService.recordParticipantJoined
          // Set-if-null: ghi lại lần join thứ hai (webhook trùng hoặc đối soát) không đổi mốc đã
          // có, nên startedAt chỉ được đặt một lần.
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
            // Member quay lại thì xoá dấu rời phòng của chính họ; timer chỉ chạy tiếp khi cả hai
            // dấu đều null.
            if (identity === call.userAId) call.disconnectedAAt = null;
            if (identity === call.userBId) call.disconnectedBAt = null;
          }
          return true;
      }
      if (call.status === CallSessionStatus.Active) {
        call.disconnectedAAt = identities.includes(call.userAId)
          ? null
          : (call.disconnectedAAt ?? now);
        call.disconnectedBAt = identities.includes(call.userBId)
          ? null
          : (call.disconnectedBAt ?? now);
        if (call.disconnectedAAt || call.disconnectedBAt) {

          // Step 9: Có member vắng mặt: mở grace hoặc GIỮ NGUYÊN mốc đã lưu
          // Chỉ mở grace khi chưa có: ??= giữ NGUYÊN mốc đã lưu (từ webhook hoặc lần poll đầu),
          // nên poll lặp lại không gia hạn cửa sổ; quá hạn thì ticker mới chốt call.
          call.reconnectStartedAt ??= now;
        } else {

          // Step 10: Cả hai có mặt: phục hồi timer và loại khoảng tạm dừng
          // Cả hai member đều có mặt mà DB vẫn ghi grace (webhook rejoin bị mất): phục hồi timer
          // và loại khoảng tạm dừng khỏi thời lượng, nên ticker không chốt nhầm một call đang
          // khoẻ.
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
          // Dời startedAt đúng bằng khoảng tạm dừng nên mốc free-limit (startedAt +
          // CALLING_FREE_CALL_SECONDS) cũng được đẩy muộn theo. Rồi xoá reconnectStartedAt, nên
          // poll hoặc webhook joined lặp lại không cộng khoảng tạm dừng lần hai.
          call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
          call.reconnectStartedAt = null;
        }
      }
      await manager.save(call);
      return call;
    });

    return call;

    return CallDto.from(
      call,
      this.callingService.getFreeCallSeconds(),
      await this.callingService.getLikeState(user, call),
    );
  }
}

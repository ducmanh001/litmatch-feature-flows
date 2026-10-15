//  POST /api/v1/calling/match-sessions/:matchSessionId/join
//  Mở phòng gọi cho cặp đã ghép: tạo CallSession duy nhất rồi cấp token LiveKit riêng cho từng người.
//  Step 1: Nhận yêu cầu vào phòng (giới hạn 20 lần/phút, user từ JWT)
//  Step 2: Kiểm user thuộc session (không phải thành viên thì 404)
//  Step 3: Lấy CallSession hiện có; session đã kết thúc thì từ chối
//  Step 4: Chưa có call thì tạo CallSession pending (roomName do server đặt)
//  Step 5: 2 người join cùng lúc: unique match_session_id, bên thua đọc lại call
//  Step 6: Đọc lại session; đã kết thúc giữa chừng thì đóng luôn call vừa tạo
//  Step 7: Ký token LiveKit riêng cho user (identity = userId)
//  Step 8: Trả call, token và URL LiveKit về controller
//  Step 9: Controller trả call, token, URL LiveKit và số giây gọi miễn phí

// [CONTROLLER] CallingController.join  ·  calling.controller.ts L26–27, L30–53
@Controller('calling')
export class CallingController {
  constructor(
    private readonly callingService: CallingService,
    // CallingService
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
    private readonly matchingService: MatchingService,
    private readonly livekit: LivekitRoomPort,
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Nhận yêu cầu vào phòng (giới hạn 20 lần/phút, user từ JWT)
  @Post('match-sessions/:matchSessionId/join')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 20, ttl: minutes(1) } })
  async join(
    @CurrentUser() user: AuthenticatedUser,
    @Param('matchSessionId', ParseUUIDPipe) matchSessionId: string,
  ): Promise<JoinCallDto> {
    const { call, token, livekitUrl } = await this.callingService.joinCall(
      user,
      matchSessionId,
    );

    // [SERVICE] CallingService.joinCall  ·  calling.service.ts L101–201
    // Step 2: Kiểm user thuộc session (không phải thành viên thì 404)
    // Không phải thành viên thì trả 404 giống như "không tồn tại", để không ai dò được sessionId
    // của người khác.
    const session = await this.matchingService.findSessionById(matchSessionId);
    if (
      !session ||
      (session.userAId !== user.userId && session.userBId !== user.userId)
    ) {
      throw new DomainException(
        CallingErrors.SESSION_NOT_FOUND,
        'Không tìm thấy session',
        HttpStatus.NOT_FOUND,
      );
    }

    // Step 3: Lấy CallSession hiện có; session đã kết thúc thì từ chối
    let call = await this.callRepo.findOneBy({ matchSessionId });
    if (
      session.matchType !== MatchType.Voice ||
      session.status !== MatchSessionStatus.Confirmed
    ) {
      // Sau timeout hoặc webhook, session đã terminal cùng với call. Re-join call cũ vẫn trả
      // CALL_ENDED để client có một nhánh xử lý dứt khoát; dòng pending mồ côi từ race cũ được
      // đóng luôn, không chờ ticker.
      if (call?.status === CallSessionStatus.Ended) {
        throw new DomainException(
          CallingErrors.CALL_ENDED,
          'Call đã kết thúc',
          HttpStatus.CONFLICT,
          { reason: call.endReason },
        );
      }
      if (call && session.matchType === MatchType.Voice) {
        await this.endById(call.id, CallEndReason.Completed);
      }
      throw new DomainException(
        CallingErrors.SESSION_NOT_CALLABLE,
        'Chỉ mở phòng call cho voice session đã đủ 2 bên xác nhận',
        HttpStatus.CONFLICT,
      );
    }

    // Step 4: Chưa có call thì tạo CallSession pending (roomName do server đặt)
    if (!call) {
      // Sinh id trước để roomName = call-{id} do server đặt; client không bao giờ tự chọn phòng.
      const id = randomUUID();
      try {
        call = await this.callRepo.save(
          this.callRepo.create({
            id,
            matchSessionId,
            roomName: callRoomName(id),
            userAId: session.userAId,
            userBId: session.userBId,
            status: CallSessionStatus.Pending,
            callKind: CallKind.VoiceMatch,
          }),
        );
      } catch (err) {

        // Step 5: 2 người join cùng lúc: unique match_session_id, bên thua đọc lại call
        // [POSTGRES] 2 bên join gần như cùng lúc đều thấy chưa có call và cùng insert: unique
        // match_session_id (xem bên dưới) chỉ cho 1 dòng, bên thua lấy lại dòng của bên kia.
        if (!isUniqueViolation(err)) throw err;
        call = await this.callRepo.findOneByOrFail({ matchSessionId });
      }
    }

    // Step 6: Đọc lại session; đã kết thúc giữa chừng thì đóng luôn call vừa tạo
    // Đọc lại session sau khi tạo call: bên kia có thể vừa bấm back hoặc đóng tab và kết thúc
    // session. Khi đó đóng luôn call vừa tạo, không để dòng pending mồ côi chờ ticker hết hạn.
    const currentSession =
      await this.matchingService.findSessionById(matchSessionId);
    if (
      !currentSession ||
      currentSession.matchType !== MatchType.Voice ||
      currentSession.status !== MatchSessionStatus.Confirmed ||
      (currentSession.userAId !== user.userId &&
        currentSession.userBId !== user.userId)
    ) {
      await this.endById(call.id, CallEndReason.Completed);
      throw new DomainException(
        CallingErrors.CALL_ENDED,
        'Phiên Voice Match đã kết thúc',
        HttpStatus.CONFLICT,
      );
    }
    if (call.status === CallSessionStatus.Ended) {
      throw new DomainException(
        CallingErrors.CALL_ENDED,
        'Call đã kết thúc',
        HttpStatus.CONFLICT,
        { reason: call.endReason },
      );
    }

    // Step 7: Ký token LiveKit riêng cho user (identity = userId)
    // [EXTERNAL] Token chỉ cấp cho thành viên của call; mỗi lần join (kể cả join lại sau rớt
    // mạng) là một token mới. identity = userId từ JWT, phòng do server đặt.
    // Token là JWT ký ngay tại core-api bằng API key và secret của LiveKit, không gọi mạng; thời
    // hạn theo CALLING_TOKEN_TTL_SECONDS.
    const token = await this.livekit.mintJoinToken(
      call.roomName,
      user.userId,
      this.config.getOrThrow('CALLING_TOKEN_TTL_SECONDS', { infer: true }),
    );

    // Step 8: Trả call, token và URL LiveKit về controller
    return {
      call,
      token,
      // URL LiveKit chọn theo region của userA: luôn dùng region của userA để cả 2 bên và mọi lần
      // join lại nhận cùng một URL. Region lệch chỉ cảnh báo, vì mọi URL trong map thuộc cùng một
      // cụm LiveKit nên call không vỡ.
      livekitUrl: await this.resolveCallLivekitUrl(call),
    };

    // Step 9: Controller trả call, token, URL LiveKit và số giây gọi miễn phí
    return JoinCallDto.from(
      call,
      token,
      livekitUrl,
      this.callingService.getFreeCallSeconds(),
    );
  }
}

// [POSTGRES] call-session.entity.ts  ·  call-session.entity.ts L35–36
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })

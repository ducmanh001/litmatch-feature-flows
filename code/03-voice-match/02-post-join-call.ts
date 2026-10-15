// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/calling/match-sessions/:matchSessionId/join
//  Mở phòng gọi cho cặp đã ghép: tạo CallSession duy nhất rồi cấp token LiveKit riêng cho từng người.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [EXTERNAL] → [SERVICE]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận yêu cầu vào phòng gọi (giới hạn 20 lần/phút); chỉ lấy id session
//                 từ URL, người dùng lấy từ JWT; trả call, token và URL LiveKit.
//    [SERVICE]    Kiểm tra user thuộc session voice đã ghép, tạo hoặc lấy CallSession
//                 duy nhất của cặp, kiểm lại session lần nữa, rồi cấp token và chọn URL
//                 LiveKit.
//    [POSTGRES]   CallSession là nguồn sự thật của cuộc gọi; unique match_session_id
//                 bảo đảm 1 lượt ghép chỉ có 1 cuộc gọi dù 2 người join cùng lúc.
//    [EXTERNAL]   LiveKit SDK ký token vào phòng call-{id} bằng API key và secret; mỗi
//                 người một token riêng, identity là userId.
//
//  Bảng       call_sessions — 1 dòng cho mỗi cuộc gọi: tên phòng LiveKit, 2 user, trạng
//               thái pending, active hoặc ended.
//             match_sessions — chỉ đọc: cặp đã ghép, loại voice, trạng thái confirmed
//               (Matching là chủ sở hữu).
//             users — chỉ đọc region của 2 người để chọn URL LiveKit khi bật nhiều
//               region.
//  Liên quan  Matching — findSessionById: đọc MatchSession; Matching giữ quyền ghi
//               trạng thái session, Calling chỉ đọc.
//             User — getByIdOrThrow: đọc region của 2 người cho bước chọn URL.
//             Calling endById — đóng CallSession khi MatchSession đã kết thúc (chi tiết
//               ở luồng webhook và ticker).
//             LiveKit — SFU thật nằm ngoài core-api; ở bước này core-api chỉ ký token,
//               chưa mở kết nối nào.

// [CONTROLLER] calling.controller.ts · L30–53
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
  return JoinCallDto.from(
    call,
    token,
    livekitUrl,
    this.callingService.getFreeCallSeconds(),
  );
}

// [SERVICE] calling.service.ts · L101–201
async joinCall(
  user: AuthenticatedUser,
  matchSessionId: string,
): Promise<JoinCallResult> {
  // Không phải thành viên thì trả 404 giống như "không tồn tại", để không ai dò được
  // sessionId của người khác.
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
  let call = await this.callRepo.findOneBy({ matchSessionId });
  if (
    session.matchType !== MatchType.Voice ||
    session.status !== MatchSessionStatus.Confirmed
  ) {
    // Sau timeout hoặc webhook, session đã terminal cùng với call. Re-join call cũ vẫn
    // trả CALL_ENDED để client có một nhánh xử lý dứt khoát; dòng pending mồ côi từ
    // race cũ được đóng luôn, không chờ ticker.
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

  if (!call) {
    // Sinh id trước để roomName = call-{id} do server đặt; client không bao giờ tự chọn
    // phòng.
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
      // [POSTGRES] 2 bên join gần như cùng lúc đều thấy chưa có call và cùng insert:
      // unique match_session_id (xem bên dưới) chỉ cho 1 dòng, bên thua lấy lại dòng
      // của bên kia.
      if (!isUniqueViolation(err)) throw err;
      call = await this.callRepo.findOneByOrFail({ matchSessionId });
    }
  }
  // Đọc lại session sau khi tạo call: bên kia có thể vừa bấm back hoặc đóng tab và kết
  // thúc session. Khi đó đóng luôn call vừa tạo, không để dòng pending mồ côi chờ
  // ticker hết hạn.
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

  // [EXTERNAL] Token chỉ cấp cho thành viên của call; mỗi lần join (kể cả join lại sau
  // rớt mạng) là một token mới. identity = userId từ JWT, phòng do server đặt.
  const token = await this.livekit.mintJoinToken(
    call.roomName,
    user.userId,
    this.config.getOrThrow('CALLING_TOKEN_TTL_SECONDS', { infer: true }),
  );
  return {
    call,
    token,
    livekitUrl: await this.resolveCallLivekitUrl(call),
  };
}

// [POSTGRES] call-session.entity.ts · L35–36
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })

// [EXTERNAL] livekit-room.adapter.ts · L16–24
mintJoinToken(
  roomName: string,
  identity: string,
  ttlSeconds: number,
): Promise<string> {
  return this.client.mintJoinToken(roomName, identity, ttlSeconds, {
    canPublish: true,
  });
}

// [EXTERNAL] livekit-sdk.client.ts · L52–72
async mintJoinToken(
  roomName: string,
  identity: string,
  ttlSeconds: number,
  grant: LivekitTokenGrant,
): Promise<string> {
  // Token là JWT ký ngay tại core-api bằng API key và secret của LiveKit, không gọi
  // mạng; thời hạn theo CALLING_TOKEN_TTL_SECONDS.
  const token = new AccessToken(this.apiKey, this.apiSecret, {
    identity,
    ttl: ttlSeconds,
  });
  token.addGrant({
    room: roomName,
    roomJoin: true,
    canPublish: grant.canPublish,
    canSubscribe: true,
    ...(grant.canPublishData === undefined
      ? {}
      : { canPublishData: grant.canPublishData }),
  });
  return token.toJwt();
}

// [SERVICE] calling.service.ts · L573–598
private async resolveCallLivekitUrl(call: CallSession): Promise<string> {
  const defaultUrl = this.config.getOrThrow('LIVEKIT_URL', { infer: true });
  const regionUrls = this.config.getOrThrow('LIVEKIT_REGION_URLS', {
    infer: true,
  });
  // Chưa bật nhiều region thì trả URL mặc định ngay, không tốn truy vấn nào.
  if (!hasLivekitRegionUrls(regionUrls)) return defaultUrl;

  try {
    const [userA, userB] = await Promise.all([
      this.userService.getByIdOrThrow(call.userAId),
      this.userService.getByIdOrThrow(call.userBId),
    ]);
    // Luôn dùng region của userA để cả 2 bên và mọi lần join lại nhận cùng một URL.
    // Region lệch chỉ cảnh báo, vì mọi URL trong map thuộc cùng một cụm LiveKit nên
    // call không vỡ.
    if ((userA.region ?? null) !== (userB.region ?? null)) {
      this.logger.warn(
        `Region lệch giữa 2 bên call ${call.id} (userA=${userA.region ?? 'null'}, userB=${userB.region ?? 'null'}) — dùng region userA theo quy tắc tất định`,
      );
    }
    return resolveLivekitUrl(regionUrls, defaultUrl, userA.region);
  } catch (err) {
    this.logger.warn(
      `Không resolve được region cho call ${call.id} — fallback LIVEKIT_URL: ${err instanceof Error ? err.message : String(err)}`,
    );
    return defaultUrl;
  }
}

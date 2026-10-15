// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/soul-match/sessions/:id/messages
//  Gửi tin nhắn trong phòng ẩn danh: giờ chat tính ở server, idempotent theo key, realtime riêng cho từng người nhận.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận tin nhắn (giới hạn 60 lần/phút) kèm Idempotency-Key bắt buộc;
//                 user lấy từ JWT, id phòng từ URL; trả tin nhắn với vai trò me hoặc
//                 partner, không lộ id đối phương.
//    [SERVICE]    Tính phase của phòng theo giờ server, kiểm user chưa bị khóa và còn
//                 trong giờ chat, ghi tin nhắn trong transaction đã khóa session, rồi
//                 phát realtime riêng cho từng người.
//    [POSTGRES]   Khóa dòng match_sessions FOR UPDATE; unique idempotency_key chặn nhân
//                 đôi tin nhắn; seq do DB cấp để phân trang theo cursor.
//    [REDIS]      Pub/sub báo soul.message tới 2 người; payload tính sẵn cho từng người
//                 nhận để giữ ẩn danh.
//
//  Bảng       match_sessions — phòng chat là view dẫn xuất từ session soul đã
//               confirmed; dòng bị khóa FOR UPDATE khi ghi tin.
//             soul_chat_messages — tin nhắn chỉ thêm, không sửa: nội dung, người gửi,
//               idempotency_key unique, seq tăng dần.
//             users — chỉ đọc trạng thái (có bị khóa hay không) của người gửi.
//             friendships — chỉ đọc: đã là bạn thì phòng không còn hết giờ chat.
//  Redis      realtime:user:{userId} — kênh pub/sub theo người nhận; core-api phát
//               soul.message, signaling-gateway chuyển xuống socket.
//  Liên quan  Matching — findSessionById: đọc MatchSession; Matching giữ quyền ghi
//               trạng thái session.
//             Friend — areFriends: Friendship là nguồn sự thật để chat không còn hết
//               giờ sau khi cả 2 đã thích nhau.
//             User — getByIdOrThrow: đọc trạng thái người gửi để kiểm tra ban.
//             Soul Match (rating) — luồng khác khóa cùng dòng session rồi chụp lịch sử
//               chat để import sang Friend Chat (xem luồng rating).
//             signaling-gateway — subscribe kênh realtime của user, đẩy soul.message
//               xuống client.

// [CONTROLLER] soul-match.controller.ts · L85–107
@Post('sessions/:id/messages')
@Throttle({ default: { limit: 60, ttl: minutes(1) } })
async sendMessage(
  @CurrentUser() user: AuthenticatedUser,
  @Param('id', ParseUUIDPipe) id: string,
  @Body() dto: SendSoulMessageDto,
  @IdempotencyKey() idempotencyKey: string,
): Promise<SoulMessageDto> {
  const message = await this.soulMatchService.sendMessage(
    user,
    id,
    dto,
    idempotencyKey,
  );
  return SoulMessageDto.from(message, user.userId);
}

// [SERVICE] soul-match.service.ts · L121–242
async sendMessage(
  user: AuthenticatedUser,
  sessionId: string,
  dto: SendSoulMessageDto,
  idempotencyKey: string,
): Promise<SoulChatMessage> {
  // Phase chat tính từ giờ SERVER (lúc cả 2 xác nhận cộng thời lượng chat), không bao
  // giờ tin đồng hồ của client.
  const room = await this.getRoomForMember(user, sessionId);
  if (room.phase !== SoulRoomPhase.Chatting) {
    throw new DomainException(
      SoulMatchErrors.CHAT_NOT_OPEN,
      'Hết giờ chat — phòng đã chuyển sang đánh giá hoặc đã đóng',
      HttpStatus.CONFLICT,
      { phase: room.phase },
    );
  }

  // Kiểm lại trạng thái ban đúng lúc gửi: bị khóa giữa chừng phải cắt ngay.
  const profile = await this.userService.getByIdOrThrow(user.userId);
  if (profile.status === UserStatus.Banned) {
    throw new DomainException(
      SoulMatchErrors.USER_BANNED,
      'Tài khoản bị khoá',
      HttpStatus.FORBIDDEN,
    );
  }

  const maxLength = this.config.getOrThrow('SOUL_CHAT_MESSAGE_MAX_LENGTH', {
    infer: true,
  });
  if (dto.content.length > maxLength) {
    throw new DomainException(
      SoulMatchErrors.MESSAGE_TOO_LONG,
      `Message dài quá ${maxLength} ký tự`,
      HttpStatus.UNPROCESSABLE_ENTITY,
      { maxLength },
    );
  }

  // Key của client được gắn prefix soul:msg:{userId}: unique ở DB, retry không nhân đôi
  // tin nhắn.
  const prefixedKey = messageIdempotencyKey(user.userId, idempotencyKey);
  try {
    const message = await this.dataSource.transaction(async (manager) => {
      // [POSTGRES] Khóa dòng MatchSession FOR UPDATE, cùng khóa với lúc rating chụp
      // lịch sử chat để import sang Friend Chat: một tin nhắn không thể lọt vào giữa
      // bước chụp và bước chuyển.
      const lockedSession = await manager.findOne(MatchSession, {
        where: { id: sessionId },
        lock: { mode: 'pessimistic_write' },
      });
      if (
        !lockedSession ||
        (lockedSession.userAId !== user.userId &&
          lockedSession.userBId !== user.userId)
      ) {
        throw new DomainException(
          SoulMatchErrors.SESSION_NOT_FOUND,
          'Không tìm thấy session',
          HttpStatus.NOT_FOUND,
        );
      }
      // Kiểm lại hạn chat sau khi có khóa: phase tính ở đầu hàm có thể đã hết hạn trong
      // lúc chờ khóa. Đã là bạn (matched) thì chat không còn hết hạn.
      if (!room.matched && Date.now() >= room.chatEndsAt.getTime()) {
        throw new DomainException(
          SoulMatchErrors.CHAT_NOT_OPEN,
          'Hết giờ chat — phòng đã chuyển sang đánh giá hoặc đã đóng',
          HttpStatus.CONFLICT,
          { phase: SoulRoomPhase.Rating },
        );
      }
      return manager.save(
        manager.create(SoulChatMessage, {
          sessionId,
          senderUserId: user.userId,
          content: dto.content,
          idempotencyKey: prefixedKey,
        }),
      );
    });
    // [REDIS] Realtime phát SAU khi lưu, best-effort. Mỗi người nhận một payload riêng:
    // senderRole (me hoặc partner) tính ngay ở đây để gateway không phải biết gì về ẩn
    // danh.
    const partnerId = this.partnerIdOf(room.session, user.userId);
    await Promise.all(
      (
        [
          [user.userId, 'me'],
          [partnerId, 'partner'],
        ] as const
      ).map(([recipientId, senderRole]) => {
        const envelope: RealtimeEnvelope<SoulMessageEventData> = {
          event: RealtimeEvents.SoulMessage,
          data: {
            sessionId,
            messageId: message.id,
            senderRole,
            content: message.content,
            sentAt: message.createdAt.toISOString(),
          },
        };
        return publishRealtimeEvent(
          this.redis,
          this.logger,
          recipientId,
          envelope,
        );
      }),
    );
    return message;
  } catch (err) {
    // Trùng Idempotency-Key: cùng phòng và cùng nội dung thì trả lại tin cũ (replay,
    // không phát realtime lại); khác nội dung thì 409.
    if (!isUniqueViolation(err)) throw err;
    const existing = await this.messageRepo.findOneBy({
      idempotencyKey: prefixedKey,
    });
    if (
      existing &&
      existing.sessionId === sessionId &&
      existing.content === dto.content
    ) {
      return existing;
    }
    throw new DomainException(
      SoulMatchErrors.MESSAGE_IDEMPOTENCY_CONFLICT,
      'Idempotency-Key đã dùng cho 1 message khác nội dung',
      HttpStatus.CONFLICT,
    );
  }
}

// [SERVICE] soul-match.service.ts · L430–495
private async getRoomForMember(
  user: AuthenticatedUser,
  sessionId: string,
): Promise<SoulRoomView> {
  // Không tồn tại hoặc không phải thành viên đều trả 404: không làm oracle dò
  // sessionId.
  const session = await this.matchingService.findSessionById(sessionId);
  if (
    !session ||
    (session.userAId !== user.userId && session.userBId !== user.userId)
  ) {
    throw new DomainException(
      SoulMatchErrors.SESSION_NOT_FOUND,
      'Không tìm thấy session',
      HttpStatus.NOT_FOUND,
    );
  }
  if (
    session.matchType !== MatchType.Soul ||
    ![MatchSessionStatus.Confirmed, MatchSessionStatus.Ended].includes(
      session.status,
    )
  ) {
    throw new DomainException(
      SoulMatchErrors.CHAT_NOT_OPEN,
      'Phòng chat chỉ mở khi session soul đã đủ 2 bên xác nhận',
      HttpStatus.CONFLICT,
    );
  }
  if (!session.confirmedAAt || !session.confirmedBAt) {
    throw new Error(
      `Session ${session.id} confirmed nhưng thiếu confirmedAAt/confirmedBAt — dữ liệu hỏng`,
    );
  }

  const confirmedAt = new Date(
    Math.max(session.confirmedAAt.getTime(), session.confirmedBAt.getTime()),
  );
  const chatSeconds = this.config.getOrThrow('SOUL_CHAT_DURATION_SECONDS', {
    infer: true,
  });
  const ratingSeconds = this.config.getOrThrow('SOUL_RATING_WINDOW_SECONDS', {
    infer: true,
  });
  // Phòng không có cột trạng thái riêng: hạn chat và hạn rating chỉ được tính khi đọc,
  // từ lúc cả 2 xác nhận cộng thời lượng cấu hình.
  const chatEndsAt = new Date(confirmedAt.getTime() + chatSeconds * 1000);
  const ratingEndsAt = new Date(chatEndsAt.getTime() + ratingSeconds * 1000);
  const matched = await this.friendService.areFriends(
    session.userAId,
    session.userBId,
  );
  if (session.status === MatchSessionStatus.Ended) {
    return {
      session,
      chatEndsAt,
      ratingEndsAt,
      phase: SoulRoomPhase.Closed,
      matched,
    };
  }
  const now = Date.now();
  // Đã là bạn thì phòng ở chatting mãi: Friendship là nguồn sự thật, hạn cũ chỉ còn là
  // dữ liệu lịch sử.
  const phase =
    matched || now < chatEndsAt.getTime()
      ? SoulRoomPhase.Chatting
      : now < ratingEndsAt.getTime()
        ? SoulRoomPhase.Rating
        : SoulRoomPhase.Closed;
  return { session, chatEndsAt, ratingEndsAt, phase, matched };
}

// [POSTGRES] 1752400000000-soul-match.ts · L44–55
// Unique idempotency_key là chốt chặn thật chống nhân đôi tin nhắn. seq bigint IDENTITY
// là thứ tự do DB cấp cho cursor: dùng created_at sẽ trùng mili-giây làm phân trang
// trùng hoặc mất dòng.
await queryRunner.query(`
  CREATE TABLE soul_chat_messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seq             bigint GENERATED ALWAYS AS IDENTITY,
    session_id      uuid         NOT NULL REFERENCES match_sessions(id),
    sender_user_id  uuid         NOT NULL REFERENCES users(id),
    content         text         NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_soul_chat_messages_idempotency_key UNIQUE (idempotency_key)
  )
`);

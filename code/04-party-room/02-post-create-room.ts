// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/party/rooms
//  Tạo phòng và membership host trong 1 transaction, rồi tạo room trên LiveKit; SFU lỗi thì đóng phòng bù trừ.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [EXTERNAL] → [SERVICE]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận request tạo phòng (giới hạn 5 lần/phút), trả phòng, membership
//                 host, token publish và URL LiveKit.
//    [SERVICE]    Kiểm tiêu đề, chốt URL LiveKit theo region host, ghi phòng + host
//                 trong 1 transaction; sau commit tạo room SFU, lỗi thì bù trừ bằng
//                 cách đóng phòng.
//    [POSTGRES]   Ghi phòng và membership host nguyên tử; unique index 1 user 1 phòng
//                 chặn host đang ở phòng khác.
//    [EXTERNAL]   LiveKit: tạo room tường minh (giới hạn người, tự đóng khi trống), rồi
//                 ký token publish cho host.
//    [REDIS]      Chỉ dùng khi bù trừ: đóng phòng thì pub/sub báo party.room.closed cho
//                 member.
//
//  Bảng       party_rooms — dòng phòng mới: host, title, status active, speaker_limit
//               chụp từ config, livekit_url chụp theo region host; đóng với
//               close_reason error khi SFU lỗi.
//             party_room_members — dòng membership role host; bị đánh dấu left_at khi
//               phòng bị đóng bù trừ.
//             users — chỉ đọc region của host khi bật multi-region, để chọn URL
//               LiveKit.
//  Redis      realtime:user:{userId} — chỉ khi bù trừ: publish party.room.closed cho
//               member đang active.
//  Liên quan  LiveKit (SFU) — tạo room party-{roomId} với maxParticipants và
//               emptyTimeout; deleteRoom khi đóng phòng; ký token host
//               (canPublish=true).
//             User — getByIdOrThrow lấy region của host (chỉ khi LIVEKIT_REGION_URLS có
//               giá trị).
//             PartyRoomSweeper — backstop: đóng phòng còn trong DB nhưng không còn trên
//               SFU, nếu bù trừ cũng lỗi (chưa chép).

// [CONTROLLER] party-room.controller.ts · L50–63
@Post()
@Throttle({ default: { limit: 5, ttl: minutes(1) } })
async create(
  @CurrentUser() user: AuthenticatedUser,
  @Body() body: CreatePartyRoomDto,
): Promise<JoinPartyRoomDto> {
  const { room, membership, token, livekitUrl } =
    await this.partyRoomService.createRoom(user, body.title, body.category);
  return JoinPartyRoomDto.from(room, membership, token, livekitUrl);
}

// [SERVICE] party-room.service.ts · L138–232
async createRoom(
  user: AuthenticatedUser,
  title: string,
  category: PartyRoomCategory = PartyRoomCategory.Talk,
): Promise<JoinPartyRoomResult> {
  const cleanTitle = title.trim();
  const maxLen = this.config.getOrThrow('PARTY_TITLE_MAX_LENGTH', {
    infer: true,
  });
  if (cleanTitle.length === 0 || cleanTitle.length > maxLen) {
    throw new DomainException(
      PartyRoomErrors.TITLE_TOO_LONG,
      `Title phải từ 1 tới ${maxLen} ký tự`,
      HttpStatus.BAD_REQUEST,
    );
  }

  // Chốt URL LiveKit theo region của HOST trước khi tạo phòng: phòng ghim 1 endpoint
  // trọn đời, cùng triết lý snapshot với speakerLimit.
  const livekitUrl = await this.resolveHostLivekitUrl(user.userId);

  let room: PartyRoom;
  let membership: PartyRoomMember;
  try {
    // [POSTGRES] Phòng và membership host ghi ATOMIC trong 1 transaction: không bao giờ
    // có phòng vô chủ ngay từ lúc sinh.
    ({ room, membership } = await this.dataSource.transaction(
      async (manager) => {
        const createdRoom = await manager.save(
          manager.create(PartyRoom, {
            hostUserId: user.userId,
            title: cleanTitle,
            category,
            status: PartyRoomStatus.Active,
            // Chụp giới hạn speaker lúc tạo: đổi config sau này không ảnh hưởng phòng
            // đang sống.
            speakerLimit: this.config.getOrThrow('PARTY_MAX_SPEAKERS', {
              infer: true,
            }),
            livekitUrl,
          }),
        );
        const member = await manager.save(
          manager.create(PartyRoomMember, {
            roomId: createdRoom.id,
            userId: user.userId,
            role: PartyRole.Host,
          }),
        );
        return { room: createdRoom, membership: member };
      },
    ));
  } catch (err) {
    if (
      isUniqueViolation(err) &&
      violatedConstraint(err, UQ_PARTY_MEMBERS_ACTIVE_USER)
    ) {
      // [POSTGRES] Host đang ở phòng active khác thì insert member dính unique index "1
      // user 1 phòng" (xem bên dưới).
      throw new DomainException(
        PartyRoomErrors.ALREADY_IN_ANOTHER_ROOM,
        'Bạn đang ở trong 1 phòng khác — rời phòng đó trước',
        HttpStatus.CONFLICT,
      );
    }
    throw err;
  }

  try {
    // [EXTERNAL] Gọi SFU SAU commit (lệnh mạng không nằm trong atomic của Postgres) và
    // tạo room TƯỜNG MINH với maxParticipants, emptyTimeout thay vì để LiveKit tự tạo
    // khi có người vào.
    await this.livekit.createRoom(partyRoomName(room.id), {
      maxParticipants: this.config.getOrThrow('PARTY_MAX_MEMBERS', {
        infer: true,
      }),
      emptyTimeoutSeconds: this.config.getOrThrow(
        'PARTY_EMPTY_ROOM_TIMEOUT_SECONDS',
        { infer: true },
      ),
    });
  } catch (err) {
    this.logger.error(
      `Tạo LiveKit room cho phòng ${room.id} thất bại: ${err instanceof Error ? err.message : String(err)}`,
    );
    // Bù trừ: SFU lỗi thì đóng phòng vừa tạo (lý do error); đóng cũng nhả membership
    // host nên host không bị kẹt trong 1 phòng hỏng.
    await this.closeRoomById(room.id, PartyRoomCloseReason.Error).catch(
      () => undefined,
    );
    // Lỗi hạ tầng (5xx) nên không dùng DomainException (chỉ 4xx); client thử lại sau.
    throw new ServiceUnavailableException(
      PartyRoomErrors.MEDIA_ROOM_CREATE_FAILED,
      'Không tạo được phòng trên media server — thử lại sau',
    );
  }

  return {
    room,
    membership,
    token: await this.mintToken(room.id, user.userId, PartyRole.Host),
    livekitUrl: this.livekitUrlOf(room),
  };
}

// [SERVICE] party-room.service.ts · L1149–1157
private async resolveHostLivekitUrl(hostUserId: string): Promise<string> {
  const defaultUrl = this.config.getOrThrow('LIVEKIT_URL', { infer: true });
  const regionUrls = this.config.getOrThrow('LIVEKIT_REGION_URLS', {
    infer: true,
  });
  // Chưa bật multi-region (map rỗng) thì trả thẳng LIVEKIT_URL, không tốn thêm query;
  // region luôn lấy từ User do server suy ra, không nhận từ client.
  if (!hasLivekitRegionUrls(regionUrls)) return defaultUrl;
  const host = await this.userService.getByIdOrThrow(hostUserId);
  return resolveLivekitUrl(regionUrls, defaultUrl, host.region);
}

// [POSTGRES] 1752700000000-party-room-gift.ts · L52–55
await queryRunner.query(
  `CREATE UNIQUE INDEX uq_party_members_active_user
     ON party_room_members(user_id) WHERE left_at IS NULL`,
);

// [EXTERNAL] livekit-party-room.adapter.ts · L17–22
createRoom(
  roomName: string,
  opts: { maxParticipants: number; emptyTimeoutSeconds: number },
): Promise<void> {
  return this.client.createRoom(roomName, opts);
}

// [EXTERNAL] livekit-sdk.client.ts · L74–83
async createRoom(
  roomName: string,
  opts: { maxParticipants: number; emptyTimeoutSeconds: number },
): Promise<void> {
  await this.roomService.createRoom({
    name: roomName,
    maxParticipants: opts.maxParticipants,
    emptyTimeout: opts.emptyTimeoutSeconds,
  });
}

// [SERVICE] party-room.service.ts · L1008–1063
async closeRoomById(
  roomId: string,
  reason: PartyRoomCloseReason,
  guard?: (room: PartyRoom) => boolean,
  onTransition?: PartyRoomTransitionHook,
): Promise<ClosePartyRoomResult> {
  const result = await this.dataSource.transaction(async (manager) => {
    // [POSTGRES] Đóng phòng idempotent: khóa dòng phòng, đã closed thì no-op, nên
    // endpoint host-leave, webhook, sweeper và bù trừ này gọi trùng vẫn an toàn.
    const room = await manager.findOne(PartyRoom, {
      where: { id: roomId },
      lock: { mode: 'pessimistic_write' },
    });
    if (!room || room.status === PartyRoomStatus.Closed) {
      return { closed: false, memberIds: [] as string[] };
    }
    // `guard` (sweeper dùng, lúc bù trừ này không truyền) chạy TRÊN dòng đã khóa, trong
    // cùng transaction: host kịp nối lại thì guard fail và không đóng nhầm.
    if (guard !== undefined && !guard(room)) {
      return { closed: false, memberIds: [] as string[] };
    }
    const activeMembers = await manager.findBy(PartyRoomMember, {
      roomId,
      leftAt: IsNull(),
    });
    const now = new Date();
    room.status = PartyRoomStatus.Closed;
    room.closeReason = reason;
    room.closedAt = now;
    await manager.save(room);
    await onTransition?.(manager, room);
    if (activeMembers.length > 0) {
      await manager.update(
        PartyRoomMember,
        { roomId, leftAt: IsNull() },
        { leftAt: now, disconnectedAt: null },
      );
    }
    return { closed: true, memberIds: activeMembers.map((m) => m.userId) };
  });

  // Chỉ lời gọi thực hiện transition mới dọn SFU và publish, nên retry không bắn event
  // đôi.
  if (result.closed) {
    await this.livekit.deleteRoom(partyRoomName(roomId)).catch((err) => {
      // Room có thể đã tự đóng (room_finished hoặc empty timeout): không phải lỗi
      // nghiệp vụ.
      this.logger.debug(
        `deleteRoom ${partyRoomName(roomId)} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
      );
    });
    const envelope: RealtimeEnvelope<PartyRoomClosedEventData> = {
      event: RealtimeEvents.PartyRoomClosed,
      data: { roomId, reason },
    };
    await Promise.all(
      result.memberIds.map((uid) =>
        // [REDIS] Báo party.room.closed cho từng member đã đóng; best-effort, client
        // còn poll.
        publishRealtimeEvent(this.redis, this.logger, uid, envelope),
      ),
    );
  }
  return result;
}

// [SERVICE] party-room.service.ts · L1301–1313
private async mintToken(
  roomId: string,
  userId: string,
  role: PartyRole,
): Promise<string> {
  return this.livekit.mintJoinToken(
    partyRoomName(roomId),
    userId,
    this.config.getOrThrow('PARTY_TOKEN_TTL_SECONDS', { infer: true }),
    { canPublish: role === PartyRole.Host || role === PartyRole.Speaker },
  );
}

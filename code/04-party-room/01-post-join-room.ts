// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/party/rooms/:id/join
//  Vào phòng Party làm audience, hoặc vào lại khi rớt mạng, rồi nhận token LiveKit theo role hiện tại (audience không được publish).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS] → [EXTERNAL]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận request join (giới hạn 20 lần/phút), gom user và id phòng, trả
//                 phòng, membership, token và URL LiveKit.
//    [SERVICE]    Khóa phòng, nhận ra join lại, kiểm sức chứa, tạo membership audience;
//                 sau commit mới bắn realtime và ký token.
//    [POSTGRES]   Nguồn sự thật của phòng và membership; khóa dòng phòng (FOR UPDATE)
//                 và partial unique index chống ghi trùng.
//    [REDIS]      Pub/sub báo cho các member còn lại, mỗi người một kênh riêng;
//                 signaling-gateway đẩy event xuống socket.
//    [EXTERNAL]   LiveKit: token là JWT chứa quyền publish; audience canPublish=false
//                 nên SFU tự chặn việc tự unmute.
//
//  Bảng       party_rooms — dòng phòng bị khóa FOR UPDATE: đọc status, livekitUrl; xóa
//               mốc host rớt (host_disconnected_at) khi host nối lại.
//             party_room_members — membership theo dòng (active = left_at IS NULL): đếm
//               sức chứa, tạo dòng role audience, hoặc xóa mốc disconnected khi join
//               lại.
//  Redis      realtime:user:{userId} — kênh pub/sub riêng của từng member; core-api
//               publish party.member.joined (hoặc reconnected), signaling-gateway
//               subscribe rồi đẩy xuống socket.
//  Liên quan  LiveKit (SFU) — áp quyền publish theo token; maxParticipants ở SFU là
//               chốt chặn sức chứa thứ hai sau giới hạn trong DB.
//             PartyRoomSweeper — job nền đóng phòng khi host rớt quá thời gian chờ;
//               tranh cùng dòng phòng với join nên không đóng nhầm phòng vừa hồi phục
//               (chưa chép).
//             signaling-gateway — subscribe pub/sub và đẩy event party.* xuống socket
//               của từng member.

// [CONTROLLER] party-room.controller.ts · L132–147
@Post(':id/join')
@HttpCode(HttpStatus.OK)
@Throttle({ default: { limit: 20, ttl: minutes(1) } })
async join(
  @CurrentUser() user: AuthenticatedUser,
  @Param('id', ParseUUIDPipe) id: string,
): Promise<JoinPartyRoomDto> {
  const { room, membership, token, livekitUrl } =
    await this.partyRoomService.joinRoom(user, id);
  return JoinPartyRoomDto.from(room, membership, token, livekitUrl);
}

// [SERVICE] party-room.service.ts · L238–362
async joinRoom(
  user: AuthenticatedUser,
  roomId: string,
): Promise<JoinPartyRoomResult> {
  const { room, membership, isNewJoin, hostReconnected, memberReconnected } =
    await this.dataSource.transaction(async (manager) => {
      // [POSTGRES] Khóa dòng phòng (FOR UPDATE) trước mọi việc khác: join, leave, đổi
      // role, close đều xếp hàng qua đúng 1 dòng này, nên việc đếm bên dưới không bị
      // race.
      const lockedRoom = await this.lockActiveRoom(manager, roomId);

      const existing = await manager.findOneBy(PartyRoomMember, {
        roomId,
        userId: user.userId,
        leftAt: IsNull(),
      });
      // Rớt mạng rồi join lại là hợp lệ: không tạo membership mới, chỉ xóa mốc rớt và
      // cấp token mới theo role hiện tại trong DB.
      if (existing) {
        // Host nối lại trong lúc chờ thì hủy lịch đóng phòng. Lock CÙNG dòng phòng với
        // grace-check của sweeper nên Postgres tự xếp thứ tự: bên nào lock trước thắng,
        // không đóng nhầm phòng vừa hồi phục.
        const hostReconnected =
          existing.role === PartyRole.Host &&
          lockedRoom.hostDisconnectedAt !== null;
        const memberReconnected =
          existing.role !== PartyRole.Host &&
          existing.disconnectedAt !== null;
        if (hostReconnected) {
          lockedRoom.hostDisconnectedAt = null;
          await manager.save(lockedRoom);
        }
        if (memberReconnected) {
          existing.disconnectedAt = null;
          await manager.save(existing);
        }
        return {
          room: lockedRoom,
          membership: existing,
          isNewJoin: false,
          hostReconnected,
          memberReconnected,
        };
      }

      // Đếm member DƯỚI lock phòng; DB và SFU (maxParticipants) dùng chung 1 giới hạn
      // từ 1 config, SFU là chốt chặn thứ hai.
      const activeCount = await manager.countBy(PartyRoomMember, {
        roomId,
        leftAt: IsNull(),
      });
      if (
        activeCount >=
        this.config.getOrThrow('PARTY_MAX_MEMBERS', { infer: true })
      ) {
        throw new DomainException(
          PartyRoomErrors.ROOM_FULL,
          'Phòng đã đầy',
          HttpStatus.CONFLICT,
        );
      }

      try {
        // [POSTGRES] 1 user chỉ ở 1 phòng active: khóa phòng chỉ xếp hàng trong từng
        // phòng, nên chốt chặn là unique index trên user_id (xem bên dưới), không phải
        // check-rồi-insert. Dính unique = user đang ở phòng khác.
        const member = await manager.save(
          manager.create(PartyRoomMember, {
            roomId,
            userId: user.userId,
            role: PartyRole.Audience,
          }),
        );
        return {
          room: lockedRoom,
          membership: member,
          isNewJoin: true,
          hostReconnected: false,
          memberReconnected: false,
        };
      } catch (err) {
        if (
          isUniqueViolation(err) &&
          violatedConstraint(err, UQ_PARTY_MEMBERS_ACTIVE_USER)
        ) {
          throw new DomainException(
            PartyRoomErrors.ALREADY_IN_ANOTHER_ROOM,
            'Bạn đang ở trong 1 phòng khác — rời phòng đó trước',
            HttpStatus.CONFLICT,
          );
        }
        throw err;
      }
    });

  // [REDIS] Realtime bắn SAU commit và best-effort; join lại (isNewJoin = false) không
  // bắn "joined", và không bắn cho chính người vừa join.
  if (isNewJoin) {
    await this.publishToRoomMembers(
      roomId,
      {
        event: RealtimeEvents.PartyMemberJoined,
        data: {
          roomId,
          userId: user.userId,
          role: membership.role,
        } satisfies PartyMemberJoinedEventData,
      },
      { excludeUserId: user.userId },
    );
  }
  if (hostReconnected) {
    await this.publishToRoomMembers(roomId, {
      event: RealtimeEvents.PartyHostReconnected,
      data: { roomId } satisfies PartyHostReconnectedEventData,
    });
  }
  if (memberReconnected) {
    await this.publishToRoomMembers(roomId, {
      event: RealtimeEvents.PartyMemberReconnected,
      data: {
        roomId,
        userId: user.userId,
      } satisfies PartyMemberReconnectedEventData,
    });
  }

  return {
    room,
    membership,
    // URL là snapshot của PHÒNG, không phải region người join: mọi người về đúng
    // endpoint đã chốt lúc tạo, kể cả khi khác region với host.
    livekitUrl: this.livekitUrlOf(room),
    token: await this.mintToken(roomId, user.userId, membership.role),
  };
}

// [SERVICE] party-room.service.ts · L1167–1190
private async lockActiveRoom(
  manager: EntityManager,
  roomId: string,
): Promise<PartyRoom> {
  const room = await manager.findOne(PartyRoom, {
    where: { id: roomId },
    lock: { mode: 'pessimistic_write' },
  });
  if (!room) {
    throw new DomainException(
      PartyRoomErrors.ROOM_NOT_FOUND,
      'Không tìm thấy phòng',
      HttpStatus.NOT_FOUND,
    );
  }
  if (room.status !== PartyRoomStatus.Active) {
    throw new DomainException(
      PartyRoomErrors.ROOM_CLOSED,
      'Phòng đã đóng',
      HttpStatus.CONFLICT,
    );
  }
  return room;
}

// [POSTGRES] 1752700000000-party-room-gift.ts · L52–55
await queryRunner.query(
  `CREATE UNIQUE INDEX uq_party_members_active_user
     ON party_room_members(user_id) WHERE left_at IS NULL`,
);

// [SERVICE] party-room.service.ts · L1316–1329
private async publishToRoomMembers(
  roomId: string,
  envelope: RealtimeEnvelope,
  opts: { excludeUserId?: string } = {},
): Promise<void> {
  const members = await this.memberRepo.findBy({ roomId, leftAt: IsNull() });
  await Promise.all(
    members
      .filter((m) => m.userId !== opts.excludeUserId)
      .map((m) =>
        // [REDIS] Mỗi member nhận event qua kênh riêng realtime:user:{userId}; không có
        // kênh theo phòng nên gateway không chứa logic. Publish lỗi chỉ log, không phá
        // join đã commit.
        publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
      ),
  );
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
    // Audience canPublish=false NGAY TỪ TOKEN: chặn tự unmute ở SFU, không chỉ ở UI.
    { canPublish: role === PartyRole.Host || role === PartyRole.Speaker },
  );
}

// [EXTERNAL] livekit-party-room.adapter.ts · L24–35
mintJoinToken(
  roomName: string,
  identity: string,
  ttlSeconds: number,
  grants: { canPublish: boolean },
): Promise<string> {
  return this.client.mintJoinToken(roomName, identity, ttlSeconds, {
    canPublish: grants.canPublish,
    // Audience vẫn gửi được data-channel (chat, reaction) dù không được publish audio.
    canPublishData: true,
  });
}

// [EXTERNAL] livekit-sdk.client.ts · L52–72
async mintJoinToken(
  roomName: string,
  identity: string,
  ttlSeconds: number,
  grant: LivekitTokenGrant,
): Promise<string> {
  const token = new AccessToken(this.apiKey, this.apiSecret, {
    identity,
    ttl: ttlSeconds,
  });
  token.addGrant({
    room: roomName,
    roomJoin: true,
    // Quyền publish nằm TRONG token (JWT ký bằng API secret LiveKit), nên SFU tự chặn
    // audience, không tin client.
    canPublish: grant.canPublish,
    canSubscribe: true,
    ...(grant.canPublishData === undefined
      ? {}
      : { canPublishData: grant.canPublishData }),
  });
  return token.toJwt();
}

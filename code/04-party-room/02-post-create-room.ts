//  POST /api/v1/party/rooms
//  Tạo phòng và membership host trong 1 transaction, rồi tạo room trên LiveKit; SFU lỗi thì đóng phòng bù trừ.
//  Step 1: Nhận request (user từ JWT, title và category từ body; 5 lần/phút)
//  Step 2: Chốt URL LiveKit theo region của host
//  Step 3: Ghi phòng và membership host trong 1 transaction
//  Step 4: Sau commit, tạo room tường minh trên LiveKit
//  Step 5: SFU lỗi: đóng phòng vừa tạo để bù trừ
//  Step 6: Đóng phòng idempotent: khóa dòng phòng, đã closed thì bỏ qua
//  Step 7: Dọn room trên SFU và báo party.room.closed cho member
//  Step 8: Trả 503 để client thử lại sau
//  Step 9: Ký token publish cho host, trả phòng và URL LiveKit
//  Step 10: Controller trả phòng, membership host, token và URL LiveKit

// [CONTROLLER] PartyRoomController.create  ·  party-room.controller.ts L46–47, L50–63
@Controller('party/rooms')
export class PartyRoomController {
  // PartyRoomService
  private readonly logger = new Logger(PartyRoomService.name);
  constructor(
    private readonly partyRoomService: PartyRoomService,
    // PartyRoomService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly livekit: PartyLivekitRoomPort,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(PARTY_REDIS) private readonly redis: Redis,
  ) {}

  // Step 1: Nhận request (user từ JWT, title và category từ body; 5 lần/phút)
  @Post()
  @Throttle({ default: { limit: 5, ttl: minutes(1) } })
  async create(
    @CurrentUser() user: AuthenticatedUser,
    @Body() body: CreatePartyRoomDto,
  ): Promise<JoinPartyRoomDto> {
    const { room, membership, token, livekitUrl } =
      await this.partyRoomService.createRoom(user, body.title, body.category);

    // [SERVICE] PartyRoomService.createRoom  ·  party-room.service.ts L138–232
    const title = body.title;
    const category = body.category;
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

    // Step 2: Chốt URL LiveKit theo region của host
    // Chốt URL LiveKit theo region của HOST (server suy ra từ User, không nhận từ client) trước
    // khi tạo phòng: phòng ghim 1 endpoint trọn đời, cùng triết lý snapshot với speakerLimit.
    const livekitUrl = await this.resolveHostLivekitUrl(user.userId);

    let room: PartyRoom;
    let membership: PartyRoomMember;
    try {

      // Step 3: Ghi phòng và membership host trong 1 transaction
      // [POSTGRES] Phòng và membership host ghi ATOMIC trong 1 transaction: không bao giờ có
      // phòng vô chủ ngay từ lúc sinh.
      ({ room, membership } = await this.dataSource.transaction(
        async (manager) => {
          const createdRoom = await manager.save(
            manager.create(PartyRoom, {
              hostUserId: user.userId,
              title: cleanTitle,
              category,
              status: PartyRoomStatus.Active,
              // Chụp giới hạn speaker lúc tạo: đổi config sau này không ảnh hưởng phòng đang
              // sống.
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
        // [POSTGRES] Host đang ở phòng active khác thì insert member dính unique index "1 user 1
        // phòng" (xem bên dưới).
        throw new DomainException(
          PartyRoomErrors.ALREADY_IN_ANOTHER_ROOM,
          'Bạn đang ở trong 1 phòng khác — rời phòng đó trước',
          HttpStatus.CONFLICT,
        );
      }
      throw err;
    }

    try {

      // Step 4: Sau commit, tạo room tường minh trên LiveKit
      // [EXTERNAL] Gọi SFU SAU commit (lệnh mạng không nằm trong atomic của Postgres) và tạo room
      // TƯỜNG MINH với maxParticipants, emptyTimeout thay vì để LiveKit tự tạo khi có người vào.
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

      // Step 5: SFU lỗi: đóng phòng vừa tạo để bù trừ
      // Bù trừ: SFU lỗi thì đóng phòng vừa tạo (lý do error); đóng cũng nhả membership host nên
      // host không bị kẹt trong 1 phòng hỏng.
      await this.closeRoomById(room.id, PartyRoomCloseReason.Error).catch(
        () => undefined,
      );

      // [SERVICE] PartyRoomService.closeRoomById  ·  party-room.service.ts L1008–1063
      const roomId = room.id;
      const reason = PartyRoomCloseReason.Error;
      const result = await this.dataSource.transaction(async (manager) => {

        // Step 6: Đóng phòng idempotent: khóa dòng phòng, đã closed thì bỏ qua
        // [POSTGRES] Đóng phòng idempotent: khóa dòng phòng, đã closed thì no-op, nên endpoint
        // host-leave, webhook, sweeper và bù trừ này gọi trùng vẫn an toàn.
        const room = await manager.findOne(PartyRoom, {
          where: { id: roomId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!room || room.status === PartyRoomStatus.Closed) {
          return { closed: false, memberIds: [] as string[] };
        }
        // `guard` (sweeper dùng, lúc bù trừ này không truyền) chạy TRÊN dòng đã khóa, trong cùng
        // transaction: host kịp nối lại thì guard fail và không đóng nhầm.
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

      // Step 7: Dọn room trên SFU và báo party.room.closed cho member
      // Chỉ lời gọi thực hiện transition mới dọn SFU và publish, nên retry không bắn event đôi.
      if (result.closed) {
        await this.livekit.deleteRoom(partyRoomName(roomId)).catch((err) => {
          // Room có thể đã tự đóng (room_finished hoặc empty timeout): không phải lỗi nghiệp vụ.
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
            // [REDIS] Báo party.room.closed cho từng member đã đóng; best-effort, client còn
            // poll.
            publishRealtimeEvent(this.redis, this.logger, uid, envelope),
          ),
        );
      }
      return result;

      // Step 8: Trả 503 để client thử lại sau
      // Lỗi hạ tầng (5xx) nên không dùng DomainException (chỉ 4xx); client thử lại sau.
      throw new ServiceUnavailableException(
        PartyRoomErrors.MEDIA_ROOM_CREATE_FAILED,
        'Không tạo được phòng trên media server — thử lại sau',
      );
    }

    // Step 9: Ký token publish cho host, trả phòng và URL LiveKit
    return {
      room,
      membership,
      token: await this.mintToken(room.id, user.userId, PartyRole.Host),
      livekitUrl: this.livekitUrlOf(room),
    };

    // Step 10: Controller trả phòng, membership host, token và URL LiveKit
    return JoinPartyRoomDto.from(room, membership, token, livekitUrl);
  }
}

// [POSTGRES] 1752700000000-party-room-gift.ts  ·  1752700000000-party-room-gift.ts L52–55
await queryRunner.query(
  `CREATE UNIQUE INDEX uq_party_members_active_user
     ON party_room_members(user_id) WHERE left_at IS NULL`,
);

//  POST /api/v1/party/rooms/:id/comments
//  Giữ FOR SHARE trên hàng phòng, kiểm membership dưới khoá, chèn bình luận append-only có idempotency, rồi phát realtime cho các thành viên SAU commit.
//  Step 1: Endpoint gửi bình luận (Idempotency-Key bắt buộc, 60 lần mỗi phút)
//  Step 2: Kiểm độ dài nội dung trước khi mở transaction
//  Step 3: Một transaction ngắn: khoá phòng, kiểm membership, chèn bình luận
//  Step 4: Bình luận giữ FOR SHARE; thao tác đổi state giữ FOR UPDATE
//  Step 5: Membership được kiểm DƯỚI khoá phòng
//  Step 6: Chèn bình luận append-only với idempotency_key duy nhất
//  Step 7: Phát realtime cho thành viên SAU commit
//  Step 8: Fanout chỉ đọc userId của thành viên còn trong phòng
//  Step 9: Replay cùng Idempotency-Key: trả bình luận cũ, không bắn lại sự kiện

// [CONTROLLER] PartyRoomController.createComment · party-room/party-room.controller.ts
@Controller('party/rooms')
export class PartyRoomController {
  // PartyRoomService
  private readonly logger = new Logger(PartyRoomService.name);
  constructor(
    private readonly partyRoomService: PartyRoomService,
    // PartyRoomService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(PartyRoomMember)
    private readonly memberRepo: Repository<PartyRoomMember>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(PARTY_REDIS) private readonly redis: RealtimePublisherPort,
  ) {}

  @Post(':id/comments')
  @Throttle({ default: { limit: 60, ttl: minutes(1) } })

  // Step 1: Endpoint gửi bình luận (Idempotency-Key bắt buộc, 60 lần mỗi phút)
  async createComment(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() body: CreatePartyRoomCommentDto,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<PartyRoomCommentDto> {
    return PartyRoomCommentDto.from(
      await this.partyRoomService.sendComment(user, id, body, idempotencyKey),
    );

    // [SERVICE] PartyRoomService.sendComment · party-room/party-room.service.ts
    const roomId = id;
    const dto = body;

    // Step 2: Kiểm độ dài nội dung trước khi mở transaction
    const content = dto.content.trim();
    const maxLength = this.config.getOrThrow('PARTY_COMMENT_MAX_LENGTH', {
      infer: true,
    });
    if (content.length === 0 || content.length > maxLength) {
      throw new DomainException(
        PartyRoomErrors.COMMENT_INVALID,
        `Bình luận phải từ 1 tới ${maxLength} ký tự`,
        HttpStatus.UNPROCESSABLE_ENTITY,
        { maxLength },
      );
    }

    const prefixedKey = partyCommentIdempotencyKey(
      user.userId,
      roomId,
      idempotencyKey,
    );
    try {

      // Step 3: Một transaction ngắn: khoá phòng, kiểm membership, chèn bình luận
      const comment = await this.dataSource.transaction(async (manager) => {
        // Bình luận chỉ cần phòng còn active và membership không đổi trong lúc kiểm tra và chèn,
        // nên chỉ giữ FOR SHARE: nhiều bình luận cùng phòng chạy song song thay vì xếp hàng trên
        // một khoá hàng cho mọi bình luận.
        await this.lockActiveRoom(manager, roomId, 'read');

        // [POSTGRES] PartyRoomService.lockActiveRoom
        const mode = 'read';
        const room = await manager.findOne(PartyRoom, {
          where: { id: roomId },
          lock: {

            // Step 4: Bình luận giữ FOR SHARE; thao tác đổi state giữ FOR UPDATE
            // FOR SHARE vẫn xung đột với FOR UPDATE: join, leave, đổi role và đóng phòng chờ các
            // bình luận đang chạy xong, nên thứ tự leave/send vẫn xác định dù bình luận không còn
            // chặn nhau.
            mode: mode === 'read' ? 'pessimistic_read' : 'pessimistic_write',
          },
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

        // Step 5: Membership được kiểm DƯỚI khoá phòng
        // Membership được kiểm dưới khoá phòng để REST leave và send đồng thời có thứ tự xác
        // định: không có bình luận nào từ người vừa rời.
        const membership = await manager.findOneBy(PartyRoomMember, {
          roomId,
          userId: user.userId,
          leftAt: IsNull(),
          disconnectedAt: IsNull(),
        });
        if (!membership) {
          throw new DomainException(
            PartyRoomErrors.NOT_A_MEMBER,
            'Bạn không ở trong phòng',
            HttpStatus.FORBIDDEN,
          );
        }

        // Step 6: Chèn bình luận append-only với idempotency_key duy nhất
        return manager.save(
          manager.create(PartyRoomComment, {
            roomId,
            senderUserId: user.userId,
            content,
            idempotencyKey: prefixedKey,
          }),
        );
      });

      // Step 7: Phát realtime cho thành viên SAU commit
      // Sự kiện chỉ bắn SAU khi transaction commit và replay không bắn lại: client không bao giờ
      // thấy một bình luận chưa tồn tại hoặc bị lặp.
      await this.publishToRoomMembers(roomId, {
        event: RealtimeEvents.PartyCommentCreated,
        data: {
          roomId,
          commentId: comment.id,
          senderUserId: comment.senderUserId,
          content: comment.content,
          sentAt: comment.createdAt.toISOString(),
        } satisfies PartyCommentCreatedEventData,
      });

      // [REDIS] PartyRoomService.publishToRoomMembers
      // envelope = đối số 2 của lời gọi ở trên
      const opts = {};

      // Step 8: Fanout chỉ đọc userId của thành viên còn trong phòng
      // Chỉ cần userId: không nạp cả hàng member (token, mốc thời gian...) cho mỗi sự kiện
      // fanout. Fanout dùng kênh riêng của từng user, gateway không có kênh theo phòng.
      const members = await this.memberRepo.find({
        select: { userId: true },
        where: { roomId, leftAt: IsNull() },
      });
      await Promise.all(
        members
          .filter((m) => m.userId !== opts.excludeUserId)
          .map((m) =>
            publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
          ),
      );

      return comment;
    } catch (err) {
      if (
        !isUniqueViolation(err) ||
        !violatedConstraint(err, UQ_PARTY_ROOM_COMMENTS_IDEMPOTENCY)
      ) {
        throw err;
      }

      // Step 9: Replay cùng Idempotency-Key: trả bình luận cũ, không bắn lại sự kiện
      // Vi phạm unique trên idempotency_key nghĩa là yêu cầu này đã được xử lý: cùng phòng, người
      // gửi và nội dung thì trả bình luận cũ; khác nội dung thì 409 thay vì ghi đè.
      const existing = await this.commentRepo.findOneBy({
        idempotencyKey: prefixedKey,
      });
      if (
        existing &&
        existing.roomId === roomId &&
        existing.senderUserId === user.userId &&
        existing.content === content
      ) {
        return existing;
      }
      throw new DomainException(
        PartyRoomErrors.COMMENT_IDEMPOTENCY_CONFLICT,
        'Idempotency-Key đã dùng cho một bình luận khác',
        HttpStatus.CONFLICT,
      );
    }
  }
}

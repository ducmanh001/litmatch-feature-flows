// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/conversations/:id/messages
//  Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận tin nhắn kèm Idempotency-Key bắt buộc; nếu có imageAssetId thì
//                 kiểm quyền sở hữu ảnh rồi đổi thành URL, HTTP chỉ tạo được attachment
//                 kiểu ảnh.
//    [SERVICE]    Kiểm tra user thuộc hội thoại và không bị block 2 chiều, ghi tin nhắn
//                 idempotent, cập nhật streak, phát realtime cho 2 người, rồi tạo thông
//                 báo cho người nhận (trừ khi đã mute).
//    [POSTGRES]   Unique idempotency_key chặn nhân đôi tin nhắn; seq do DB cấp cho
//                 cursor; streak cập nhật trong transaction riêng, khóa dòng FOR
//                 UPDATE.
//    [REDIS]      Pub/sub báo friend.message (và friend.streak.increased khi chạm mốc)
//                 cho cả 2 người.
//
//  Bảng       conversations — hội thoại 1-1 của một cặp (low < high), unique theo cặp;
//               last_message_at cập nhật sau mỗi tin để sắp xếp danh sách chat.
//             messages — tin nhắn chỉ thêm: nội dung, attachment, idempotency_key
//               unique, seq tăng dần.
//             conversation_streaks — chuỗi ngày cả 2 cùng nhắn của hội thoại; dòng bị
//               khóa FOR UPDATE khi cập nhật.
//             conversation_member_states — chỉ đọc: người nhận có đang mute hội thoại
//               hay không.
//             blocks — chỉ đọc (qua Safety): dòng block mới nhất của mỗi chiều cho biết
//               đang block hay đã bỏ block.
//             notifications — thông báo friend_message cho người nhận, tạo ở
//               transaction riêng và best-effort.
//  Redis      realtime:user:{userId} — kênh pub/sub theo người nhận; core-api phát
//               friend.message và friend.streak.increased, signaling-gateway chuyển
//               xuống socket.
//  Liên quan  Safety — isBlocked: kiểm block theo từng chiều; bị block trả cùng lỗi 404
//               với "không phải thành viên".
//             Media — resolveImageUrl: kiểm ảnh thuộc user và đã upload, trả URL để làm
//               attachment.
//             Notification — create tạo thông báo friend_message, sendPush gửi push; cả
//               hai best-effort sau khi tin đã lưu.
//             signaling-gateway — subscribe kênh realtime của user, đẩy friend.message
//               xuống client.
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] FriendController.sendMessage  ·  friend.controller.ts L47–48, L198–228
@Controller()
export class FriendController {
  constructor(
    private readonly friendService: FriendService,
    private readonly mediaService: MediaService,
  ) {}

  // Step 1: Nhận request (user từ JWT, id hội thoại từ URL, Idempotency-Key)
  @Post('conversations/:id/messages')
  @HttpCode(HttpStatus.CREATED)
  async sendMessage(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: SendFriendMessageDto,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<MessageDto> {

    // Step 2: Có ảnh đính kèm thì kiểm quyền sở hữu và đổi thành URL
    const imageUrl = dto.imageAssetId
      ? await this.mediaService.resolveImageUrl(
          user.userId,
          dto.imageAssetId,
          ImageAssetPurpose.Message,
        )
      : undefined;
    const message = await this.friendService.sendMessage(
      user.userId,
      id,
      dto.content ?? '',
      idempotencyKey,
      // HTTP chỉ tạo được attachment kiểu ảnh, từ asset đã được xác thực. Các kind khác (vd
      // story_reply) chỉ đi qua DI giữa module, client không tự gắn tùy ý.
      imageUrl !== undefined
        ? { kind: 'image', payload: { url: imageUrl } }
        : null,
    );

      // [SERVICE] FriendService.sendMessage  ·  friend.service.ts L330–459
      private readonly logger = new Logger(FriendService.name);
      constructor(
        @InjectRepository(ConversationMemberState)
        private readonly memberStateRepo: Repository<ConversationMemberState>,
        private readonly conversationService: ConversationService,
        private readonly streakService: StreakService,
        private readonly safetyService: SafetyService,
        private readonly notificationService: NotificationService,
        private readonly config: ConfigService<CoreApiEnv, true>,
        @Inject(FRIEND_REDIS) private readonly redis: Redis,
        // ConversationService
        @InjectRepository(Conversation)
        private readonly conversationRepo: Repository<Conversation>,
        @InjectRepository(Message)
        private readonly messageRepo: Repository<Message>,
        // StreakService
        @InjectDataSource() private readonly dataSource: DataSource,
      ) {}

      const userId = user.userId;
      const conversationId = id;
      const content = dto.content ?? '';
      const attachment = imageUrl !== undefined
        ? { kind: 'image', payload: { url: imageUrl } }
        : null;

      // Step 3: Kiểm user thuộc hội thoại
      const conversation = await this.getConversationForMember(
        userId,
        conversationId,
      );

      // Step 4: Kiểm block 2 chiều, độ dài tin nhắn, tin rỗng
      // Block kiểm cả 2 chiều và trả CÙNG mã lỗi 404 với "không phải thành viên", để không ai
      // biết ai block ai qua mã lỗi.
      const partnerId =
        conversation.userLowId === userId
          ? conversation.userHighId
          : conversation.userLowId;
      if (
        (await this.safetyService.isBlocked(userId, partnerId)) ||
        (await this.safetyService.isBlocked(partnerId, userId))
      ) {
        throw new DomainException(
          FriendErrors.CONVERSATION_NOT_FOUND,
          'Không tìm thấy conversation',
          HttpStatus.NOT_FOUND,
        );
      }
      const maxLength = this.config.getOrThrow('FRIEND_MESSAGE_MAX_LENGTH', {
        infer: true,
      });
      if (content.length > maxLength) {
        throw new DomainException(
          FriendErrors.MESSAGE_TOO_LONG,
          `Message dài quá ${maxLength} ký tự`,
          HttpStatus.UNPROCESSABLE_ENTITY,
          { maxLength },
        );
      }
      if (content.trim().length === 0 && attachment === null) {
        throw new DomainException(
          FriendErrors.MESSAGE_EMPTY,
          'Nhập nội dung hoặc đính kèm ảnh',
          HttpStatus.UNPROCESSABLE_ENTITY,
        );
      }

      const message = await this.conversationService.sendMessage(
        conversation,
        userId,
        content,
        idempotencyKey,
        attachment,
      );

        // [SERVICE] ConversationService.sendMessage  ·  conversation.service.ts L152–195
        const senderUserId = userId;

        // Step 5: Lưu tin nhắn idempotent (retry trả lại tin cũ, khác nội dung thì 409)
        // [POSTGRES] Key của client được gắn prefix friend:msg:{userId}: unique ở DB (xem bên
        // dưới), retry không nhân đôi tin nhắn.
        const prefixedKey = messageIdempotencyKey(senderUserId, idempotencyKey);
        let message: Message;
        try {
          message = await this.messageRepo.save(
            this.messageRepo.create({
              conversationId: conversation.id,
              senderUserId,
              content,
              idempotencyKey: prefixedKey,
              attachment,
            }),
          );
        } catch (err) {
          // Retry sau timeout mạng: dính unique thì đọc lại. Cùng hội thoại và cùng nội dung thì
          // trả tin cũ (replay); khác thì 409.
          if (!isUniqueViolation(err)) throw err;
          const existing = await this.messageRepo.findOneBy({
            idempotencyKey: prefixedKey,
          });
          if (
            existing &&
            existing.conversationId === conversation.id &&
            existing.content === content
          ) {
            return existing;
          }
          throw new DomainException(
            FriendErrors.MESSAGE_IDEMPOTENCY_CONFLICT,
            'Idempotency-Key đã dùng cho 1 message khác nội dung',
            HttpStatus.CONFLICT,
          );
        }
        // last_message_at chỉ để sắp xếp danh sách chat, không phải nguồn sự thật nên không cần
        // chung transaction.
        await this.conversationRepo.update(
          { id: conversation.id },
          { lastMessageAt: message.createdAt },
        );
        return message;

      try {

        // Step 6: Cập nhật streak sau khi tin đã lưu, trong transaction riêng
        // Streak chạy SAU khi tin đã lưu, trong transaction RIÊNG: nếu chung transaction với
        // insert tin thì lần dính unique của retry làm cả transaction bị abort. Replay gọi lại
        // cũng không tăng streak lần 2.
        const { milestoneHit } = await this.streakService.recordActivity(
          conversation,
          userId,
        );

          // [SERVICE] StreakService.recordActivity  ·  streak.service.ts L52–103
          const senderUserId = userId;
          const isLow = conversation.userLowId === senderUserId;
          const today = todayUtc();

          return this.dataSource.transaction(async (manager) => {

            // Step 7: Tạo dòng streak nếu chưa có, khóa dòng FOR UPDATE
            // [POSTGRES] Tạo dòng streak khi cần bằng ON CONFLICT DO NOTHING rồi khóa FOR UPDATE:
            // 2 bên nhắn gần như cùng lúc vẫn tăng streak đúng 1 lần.
            await manager.query(
              `INSERT INTO conversation_streaks (conversation_id) VALUES ($1)
               ON CONFLICT (conversation_id) DO NOTHING`,
              [conversation.id],
            );
            const row = await manager
              .createQueryBuilder(ConversationStreak, 'cs')
              .where('cs.conversationId = :id', { id: conversation.id })
              .setLock('pessimistic_write')
              .getOneOrFail();

            if (isLow) row.userLowLastActiveDate = today;
            else row.userHighLastActiveDate = today;

            let milestoneHit: number | null = null;
            const bothActiveToday =
              row.userLowLastActiveDate === today &&
              row.userHighLastActiveDate === today;

            // Step 8: Cả 2 đã nhắn hôm nay thì tăng streak 1 lần/ngày, cứu lỡ 1 ngày
            // Chỉ tăng khi cả 2 đã nhắn hôm nay và hôm nay chưa xác nhận, nên gọi lại trong cùng
            // ngày không tăng lần 2. Ngày tính theo UTC của server, không theo múi giờ client.
            if (bothActiveToday && row.lastConfirmedDate !== today) {
              const gapDays = row.lastConfirmedDate
                ? daysBetweenUtc(row.lastConfirmedDate, today)
                : null;
              if (gapDays === null || gapDays === 1) {
                row.currentStreak += 1;
              } else if (gapDays === 2) {
                // Lỡ đúng 1 ngày vẫn được cứu: vẫn +1 chứ không reset, ghi lại ngày được cứu. Lỡ
                // từ 2 ngày trở lên thì reset về 1.
                row.currentStreak += 1;
                row.graceUsedForDate = addDaysUtc(row.lastConfirmedDate as string, 1);
              } else {
                row.currentStreak = 1;
              }
              row.longestStreak = Math.max(row.longestStreak, row.currentStreak);
              row.lastConfirmedDate = today;

              const milestones = this.parseMilestoneDays();
              if (milestones.includes(row.currentStreak)) {
                milestoneHit = row.currentStreak;
              }
            }

            await manager.save(row);
            return { streak: row, milestoneHit };
          });

        // Step 9: Chạm mốc streak thì phát friend.streak.increased qua Redis
        if (milestoneHit !== null) {
          const streakEnvelope: RealtimeEnvelope<FriendStreakIncreasedEventData> =
            {
              event: RealtimeEvents.FriendStreakIncreased,
              data: { conversationId, currentStreak: milestoneHit },
            };
          await Promise.all(
            [userId, partnerId].map((uid) =>
              publishRealtimeEvent(this.redis, this.logger, uid, streakEnvelope),
            ),
          );
        }
      } catch (err) {
        // Streak là phụ: lỗi tính streak chỉ ghi log, không rollback tin đã gửi.
        this.logger.error(
          `Tính streak cho conversation ${conversationId} lỗi (bỏ qua, message vẫn gửi thành công): ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }

      // Step 10: Phát friend.message cho cả 2 người qua Redis (sau khi lưu)
      const envelope: RealtimeEnvelope<FriendMessageEventData> = {
        event: RealtimeEvents.FriendMessage,
        data: {
          conversationId,
          messageId: message.id,
          senderUserId: message.senderUserId,
          content: message.content,
          attachment: message.attachment,
          sentAt: message.createdAt.toISOString(),
        },
      };
      // [REDIS] Phát friend.message cho cả 2 thành viên qua kênh realtime:user:{id}, SAU khi lưu.
      // Best-effort, không dùng outbox: client vẫn poll được GET /conversations/:id/messages.
      await Promise.all(
        [userId, partnerId].map((uid) =>
          publishRealtimeEvent(this.redis, this.logger, uid, envelope),
        ),
      );

      try {

        // Step 11: Tạo thông báo và push cho người nhận, trừ khi đã mute
        const partnerState = await this.memberStateRepo.findOne({
          where: { conversationId, userId: partnerId },
        });
        // Người nhận đã mute thì bỏ qua cả in-app lẫn push; tin nhắn, realtime và unread vẫn chạy
        // bình thường.
        if (partnerState?.mutedAt != null) return message;
        // Thông báo chạy transaction riêng và best-effort: tin đã lưu xong nên lỗi ở đây không
        // được làm hỏng luồng gửi. Chat không ẩn danh nên lộ senderUserId là đúng.
        const notification = await this.notificationService.create({
          userId: partnerId,
          type: NotificationType.FriendMessage,
          payload: {
            conversationId,
            senderUserId: userId,
            preview: message.content,
          },
        });
        await this.notificationService.sendPush(notification);
      } catch (err) {
        this.logger.warn(
          `Tạo notification friend_message lỗi (bỏ qua, message vẫn gửi thành công): ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }

      return message;

    // Step 12: Controller trả tin nhắn vừa lưu
    return MessageDto.from(message);
  }
}

// [POSTGRES] CREATE TABLE messages  ·  1752600000000-friend-chat.ts L30–41
// Unique idempotency_key là chốt chặn thật chống nhân đôi tin nhắn. seq bigint IDENTITY là thứ tự
// do DB cấp cho cursor, tránh trùng hoặc mất dòng khi phân trang theo created_at.
await queryRunner.query(`
  CREATE TABLE messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seq             bigint GENERATED ALWAYS AS IDENTITY,
    conversation_id uuid         NOT NULL REFERENCES conversations(id),
    sender_user_id  uuid         NOT NULL REFERENCES users(id),
    content         text         NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_messages_idempotency_key UNIQUE (idempotency_key)
  )
`);

//  POST /api/v1/conversations/:id/messages
//  Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận.
//  Step 1: Nhận request (user từ JWT, id hội thoại từ URL, Idempotency-Key)
//  Step 2: Có ảnh đính kèm thì kiểm quyền sở hữu và đổi thành URL
//  Step 3: Kiểm user thuộc hội thoại
//  Step 4: Kiểm block 2 chiều, độ dài tin nhắn, tin rỗng
//  Step 5: Lưu tin nhắn idempotent (retry trả lại tin cũ, khác nội dung thì 409)
//  Step 6: Cập nhật streak sau khi tin đã lưu, trong transaction riêng
//  Step 7: Tạo dòng streak nếu chưa có, khóa dòng FOR UPDATE
//  Step 8: Cả 2 đã nhắn hôm nay thì tăng streak 1 lần/ngày, cứu lỡ 1 ngày
//  Step 9: Chạm mốc streak thì phát friend.streak.increased qua Redis
//  Step 10: Phát friend.message cho cả 2 người qua Redis (sau khi lưu)
//  Step 11: Thông báo cho người nhận (best-effort, transaction riêng): gộp theo cửa sổ và tôn trọng mute
//  Step 12: Cổng gộp trên Redis: chỉ tin ĐẦU TIÊN của (người nhận, hội thoại) trong cửa sổ được thông báo
//  Step 13: Người nhận mute hội thoại thì bỏ qua cả in-app lẫn push
//  Step 14: Tạo notification (insert) rồi gửi push
//  Step 15: Controller trả tin nhắn vừa lưu

// [CONTROLLER] FriendController.sendMessage · friend/friend.controller.ts
@Controller()
export class FriendController {
  // FriendService
  private readonly logger = new Logger(FriendService.name);
  // FriendMessageNotifier
  private readonly logger = new Logger(FriendMessageNotifier.name);
  constructor(
    private readonly friendService: FriendService,
    private readonly mediaService: MediaService,
    // FriendService
    private readonly conversationService: ConversationService,
    private readonly streakService: StreakService,
    private readonly safetyService: SafetyService,
    private readonly messageNotifier: FriendMessageNotifier,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(FRIEND_REDIS) private readonly redis: RealtimePublisherPort,
    // ConversationService
    @InjectRepository(Conversation)
    private readonly conversationRepo: Repository<Conversation>,
    @InjectRepository(Message)
    private readonly messageRepo: Repository<Message>,
    // StreakService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(ConversationStreak)
    private readonly streakRepo: Repository<ConversationStreak>,
    // FriendMessageNotifier
    @InjectRepository(ConversationMemberState)
    private readonly memberStateRepo: Repository<ConversationMemberState>,
    private readonly notificationService: NotificationService,
    @Inject(FRIEND_NOTIFY_GATE) private readonly gate: WindowGatePort,
    // RedisWindowGateAdapter
    private readonly redis: Redis,
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

    // [SERVICE] FriendService.sendMessage · friend/friend.service.ts
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
    // Block kiểm cả 2 chiều và trả CÙNG mã lỗi 404 với "không phải thành viên", để không ai biết
    // ai block ai qua mã lỗi.
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

    // [SERVICE] ConversationService.sendMessage · friend/services/conversation.service.ts
    const senderUserId = userId;

    // Step 5: Lưu tin nhắn idempotent (retry trả lại tin cũ, khác nội dung thì 409)
    // [POSTGRES] Key của client được gắn prefix friend:msg:{userId}: unique ở DB (xem bên dưới),
    // retry không nhân đôi tin nhắn.
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
      // Retry sau timeout mạng: dính unique thì đọc lại. Cùng hội thoại và cùng nội dung thì trả
      // tin cũ (replay); khác thì 409.
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
    // last_message_at chỉ để sắp xếp danh sách chat, không phải nguồn sự thật nên không cần chung
    // transaction.
    await this.conversationRepo.update(
      { id: conversation.id },
      { lastMessageAt: message.createdAt },
    );
    return message;

    try {

      // Step 6: Cập nhật streak sau khi tin đã lưu, trong transaction riêng
      // Streak chạy SAU khi tin đã lưu, trong transaction RIÊNG: nếu chung transaction với insert
      // tin thì lần dính unique của retry làm cả transaction bị abort. Replay gọi lại cũng không
      // tăng streak lần 2.
      const { milestoneHit } = await this.streakService.recordActivity(
        conversation,
        userId,
      );

      // [SERVICE] StreakService.recordActivity · friend/services/streak.service.ts
      // senderUserId = userId
      const isLow = conversation.userLowId === senderUserId;
      const today = todayUtc();

      const current = await this.streakRepo.findOneBy({
        conversationId: conversation.id,
      });
      if (current && isNoOpActivity(current, isLow, today)) {
        return { streak: current, milestoneHit: null };
      }

      return this.dataSource.transaction(async (manager) => {

        // Step 7: Tạo dòng streak nếu chưa có, khóa dòng FOR UPDATE
        // [POSTGRES] Tạo dòng streak khi cần bằng ON CONFLICT DO NOTHING rồi khóa FOR UPDATE: 2
        // bên nhắn gần như cùng lúc vẫn tăng streak đúng 1 lần.
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
        // Chỉ tăng khi cả 2 đã nhắn hôm nay và hôm nay chưa xác nhận, nên gọi lại trong cùng ngày
        // không tăng lần 2. Ngày tính theo UTC của server, không theo múi giờ client.
        if (bothActiveToday && row.lastConfirmedDate !== today) {
          const gapDays = row.lastConfirmedDate
            ? daysBetweenUtc(row.lastConfirmedDate, today)
            : null;
          if (gapDays === null || gapDays === 1) {
            row.currentStreak += 1;
          } else if (gapDays === 2) {
            // Lỡ đúng 1 ngày vẫn được cứu: vẫn +1 chứ không reset, ghi lại ngày được cứu. Lỡ từ 2
            // ngày trở lên thì reset về 1.
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

      // Step 11: Thông báo cho người nhận (best-effort, transaction riêng): gộp theo cửa sổ và tôn
      // trọng mute
      // Thông báo chạy transaction riêng và best-effort: tin đã lưu xong nên lỗi ở đây không được
      // làm hỏng luồng gửi. Gộp theo cửa sổ và tôn trọng mute nằm trong FriendMessageNotifier.
      await this.messageNotifier.notify({
        conversationId,
        senderUserId: userId,
        recipientId: partnerId,
        preview: message.content,
      });

      // [SERVICE] FriendMessageNotifier.notify · friend/services/friend-message-notifier.service.ts
      const input = {
        conversationId,
        senderUserId: userId,
        recipientId: partnerId,
        preview: message.content,
      };
      const windowSeconds = this.config.getOrThrow(
        'FRIEND_MESSAGE_NOTIFY_WINDOW_SECONDS',
        { infer: true },
      );
      const gateKey = `friend:msg-notify:${input.recipientId}:${input.conversationId}`;

      let acquired = false;
      if (windowSeconds > 0) {
        try {

          // Step 12: Cổng gộp trên Redis: chỉ tin ĐẦU TIÊN của (người nhận, hội thoại) trong cửa
          // sổ được thông báo
          // Chỉ tin ĐẦU TIÊN của (người nhận, hội thoại) trong mỗi cửa sổ tạo notification +
          // push; tin sau vẫn được lưu, phát realtime và tính unread. Ở 3.000 tin/giây đây là
          // khác biệt giữa 3.000 insert notification/giây và ~83 (mô hình, ADR 0013).
          // Không dựa vào "đang online" (presence trễ tới một lease, có thể nuốt push của app
          // nền) hay "còn notification chưa đọc" (đọc hội thoại không đánh dấu notification đã
          // đọc nên sẽ im lặng vĩnh viễn).

          // [REDIS] RedisWindowGateAdapter.tryAcquire · common/redis/redis-window-gate.adapter.ts
          const key = gateKey;
          if (!key) throw new Error('Window gate key is required');
          if (!Number.isInteger(windowSeconds) || windowSeconds < 1) {
            throw new Error('Window gate windowSeconds must be a positive integer');
          }
          // SET key 1 EX window NX: một lệnh nguyên tử và tự hết hạn nên không cần dọn; hai tin
          // đồng thời chỉ một tin thắng.
          return (await this.redis.set(key, '1', 'EX', windowSeconds, 'NX')) === 'OK';

          if (!(await this.gate.tryAcquire(gateKey, windowSeconds))) {
            this.observe('coalesced');
            return;
          }
          acquired = true;
        } catch (err) {
          // Fail-open: lỗi Redis không được nuốt thông báo — quay về hành vi cũ (mỗi tin một
          // thông báo). Cổng chỉ là trạng thái tạm để giảm tải, không phải nguồn sự thật.
          this.observe('gate_error');
          this.logger.warn(
            `Cổng gộp thông báo lỗi, thông báo như cũ: ${errorMessage(err)}`,
          );
        }
      }

      let notified = false;
      try {

        // Step 13: Người nhận mute hội thoại thì bỏ qua cả in-app lẫn push
        const state = await this.memberStateRepo.findOne({
          where: {
            conversationId: input.conversationId,
            userId: input.recipientId,
          },
        });
        if (state?.mutedAt != null) {
          this.observe('muted');
          return;
        }

        // Step 14: Tạo notification (insert) rồi gửi push
        const notification = await this.notificationService.create({
          userId: input.recipientId,
          type: NotificationType.FriendMessage,
          payload: {
            conversationId: input.conversationId,
            senderUserId: input.senderUserId,
            preview: input.preview,
          },
        });
        notified = true;
        this.observe('notified');
        await this.notificationService.sendPush(notification);
      } catch (err) {
        if (!notified) this.observe('error');
        throw err;
      } finally {
        // Đã chiếm cổng mà cuối cùng không tạo được thông báo (mute hoặc lỗi) thì mở lại cổng, để
        // tin kế tiếp trong cửa sổ không bị nuốt.
        if (acquired && !notified) await this.releaseGate(gateKey);

        // [SERVICE] FriendMessageNotifier.releaseGate
        try {
          await this.gate.release(gateKey);

          // [REDIS] RedisWindowGateAdapter.release · common/redis/redis-window-gate.adapter.ts
          // key = gateKey
          await this.redis.del(key);
        } catch (err) {
          this.logger.warn(`Mở lại cổng gộp thông báo lỗi: ${errorMessage(err)}`);
        }
      }
    } catch (err) {
      this.logger.warn(
        `Tạo notification friend_message lỗi (bỏ qua, message vẫn gửi thành công): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }

    return message;

    // Step 15: Controller trả tin nhắn vừa lưu
    return MessageDto.from(message);
  }
}

// [POSTGRES] CREATE TABLE messages · database/migrations/1752600000000-friend-chat.ts
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

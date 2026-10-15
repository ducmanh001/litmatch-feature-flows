//  POST /api/v1/soul-match/sessions/:id/rating
//  Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction.
//  Step 1: Nhận request (user từ JWT, id phòng từ URL, verdict trong body)
//  Step 2: Kiểm tra user thuộc phòng và còn trong cửa sổ đánh giá
//  Step 3: Trong transaction, khóa dòng session FOR UPDATE
//  Step 4: Đã đánh giá rồi: cùng verdict thì trả kết quả cũ, khác thì 409
//  Step 5: Ghi rating (unique session + người đánh giá)
//  Step 6: Cả 2 cùng like: chụp lịch sử chat ẩn danh theo seq
//  Step 7: Nhờ Friend tạo Friendship và Conversation, cùng transaction
//  Step 8: Chép lịch sử chat ẩn danh sang Conversation
//  Step 9: Double-submit song song: đọc lại rồi replay hoặc 409
//  Step 10: Sau commit, báo soul.matched qua Redis cho cả 2 người
//  Step 11: Controller trả verdict và cờ matched

// [CONTROLLER] SoulMatchController.rate  ·  soul-match.controller.ts L43–44, L109–123
@Controller('soul-match')
export class SoulMatchController {
  // SoulMatchService
  private readonly logger = new Logger(SoulMatchService.name);
  constructor(
    private readonly soulMatchService: SoulMatchService,
    // SoulMatchService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(SoulMatchRating)
    private readonly ratingRepo: Repository<SoulMatchRating>,
    private readonly friendService: FriendService,
    @Inject(SOUL_MATCH_REDIS) private readonly redis: Redis,
  ) {}

  // Step 1: Nhận request (user từ JWT, id phòng từ URL, verdict trong body)
  @Post('sessions/:id/rating')
  @HttpCode(HttpStatus.OK)
  async rate(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: RateSessionDto,
  ): Promise<RatingResultDto> {
    const result = await this.soulMatchService.rate(user, id, dto);

    // [SERVICE] SoulMatchService.rate  ·  soul-match.service.ts L291–403
    const sessionId = id;

    // Step 2: Kiểm tra user thuộc phòng và còn trong cửa sổ đánh giá
    const room = await this.getRoomForMember(user, sessionId);
    if (room.phase === SoulRoomPhase.Closed) {
      throw new DomainException(
        SoulMatchErrors.RATING_NOT_OPEN,
        'Hết cửa sổ đánh giá',
        HttpStatus.CONFLICT,
      );
    }
    const partnerId = this.partnerIdOf(room.session, user.userId);

    let result: RateResult;
    let newlyMatched = false;
    try {
      result = await this.dataSource.transaction(async (manager) => {

        // Step 3: Trong transaction, khóa dòng session FOR UPDATE
        // [POSTGRES] Khóa dòng MatchSession FOR UPDATE TRƯỚC khi ghi rating: 2 lượt like chạy
        // tuần tự. Không khóa thì mỗi bên đều thấy "bên kia chưa like" (READ COMMITTED) và không
        // ai tạo Friendship. Chỉ khóa, không ghi.
        await manager.findOne(MatchSession, {
          where: { id: sessionId },
          lock: { mode: 'pessimistic_write' },
        });

        const existing = await manager.findOneBy(SoulMatchRating, {
          sessionId,
          raterUserId: user.userId,
        });

        // Step 4: Đã đánh giá rồi: cùng verdict thì trả kết quả cũ, khác thì 409
        // Rating bất biến: gửi lại cùng verdict thì trả kết quả cũ, khác verdict thì 409. Cho đổi
        // ý sẽ mở đường "like thử xem bên kia like chưa rồi đổi lại".
        if (existing) {
          this.assertSameVerdict(existing.verdict, dto.verdict);
          return {
            verdict: existing.verdict,
            matched: await this.friendService.areFriends(
              user.userId,
              partnerId,
            ),
          };
        }

        // Step 5: Ghi rating (unique session + người đánh giá)
        await manager.save(
          manager.create(SoulMatchRating, {
            sessionId,
            raterUserId: user.userId,
            verdict: dto.verdict,
          }),
        );

        let matched = await this.friendService.areFriends(
          user.userId,
          partnerId,
        );
        if (dto.verdict === SoulMatchVerdict.Like) {
          const partnerRating = await manager.findOneBy(SoulMatchRating, {
            sessionId,
            raterUserId: partnerId,
          });
          if (partnerRating?.verdict === SoulMatchVerdict.Like) {

            // Step 6: Cả 2 cùng like: chụp lịch sử chat ẩn danh theo seq
            // Đủ 2 like thì chụp toàn bộ lịch sử chat ẩn danh theo seq để chép sang Friend Chat.
            // Key import ổn định theo (session, message) nên chạy lại không nhân đôi; dòng gốc
            // vẫn giữ cho T&S.
            const anonymousMessages = await manager.find(SoulChatMessage, {
              where: { sessionId },
              order: { seq: 'ASC' },
            });
            const messageSeeds: FriendMessageSeed[] = anonymousMessages.map(
              (message) => ({
                senderUserId: message.senderUserId,
                content: message.content,
                createdAt: message.createdAt,
                idempotencyKey: friendHistoryImportKey(sessionId, message.id),
              }),
            );

            // Step 7: Nhờ Friend tạo Friendship và Conversation, cùng transaction
            // Friendship, Conversation và lịch sử chat nằm CÙNG transaction với rating: hoặc có
            // đủ cả, hoặc không có gì.
            const { created } = await this.friendService.ensureFriendship(
              manager,
              user.userId,
              partnerId,
              FriendshipSource.SoulMatch,
              messageSeeds,
            );

            // [SERVICE] FriendService.ensureFriendship  ·  friend.service.ts L104–179
            const userAId = user.userId;
            const userBId = partnerId;
            const source = FriendshipSource.SoulMatch;
            if (userAId === userBId) {
              throw new Error(
                `Không thể tạo friendship với chính mình (${userAId}) — dữ liệu session hỏng`,
              );
            }
            const [userLowId, userHighId] = canonicalPair(userAId, userBId);
            // [POSTGRES] ON CONFLICT DO NOTHING trên cặp canonical (low < high): 2 like song song
            // hoặc match lại cặp cũ đều không tạo đôi và không lỗi.
            const result = await manager
              .createQueryBuilder()
              .insert()
              .into(Friendship)
              .values({ userLowId, userHighId, source })
              .orIgnore()
              .execute();
            // Conversation luôn được ensure cùng lúc, dù Friendship mới hay replay: bất biến "có
            // Friendship thì có Conversation" không phụ thuộc nhánh nào.
            await manager
              .createQueryBuilder()
              .insert()
              .into(Conversation)
              .values({ userLowId, userHighId, lastMessageAt: null })
              .orIgnore()
              .execute();

            // Step 8: Chép lịch sử chat ẩn danh sang Conversation
            // Chép lịch sử chat ẩn danh: chèn ON CONFLICT DO NOTHING theo idempotency_key nên
            // chạy lại không nhân đôi. Friend chỉ nhận seed trung lập, không import entity của
            // luồng nguồn.
            if (messageSeeds.length > 0) {
              const conversation = await manager.findOne(Conversation, {
                where: { userLowId, userHighId },
              });
              if (!conversation) {
                throw new Error(
                  `Không tìm thấy conversation sau khi ensure friendship ${userLowId}/${userHighId}`,
                );
              }

              await manager
                .createQueryBuilder()
                .insert()
                .into(Message)
                .values(
                  messageSeeds.map((message) => ({
                    conversationId: conversation.id,
                    senderUserId: message.senderUserId,
                    content: message.content,
                    idempotencyKey: message.idempotencyKey,
                    createdAt: message.createdAt,
                  })),
                )
                .orIgnore()
                .execute();

              // Không làm lùi mốc sắp xếp của conversation nếu đây là lần chép lại của một cặp đã
              // chat trước đó.
              const latestMessageAt = messageSeeds.reduce(
                (latest, message) =>
                  message.createdAt > latest ? message.createdAt : latest,
                messageSeeds[0].createdAt,
              );
              await manager
                .createQueryBuilder()
                .update(Conversation)
                .set({ lastMessageAt: latestMessageAt })
                .where('id = :conversationId', { conversationId: conversation.id })
                .andWhere(
                  '(last_message_at IS NULL OR last_message_at < :latestMessageAt)',
                  { latestMessageAt },
                )
                .execute();
            }
            return { created: result.raw.length > 0 };

            newlyMatched = created;
            matched = true;
          }
        }
        return { verdict: dto.verdict, matched };
      });
    } catch (err) {

      // Step 9: Double-submit song song: đọc lại rồi replay hoặc 409
      // Double-submit song song của CÙNG user: bên thua unique (session, rater) thì đọc lại rồi
      // replay hoặc 409.
      if (!isUniqueViolation(err)) throw err;
      const existing = await this.ratingRepo.findOneBy({
        sessionId,
        raterUserId: user.userId,
      });
      if (!existing) throw err;
      this.assertSameVerdict(existing.verdict, dto.verdict);
      return {
        verdict: existing.verdict,
        matched: await this.friendService.areFriends(user.userId, partnerId),
      };
    }

    // Step 10: Sau commit, báo soul.matched qua Redis cho cả 2 người
    // [REDIS] soul.matched phát SAU khi commit, best-effort, và chỉ khi Friendship MỚI được tạo;
    // replay không bắn lại.
    if (newlyMatched) {
      const envelope: RealtimeEnvelope<SoulMatchedEventData> = {
        event: RealtimeEvents.SoulMatched,
        data: { sessionId },
      };
      await Promise.all(
        [user.userId, partnerId].map((uid) =>
          publishRealtimeEvent(this.redis, this.logger, uid, envelope),
        ),
      );
    }
    return result;

    // Step 11: Controller trả verdict và cờ matched
    return RatingResultDto.from(result.verdict, result.matched);
  }
}

// [POSTGRES] CREATE TABLE soul_match_ratings  ·  1752400000000-soul-match.ts L33–42
// Unique (session_id, rater_user_id) chính là idempotency tự nhiên của rating, không cần
// Idempotency-Key.
await queryRunner.query(`
  CREATE TABLE soul_match_ratings (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id    uuid        NOT NULL REFERENCES match_sessions(id),
    rater_user_id uuid        NOT NULL REFERENCES users(id),
    verdict       varchar(8)  NOT NULL CHECK (verdict IN ('rude','boring','like')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_soul_match_ratings_session_rater UNIQUE (session_id, rater_user_id)
  )
`);

// [POSTGRES] CREATE TABLE friendships  ·  1752400000000-soul-match.ts L16–27
await queryRunner.query(`
  CREATE TABLE friendships (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_low_id  uuid        NOT NULL REFERENCES users(id),
    user_high_id uuid        NOT NULL REFERENCES users(id),
    source       varchar(16) NOT NULL CHECK (source IN ('soul_match','voice_match')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_friendships_pair UNIQUE (user_low_id, user_high_id),
    CONSTRAINT chk_friendships_canonical CHECK (user_low_id < user_high_id)
  )
`);

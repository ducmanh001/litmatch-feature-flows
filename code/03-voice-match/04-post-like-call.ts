//  POST /api/v1/calling/calls/:id/like
//  Ghi lượt thích bất biến; khi cả 2 cùng thích thì tạo bạn bè và phòng chat trong cùng transaction.
//  Step 1: Nhận yêu cầu thích (id call từ URL, user từ JWT)
//  Step 2: Kiểm user thuộc call (không phải thành viên thì 404)
//  Step 3: Mở transaction DB, khóa dòng CallSession để 2 lượt thích chạy tuần tự
//  Step 4: Chỉ cho thích khi call đang active hoặc đã ended
//  Step 5: Ghi lượt thích bất biến của mình (bấm lại không ghi thêm)
//  Step 6: Cả 2 đã thích thì tạo Friendship và Conversation, cùng transaction
//  Step 7: Trả { liked, matched, friendUserId } về controller
//  Step 8: Dính unique: coi như đã thích, đọc lại trạng thái bạn bè

// [CONTROLLER] CallingController.likeCall  ·  calling.controller.ts L26–27, L126–138
@Controller('calling')
export class CallingController {
  constructor(
    private readonly callingService: CallingService,
    // CallingService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly friendService: FriendService,
  ) {}

  // Step 1: Nhận yêu cầu thích (id call từ URL, user từ JWT)
  @Post('calls/:id/like')
  @HttpCode(HttpStatus.OK)
  async likeCall(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
  ): Promise<VoiceMatchLikeDto> {
    return await this.callingService.likeCall(user, id);

    // [SERVICE] CallingService.likeCall  ·  calling.service.ts L350–435
    const callId = id;

    // Step 2: Kiểm user thuộc call (không phải thành viên thì 404)
    // Không phải thành viên thì trả 404 giống như "không tồn tại", để không ai dò được callId của
    // người khác.
    await this.getCall(user, callId);
    try {

      // Step 3: Mở transaction DB, khóa dòng CallSession để 2 lượt thích chạy tuần tự
      return await this.dataSource.transaction(async (manager) => {
        const call = await manager.findOne(CallSession, {
          where: { id: callId },
          // [POSTGRES] Khóa dòng CallSession FOR UPDATE: lượt thích của 2 người chạy tuần tự.
          // Không khóa thì mỗi người đều thấy "bên kia chưa thích" và không ai tạo Friendship.
          lock: { mode: 'pessimistic_write' },
        });
        if (
          !call ||
          (call.userAId !== user.userId && call.userBId !== user.userId)
        ) {
          throw new DomainException(
            CallingErrors.CALL_NOT_FOUND,
            'Không tìm thấy call',
            HttpStatus.NOT_FOUND,
          );
        }

        // Step 4: Chỉ cho thích khi call đang active hoặc đã ended
        // Cho thích cả khi call đang active, không bắt user phải end mới bấm tim được. Call
        // pending chưa có cuộc nói chuyện thật nên bị chặn.
        if (
          call.callKind === CallKind.Friend ||
          (call.status !== CallSessionStatus.Active &&
            call.status !== CallSessionStatus.Ended)
        ) {
          throw new DomainException(
            CallingErrors.CALL_NOT_ENDED,
            'Chỉ có thể yêu thích khi cuộc gọi đã kết nối hoặc đã kết thúc',
            HttpStatus.CONFLICT,
          );
        }
        const partnerId =
          call.userAId === user.userId ? call.userBId : call.userAId;

        // Step 5: Ghi lượt thích bất biến của mình (bấm lại không ghi thêm)
        // Lượt thích bất biến: bấm lại không ghi thêm dòng. Unique (call_id, rater_user_id) ở DB
        // là chốt chặn cuối cho double tap và retry.
        const existing = await manager.findOneBy(VoiceMatchReaction, {
          callId,
          raterUserId: user.userId,
        });
        if (!existing) {
          await manager.save(
            manager.create(VoiceMatchReaction, {
              callId,
              raterUserId: user.userId,
            }),
          );
        }
        const partnerLiked = await manager.existsBy(VoiceMatchReaction, {
          callId,
          raterUserId: partnerId,
        });

        // Step 6: Cả 2 đã thích thì tạo Friendship và Conversation, cùng transaction
        // [POSTGRES] Chỉ khi cả 2 đã thích mới tạo Friendship và Conversation, ngay trong
        // transaction đã khóa. ensureFriendship dùng ON CONFLICT DO NOTHING nên gọi lại không tạo
        // đôi, và không có Friendship thiếu Conversation.
        if (partnerLiked) {
          await this.friendService.ensureFriendship(
            manager,
            user.userId,
            partnerId,
            FriendshipSource.VoiceMatch,
          );
        }
        const matched =
          partnerLiked ||
          (await this.friendService.areFriends(user.userId, partnerId));

        // Step 7: Trả { liked, matched, friendUserId } về controller
        return {
          liked: true,
          matched,
          friendUserId: matched ? partnerId : null,
        };
      });
    } catch (err) {

      // Step 8: Dính unique: coi như đã thích, đọc lại trạng thái bạn bè
      // Nếu unique vẫn bị đụng (chốt chặn cuối) thì coi như lượt thích đã được ghi: đọc lại trạng
      // thái bạn bè rồi trả như bình thường, không báo lỗi.
      if (!isUniqueViolation(err)) throw err;
      const call = await this.getCall(user, callId);
      const partnerId =
        call.userAId === user.userId ? call.userBId : call.userAId;
      const matched = await this.friendService.areFriends(
        user.userId,
        partnerId,
      );
      return {
        liked: true,
        matched,
        friendUserId: matched ? partnerId : null,
      };
    }
  }
}

// [POSTGRES] CREATE TABLE voice_match_reactions  ·  1756000000000-voice-match-completion.ts L16–24
await queryRunner.query(`
  CREATE TABLE voice_match_reactions (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    call_id      uuid NOT NULL REFERENCES call_sessions(id),
    rater_user_id uuid NOT NULL REFERENCES users(id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_voice_match_reactions_call_rater UNIQUE (call_id, rater_user_id)
  );
`);

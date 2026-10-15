// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/mini-game/sessions/:id/moves
//  Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận nước đi của user, giao cho service, trả trạng thái ván theo góc
//                 nhìn của người nộp (chưa resolved thì không lộ move đối phương).
//    [SERVICE]    Kiểm user thuộc ván, xác định cột move của người nộp, nộp bằng UPDATE
//                 có điều kiện, đủ 2 move thì tính người thắng; chỉ publish sau commit.
//    [POSTGRES]   UPDATE có điều kiện là chốt chặn duy nhất (move chưa nộp và ván còn
//                 chờ), đồng thời khóa dòng ván giữa 2 bên nộp cùng lúc; xóa dòng
//                 participant khi ván kết thúc.
//    [REDIS]      Pub/sub báo kết quả (kèm cả 2 move) cho 2 người; không có event trung
//                 gian khi mới 1 bên nộp.
//
//  Bảng       mini_game_sessions — ván của cặp user (low/high theo thứ tự canonical):
//               low_move, high_move, status waiting_moves hoặc resolved, winner_user_id
//               (null nếu hòa).
//             mini_game_active_participants — bảng phụ chặn mỗi user chỉ có 1 ván đang
//               chờ move; luồng này xóa 2 dòng khi ván resolved.
//  Redis      realtime:user:{userId} — publish minigame.session.resolved (kèm lowMove,
//               highMove, winnerUserId) cho từng người trong ván.
//  Liên quan  signaling-gateway — subscribe pub/sub và đẩy minigame.session.resolved
//               xuống socket của 2 người.
//             MiniGameSessionDto — chỉ trả opponentMove khi ván đã resolved; trước đó
//               chỉ có cờ opponentHasMoved (không chép DTO).
//             GET /mini-game/sessions/:id — poll dự phòng khi realtime rớt (chưa chép).
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] MiniGameController.submitMove  ·  mini-game.controller.ts L30–31, L53–71
@Controller('mini-game')
export class MiniGameController {
  constructor(private readonly miniGameService: MiniGameService) {}

  // Step 1: Nhận request (user từ JWT, id ván từ URL, nước đi trong body)
  @Post('sessions/:id/moves')
  @HttpCode(HttpStatus.OK)
  async submitMove(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: SubmitMiniGameMoveDto,
  ): Promise<MiniGameSessionDto> {
    const session = await this.miniGameService.submitMove(
      user.userId,
      id,
      dto.move,
    );

      // [SERVICE] MiniGameService.submitMove  ·  mini-game.service.ts L144–238
      private readonly logger = new Logger(MiniGameService.name);
      constructor(
        @InjectDataSource() private readonly dataSource: DataSource,
        @Inject(MINI_GAME_REDIS) private readonly redis: Redis,
      ) {}

      const userId = user.userId;
      const sessionId = id;
      const move = dto.move;

      // Step 2: Kiểm tra user thuộc ván (không thì 404)
      // Gộp 404 cho "không tồn tại" và "không phải thành viên ván": không lộ ván của người khác.
      const session = await this.getSessionForParticipant(userId, sessionId);

      // Step 3: Xác định cột move của người nộp (low hay high)
      const moveColumn: MoveColumn =
        session.userLowId === userId ? 'lowMove' : 'highMove';
      const moveColumnSql = moveColumn === 'lowMove' ? 'low_move' : 'high_move';

      const { session: updatedSession, resolved } =
        await this.dataSource.transaction(async (manager) => {

          // Step 4: Nộp move bằng UPDATE có điều kiện trong transaction (0 dòng: 409)
          // [POSTGRES] Chốt chặn DUY NHẤT là UPDATE có điều kiện (move chưa nộp VÀ ván còn
          // waiting_moves), không phải check-rồi-ghi. UPDATE cũng khóa dòng ván: 2 bên nộp cùng
          // lúc bị xếp hàng, bên sau thấy đủ 2 move nên chỉ 1 bên resolve.
          const updateResult = await manager
            .createQueryBuilder()
            .update(MiniGameSession)
            .set({ [moveColumn]: move } as Partial<MiniGameSession>)
            .where(`id = :id AND ${moveColumnSql} IS NULL AND status = :status`, {
              id: sessionId,
              status: MiniGameSessionStatus.WaitingMoves,
            })
            .execute();

          // 0 dòng bị ảnh hưởng = đã nộp rồi hoặc ván không còn chờ: trả 409, move đã nộp không
          // đổi lại được.
          if (!updateResult.affected) {
            throw new DomainException(
              MiniGameErrors.MOVE_ALREADY_SUBMITTED,
              'Move đã được nộp hoặc ván không còn chờ move',
              HttpStatus.CONFLICT,
            );
          }

          // Step 5: Đọc lại ván: mới 1 bên nộp thì trả về, chưa resolve
          const reloaded = await manager
            .getRepository(MiniGameSession)
            .findOneByOrFail({ id: sessionId });

          // Mới 1 bên nộp: KHÔNG resolve, KHÔNG publish gì, để không lộ trạng thái đối phương cho
          // ai, kể cả người vừa nộp.
          if (!reloaded.lowMove || !reloaded.highMove) {
            return { session: reloaded, resolved: null };
          }

          // Step 6: Đủ 2 move: tính người thắng
          // Luật thắng thua là hàm thuần tách riêng khỏi service, để thêm game thứ 2 không phải
          // nhét if/else theo gameType vào service chung.
          const outcome = resolveWinner(reloaded.lowMove, reloaded.highMove);

            // [SERVICE] resolveWinner  ·  mini-game.constants.ts L23–35
            const lowMove = reloaded.lowMove;
            const highMove = reloaded.highMove;
            if (lowMove === highMove) return 'draw';

            const beats: Record<RockPaperScissorsMove, RockPaperScissorsMove> = {
              [RockPaperScissorsMove.Rock]: RockPaperScissorsMove.Scissors,
              [RockPaperScissorsMove.Scissors]: RockPaperScissorsMove.Paper,
              [RockPaperScissorsMove.Paper]: RockPaperScissorsMove.Rock,
            };
            return beats[lowMove] === highMove ? 'low' : 'high';

          const winnerUserId =
            outcome === 'draw'
              ? null
              : outcome === 'low'
                ? reloaded.userLowId
                : reloaded.userHighId;
          const resolvedAt = new Date();

          // Step 7: Ghi kết quả ván và nhả slot participant của cả 2 người
          // Update theo id, không cần điều kiện status: dòng đã bị khóa từ câu UPDATE đầu
          // transaction nên không ai chen vào được.
          await manager.update(
            MiniGameSession,
            { id: sessionId },
            {
              status: MiniGameSessionStatus.Resolved,
              winnerUserId,
              resolvedAt,
            },
          );
          // Nhả slot "mỗi user 1 ván đang chờ" của cả 2 người, cùng transaction với việc đổi
          // status.
          await manager.delete(MiniGameActiveParticipant, { sessionId });

          const resolvedSession: MiniGameSession = {
            ...reloaded,
            status: MiniGameSessionStatus.Resolved,
            winnerUserId,
            resolvedAt,
          };
          return {
            session: resolvedSession,
            resolved: {
              lowMove: reloaded.lowMove,
              highMove: reloaded.highMove,
              winnerUserId,
            },
          };
        });

      // Step 8: Sau commit, báo minigame.session.resolved qua Redis cho cả 2
      // [REDIS] Chỉ publish khi đã resolve, SAU commit, kèm cả 2 move cho cả 2 người. Không có
      // event "1 bên đã nộp" để bên kia không suy ra ai nộp trước.
      if (resolved) {
        const publishData: MiniGameSessionResolvedEventData = {
          sessionId,
          lowMove: resolved.lowMove,
          highMove: resolved.highMove,
          winnerUserId: resolved.winnerUserId,
        };
        const envelope: RealtimeEnvelope<MiniGameSessionResolvedEventData> = {
          event: RealtimeEvents.MiniGameSessionResolved,
          data: publishData,
        };
        await Promise.all(
          [session.userLowId, session.userHighId].map((uid) =>
            publishRealtimeEvent(this.redis, this.logger, uid, envelope),
          ),
        );
      }

      return updatedSession;

    // Step 9: Controller trả ván theo góc nhìn của người nộp
    return MiniGameSessionDto.from(session, user.userId);
  }
}

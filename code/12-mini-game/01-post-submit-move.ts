//  POST /api/v1/mini-game/sessions/:id/moves
//  Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai.
//  Step 1: Nhận request (user từ JWT, id ván từ URL, nước đi trong body)
//  Step 2: Kiểm tra user thuộc ván (không thì 404)
//  Step 3: Xác định cột move của người nộp (low hay high)
//  Step 4: Nộp move bằng UPDATE có điều kiện trong transaction (0 dòng: 409)
//  Step 5: Đọc lại ván: mới 1 bên nộp thì trả về, chưa resolve
//  Step 6: Đủ 2 move: tính người thắng
//  Step 7: Ghi kết quả ván và nhả slot participant của cả 2 người
//  Step 8: Sau commit, báo minigame.session.resolved qua Redis cho cả 2
//  Step 9: Controller trả ván theo góc nhìn của người nộp

// [CONTROLLER] MiniGameController.submitMove  ·  mini-game.controller.ts L30–31, L53–71
@Controller('mini-game')
export class MiniGameController {
  // MiniGameService
  private readonly logger = new Logger(MiniGameService.name);
  constructor(
    private readonly miniGameService: MiniGameService,
    // MiniGameService
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(MINI_GAME_REDIS) private readonly redis: Redis,
  ) {}

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
        // waiting_moves), không phải check-rồi-ghi. UPDATE cũng khóa dòng ván: 2 bên nộp cùng lúc
        // bị xếp hàng, bên sau thấy đủ 2 move nên chỉ 1 bên resolve.
        const updateResult = await manager
          .createQueryBuilder()
          .update(MiniGameSession)
          .set({ [moveColumn]: move } as Partial<MiniGameSession>)
          .where(`id = :id AND ${moveColumnSql} IS NULL AND status = :status`, {
            id: sessionId,
            status: MiniGameSessionStatus.WaitingMoves,
          })
          .execute();

        // 0 dòng bị ảnh hưởng = đã nộp rồi hoặc ván không còn chờ: trả 409, move đã nộp không đổi
        // lại được.
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

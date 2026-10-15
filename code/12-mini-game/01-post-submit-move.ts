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

// [CONTROLLER] mini-game.controller.ts · L53–71
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
  return MiniGameSessionDto.from(session, user.userId);
}

// [SERVICE] mini-game.service.ts · L144–238
async submitMove(
  userId: string,
  sessionId: string,
  move: RockPaperScissorsMove,
): Promise<MiniGameSession> {
  const session = await this.getSessionForParticipant(userId, sessionId);
  const moveColumn: MoveColumn =
    session.userLowId === userId ? 'lowMove' : 'highMove';
  const moveColumnSql = moveColumn === 'lowMove' ? 'low_move' : 'high_move';

  const { session: updatedSession, resolved } =
    await this.dataSource.transaction(async (manager) => {
      // [POSTGRES] Chốt chặn DUY NHẤT là UPDATE có điều kiện (move chưa nộp VÀ ván còn
      // waiting_moves), không phải check-rồi-ghi. UPDATE cũng khóa dòng ván: 2 bên nộp
      // cùng lúc bị xếp hàng, bên sau thấy đủ 2 move nên chỉ 1 bên resolve.
      const updateResult = await manager
        .createQueryBuilder()
        .update(MiniGameSession)
        .set({ [moveColumn]: move } as Partial<MiniGameSession>)
        .where(`id = :id AND ${moveColumnSql} IS NULL AND status = :status`, {
          id: sessionId,
          status: MiniGameSessionStatus.WaitingMoves,
        })
        .execute();

      // 0 dòng bị ảnh hưởng = đã nộp rồi hoặc ván không còn chờ: trả 409, move đã nộp
      // không đổi lại được.
      if (!updateResult.affected) {
        throw new DomainException(
          MiniGameErrors.MOVE_ALREADY_SUBMITTED,
          'Move đã được nộp hoặc ván không còn chờ move',
          HttpStatus.CONFLICT,
        );
      }

      const reloaded = await manager
        .getRepository(MiniGameSession)
        .findOneByOrFail({ id: sessionId });

      // Mới 1 bên nộp: KHÔNG resolve, KHÔNG publish gì, để không lộ trạng thái đối
      // phương cho ai, kể cả người vừa nộp.
      if (!reloaded.lowMove || !reloaded.highMove) {
        return { session: reloaded, resolved: null };
      }

      // Luật thắng thua là hàm thuần tách riêng khỏi service, để thêm game thứ 2 không
      // phải nhét if/else theo gameType vào service chung.
      const outcome = resolveWinner(reloaded.lowMove, reloaded.highMove);
      const winnerUserId =
        outcome === 'draw'
          ? null
          : outcome === 'low'
            ? reloaded.userLowId
            : reloaded.userHighId;
      const resolvedAt = new Date();

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
      // Nhả slot "mỗi user 1 ván đang chờ" của cả 2 người, cùng transaction với việc
      // đổi status.
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

  // [REDIS] Chỉ publish khi đã resolve, SAU commit, kèm cả 2 move cho cả 2 người. Không
  // có event "1 bên đã nộp" để bên kia không suy ra ai nộp trước.
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
}

// [SERVICE] mini-game.service.ts · L309–325
private async getSessionForParticipant(
  userId: string,
  sessionId: string,
): Promise<MiniGameSession> {
  const session = await this.sessionRepo.findOneBy({ id: sessionId });
  if (
    !session ||
    (session.userLowId !== userId && session.userHighId !== userId)
  ) {
    // Gộp 404 cho "không tồn tại" và "không phải thành viên ván": không lộ ván của
    // người khác.
    throw new DomainException(
      MiniGameErrors.NOT_FOUND,
      'Không tìm thấy ván chơi',
      HttpStatus.NOT_FOUND,
    );
  }
  return session;
}

// [SERVICE] mini-game.constants.ts · L23–35
export function resolveWinner(
  lowMove: RockPaperScissorsMove,
  highMove: RockPaperScissorsMove,
): RockPaperScissorsOutcome {
  if (lowMove === highMove) return 'draw';

  const beats: Record<RockPaperScissorsMove, RockPaperScissorsMove> = {
    [RockPaperScissorsMove.Rock]: RockPaperScissorsMove.Scissors,
    [RockPaperScissorsMove.Scissors]: RockPaperScissorsMove.Paper,
    [RockPaperScissorsMove.Paper]: RockPaperScissorsMove.Rock,
  };
  return beats[lowMove] === highMove ? 'low' : 'high';
}

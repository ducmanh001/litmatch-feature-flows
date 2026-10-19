//  POST /api/v1/palm-match/queue
//  Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match.
//  Step 1: Nhận request (user lấy từ JWT)
//  Step 2: Khoá advisory THEO USER (không toàn cục): chỉ tuần tự hoá các lượt join của chính user này
//  Step 3: Đóng session đã quá hạn của chính user (hết hạn kiểu lười)
//  Step 4: Còn session chưa dismiss thì trả lại state đó, không xếp hàng
//  Step 5: Dọn tối đa 50 entry chờ quá hạn (SKIP LOCKED, không chờ ai)
//  Step 6: Xếp user vào hàng đợi (INSERT ... ON CONFLICT DO NOTHING, PK user_id)
//  Step 7: Khoá hàng của chính mình để không ai giành được mình từ đây
//  Step 8: Kiểm lại active: có thể một user khác đã ghép mình giữa lúc kiểm tra và lúc chèn
//  Step 9: Đọc entry của mình (nếu đã bị giành thì dựng state từ bảng active)
//  Step 10: Giành người chờ CŨ HƠN mình, cũ nhất còn rảnh, mà không bao giờ chờ khoá
//  Step 11: Kiểm lại block/report đúng lúc ghép
//  Step 12: Xoá entry của mình để chốt lượt ghép (nếu mình đang bị giành thì DELETE chờ rồi thấy hàng đã mất)
//  Step 13: Có cặp: tạo session snapshot (seed tất định)
//  Step 14: Rút đối tác khỏi hàng đợi và ghi 2 participant (PK user_id là chốt cuối)
//  Step 15: Trả trạng thái đã ghép theo góc nhìn của user

// [CONTROLLER] PalmMatchController.joinQueue · palm-match/palm-match.controller.ts
@Controller('palm-match')
export class PalmMatchController {
  constructor(
    private readonly palmMatchService: PalmMatchService,
    // PalmMatchService
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly dataSource: DataSource,
    private readonly safetyService: SafetyService,
  ) {}

  // Step 1: Nhận request (user lấy từ JWT)
  @Post('queue')
  @HttpCode(HttpStatus.OK)
  async joinQueue(
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<PalmMatchStateDto> {
    return this.palmMatchService.joinQueue(user.userId);

    // [SERVICE] PalmMatchService.joinQueue · palm-match/palm-match.service.ts
    const userId = user.userId;
    return this.dataSource.transaction(async (manager) => {

      // Step 2: Khoá advisory THEO USER (không toàn cục): chỉ tuần tự hoá các lượt join của chính
      // user này
      await lockAdvisoryXact(manager, 'palm-match-join', userId);

      // [SERVICE] lockAdvisoryXact · database/advisory-lock.ts
      const scope = 'palm-match-join';
      const id = userId;
      await manager.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `${scope}:${id}`,
      ]);

      // Step 3: Đóng session đã quá hạn của chính user (hết hạn kiểu lười)
      // Hết hạn kiểu lười: module không có job quét, mỗi lần user vào hoặc hỏi trạng thái mới
      // đóng session đã quá deadline của chính họ.
      await this.expireActiveForUser(manager, userId);

      // Step 4: Còn session chưa dismiss thì trả lại state đó, không xếp hàng
      const activeState = await this.activeState(manager, userId);
      if (activeState) return activeState;

      // Step 5: Dọn tối đa 50 entry chờ quá hạn (SKIP LOCKED, không chờ ai)
      await this.purgeStaleEntries(manager);

      // [SERVICE] PalmMatchService.purgeStaleEntries
      const maxWaitSeconds = this.config.getOrThrow(
        'PALM_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      await manager.query(
        `DELETE FROM palm_match_queue_entries
          WHERE user_id IN (
            SELECT user_id FROM palm_match_queue_entries
             WHERE queued_at < $1
             ORDER BY queued_at ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
          )`,
        [new Date(Date.now() - maxWaitSeconds * 1000), STALE_PURGE_BATCH],
      );

      // Step 6: Xếp user vào hàng đợi (INSERT ... ON CONFLICT DO NOTHING, PK user_id)
      // PK user_id làm enqueue idempotent: 2 tab hoặc retry không xếp hàng 2 lần.
      await manager
        .createQueryBuilder()
        .insert()
        .into(PalmMatchQueueEntry)
        .values({ userId })
        .orIgnore()
        .execute();

      // Step 7: Khoá hàng của chính mình để không ai giành được mình từ đây
      await manager.query(
        `SELECT 1 FROM palm_match_queue_entries WHERE user_id = $1 FOR UPDATE`,
        [userId],
      );

      // Step 8: Kiểm lại active: có thể một user khác đã ghép mình giữa lúc kiểm tra và lúc chèn
      // [POSTGRES] Khoá advisory theo user KHÔNG chặn một user KHÁC giành entry của mình: nếu D
      // ghép mình ngay sau lần kiểm tra active đầu và commit, lượt join này sẽ chèn LẠI entry và
      // trả queued cho user đã active, để lại một hàng queue thừa.
      // Khoá hàng của chính mình (dòng trên) rồi kiểm lại active ở ranh giới ghi: nếu đã bị ghép
      // thì dọn entry vừa chèn và trả đúng phiên.
      const claimedMeanwhile = await this.activeState(manager, userId);
      if (claimedMeanwhile) {
        await manager.delete(PalmMatchQueueEntry, { userId });
        return claimedMeanwhile;
      }
      return this.tryMatch(manager, userId);

      // [SERVICE] PalmMatchService.tryMatch
      // Step 9: Đọc entry của mình (nếu đã bị giành thì dựng state từ bảng active)
      const own = (await manager.query(
        `SELECT queued_at FROM palm_match_queue_entries WHERE user_id = $1`,
        [userId],
      )) as Array<{ queued_at: Date }>;
      const ownEntry = own[0];
      if (ownEntry === undefined) {
        return (
          (await this.activeState(manager, userId)) ?? {
            state: PalmMatchClientState.Idle,
          }
        );
      }

      const maxWaitSeconds = this.config.getOrThrow(
        'PALM_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      const cutoff = new Date(Date.now() - maxWaitSeconds * 1000);
      const skipped: string[] = [];
      let partnerId: string | undefined;
      for (let i = 0; i < MATCH_MAX_CANDIDATES; i += 1) {

        // Step 10: Giành người chờ CŨ HƠN mình, cũ nhất còn rảnh, mà không bao giờ chờ khoá
        // [POSTGRES] "Người MỚI giành người CŨ": chỉ xét entry cũ hơn mình theo (queued_at,
        // user_id), so sánh ngay trong SQL (đủ độ chính xác micro giây; qua JS Date sẽ cắt về
        // mili giây và hai bên có thể cùng coi nhau là không cũ hơn).
        // SKIP LOCKED nên không bao giờ chờ (không deadlock) và các lượt join của user khác nhau
        // chạy song song thay vì xếp hàng sau một khoá toàn cục.
        const rows = (await manager.query(
          `SELECT c.user_id FROM palm_match_queue_entries c
            WHERE (c.queued_at, c.user_id) < (
                    SELECT o.queued_at, o.user_id FROM palm_match_queue_entries o WHERE o.user_id = $1
                  )
              AND c.queued_at >= $2 AND c.user_id <> ALL($3::uuid[])
            ORDER BY c.queued_at ASC, c.user_id ASC
            LIMIT 1
            FOR UPDATE OF c SKIP LOCKED`,
          [userId, cutoff, skipped],
        )) as Array<{ user_id: string }>;
        const candidateId = rows[0]?.user_id;
        if (candidateId === undefined) break;

        const candidateActive = await manager.findOneBy(
          PalmMatchActiveParticipant,
          { userId: candidateId },
        );
        if (candidateActive) {
          await manager.delete(PalmMatchQueueEntry, { userId: candidateId });
          skipped.push(candidateId);
          continue;
        }

        // Step 11: Kiểm lại block/report đúng lúc ghép
        // Kiểm lại block/report ĐÚNG LÚC ghép, trên cùng transaction; không tin state ở thời điểm
        // vào hàng đợi.
        if (await this.safetyService.canPair(userId, candidateId, manager)) {
          partnerId = candidateId;
          break;
        }
        skipped.push(candidateId);
      }

      if (partnerId === undefined) {
        return {
          state: PalmMatchClientState.Queued,
          queuedAt: ownEntry.queued_at.toISOString(),
        };
      }

      // Step 12: Xoá entry của mình để chốt lượt ghép (nếu mình đang bị giành thì DELETE chờ rồi
      // thấy hàng đã mất)
      // Xoá entry của mình để "chốt" lượt ghép. Nếu một lượt mới hơn đang giành mình thì DELETE
      // chờ nó commit rồi không còn hàng → bỏ lượt này (nhả đối tác) và dựng lại state từ bảng
      // active.
      const claimed = await manager.delete(PalmMatchQueueEntry, { userId });
      if (!claimed.affected) {
        return (
          (await this.activeState(manager, userId)) ?? {
            state: PalmMatchClientState.Idle,
          }
        );
      }

      // Step 13: Có cặp: tạo session snapshot (seed tất định)
      const session = await this.createSessionSnapshot(
        manager,
        userId,
        partnerId,
      );

      // [SERVICE] PalmMatchService.createSessionSnapshot
      const userAId = userId;
      const userBId = partnerId;
      const templates = await manager.find(PalmReadingTemplate, {
        where: { category: PalmMatchCategory.Love, isActive: true },
        order: { id: 'ASC' },
      });
      if (templates.length === 0) {
        throw new DomainException(
          PalmMatchErrors.CATEGORY_EMPTY,
          'Không có nội dung Palm Match đang hoạt động',
          HttpStatus.CONFLICT,
        );
      }

      const id = randomUUID();
      const [userLowId, userHighId] = canonicalPair(userAId, userBId);
      const lowSeed = fnv1aHash(`${id}:${userLowId}:sign`);
      const highSeed = fnv1aHash(`${id}:${userHighId}:sign`);
      const resultSeed = fnv1aHash(`${id}:${userLowId}:${userHighId}:result`);
      const durationSeconds = this.config.getOrThrow(
        'PALM_MATCH_SESSION_DURATION_SECONDS',
        { infer: true },
      );
      const template = templates[resultSeed % templates.length];

      return manager.save(
        manager.create(PalmMatchSession, {
          id,
          userLowId,
          userHighId,
          lowSign: PALM_ZODIAC_SIGNS[lowSeed % PALM_ZODIAC_SIGNS.length].key,
          highSign: PALM_ZODIAC_SIGNS[highSeed % PALM_ZODIAC_SIGNS.length].key,
          compatibilityPercent: 60 + (resultSeed % 40),
          fortune: template.content.split('{name}').join('Người ấy'),
          lowFlippedAt: null,
          highFlippedAt: null,
          lowRating: null,
          highRating: null,
          status: PalmMatchSessionStatus.Active,
          outcome: null,
          expiresAt: new Date(Date.now() + durationSeconds * 1000),
          closedAt: null,
        }),
      );

      await manager.delete(PalmMatchQueueEntry, { userId: partnerId });

      // Step 14: Rút đối tác khỏi hàng đợi và ghi 2 participant (PK user_id là chốt cuối)
      await manager.insert(PalmMatchActiveParticipant, [
        { userId, sessionId: session.id },
        { userId: partnerId, sessionId: session.id },
      ]);

      // Step 15: Trả trạng thái đã ghép theo góc nhìn của user
      return this.sessionView(session, userId);
    });
  }
}

// [POSTGRES] CREATE TABLE palm_match_queue_entries · database/migrations/1755500000000-palm-match-session.ts
await queryRunner.query(`
  CREATE TABLE palm_match_queue_entries (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    queued_at timestamptz NOT NULL DEFAULT now()
  )
`);

await queryRunner.query(`
  CREATE INDEX idx_palm_match_queue_order ON palm_match_queue_entries (queued_at, user_id)
`);

await queryRunner.query(`
  CREATE TABLE palm_match_active_participants (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    session_id uuid NOT NULL REFERENCES palm_match_sessions(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
  )
`);

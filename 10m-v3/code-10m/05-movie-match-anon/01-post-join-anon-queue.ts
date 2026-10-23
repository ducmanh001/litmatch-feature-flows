//  POST /api/v1/movie-match/anon/queue
//  Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành một phiên xem chung với video do server chọn.
//  Step 1: Nhận request (user lấy từ JWT); server tự chọn video, client không gửi URL
//  Step 2: Khoá advisory THEO USER (không toàn cục): chỉ tuần tự hoá các lượt join của chính user này
//  Step 3: Đóng phiên đã quá hạn của chính user (hết hạn kiểu lười)
//  Step 4: Còn phiên chưa kết thúc thì trả lại state đó, không xếp hàng
//  Step 5: Dọn tối đa 50 entry chờ quá hạn (SKIP LOCKED, không chờ ai)
//  Step 6: Xếp user vào hàng đợi (INSERT ... ON CONFLICT DO NOTHING, PK user_id)
//  Step 7: Khoá hàng của chính mình để không ai giành được mình từ đây
//  Step 8: Kiểm lại active: có thể một user khác đã ghép mình giữa lúc kiểm tra và lúc chèn
//  Step 9: Đọc entry của mình (nếu đã bị giành thì dựng state từ bảng active)
//  Step 10: Giành người chờ CŨ HƠN mình, cũ nhất còn rảnh, mà không bao giờ chờ khoá
//  Step 11: Ứng viên đã active ở phiên khác thì dọn entry thừa của nó và xét người kế (tối đa 20 ứng viên)
//  Step 12: Kiểm lại block/report đúng lúc ghép
//  Step 13: Xoá entry của mình để chốt lượt ghép (nếu mình đang bị giành thì DELETE chờ rồi thấy hàng đã mất)
//  Step 14: Có cặp: tạo phiên ẩn danh, video do server chọn tất định theo id phiên
//  Step 15: Rút đối tác khỏi hàng đợi và ghi 2 participant (PK user_id là chốt cuối)
//  Step 16: Trả trạng thái đã ghép theo góc nhìn của user (không lộ đối tác)

// [CONTROLLER] MovieMatchController.joinAnonQueue · movie-match/movie-match.controller.ts
@Controller('movie-match')
export class MovieMatchController {
  constructor(
    private readonly movieMatchService: MovieMatchService,
    // MovieMatchService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly safetyService: SafetyService,
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Nhận request (user lấy từ JWT); server tự chọn video, client không gửi URL
  @Post('anon/queue')
  @HttpCode(HttpStatus.OK)
  async joinAnonQueue(
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<MovieAnonStateDto> {
    return MovieAnonStateDto.from(
      await this.movieMatchService.joinAnonQueue(user.userId),
    );

    // [SERVICE] MovieMatchService.joinAnonQueue · movie-match/movie-match.service.ts
    const userId = user.userId;
    return this.dataSource.transaction(async (manager) => {

      // Step 2: Khoá advisory THEO USER (không toàn cục): chỉ tuần tự hoá các lượt join của chính
      // user này
      await lockAdvisoryXact(manager, 'movie-match-join', userId);

      // [SERVICE] lockAdvisoryXact · database/advisory-lock.ts
      const scope = 'movie-match-join';
      const id = userId;
      await manager.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `${scope}:${id}`,
      ]);

      // Step 3: Đóng phiên đã quá hạn của chính user (hết hạn kiểu lười)
      // Hết hạn kiểu lười: không có job quét phiên, mỗi lần user vào hoặc hỏi trạng thái mới đóng
      // phiên đã quá deadline của chính họ.
      await this.expireAnonForUser(manager, userId);

      // Step 4: Còn phiên chưa kết thúc thì trả lại state đó, không xếp hàng
      const activeState = await this.anonActiveState(manager, userId);
      if (activeState) return activeState;

      // Step 5: Dọn tối đa 50 entry chờ quá hạn (SKIP LOCKED, không chờ ai)
      await this.purgeStaleAnonEntries(manager);

      // [SERVICE] MovieMatchService.purgeStaleAnonEntries
      const maxWaitSeconds = this.config.getOrThrow(
        'MOVIE_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      await manager.query(
        `DELETE FROM movie_match_queue_entries
          WHERE user_id IN (
            SELECT user_id FROM movie_match_queue_entries
             WHERE queued_at < $1
             ORDER BY queued_at ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
          )`,
        [new Date(Date.now() - maxWaitSeconds * 1000), ANON_STALE_PURGE_BATCH],
      );

      // Step 6: Xếp user vào hàng đợi (INSERT ... ON CONFLICT DO NOTHING, PK user_id)
      // PK user_id làm enqueue idempotent: bấm đúp, 2 tab hoặc retry không xếp hàng 2 lần.
      await manager
        .createQueryBuilder()
        .insert()
        .into(MovieMatchQueueEntry)
        .values({ userId })
        .orIgnore()
        .execute();

      // Step 7: Khoá hàng của chính mình để không ai giành được mình từ đây
      await manager.query(
        `SELECT 1 FROM movie_match_queue_entries WHERE user_id = $1 FOR UPDATE`,
        [userId],
      );

      // Step 8: Kiểm lại active: có thể một user khác đã ghép mình giữa lúc kiểm tra và lúc chèn
      // [POSTGRES] Khoá advisory theo user KHÔNG chặn một user KHÁC giành entry của mình: nếu
      // người khác ghép mình ngay sau lần kiểm tra active đầu và commit, lượt join này sẽ chèn
      // LẠI entry và trả queued cho user đã active, để lại một hàng queue thừa.
      // Khoá hàng của chính mình (dòng trên) rồi kiểm lại active ở ranh giới ghi: nếu đã bị ghép
      // thì dọn entry vừa chèn và trả đúng phiên.
      const claimedMeanwhile = await this.anonActiveState(manager, userId);
      if (claimedMeanwhile) {
        await manager.delete(MovieMatchQueueEntry, { userId });
        return claimedMeanwhile;
      }
      return this.tryMatchAnon(manager, userId);

      // [SERVICE] MovieMatchService.tryMatchAnon
      // Step 9: Đọc entry của mình (nếu đã bị giành thì dựng state từ bảng active)
      const own = (await manager.query(
        `SELECT queued_at FROM movie_match_queue_entries WHERE user_id = $1`,
        [userId],
      )) as Array<{ queued_at: Date }>;
      const ownEntry = own[0];
      if (ownEntry === undefined) {
        return (
          (await this.anonActiveState(manager, userId)) ?? {
            state: MovieMatchClientState.Idle,
          }
        );
      }

      const maxWaitSeconds = this.config.getOrThrow(
        'MOVIE_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      const cutoff = new Date(Date.now() - maxWaitSeconds * 1000);
      const skipped: string[] = [];
      let partnerId: string | undefined;
      for (let i = 0; i < ANON_MATCH_MAX_CANDIDATES; i += 1) {

        // Step 10: Giành người chờ CŨ HƠN mình, cũ nhất còn rảnh, mà không bao giờ chờ khoá
        // [POSTGRES] "Người MỚI giành người CŨ": chỉ xét entry cũ hơn mình theo (queued_at,
        // user_id), so sánh ngay trong SQL (đủ độ chính xác micro giây; qua JS Date sẽ cắt về
        // mili giây và hai bên có thể cùng coi nhau là không cũ hơn).
        // SKIP LOCKED nên không bao giờ chờ (không deadlock) và các lượt join của user khác nhau
        // chạy song song thay vì xếp hàng sau một khoá toàn cục. Không khoá hàng của chính mình
        // khi tìm: nếu khoá sớm thì mọi người mới hơn đều thấy mình đang bị khoá và bỏ qua, cả
        // hàng đợi poll cùng lúc sẽ không ai ghép được.
        const rows = (await manager.query(
          `SELECT c.user_id FROM movie_match_queue_entries c
            WHERE (c.queued_at, c.user_id) < (
                    SELECT o.queued_at, o.user_id FROM movie_match_queue_entries o WHERE o.user_id = $1
                  )
              AND c.queued_at >= $2 AND c.user_id <> ALL($3::uuid[])
            ORDER BY c.queued_at ASC, c.user_id ASC
            LIMIT 1
            FOR UPDATE OF c SKIP LOCKED`,
          [userId, cutoff, skipped],
        )) as Array<{ user_id: string }>;
        const candidateId = rows[0]?.user_id;
        if (candidateId === undefined) break;

        // Step 11: Ứng viên đã active ở phiên khác thì dọn entry thừa của nó và xét người kế (tối
        // đa 20 ứng viên)
        const candidateActive = await manager.findOneBy(
          MovieSessionActiveParticipant,
          { userId: candidateId },
        );
        if (candidateActive) {
          await manager.delete(MovieMatchQueueEntry, { userId: candidateId });
          skipped.push(candidateId);
          continue;
        }

        // Step 12: Kiểm lại block/report đúng lúc ghép
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
          state: MovieMatchClientState.Queued,
          queuedAt: ownEntry.queued_at.toISOString(),
        };
      }

      // Step 13: Xoá entry của mình để chốt lượt ghép (nếu mình đang bị giành thì DELETE chờ rồi
      // thấy hàng đã mất)
      // Xoá entry của mình để "chốt" lượt ghép. Nếu một lượt mới hơn đang giành mình thì DELETE
      // chờ nó commit rồi không còn hàng → bỏ lượt này (nhả đối tác) và dựng lại state từ bảng
      // active.
      const claimed = await manager.delete(MovieMatchQueueEntry, { userId });
      if (!claimed.affected) {
        return (
          (await this.anonActiveState(manager, userId)) ?? {
            state: MovieMatchClientState.Idle,
          }
        );
      }

      // Step 14: Có cặp: tạo phiên ẩn danh, video do server chọn tất định theo id phiên
      const session = await this.createAnonSession(manager, userId, partnerId);

      // [SERVICE] MovieMatchService.createAnonSession
      const userAId = userId;
      const userBId = partnerId;
      const pool = this.config
        .getOrThrow('MOVIE_MATCH_ANON_VIDEO_URLS', { infer: true })
        .split(',')
        .map((url) => url.trim())
        .filter((url) => url.length > 0);
      if (pool.length === 0) {
        throw new DomainException(
          MovieMatchErrors.ANON_VIDEO_POOL_EMPTY,
          'Chưa cấu hình video cho Movie Match ẩn danh',
          HttpStatus.CONFLICT,
        );
      }

      const id = randomUUID();
      const [userLowId, userHighId] = canonicalPair(userAId, userBId);
      const durationSeconds = this.config.getOrThrow(
        'MOVIE_MATCH_ANON_DURATION_SECONDS',
        { infer: true },
      );
      const now = new Date();
      return manager.save(
        manager.create(MovieSession, {
          id,
          userLowId,
          userHighId,
          // Server chọn video tất định theo id phiên: client không gửi và không chọn được URL.
          videoUrl: pool[fnv1aHash(id) % pool.length],
          positionSeconds: 0,
          isPlaying: false,
          positionUpdatedAt: now,
          status: MovieSessionStatus.Active,
          mode: MovieSessionMode.Anonymous,
          expiresAt: new Date(now.getTime() + durationSeconds * 1000),
          watchEndedAt: null,
          lowRating: null,
          highRating: null,
          outcome: null,
        }),
      );

      // Step 15: Rút đối tác khỏi hàng đợi và ghi 2 participant (PK user_id là chốt cuối)
      await manager.delete(MovieMatchQueueEntry, { userId: partnerId });
      await manager.insert(MovieSessionActiveParticipant, [
        { userId, sessionId: session.id },
        { userId: partnerId, sessionId: session.id },
      ]);

      // Step 16: Trả trạng thái đã ghép theo góc nhìn của user (không lộ đối tác)
      return this.anonView(session, userId);
    });
  }
}

// [POSTGRES] CREATE TABLE movie_match_queue_entries · database/migrations/1755900000000-movie-match-anon.ts
await queryRunner.query(`
  CREATE TABLE movie_match_queue_entries (
    user_id uuid PRIMARY KEY,
    queued_at timestamptz NOT NULL DEFAULT now()
  )
`);

// [POSTGRES] CREATE INDEX IF NOT EXISTS idx_movie_match_queue_order · database/migrations/1758100000000-movie-match-queue-order-index.ts
await queryRunner.query(`
  CREATE INDEX IF NOT EXISTS idx_movie_match_queue_order
  ON movie_match_queue_entries (queued_at, user_id)
`);

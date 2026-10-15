// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/palm-match/queue
//  Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận request vào hàng đợi, giao cho service và trả trạng thái hiện
//                 tại (đang chờ hoặc đã ghép).
//    [SERVICE]    Trong 1 transaction: lấy khóa ghép, đóng session quá hạn của user,
//                 xếp user vào hàng đợi, duyệt người chờ lâu nhất, kiểm block/report,
//                 tạo session snapshot.
//    [POSTGRES]   Hàng đợi là bảng (PK user_id); khóa advisory theo transaction tuần tự
//                 hóa việc chọn cặp; PK bảng participant là chốt cuối cho "1 user 1
//                 session".
//
//  Bảng       palm_match_queue_entries — hàng đợi: mỗi user đang chờ 1 dòng (PK
//               user_id), sắp theo queued_at; bị xóa khi quá hạn chờ hoặc khi được
//               ghép.
//             palm_match_active_participants — mỗi user 1 dòng trỏ tới session chưa
//               dismiss (PK user_id); ghi 2 dòng khi ghép.
//             palm_match_sessions — snapshot 1 lượt ghép: cặp (low, high), cung hoàng
//               đạo mỗi bên, % hợp, lời bói, deadline expires_at.
//             palm_reading_templates — đọc các mẫu lời bói category love đang hoạt động
//               để chọn lời bói cho cặp.
//  Liên quan  Safety — canPair: hai người đã block hoặc report nhau gần đây thì không
//               ghép (không chép).
//             Movie Match ẩn danh — dùng cùng kỹ thuật hàng đợi + khóa advisory, mỗi
//               bên một khóa riêng (không chép).
//             flip / rating — bước sau: mỗi bên lật lá, rồi like hoặc skip; mutual-like
//               tạo Friendship (chưa chép).
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             chép ngay tại dòng gọi nó, ngang cấp (chỉ khối { } của chính code mới
//             thụt vào), không chép class hay chữ ký hàm (tên hàm nằm ở dòng tiêu đề //
//             [TẦNG]). Constructor gom hết ở đầu, dưới class của điểm vào, chỉ gồm các
//             dependency mà code bên dưới dùng (this.xxx); các dòng const là tham số
//             truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một biểu thức
//             được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên văn trong
//             repo.

// [CONTROLLER] PalmMatchController.joinQueue  ·  palm-match.controller.ts L33–34, L54–64
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

    // [SERVICE] PalmMatchService.joinQueue  ·  palm-match.service.ts L114–186
    const userId = user.userId;
    return this.dataSource.transaction(async (manager) => {

      // Step 2: Trong transaction, lấy khóa advisory để chọn cặp tuần tự
      // [POSTGRES] Khóa advisory theo transaction tuần tự hóa phần chọn cặp: không có 2 người
      // cùng ghép với 1 ứng viên. Đánh đổi: 1 khóa toàn cục nên ghép tuần tự trên cả hệ thống, đủ
      // cho tải pilot; khi cần thì chuyển hàng đợi sang Redis shard như Matching.
      await this.lockMatcher(manager);

      // [SERVICE] PalmMatchService.lockMatcher  ·  palm-match.service.ts L328–332
      await manager.query('SELECT pg_advisory_xact_lock(hashtext($1))', [
        PALM_MATCH_ADVISORY_LOCK_KEY,
      ]);

      // Step 3: Đóng session đã quá hạn của chính user (hết hạn kiểu lười)
      // Hết hạn kiểu lười: module không có job quét, mỗi lần user vào hoặc hỏi trạng thái mới
      // đóng session đã quá deadline của chính họ.
      await this.expireActiveForUser(manager, userId);

      // Step 4: Còn session chưa dismiss thì trả lại state đó, không xếp hàng
      // Còn trỏ tới 1 session (đang chơi, hoặc đã xong nhưng chưa dismiss) thì trả lại state đó,
      // không xếp hàng thêm.
      const activeState = await this.activeState(manager, userId);
      if (activeState) return activeState;

      const queueMaxWaitSeconds = this.config.getOrThrow(
        'PALM_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      const queueCutoff = new Date(Date.now() - queueMaxWaitSeconds * 1000);

      // Step 5: Xóa entry chờ quá hạn, rồi xếp user vào hàng đợi (PK user_id)
      // Xóa entry chờ quá hạn ngay lúc ghép, để không ghép với người đã bỏ đi.
      await manager.query(
        'DELETE FROM palm_match_queue_entries WHERE queued_at < $1',
        [queueCutoff],
      );
      // INSERT ... ON CONFLICT DO NOTHING: PK user_id làm enqueue idempotent, 2 tab hoặc retry
      // không xếp hàng 2 lần.
      await manager
        .createQueryBuilder()
        .insert()
        .into(PalmMatchQueueEntry)
        .values({ userId })
        .orIgnore()
        .execute();

      // Step 6: Lấy tối đa 20 người chờ lâu nhất (FIFO)
      // FIFO: người chờ lâu nhất trước, user_id làm tie-break cho thứ tự ổn định; chỉ xét tối đa
      // 20 người.
      const candidates = (await manager.query(
        `SELECT user_id FROM palm_match_queue_entries
         WHERE user_id <> $1 ORDER BY queued_at ASC, user_id ASC LIMIT 20`,
        [userId],
      )) as Array<{ user_id: string }>;

      let partnerId: string | undefined;

      // Step 7: Duyệt ứng viên: bỏ người đã có session, kiểm block/report
      for (const candidate of candidates) {
        const candidateActive = await manager.findOneBy(
          PalmMatchActiveParticipant,
          { userId: candidate.user_id },
        );
        // Ứng viên đã có session active thì entry hàng đợi của họ là thừa: xóa rồi xét người kế.
        if (candidateActive) {
          await manager.delete(PalmMatchQueueEntry, {
            userId: candidate.user_id,
          });
          continue;
        }
        // Kiểm lại block/report ĐÚNG LÚC ghép, không tin state ở thời điểm vào hàng đợi.
        if (await this.safetyService.canPair(userId, candidate.user_id)) {
          partnerId = candidate.user_id;
          break;
        }
      }

      // Step 8: Chưa có ứng viên hợp lệ: user ở lại hàng đợi, trả queued
      if (!partnerId) {
        const queued = await manager.findOneByOrFail(PalmMatchQueueEntry, {
          userId,
        });
        return {
          state: PalmMatchClientState.Queued,
          queuedAt: queued.queuedAt.toISOString(),
        };
      }

      // Step 9: Có cặp: tạo session snapshot (seed tất định)
      const session = await this.createSessionSnapshot(
        manager,
        userId,
        partnerId,
      );

      // [SERVICE] PalmMatchService.createSessionSnapshot  ·  palm-match.service.ts L334–381
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
      // Seed tất định (FNV-1a) từ id session và id từng user: cùng session luôn ra cùng kết quả,
      // session mới ra kết quả khác. Chỉ để giải trí, không dùng cho bảo mật.
      const lowSeed = fnv1aHash(`${id}:${userLowId}:sign`);
      const highSeed = fnv1aHash(`${id}:${userHighId}:sign`);
      const resultSeed = fnv1aHash(`${id}:${userLowId}:${userHighId}:result`);
      const durationSeconds = this.config.getOrThrow(
        'PALM_MATCH_SESSION_DURATION_SECONDS',
        { infer: true },
      );
      const template = templates[resultSeed % templates.length];

      // Snapshot: cung hoàng đạo, % hợp và lời bói chốt MỘT lần lúc tạo session, nên reload hay
      // retry không quay ra kết quả khác.
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

      // Step 10: Rút cả 2 khỏi hàng đợi, ghi 2 participant (PK user_id)
      // Cùng transaction: ghép xong thì rút cả 2 khỏi hàng đợi và ghi 2 participant. PK
      // participant là chốt DB cuối cho "1 user 1 session" dù logic phía trên đổi sau này.
      await manager.delete(PalmMatchQueueEntry, [userId, partnerId]);
      await manager.insert(PalmMatchActiveParticipant, [
        { userId, sessionId: session.id },
        { userId: partnerId, sessionId: session.id },
      ]);

      // Step 11: Trả trạng thái đã ghép theo góc nhìn của user
      return this.sessionView(session, userId);
    });
  }
}

// [POSTGRES] CREATE TABLE palm_match_queue_entries  ·  1755500000000-palm-match-session.ts L49–64
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

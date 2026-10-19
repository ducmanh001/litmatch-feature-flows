//  JOB VideoRankingService.runOnce (mỗi VIDEO_RANKING_JOB_INTERVAL_MS)
//  Mỗi tick chạy vài lô: mỗi lô là một câu UPDATE chọn các video published đến hạn (SKIP LOCKED), tính điểm mới và đặt hạn tính lại kế tiếp.
//  Step 1: Một tick: tính lại tối đa VIDEO_RANKING_MAX_BATCHES_PER_TICK lô video đến hạn
//  Step 2: Đọc trọng số công thức điểm và tham số lịch tính lại (cả hai chỉnh bằng cấu hình)
//  Step 3: Lặp từng lô; mỗi lô là một câu UPDATE, không đọc về app rồi ghi từng dòng
//  Step 4: Chọn video published đến hạn (SKIP LOCKED), tính điểm mới và hạn kế tiếp trong cùng câu lệnh
//  Step 5: Lô chưa đầy nghĩa là hết video đến hạn: dừng sớm, tick sau mới chạy tiếp

// [WORKER] VideoRankingService.runOnce · short-video/jobs/video-ranking.service.ts
@Injectable()
export class VideoRankingService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly job = new ManagedInterval();
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Một tick: tính lại tối đa VIDEO_RANKING_MAX_BATCHES_PER_TICK lô video đến hạn
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {

      // Step 2: Đọc trọng số công thức điểm và tham số lịch tính lại (cả hai chỉnh bằng cấu hình)
      const weightView = this.config.getOrThrow('VIDEO_RANK_WEIGHT_VIEW', {
        infer: true,
      });
      const weightLike = this.config.getOrThrow('VIDEO_RANK_WEIGHT_LIKE', {
        infer: true,
      });
      const weightComment = this.config.getOrThrow(
        'VIDEO_RANK_WEIGHT_COMMENT',
        { infer: true },
      );
      const decayHours = this.config.getOrThrow('VIDEO_RANK_TIME_DECAY_HOURS', {
        infer: true,
      });

      const batchSize = this.config.getOrThrow('VIDEO_RANKING_BATCH_SIZE', {
        infer: true,
      });
      const maxBatches = this.config.getOrThrow(
        'VIDEO_RANKING_MAX_BATCHES_PER_TICK',
        { infer: true },
      );

      // Tỉ lệ lệch tương đối cho phép (VIDEO_RANKING_STALENESS_PCT %) và trần khoảng tính lại:
      // hai núm quyết định bao lâu một video được tính lại. Điểm = tương tác / (1 + tuổi/decay)
      // chỉ lệch tương đối ≈ Δ / (decay + tuổi) sau Δ giờ, nên video càng già càng ít cần tính
      // lại.
      const stalenessFraction =
        this.config.getOrThrow('VIDEO_RANKING_STALENESS_PCT', { infer: true }) /
        100;
      const maxIntervalSeconds =
        this.config.getOrThrow('VIDEO_RANKING_MAX_INTERVAL_HOURS', {
          infer: true,
        }) * 3600;

      let total = 0;

      // Step 3: Lặp từng lô; mỗi lô là một câu UPDATE, không đọc về app rồi ghi từng dòng
      for (let batch = 0; batch < maxBatches; batch += 1) {

        // Step 4: Chọn video published đến hạn (SKIP LOCKED), tính điểm mới và hạn kế tiếp trong
        // cùng câu lệnh
        // [POSTGRES] Cả việc chọn lẫn tính nằm trong MỘT câu UPDATE: subquery lấy id đến hạn theo
        // thứ tự hạn (rank_due_at ASC NULLS FIRST, id ASC) bằng index một phần chỉ chứa video
        // published, nên mỗi lô chỉ chạm đúng LIMIT dòng, không quét cả bảng.
        // FOR UPDATE SKIP LOCKED: video đang bị moderation chuyển trạng thái thì bỏ qua thay vì
        // chờ, tick sau nhặt lại.
        // Hạn kế tiếp = now + LEAST(trần, GREATEST(sàn 60 giây, tỉ lệ × (decay + tuổi_giờ) ×
        // 3600)). Sàn 60 giây bảo đảm hạn luôn nằm trong tương lai nên một video không bị tính
        // hai lần trong một tick; công thức điểm không đổi so với bản cũ.
        const [, count] = (await this.dataSource.query(
          `UPDATE videos
          SET rank_score = (view_count * $1 + like_count * $2 + comment_count * $3)
                              / (1 + EXTRACT(EPOCH FROM (now() - created_at)) / 3600.0 / $4),
              rank_computed_at = now(),
              rank_due_at = now() + make_interval(secs => LEAST($7::float8, GREATEST($8::float8,
                $9::float8 * ($10::float8 + EXTRACT(EPOCH FROM (now() - created_at)) / 3600.0) * 3600.0)))
          WHERE id IN (
            SELECT id FROM videos
             WHERE status = $5
               AND (rank_due_at IS NULL OR rank_due_at <= now())
             ORDER BY rank_due_at ASC NULLS FIRST, id ASC
             LIMIT $6
             FOR UPDATE SKIP LOCKED
          )`,
          [
            weightView,
            weightLike,
            weightComment,
            decayHours,
            VideoStatus.Published,
            batchSize,
            maxIntervalSeconds,
            MIN_REFRESH_SECONDS,
            stalenessFraction,
            decayHours,
          ],
        )) as [unknown, number];
        total += count;

        // Step 5: Lô chưa đầy nghĩa là hết video đến hạn: dừng sớm, tick sau mới chạy tiếp
        // Trước đây xoay vòng theo rank_computed_at: chu kỳ đầy đủ = số video / lô × chu kỳ tick,
        // tăng tuyến tính theo số video (hàng chục triệu video không bao giờ kịp). Giờ lô chỉ gồm
        // video ĐẾN HẠN nên số lô mỗi giờ tỉ lệ với video trẻ và hoạt động; lô chưa đầy nghĩa là
        // đã hết việc.
        if (count < batchSize) break;
      }
      return total;
    }, 0);
  }
}

// [POSTGRES] 1758700000000-video-rank-due.ts · database/migrations/1758700000000-video-rank-due.ts
// Index một phần theo đúng thứ tự job đọc (rank_due_at ASC NULLS FIRST, id ASC) và chỉ gồm video
// published: lô kế tiếp là đọc đầu index chứ không quét bảng. CONCURRENTLY nên không khoá ghi
// bảng videos khi migrate (vì vậy migration chạy ngoài transaction).
await queryRunner.query(
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_videos_rank_due_at ON videos (rank_due_at ASC NULLS FIRST, id ASC) WHERE status = 'published'",
);

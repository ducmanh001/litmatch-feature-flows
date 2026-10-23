//  JOB StorySweeperService.runOnce (mỗi STORY_SWEEPER_INTERVAL_MS)
//  Xoá story hết hạn theo lô 500; story_views tự xoá theo FK ON DELETE CASCADE; tick lặp lô tới hết backlog hoặc hết ngân sách 30 giây.
//  Step 1: Một tick: lặp lô tới hết backlog hoặc hết ngân sách thời gian
//  Step 2: Trần thời gian của tick (30 giây)
//  Step 3: Xoá một lô story hết hạn cũ nhất (story_views xoá cascade theo FK)
//  Step 4: Hết backlog, shutdown hoặc hết ngân sách thì dừng
//  Step 5: Nghỉ giữa các lô để vacuum và replica theo kịp
//  Step 6: Log số story đã xoá và cờ "còn backlog"

// [WORKER] StorySweeperService.runOnce · feed/jobs/story-sweeper.service.ts
@Injectable()
export class StorySweeperService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(StorySweeperService.name);
  private readonly job = new ManagedInterval();
  private stopping = false;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  // Step 1: Một tick: lặp lô tới hết backlog hoặc hết ngân sách thời gian
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {

      // Step 2: Trần thời gian của tick (30 giây)
      const deadline = Date.now() + STORY_SWEEP_TICK_BUDGET_MS;
      let total = 0;
      let capped: boolean;
      for (;;) {

        // Step 3: Xoá một lô story hết hạn cũ nhất (story_views xoá cascade theo FK)
        // [POSTGRES] Mỗi story kéo theo hàng trăm story_views xoá cascade nên lô giữ ở 500 story
        // để mỗi câu DELETE ngắn. Trước đây mỗi tick chỉ xoá ĐÚNG 1 lô (cứng 500 story/giờ): tạo
        // story nhanh hơn mức đó thì backlog tăng vô hạn.
        // Đo lab (12.000 story × 100 lượt xem, hàng xen kẽ, dữ liệu trong cache): một lô p50 ≈ 88
        // ms. Chu kỳ mặc định 1 giờ chỉ đủ nếu một lô ≤ ~325 ms ở quy mô đề bài; ở đĩa chậm hơn
        // lab 10 lần cần hạ STORY_SWEEPER_INTERVAL_MS xuống ≈ 24 phút.
        const [row] = (await this.dataSource.query(
          `WITH removed AS (
             DELETE FROM stories
              WHERE id IN (
                SELECT id
                  FROM stories
                 WHERE expires_at <= now()
                 ORDER BY expires_at ASC, id ASC
                 LIMIT $1
              )
             RETURNING 1
           )
           SELECT count(*)::int AS n FROM removed`,
          [STORY_SWEEP_BATCH],
        )) as Array<{ n: number }>;
        const removed = row?.n ?? 0;
        total += removed;
        capped = removed >= STORY_SWEEP_BATCH;

        // Step 4: Hết backlog, shutdown hoặc hết ngân sách thì dừng
        // Shutdown: lô đang chạy xong rồi dừng, không bắt đầu lô mới — SIGTERM không phải chờ hết
        // ngân sách 30 giây.
        if (!capped || this.stopping || Date.now() >= deadline) break;

        // Step 5: Nghỉ giữa các lô để vacuum và replica theo kịp
        await sleep(STORY_SWEEP_PAUSE_MS);
      }

      // Step 6: Log số story đã xoá và cờ "còn backlog"
      if (total > 0) {
        this.logger.log(
          `story sweeper: đã xoá ${total} story hết hạn${capped ? ', còn backlog (tick sau tiếp tục)' : ''}`,
        );
      }
      return total;
    }, 0);
  }
}

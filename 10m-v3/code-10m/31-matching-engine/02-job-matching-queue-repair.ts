//  JOB QueueRepairService.runOnce (mỗi MATCHING_QUEUE_REPAIR_INTERVAL_MS, một pod duy nhất)
//  Quét một trang vé queued còn trong hạn theo cursor bền vững (enqueued_at, id), khôi phục vé thiếu trong Redis bằng NX rồi refresh score theo DB bằng XX LT, đánh dấu shard active và đánh thức matcher.
//  Step 1: Một lượt repair: chỉ một lượt chạy trong process tại một thời điểm
//  Step 2: Đọc cursor bền vững: pod mới (đổi leader) tiếp tục chỗ cũ, không quay lại hàng đầu
//  Step 3: Lấy một trang vé queued còn trong hạn theo keyset (enqueued_at, id)
//  Step 4: NX: khôi phục vé còn thiếu trong Redis, không đụng vé đã có
//  Step 5: Refresh score theo DB bằng XX LT: chỉ hạ score, không ghi đè boost mới bằng score cũ
//  Step 6: Cursor: trang đầy thì tiếp tục sau hàng cuối, trang chưa đầy thì vòng sau bắt đầu lại
//  Step 7: Có vé được khôi phục thì đánh thức matcher ngay

// [WORKER] QueueRepairService.runOnce · matching/jobs/queue-repair.service.ts
@Injectable()
export class QueueRepairService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(QueueRepairService.name);
  private readonly job = new ManagedInterval();
  private stopping = false;
  // RedisMatchingQueue
  private readonly claimRedis: ClaimPairRedis;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(MATCHING_QUEUE) private readonly queue: MatchingQueuePort,
    private readonly wakeup: MatcherWakeup,
    // RedisMatchingQueue
    private readonly redis: Redis,
  ) {
    // RedisMatchingQueue
    redis.defineCommand('matchingClaimPair', {
      numberOfKeys: 1,
      lua: CLAIM_PAIR_SCRIPT,
    });
    this.claimRedis = redis as ClaimPairRedis;
  }

  // Step 1: Một lượt repair: chỉ một lượt chạy trong process tại một thời điểm
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {

      // Step 2: Đọc cursor bền vững: pod mới (đổi leader) tiếp tục chỗ cũ, không quay lại hàng
      // đầu
      const [checkpoint] = (await this.dataSource.query(
        'SELECT state FROM runtime_job_checkpoints WHERE job_key = $1',
        [REPAIR_JOB],
      )) as Array<{ state: RepairCursor | null }>;
      const cursor = checkpoint?.state;
      const batchSize = this.config.getOrThrow(
        'MATCHING_QUEUE_REPAIR_BATCH_SIZE',
        { infer: true },
      );

      // Step 3: Lấy một trang vé queued còn trong hạn theo keyset (enqueued_at, id)
      // Chỉ vé queued còn trong hạn chờ tối đa và theo keyset (enqueued_at, id), nên mỗi lượt tốn
      // một lượng bị chặn bởi BATCH_SIZE chứ không tăng theo số vé sống. Thời gian hồi phục sau
      // khi Redis mất dữ liệu vì thế phụ thuộc số vé sống và ngân sách quét.
      const rows = (await this.dataSource.query(
        `SELECT id, enqueued_at::text AS at FROM match_tickets
          WHERE status = 'queued'
            AND enqueued_at >= clock_timestamp() - ($1::double precision * interval '1 second')
            ${cursor ? 'AND (enqueued_at, id) > ($3::timestamptz, $4::uuid)' : ''}
          ORDER BY enqueued_at, id LIMIT $2`,
        [
          this.config.getOrThrow('MATCHING_QUEUE_MAX_WAIT_SECONDS', {
            infer: true,
          }),
          batchSize,
          ...(cursor ? [cursor.at, cursor.id] : []),
        ],
      )) as RepairCursor[];
      let next = 0;
      let repaired = 0;
      let failures = 0;
      const tickets =
        rows.length === 0
          ? []
          : await this.dataSource.manager.find(MatchTicket, {
              where: {
                id: In(rows.map((row) => row.id)),
                status: MatchTicketStatus.Queued,
              },
            });
      const enqueued: MatchTicket[] = [];
      await Promise.all(
        Array.from(
          { length: Math.min(REPAIR_CONCURRENCY, tickets.length) },
          async () => {
            while (!this.stopping && next < tickets.length) {
              const ticket = tickets[next++];
              try {
                const shard = shardKeyOfTicket(ticket);

                // Step 4: NX: khôi phục vé còn thiếu trong Redis, không đụng vé đã có
                // Redis chỉ là hàng đợi dẫn xuất nên mất dữ liệu (failover, restart) được dựng
                // lại từ DB mà không cần client gửi lại yêu cầu. NX chỉ thêm vé đang thiếu: vé đã
                // có, kể cả vé vừa được speed-up, không bị đụng.
                await this.queue.enqueue(
                  shard,
                  String(ticketScore(ticket)),
                  ticket.id,
                  'NX',
                );

                // [REDIS] RedisMatchingQueue.enqueue · matching/redis/redis-matching-queue.adapter.ts
                const score = String(
                  ticket.enqueuedAt.getTime() + ticket.trustPenaltyMs - ticket.priorityBoostMs,
                );
                const ticketId = ticket.id;
                const mode = 'NX';
                if (mode === 'XX_LT') {
                  // Redis >= 6.2: XX + LT trong một lệnh nguyên tử: chỉ cập nhật member đang có
                  // VÀ chỉ khi score mới NHỎ HƠN score hiện tại.
                  await this.redis.zadd(shard, 'XX', 'LT', score, ticketId);
                  return;
                }
                await this.redis.zadd(shard, mode, score, ticketId);

                enqueued.push(ticket);
              } catch {
                failures++;
              }
            }
          },
        ),
      );
      const refreshed =
        enqueued.length === 0
          ? []
          : await this.dataSource.manager.find(MatchTicket, {
              where: {
                id: In(enqueued.map((ticket) => ticket.id)),
                status: MatchTicketStatus.Queued,
              },
            });
      const freshById = new Map(refreshed.map((ticket) => [ticket.id, ticket]));
      next = 0;
      await Promise.all(
        Array.from(
          { length: Math.min(REPAIR_CONCURRENCY, enqueued.length) },
          async () => {
            while (!this.stopping && next < enqueued.length) {
              const ticket = enqueued[next++];
              const shard = shardKeyOfTicket(ticket);
              try {
                const fresh = freshById.get(ticket.id);
                if (fresh) {
                  await this.queue.enqueue(
                    shard,
                    String(ticketScore(fresh)),
                    ticket.id,

                    // Step 5: Refresh score theo DB bằng XX LT: chỉ hạ score, không ghi đè boost
                    // mới bằng score cũ
                    // Refresh score theo boost mới nhất trong DB bằng XX LT: boost chỉ tăng nên
                    // score chỉ giảm, và LT chặn việc ghi đè một boost đã trả tiền mới hơn bằng
                    // score cũ khi repair chạy đồng thời với speed-up.
                    'XX_LT',
                  );
                  await this.queue.markActive(shard);
                  repaired++;
                } else await this.queue.remove(shard, ticket.id);
              } catch {
                failures++;
              }
            }
          },
        ),
      );
      if (!this.stopping) {

        // Step 6: Cursor: trang đầy thì tiếp tục sau hàng cuối, trang chưa đầy thì vòng sau bắt
        // đầu lại
        // Cursor đi tiếp ngay cả khi một Redis lỗi, để các Redis khoẻ vẫn được phục hồi; vé lỗi
        // được thăm lại ở chu kỳ quét sau. Cursor được lưu bền nên đổi leader không làm quét lại
        // từ hàng 1.
        const state = rows.length === batchSize ? rows[rows.length - 1] : null;
        await this.dataSource.query(
          `INSERT INTO runtime_job_checkpoints (job_key, state) VALUES ($1, $2::jsonb)
           ON CONFLICT (job_key) DO UPDATE SET state = EXCLUDED.state, updated_at = now()`,
          [REPAIR_JOB, JSON.stringify(state)],
        );
      }

      // Step 7: Có vé được khôi phục thì đánh thức matcher ngay
      // Đánh thức matcher thay vì chờ chu kỳ backstop. Repair có thể chạy đồng thời với claim,
      // cancel hay speed-up; điều đó vô hại vì matcher kiểm lại trạng thái trong DB dưới khoá
      // trước khi ghép.
      if (repaired > 0) this.wakeup.notify();
      if (failures > 0)
        this.logger.warn(
          { failures },
          'Matching queue repair will retry failed tickets next cycle',
        );
      return repaired;
    }, 0);
  }
}

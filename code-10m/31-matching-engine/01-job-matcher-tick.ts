//  JOB MatcherWorkerService.runOnce (đánh thức khi có ticket + backstop định kỳ)
//  Duyệt các shard đang có vé, chọn cặp tương thích trong cửa sổ đầu shard, claim cả hai vé nguyên tử, xác minh lại dưới khoá rồi tạo session.
//  Step 1: Một tick: chạy các vòng liên tiếp khi có wake-up đến giữa chừng (tối đa MAX_RERUN_ROUNDS)
//  Step 2: Lấy các shard đang có vé và xáo ngẫu nhiên để các pod không cùng đập vào một shard
//  Step 3: Số shard drain song song, bị chặn theo ¼ pool Postgres
//  Step 4: Mỗi worker lấy shard kế tiếp và drain
//  Step 5: Đọc (KHÔNG lấy ra) cửa sổ ứng viên đầu shard
//  Step 6: Chọn cặp tương thích đầu tiên theo thứ tự ưu tiên
//  Step 7: Lấy cả hai vé khỏi Redis bằng một script Lua nguyên tử; thua thì nhìn lại cửa sổ
//  Step 8: Xác minh lại dưới khoá trong một transaction Postgres rồi tạo session
//  Step 9: Tạo MatchSession, đổi hai ticket sang confirmed (UPDATE có điều kiện), ghi thông báo
//  Step 10: Không ghép được giữa chừng: vé còn hợp lệ được trả về hàng đợi, vé không hợp lệ thì expire
//  Step 11: Transaction lỗi sau khi đã claim: trả cả hai vé về hàng đợi rồi ném lỗi gốc
//  Step 12: Bước 1: khôi phục sự hiện diện trong Redis bằng ZADD NX với score đã đọc
//  Step 13: Bước 2: đọc lại boost từ DB và đặt score tuyệt đối bằng ZADD XX LT
//  Step 14: Trả vé còn hợp lệ về hàng đợi mà không mất ưu tiên đã mua
//  Step 15: Ghi metric thời gian chờ, phát realtime và push SAU commit

// [WORKER] MatcherWorkerService.runOnce · matching/jobs/matcher-worker.service.ts
@Injectable()
export class MatcherWorkerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(MatcherWorkerService.name);
  private readonly job = new ManagedInterval();
  private rerunRequested = false;
  // RedisMatchingQueue
  private readonly claimRedis: ClaimPairRedis;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(MATCHING_QUEUE) private readonly queue: MatchingQueuePort,
    @Inject(MATCHING_REALTIME)
    private readonly realtime: RealtimePublisherPort,
    @Inject(MATCH_INTERACTION_POLICY)
    private readonly interactionPolicy: MatchInteractionPolicy,
    private readonly metrics: MatchingMetrics,
    private readonly notificationService: NotificationService,
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

  // Step 1: Một tick: chạy các vòng liên tiếp khi có wake-up đến giữa chừng (tối đa
  // MAX_RERUN_ROUNDS)
  async runOnce(): Promise<number> {
    const matched = await this.job.runExclusive(async () => {
      let total = 0;
      for (let round = 0; round < MAX_RERUN_ROUNDS; round += 1) {
        this.rerunRequested = false;
        const roundMatched = await this.runRound();

        // [WORKER] MatcherWorkerService.runRound
        return withSpan(
          'litmatch.matching',
          'matching.matcher.tick',
          async (span) => {

            // Step 2: Lấy các shard đang có vé và xáo ngẫu nhiên để các pod không cùng đập vào
            // một shard
            const shards = shuffled(await this.queue.listActiveShards());

            // Step 3: Số shard drain song song, bị chặn theo ¼ pool Postgres
            // Các shard có ticket rời nhau nên drain song song không tranh lock; số worker bị
            // chặn theo pool Postgres để matcher không chiếm hết kết nối của request thường.
            const workers = Math.min(this.effectiveConcurrency(), shards.length);

            // [WORKER] MatcherWorkerService.effectiveConcurrency
            const configured = this.config.getOrThrow('MATCHING_MATCHER_CONCURRENCY', {
              infer: true,
            });
            const poolMax = this.config.getOrThrow('DATABASE_POOL_MAX', {
              infer: true,
            });
            return Math.max(1, Math.min(configured, Math.floor(poolMax / 4)));

            let next = 0;
            let matched = 0;
            const results = await Promise.allSettled(
              Array.from({ length: workers }, async () => {
                while (next < shards.length) {

                  // Step 4: Mỗi worker lấy shard kế tiếp và drain
                  // Không viết `matched += await ...`: vế trái được đọc TRƯỚC khi await nên hai
                  // worker chạy xen kẽ sẽ ghi đè kết quả của nhau.
                  const drained = await this.drainShard(shards[next++]);

                  // [WORKER] MatcherWorkerService.drainShard
                  const shard = shards[next++];
                  const batchSize = this.config.getOrThrow('MATCHING_MATCHER_BATCH_SIZE', {
                    infer: true,
                  });
                  const windowSize = this.config.getOrThrow(
                    'MATCHING_MATCHER_CANDIDATE_WINDOW',
                    { infer: true },
                  );
                  let matched = 0;
                  for (let i = 0; i < batchSize; i++) {

                    // Step 5: Đọc (KHÔNG lấy ra) cửa sổ ứng viên đầu shard
                    const window = await this.queue.peekWindow(shard, windowSize);

                    // [REDIS] RedisMatchingQueue.peekWindow · matching/redis/redis-matching-queue.adapter.ts
                    const max = windowSize;
                    const values = await this.redis.zrange(shard, 0, max - 1, 'WITHSCORES');
                    const entries: Array<[string, string]> = [];
                    for (let index = 0; index + 1 < values.length; index += 2) {
                      entries.push([values[index], values[index + 1]]);
                    }
                    return entries;

                    if (window.length === 0) {
                      await this.cleanupShardIfEmpty(shard);

                      // [WORKER] MatcherWorkerService.cleanupShardIfEmpty
                      await this.queue.unmarkActive(shard);
                      if (await this.queue.hasEntries(shard)) await this.queue.markActive(shard);

                      break;
                    }
                    // Chỉ còn 1 ticket: không động vào, chờ người tiếp theo (không pop rồi trả
                    // lại như trước, nên vé đơn lẻ không bao giờ bị xáo score hay mất chỗ).
                    if (window.length < 2) break;
                    const outcome = await this.matchWindow(shard, window);

                    // [WORKER] MatcherWorkerService.matchWindow
                    const poppedById = new Map<string, PoppedTicket>(
                      window.map(([id, score]) => [id, { id, score: Number(score) }]),
                    );
                    const { manager } = this.dataSource;
                    const tickets = await manager.find(MatchTicket, {
                      where: { id: In([...poppedById.keys()]) },
                    });
                    const ticketById = new Map(tickets.map((t) => [t.id, t]));
                    const userIds = [...new Set(tickets.map((t) => t.userId))];
                    const users =
                      userIds.length > 0
                        ? await manager.find(User, {
                            where: { id: In(userIds) },
                            select: { id: true, status: true, gender: true },
                          })
                        : [];
                    const userById = new Map(users.map((u) => [u.id, u]));

                    const candidates: MatchTicket[] = [];
                    const dropIds: string[] = [];
                    const expireIds: string[] = [];
                    for (const [id] of window) {
                      const ticket = ticketById.get(id);
                      if (!ticket || ticket.status !== MatchTicketStatus.Queued) {
                        dropIds.push(id);
                      } else if (userById.get(ticket.userId)?.status !== UserStatus.Active) {
                        expireIds.push(id);
                      } else {
                        candidates.push(ticket);
                      }
                    }
                    await this.discard(shard, dropIds, expireIds);

                    // [WORKER] MatcherWorkerService.discard
                    if (expireIds.length > 0) {
                      await this.dataSource.manager.update(
                        MatchTicket,
                        { id: In([...expireIds]), status: MatchTicketStatus.Queued },
                        { status: MatchTicketStatus.Expired },
                      );
                    }
                    await Promise.all(
                      [...dropIds, ...expireIds].map((id) => this.queue.remove(shard, id)),
                    );

                    // [REDIS] RedisMatchingQueue.remove · matching/redis/redis-matching-queue.adapter.ts
                    const ticketId = id;
                    await this.redis.zrem(shard, ticketId);

                    const discarded = dropIds.length + expireIds.length > 0;

                    // Step 6: Chọn cặp tương thích đầu tiên theo thứ tự ưu tiên
                    // Cặp 2 ticket đầu shard không khớp (giới tính, block/report) KHÔNG còn chặn
                    // cả shard: ticket cũ nhất tìm đối tác tương thích đầu tiên trong cửa sổ. Cửa
                    // sổ chỉ ĐỌC; chỉ cặp được chọn mới bị lấy ra khỏi Redis. Mô phỏng: 46% vé
                    // được ghép → 100% khi đa số chọn "khác giới".
                    const pair = await this.findPair(candidates, userById);

                    // [WORKER] MatcherWorkerService.findPair
                    let canPairChecks = 0;
                    for (let i = 0; i < candidates.length; i++) {
                      for (let j = i + 1; j < candidates.length; j++) {
                        const a = candidates[i];
                        const b = candidates[j];
                        if (a.userId === b.userId) continue;
                        if (
                          !genderPreferenceAllows(
                            a.genderPreference,
                            userById.get(b.userId)?.gender,
                          ) ||
                          !genderPreferenceAllows(
                            b.genderPreference,
                            userById.get(a.userId)?.gender,
                          )
                        ) {
                          continue;
                        }
                        if (this.isKnownUnpairable(a.userId, b.userId)) continue;
                        // canPair (đọc block/report) chỉ chạy cho cặp đã qua bộ lọc giới tính và
                        // tối đa MAX_CAN_PAIR_CHECKS lần mỗi cửa sổ, kèm cache cặp không ghép
                        // được: ở tốc độ ghép cao điểm, hỏi lại cùng một cặp bị cấm mỗi tick là
                        // lãng phí đọc DB.
                        if (canPairChecks >= MAX_CAN_PAIR_CHECKS) return undefined;
                        canPairChecks += 1;
                        if (await this.interactionPolicy.canPair(a.userId, b.userId)) {
                          return [a, b];
                        }
                        this.rememberUnpairable(a.userId, b.userId);
                      }
                    }
                    return undefined;

                    if (!pair) return discarded ? 'progress' : 'stalled';

                    // Step 7: Lấy cả hai vé khỏi Redis bằng một script Lua nguyên tử; thua thì
                    // nhìn lại cửa sổ
                    // [REDIS] Hai matcher cùng chọn một vé thì đúng một bên thắng: bên thua nhận
                    // false và không phải "trả lại" vé nào (khác ZPOPMIN, vốn lấy ra rồi mới biết
                    // có ghép được không).

                    // [REDIS] RedisMatchingQueue.claimPair · matching/redis/redis-matching-queue.adapter.ts
                    const ticketA = pair[0].id;
                    const ticketB = pair[1].id;
                    return (
                      (await this.claimRedis.matchingClaimPair(shard, ticketA, ticketB)) === 1
                    );

                    if (!(await this.queue.claimPair(shard, pair[0].id, pair[1].id))) {
                      return 'contended';
                    }
                    return this.confirmPair(shard, pair, poppedById);

                    // [WORKER] MatcherWorkerService.confirmPair
                    const pairIds = pair.map((ticket) => ticket.id);

                    // Step 8: Xác minh lại dưới khoá trong một transaction Postgres rồi tạo
                    // session
                    const attempt = this.dataSource.transaction(async (manager) => {
                      const tickets = await manager.find(MatchTicket, {
                        where: { id: In(pairIds) },
                        // [POSTGRES] Khoá hai ticket theo thứ tự id cố định để không deadlock với
                        // các writer khác (speed-up, cancel, sweeper).
                        order: { id: 'ASC' },
                        lock: { mode: 'pessimistic_write' },
                      });
                      const byId = new Map(tickets.map((t) => [t.id, t]));
                      const ta = byId.get(pair[0].id);
                      const tb = byId.get(pair[1].id);

                      const userIds = [...new Set(tickets.map((t) => t.userId))];
                      const users =
                        userIds.length > 0
                          ? await manager.find(User, { where: { id: In(userIds) } })
                          : [];
                      const userById = new Map(users.map((u) => [u.id, u]));
                      const isValid = (t?: MatchTicket): t is MatchTicket =>
                        !!t &&
                        t.status === MatchTicketStatus.Queued &&
                        userById.get(t.userId)?.status === UserStatus.Active;

                      // Giữa lúc chọn cặp và lúc khoá hàng, một bên có thể vừa cancel, bị ban,
                      // đổi gender hoặc bị block/report. canPair lúc chọn chỉ là bộ lọc; quyết
                      // định cuối nằm Ở ĐÂY, dưới khoá, vì từ lúc chọn tới lúc commit gồm cả thời
                      // gian chờ claim và chờ lấy kết nối pool (tới vài giây khi tải cao).
                      const aValid = isValid(ta);
                      const bValid = isValid(tb);

                      if (
                        aValid &&
                        bValid &&
                        ta.userId !== tb.userId &&
                        genderPreferenceAllows(
                          ta.genderPreference,
                          userById.get(tb.userId)?.gender,
                        ) &&
                        genderPreferenceAllows(
                          tb.genderPreference,
                          userById.get(ta.userId)?.gender,
                        ) &&
                        (await this.interactionPolicy.canPair(ta.userId, tb.userId))
                      ) {
                        const confirmedAt = new Date();

                        // Step 9: Tạo MatchSession, đổi hai ticket sang confirmed (UPDATE có điều
                        // kiện), ghi thông báo
                        const session = await manager.save(
                          manager.create(MatchSession, {
                            matchType: ta.matchType,
                            userAId: ta.userId,
                            userBId: tb.userId,
                            ticketAId: ta.id,
                            ticketBId: tb.id,
                            status: MatchSessionStatus.Confirmed,
                            confirmedAAt: confirmedAt,
                            confirmedBAt: confirmedAt,
                          }),
                        );
                        // Một UPDATE có điều kiện thay cho 2 lần save entity (mỗi lần kèm
                        // SELECT): hai ticket đã được khoá và xác minh queued nên affected luôn
                        // là 2; điều kiện status là chốt chặn thứ hai.
                        const updated = await manager.update(
                          MatchTicket,
                          { id: In([ta.id, tb.id]), status: MatchTicketStatus.Queued },
                          { status: MatchTicketStatus.Confirmed, sessionId: session.id },
                        );
                        if (updated.affected !== 2) {
                          throw new Error('Ghép cặp: ticket đổi trạng thái dưới lock');
                        }
                        const matchNotifications = await Promise.all(
                          [ta, tb].map((ticket) =>
                            this.notificationService.createWithManager(manager, {
                              userId: ticket.userId,
                              type: NotificationType.MatchConfirmed,
                              payload: { sessionId: session.id, matchType: ticket.matchType },
                            }),
                          ),
                        );
                        const matchedAt = Date.now();
                        return {
                          kind: 'matched' as const,
                          requeue: [] as PoppedTicket[],
                          matchedPair: [
                            {
                              userId: ta.userId,
                              ticketId: ta.id,
                              sessionId: session.id,
                              matchType: ta.matchType,
                              waitSeconds: (matchedAt - ta.enqueuedAt.getTime()) / 1000,
                            },
                            {
                              userId: tb.userId,
                              ticketId: tb.id,
                              sessionId: session.id,
                              matchType: tb.matchType,
                              waitSeconds: (matchedAt - tb.enqueuedAt.getTime()) / 1000,
                            },
                          ],
                          matchNotifications,
                        };
                      }

                      // Step 10: Không ghép được giữa chừng: vé còn hợp lệ được trả về hàng đợi,
                      // vé không hợp lệ thì expire
                      // Không ghép được (đã đổi giữa chừng): vé còn hợp lệ được đẩy lại Redis với
                      // score gốc sau commit (không mất lượt chờ); vé không hợp lệ nhưng vẫn
                      // queued thì expire; vé đã rời queued thì chỉ gỡ.
                      const requeue: PoppedTicket[] = [];
                      const entries: Array<[MatchTicket | undefined, boolean, string]> = [
                        [ta, aValid, pair[0].id],
                        [tb, bValid, pair[1].id],
                      ];
                      for (const [ticket, valid, id] of entries) {
                        const popped = poppedById.get(id);
                        if (valid && popped) {
                          requeue.push(popped);
                        } else if (ticket && ticket.status === MatchTicketStatus.Queued) {
                          ticket.status = MatchTicketStatus.Expired;
                          await manager.save(ticket);
                        }
                      }
                      return {
                        kind: 'dropped' as const,
                        requeue,
                        matchedPair: undefined,
                        matchNotifications: [] as Notification[],
                      };
                    });

                    // Step 11: Transaction lỗi sau khi đã claim: trả cả hai vé về hàng đợi rồi ném
                    // lỗi gốc
                    // Transaction lỗi (hết pool, deadlock, mất kết nối) SAU khi cặp đã bị claim
                    // khỏi Redis: trả cả hai về hàng đợi thay vì để thành zombie tới khi sweeper
                    // expire. Dưới tải lớn, lỗi lấy kết nối là chuyện thường. Nếu thực ra
                    // transaction đã commit, vé confirmed quay lại Redis sẽ bị matchWindow gỡ ở
                    // lần sau, không ghép đôi.
                    const result = await attempt.catch(async (error: unknown) => {
                      await this.requeueClaimed(
                        shard,
                        pair.flatMap((ticket) => poppedById.get(ticket.id) ?? []),
                        false,
                      );

                      // [WORKER] MatcherWorkerService.requeueClaimed
                      const claimed = pair.flatMap((ticket) => poppedById.get(ticket.id) ?? []);
                      const strict = false;
                      if (claimed.length === 0) return;

                      // Step 12: Bước 1: khôi phục sự hiện diện trong Redis bằng ZADD NX với score
                      // đã đọc
                      // Score đọc lúc lấy cửa sổ có thể đã cũ: speed-up hoàn tất SAU lúc claim
                      // chỉ ghi được DB (ZADD XX của nó bỏ qua vì vé đang không nằm trong Redis).
                      // Trả lại bằng score cũ sẽ xoá mất boost đã trả tiền.
                      // Bước 1: khôi phục sự hiện diện ngay bằng NX (phòng trùng).
                      const restored = await Promise.allSettled(
                        claimed.map((c) =>
                          this.queue.enqueue(shard, String(c.score), c.id, 'NX'),
                        ),
                      );

                      // [REDIS] RedisMatchingQueue.enqueue · matching/redis/redis-matching-queue.adapter.ts
                      const score = String(c.score);
                      // ticketId = c.id
                      const mode = 'NX';
                      if (mode === 'XX_LT') {
                        // Redis >= 6.2: XX + LT trong một lệnh nguyên tử: chỉ cập nhật member
                        // đang có VÀ chỉ khi score mới NHỎ HƠN score hiện tại.
                        await this.redis.zadd(shard, 'XX', 'LT', score, ticketId);
                        return;
                      }
                      await this.redis.zadd(shard, mode, score, ticketId);

                      const failed = restored.find((r) => r.status === 'rejected');
                      if (strict && failed?.status === 'rejected') throw failed.reason;
                      try {
                        const scoreAtRead = new Map(claimed.map((c) => [c.id, c.score]));

                        // Step 13: Bước 2: đọc lại boost từ DB và đặt score tuyệt đối bằng ZADD XX
                        // LT
                        // Bước 2: đọc lại priorityBoostMs từ DB và đặt score TUYỆT ĐỐI bằng ZADD
                        // XX LT. Giữa lúc đọc DB và lệnh Redis, một speed-up khác có thể hoàn tất
                        // cả DB lẫn Redis; XX thuần để lệnh đến muộn ghi đè bằng score cũ hơn.
                        // Score chỉ có thể GIẢM trong đời ticket (boost chỉ tăng; enqueuedAt và
                        // trustPenalty là snapshot lúc tạo), nên LT chặn đúng loại ghi lỗi thời
                        // mà vẫn cho mọi lần hạ score hợp lệ qua. Bước này best-effort: DB lỗi
                        // thì vé giữ score lúc claim cho tới khi speed-up được gọi lại cùng
                        // Idempotency-Key.
                        const rows = await this.dataSource.manager.find(MatchTicket, {
                          where: {
                            id: In(claimed.map((c) => c.id)),
                            status: MatchTicketStatus.Queued,
                          },
                        });
                        await Promise.all(
                          rows
                            .filter((row) => ticketScore(row) !== scoreAtRead.get(row.id))
                            .map((row) =>
                              this.queue.enqueue(
                                shardKeyOfTicket(row),
                                String(ticketScore(row)),
                                row.id,
                                'XX_LT',
                              ),
                            ),
                        );
                      } catch (error) {
                        this.logger.warn(
                          `Không đọc lại được score từ DB khi trả ticket về hàng đợi: ${String(error)}`,
                        );
                      }

                      await this.queue.markActive(shard).catch(() => undefined);
                      throw error;
                    });

                    // Step 14: Trả vé còn hợp lệ về hàng đợi mà không mất ưu tiên đã mua
                    if (result.requeue.length > 0) {
                      await this.requeueClaimed(shard, result.requeue, true);
                      await this.queue.markActive(shard);
                    }
                    if (result.matchedPair) {

                      // Step 15: Ghi metric thời gian chờ, phát realtime và push SAU commit
                      // Realtime và push SAU commit, best-effort: client vẫn có GET
                      // /matching/tickets/:id để poll nếu lỡ event.
                      for (const { matchType, waitSeconds } of result.matchedPair) {
                        this.metrics.observeMatched(matchType, waitSeconds);
                      }
                      await Promise.all(
                        result.matchedPair.map(({ userId, ticketId, sessionId }) => {
                          const envelope: RealtimeEnvelope<MatchConfirmedEventData> = {
                            event: RealtimeEvents.MatchConfirmed,
                            data: { ticketId, sessionId },
                          };
                          return publishRealtimeEvent(
                            this.realtime,
                            this.logger,
                            userId,
                            envelope,
                          );
                        }),
                      );
                      await Promise.all(
                        result.matchNotifications.map((n) =>
                          this.notificationService.sendPush(n),
                        ),
                      );
                    }
                    return result.kind === 'matched' ? 'matched' : 'progress';

                    this.metrics.observeWindow(outcome);
                    if (outcome === 'matched') matched += 1;
                    // stalled: không có cặp hợp lệ nào trong cửa sổ đầu shard. Nhìn lại sẽ thấy
                    // đúng cửa sổ đó nên sẽ busy-loop; thử lại khi có ticket chen vào cửa sổ hoặc
                    // sweeper expire ticket quá hạn.
                    if (outcome === 'stalled') break;
                  }
                  return matched;

                  matched += drained;
                }
              }),
            );
            span.setAttribute('matching.matched_pairs', matched);
            const failed = results.find(
              (result): result is PromiseRejectedResult =>
                result.status === 'rejected',
            );
            if (failed) throw failed.reason;
            return matched;
          },
        );

        total += roundMatched;
        // Wake-up đến khi tick đang chạy KHÔNG bị bỏ: vé vừa vào có thể nằm ở shard tick đó đã xử
        // lý xong, bỏ thì nó phải chờ wake-up kế hoặc backstop vài giây. Cờ rerunRequested làm
        // tick chạy lại ngay khi xong.
        if (!this.rerunRequested) break;
      }
      return total;
    }, BUSY_TICK);
    if (matched === BUSY_TICK) {
      this.rerunRequested = true;
      return 0;
    }
    return matched;
  }
}

// [REDIS] redis-matching-queue.adapter.ts · matching/redis/redis-matching-queue.adapter.ts
const CLAIM_PAIR_SCRIPT = `
local a = redis.call('ZSCORE', KEYS[1], ARGV[1])
local b = redis.call('ZSCORE', KEYS[1], ARGV[2])
if a and b then
  redis.call('ZREM', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
`;

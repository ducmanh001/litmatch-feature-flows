//  POST /api/v1/matching/tickets
//  Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession.
//  Step 1: Nhận request vào hàng đợi (user, body, Idempotency-Key, guest)
//  Step 2: Mở transaction DB; khóa dòng user, kiểm guest và tài khoản bị khóa
//  Step 3: Đã có ticket cùng Idempotency-Key thì trả lại luôn, không trừ quota
//  Step 4: Trừ lượt miễn phí theo ngày (khóa dòng quota) hoặc trả Diamond
//  Step 5: Ghi ticket queued vào Postgres (unique index: mỗi user 1 vé đang chờ)
//  Step 6: Dính unique: retry cùng key thì trả vé cũ, còn lại 409
//  Step 7: Sau commit, đẩy ticket vào Redis (ensureEnqueued: ZADD NX, đánh dấu shard active)
//  Step 8: Trả ticket về controller (bọc thành TicketDto kèm giá speed-up)
//  Step 9: Worker nền: ZPOPMIN lấy 2 vé điểm thấp nhất của shard (nguyên tử)
//  Step 10: Khóa 2 vé FOR UPDATE, verify lại trong DB (giới tính, canPair)
//  Step 11: Ghép được: tạo MatchSession, cập nhật 2 vé, tạo thông báo
//  Step 12: Sau commit: vé không ghép được về lại Redis, hoặc phát match.confirmed

// [CONTROLLER] MatchingController.joinQueue  ·  matching.controller.ts L42–43, L46–72
@Controller('matching')
export class MatchingController {
  constructor(
    private readonly matchingService: MatchingService,
    // MatchingService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(MatchTicket)
    private readonly ticketRepo: Repository<MatchTicket>,
    private readonly economy: EconomyService,
    @Inject(MATCHING_QUEUE) private readonly queue: MatchingQueuePort,
    private readonly matcherWakeup: MatcherWakeup,
    private readonly guestQuota: GuestMatchQuotaService,
    // GuestMatchQuotaService
    private readonly deviceTokens: GuestDeviceTokenService,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // RedisMatchingQueue
    private readonly redis: Redis,
    private readonly activeShardsKey: string,
  ) {}

  // Step 1: Nhận request vào hàng đợi (user, body, Idempotency-Key, guest)
  @Post('tickets')
  @Throttle({ default: { limit: 10, ttl: minutes(1) } })
  async joinQueue(
    @CurrentUser() user: AuthenticatedUser,
    @Body() dto: JoinQueueDto,
    @IdempotencyKey() idempotencyKey: string,
    @Headers('x-guest-device-token') guestDeviceToken: string | undefined,
    @Req() req: Request,
  ): Promise<TicketDto> {
    return TicketDto.from(
      await this.matchingService.joinQueue(user, dto, idempotencyKey, {
        deviceToken: guestDeviceToken,
        ip: req.ip || req.socket.remoteAddress || 'unknown',
      }),
      await this.matchingService.getSpeedupPriceDiamondForUser(user.userId),
    );

    // [SERVICE] MatchingService.joinQueue  ·  matching.service.ts L89–202
    const quotaContext = {
      deviceToken: guestDeviceToken,
      ip: req.ip || req.socket.remoteAddress || 'unknown',
    };
    const genderPreference = dto.genderPreference ?? GenderPreference.Any;
    const useDiamond = dto.useDiamond ?? false;
    const prefixedKey = joinIdempotencyKey(user.userId, idempotencyKey);

    let ticket: MatchTicket;
    try {

      // Step 2: Mở transaction DB; khóa dòng user, kiểm guest và tài khoản bị khóa
      // [POSTGRES] Ghi DB trong 1 transaction, Redis chỉ thêm SAU commit: Postgres là nguồn sự
      // thật của ticket, Redis chỉ là hàng đợi dẫn xuất.
      ticket = await this.dataSource.transaction(async (manager) => {
        const authorization = await this.guestQuota.authorize(
          manager,
          user.userId,
          quotaContext,
        );

        // [SERVICE] GuestMatchQuotaService.authorize  ·  guest-match-quota.service.ts L48–81
        const userId = user.userId;
        const context = quotaContext;
        // [POSTGRES] Khóa dòng user (FOR UPDATE) và đọc isGuest tươi từ DB: JWT cũ sau khi nâng
        // cấp tài khoản không né được giới hạn của guest.
        const user = await manager.findOneOrFail(User, {
          where: { id: userId },
          lock: { mode: 'pessimistic_write' },
        });
        const quotaDate = new Date().toISOString().slice(0, 10);
        if (!user.isGuest) return { user, quotaDate, keyHashes: [] };

        if (!context.deviceToken) {
          throw new DomainException(
            MatchingErrors.GUEST_DEVICE_TOKEN_REQUIRED,
            'Guest phải gửi X-Guest-Device-Token được cấp lúc đăng nhập',
            HttpStatus.UNAUTHORIZED,
          );
        }
        const deviceKey = await this.deviceTokens.verifyForUser(
          context.deviceToken,
          userId,
        );
        const keyHashes = [
          this.keyHash('user', userId),
          this.keyHash('device', deviceKey),
          this.keyHash('network', this.normalizeNetwork(context.ip)),
        ];
        return {
          user,
          quotaDate,
          keyHashes: keyHashes.sort(),
        };

        const profile = authorization.user;
        if (profile.status === UserStatus.Banned) {
          throw new DomainException(
            MatchingErrors.USER_BANNED,
            'Tài khoản bị khoá, không thể vào hàng đợi',
            HttpStatus.FORBIDDEN,
          );
        }

        // Step 3: Đã có ticket cùng Idempotency-Key thì trả lại luôn, không trừ quota
        // Nhận ra retry (cùng Idempotency-Key) TRƯỚC khi trừ quota, để retry không tốn thêm lượt
        // hay Diamond.
        const replay = await manager.findOneBy(MatchTicket, {
          idempotencyKey: prefixedKey,
        });
        if (replay) {
          this.assertJoinReplay(
            replay,
            user.userId,
            dto.matchType,
            genderPreference,
            useDiamond,
          );
          return replay;
        }

        // Step 4: Trừ lượt miễn phí theo ngày (khóa dòng quota) hoặc trả Diamond
        const quota = await this.guestQuota.consumeForMatch(
          manager,
          authorization,
          dto.matchType,
          useDiamond,
        );

        // [SERVICE] GuestMatchQuotaService.consumeForMatch  ·  guest-match-quota.service.ts L132–202
        const matchType = dto.matchType;
        const guestLimit = this.config.getOrThrow('MATCHING_GUEST_DAILY_LIMIT', {
          infer: true,
        });
        if (useDiamond)
          return {
            quotaDate: authorization.quotaDate,
            freeLimit: guestLimit,
            tier: null,
            paidDiamond: true,
          };

        if (authorization.keyHashes.length > 0) {
          await this.consume(manager, authorization);
          return {
            quotaDate: authorization.quotaDate,
            freeLimit: guestLimit,
            tier: null,
            paidDiamond: false,
          };
        }

        const tier = await this.economy.getActiveVipTier(
          authorization.user.id,
          manager,
        );
        const freeLimit = this.dailyLimit(tier, matchType);
        // [POSTGRES] Đếm lượt miễn phí an toàn khi đồng thời: INSERT … ON CONFLICT DO NOTHING tạo
        // dòng đếm nếu chưa có, rồi SELECT … FOR UPDATE khóa dòng đó trước khi kiểm và cộng 1,
        // nên 2 request cùng lúc không cùng lọt qua giới hạn.
        const quotaDate = authorization.quotaDate;
        await manager
          .createQueryBuilder()
          .insert()
          .into(MatchDailyQuota)
          .values({
            userId: authorization.user.id,
            quotaDate,
            matchType,
            count: 0,
          })
          .orIgnore()
          .execute();
        const row = await manager
          .getRepository(MatchDailyQuota)
          .createQueryBuilder('q')
          .setLock('pessimistic_write')
          .where('q.user_id = :userId', { userId: authorization.user.id })
          .andWhere('q.quota_date = :quotaDate', { quotaDate })
          .andWhere('q.match_type = :matchType', { matchType })
          .getOneOrFail();
        if (row.count >= freeLimit) {
          throw new DomainException(
            MatchingErrors.DAILY_QUOTA_EXCEEDED,
            `Đã hết ${freeLimit} lượt ${matchType} miễn phí trong ngày UTC`,
            HttpStatus.TOO_MANY_REQUESTS,
            { limit: freeLimit, quotaDate, matchType },
          );
        }
        await manager
          .createQueryBuilder()
          .update(MatchDailyQuota)
          .set({ count: () => 'count + 1', updatedAt: () => 'CURRENT_TIMESTAMP' })
          .where('user_id = :userId', { userId: authorization.user.id })
          .andWhere('quota_date = :quotaDate', { quotaDate })
          .andWhere('match_type = :matchType', { matchType })
          .execute();
        return { quotaDate, freeLimit, tier, paidDiamond: false };

        // Trừ Diamond CÙNG transaction với insert ticket: insert lỗi thì tiền rollback theo.
        if (quota.paidDiamond) {
          await this.economy.spendDiamondInTransaction(
            manager,
            user.userId,
            TransactionType.MatchingExtraMatch,
            this.getExtraMatchPriceDiamond(dto.matchType),
            extraMatchIdempotencyKey(user.userId, idempotencyKey),
            {
              matchType: dto.matchType,
              quotaDate: quota.quotaDate,
              freeLimit: quota.freeLimit,
            },
          );
        }

        // Step 5: Ghi ticket queued vào Postgres (unique index: mỗi user 1 vé đang chờ)
        return manager.save(
          manager.create(MatchTicket, {
            userId: user.userId,
            matchType: dto.matchType,
            region: profile.region ?? DEFAULT_REGION,
            ageBand: this.ageBandOf(profile.birthDate),
            genderPreference,
            status: MatchTicketStatus.Queued,
            enqueuedAt: new Date(),
            priorityBoostMs: 0,
            trustPenaltyMs: this.trustPenaltyMsOf(profile.trustScore),
            sessionId: null,
            idempotencyKey: prefixedKey,
            paidDiamond: quota.paidDiamond,
          }),
        );
      });
    } catch (err) {

      // Step 6: Dính unique: retry cùng key thì trả vé cũ, còn lại 409
      // [POSTGRES] Mỗi user chỉ 1 vé đang chờ được chặn bằng unique index (xem bên dưới) chứ
      // không check-rồi-insert, vì 2 request đồng thời sẽ cùng lọt qua check. Dính unique thì
      // kiểm retry TRƯỚC, vì cả 2 unique có thể cùng dính; không phải retry thì 409.
      if (!isUniqueViolation(err)) throw err;
      const existing = await this.ticketRepo.findOneBy({
        idempotencyKey: prefixedKey,
      });
      if (existing) {
        this.assertJoinReplay(
          existing,
          user.userId,
          dto.matchType,
          genderPreference,
          useDiamond,
        );
        if (existing.status === MatchTicketStatus.Queued)
          await this.ensureEnqueued(existing);
        return existing;
      }
      if (violatedConstraint(err, UQ_ACTIVE_USER)) {
        throw new DomainException(
          MatchingErrors.TICKET_ALREADY_QUEUED,
          'Đã có 1 ticket đang chờ/đang ghép — 1 user chỉ ở trong 1 queue tại 1 thời điểm (docs/06)',
          HttpStatus.CONFLICT,
        );
      }
      throw err;
    }

    // Step 7: Sau commit, đẩy ticket vào Redis (ensureEnqueued: ZADD NX, đánh dấu shard active)
    // [REDIS] Redis lỗi ở bước này thì ticket "mồ côi" trong DB: client retry cùng key sẽ đẩy lại
    // vào Redis, sweeper expire là chốt chặn cuối.
    await this.ensureEnqueued(ticket);

    // [SERVICE] MatchingService.ensureEnqueued  ·  matching.service.ts L616–627
    const shard = shardKeyOfTicket(ticket);
    // [REDIS] NX: chỉ thêm nếu chưa có, để không đè score đã được boost (speed-up).
    await this.queue.enqueue(
      shard,
      String(ticketScore(ticket)),
      ticket.id,
      'NX',
    );

    // [REDIS] RedisMatchingQueue.enqueue  ·  redis-matching-queue.adapter.ts L12–19
    const score = String(
      ticket.enqueuedAt.getTime() + ticket.trustPenaltyMs - ticket.priorityBoostMs,
    );
    const ticketId = ticket.id;
    const mode = 'NX';
    await this.redis.zadd(shard, mode, score, ticketId);

    // SADD sau ZADD: matcher có thể vừa SREM shard rỗng giữa 2 lệnh, SADD sau cùng khôi phục lại
    // shard.
    await this.queue.markActive(shard);

    // [REDIS] RedisMatchingQueue.markActive  ·  redis-matching-queue.adapter.ts L38–40
    await this.redis.sadd(this.activeShardsKey, shard);

    this.matcherWakeup.notify();

    // Step 8: Trả ticket về controller (bọc thành TicketDto kèm giá speed-up)
    return ticket;
  }
}

// [WORKER] MatcherWorkerService.runOnce  ·  matcher-worker.service.ts L71–74, L172–191
@Injectable()
export class MatcherWorkerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(MatcherWorkerService.name);
  private readonly job = new ManagedInterval();
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
    private readonly activeShardsKey: string,
  ) {}

  // Chạy khi có enqueue (gom tín hiệu liên tiếp thành 1 batch) và theo interval dự phòng thưa;
  // nhiều pod chạy song song vẫn an toàn nhờ ZPOPMIN nguyên tử.
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {
      return withSpan(
        'litmatch.matching',
        'matching.matcher.tick',
        async (span) => {
          let matched = 0;

          // [REDIS] RedisMatchingQueue.listActiveShards  ·  redis-matching-queue.adapter.ts L34–36
          return this.redis.smembers(this.activeShardsKey);

          for (const shard of await this.queue.listActiveShards()) {
            matched += await this.drainShard(shard);

            // [WORKER] MatcherWorkerService.drainShard  ·  matcher-worker.service.ts L193–222
            const batchSize = this.config.getOrThrow('MATCHING_MATCHER_BATCH_SIZE', {
              infer: true,
            });
            let matched = 0;
            for (let i = 0; i < batchSize; i++) {

              // Step 9: Worker nền: ZPOPMIN lấy 2 vé điểm thấp nhất của shard (nguyên tử)
              // [REDIS] ZPOPMIN lấy 2 vé điểm thấp nhất (điểm nhỏ = ghép trước) trong 1 lệnh
              // nguyên tử. Redis đơn luồng nên không cần Lua, và 2 matcher không bao giờ lấy
              // trùng 1 vé.
              const popped = await this.queue.popMin(shard, 2);

              // [REDIS] RedisMatchingQueue.popMin  ·  redis-matching-queue.adapter.ts L25–32
              const count = 2;
              const values = await this.redis.zpopmin(shard, count);
              const entries: Array<[string, string]> = [];
              for (let index = 0; index + 1 < values.length; index += 2) {
                entries.push([values[index], values[index + 1]]);
              }
              return entries;

              if (popped.length === 0) {
                await this.cleanupShardIfEmpty(shard);

                // [WORKER] MatcherWorkerService.cleanupShardIfEmpty  ·  matcher-worker.service.ts L395–398
                await this.queue.unmarkActive(shard);

                // [REDIS] RedisMatchingQueue.unmarkActive  ·  redis-matching-queue.adapter.ts L42–44
                await this.redis.srem(this.activeShardsKey, shard);

                // [REDIS] RedisMatchingQueue.hasEntries  ·  redis-matching-queue.adapter.ts L46–48
                // Kiểm lại sau SREM: nếu có enqueue chen giữa thì khôi phục shard, không nuốt
                // mất.
                return (await this.redis.zcard(shard)) > 0;

                if (await this.queue.hasEntries(shard)) await this.queue.markActive(shard);

                break;
              }
              if (popped.length < 2) {
                await this.queue.enqueue(shard, popped[0][1], popped[0][0], 'NX');
                break;
              }
              const outcome = await this.tryPair(
                shard,
                { id: popped[0][0], score: Number(popped[0][1]) },
                { id: popped[1][0], score: Number(popped[1][1]) },
              );

              // [WORKER] MatcherWorkerService.tryPair  ·  matcher-worker.service.ts L228–392
              const a = { id: popped[0][0], score: Number(popped[0][1]) };
              const b = { id: popped[1][0], score: Number(popped[1][1]) };
              const result = await this.dataSource.transaction(async (manager) => {

                // Step 10: Khóa 2 vé FOR UPDATE, verify lại trong DB (giới tính, canPair)
                // [POSTGRES] Redis không phải nguồn sự thật: khóa 2 vé FOR UPDATE rồi verify lại
                // trong DB. Khóa theo thứ tự id cố định để không deadlock với writer khác.
                const tickets = await manager.find(MatchTicket, {
                  where: { id: In([a.id, b.id]) },
                  order: { id: 'ASC' },
                  lock: { mode: 'pessimistic_write' },
                });
                const byId = new Map(tickets.map((t) => [t.id, t]));
                const ta = byId.get(a.id);
                const tb = byId.get(b.id);

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

                const aValid = isValid(ta);
                const bValid = isValid(tb);

                // Giới tính lọc ở bước verify (đọc tươi từ users) chứ không chia shard theo giới
                // tính: shard nhân 3 và preference "any" phải quét nhiều shard. Đánh đổi: cặp
                // không khớp ở đầu shard chặn shard đó tới khi có người điểm thấp hơn vào hoặc
                // sweeper expire.
                const genderOk =
                  aValid &&
                  bValid &&
                  genderPreferenceAllows(
                    ta.genderPreference,
                    userById.get(tb.userId)?.gender,
                  ) &&
                  genderPreferenceAllows(
                    tb.genderPreference,
                    userById.get(ta.userId)?.gender,
                  );

                if (
                  aValid &&
                  bValid &&
                  ta.userId !== tb.userId &&
                  genderOk &&
                  (await this.interactionPolicy.canPair(ta.userId, tb.userId))
                ) {
                  const confirmedAt = new Date();

                  // Step 11: Ghép được: tạo MatchSession, cập nhật 2 vé, tạo thông báo
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
                  ta.status = MatchTicketStatus.Confirmed;
                  tb.status = MatchTicketStatus.Confirmed;
                  ta.sessionId = session.id;
                  tb.sessionId = session.id;
                  await manager.save([ta, tb]);
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

                const requeue: PoppedTicket[] = [];
                const entries: Array<[MatchTicket | undefined, boolean, PoppedTicket]> = [
                  [ta, aValid, a],
                  [tb, bValid, b],
                ];
                for (const [ticket, valid, poppedTicket] of entries) {
                  if (valid) {
                    requeue.push(poppedTicket);
                  } else if (ticket && ticket.status === MatchTicketStatus.Queued) {
                    ticket.status = MatchTicketStatus.Expired;
                    await manager.save(ticket);
                  }
                }
                return {
                  kind:
                    requeue.length === 2
                      ? ('requeued_pair' as const)
                      : ('dropped' as const),
                  requeue,
                  matchedPair: undefined,
                  matchNotifications: [] as Notification[],
                };
              });

              // Step 12: Sau commit: vé không ghép được về lại Redis, hoặc phát match.confirmed
              // [REDIS] Không ghép được thì trả vé còn hợp lệ về Redis với score gốc (không mất
              // lượt chờ); vé hỏng mà vẫn queued đã bị expire ở trên. Việc này làm SAU commit.
              if (result.requeue.length > 0) {
                for (const r of result.requeue) {
                  await this.queue.enqueue(shard, String(r.score), r.id, 'NX');
                }
                await this.queue.markActive(shard);
              }
              if (result.matchedPair) {
                for (const { matchType, waitSeconds } of result.matchedPair) {
                  this.metrics.observeMatched(matchType, waitSeconds);
                }
                // [REDIS] Realtime (pub/sub) làm SAU commit và là best-effort: client vẫn còn
                // poll GET /matching/tickets/:id.
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
              return result.kind;

              if (outcome === 'matched') matched += 1;
              // Cặp hợp lệ nhưng không ghép được (block/report, giới tính): dừng shard này, nếu
              // không sẽ pop lại đúng cặp đó và lặp vô hạn. Họ được thử lại khi có người thứ 3
              // vào hoặc sweeper expire.
              if (outcome === 'requeued_pair') break;
            }
            return matched;
          }
          span.setAttribute('matching.matched_pairs', matched);
          return matched;
        },
      );
    }, 0);
  }
}

// [POSTGRES] CREATE UNIQUE INDEX uq_match_tickets_active_user  ·  1752200000000-matching-core.ts L34–38
await queryRunner.query(`
  CREATE UNIQUE INDEX uq_match_tickets_active_user
    ON match_tickets(user_id)
    WHERE status IN ('queued','matched')
`);

// [REDIS] matching-redis.provider.ts  ·  matching-redis.provider.ts L21
// Worker chỉ quét set này thay vì quét cả keyspace Redis.
export const MATCHING_ACTIVE_SHARDS_KEY = 'matching:shards:active';

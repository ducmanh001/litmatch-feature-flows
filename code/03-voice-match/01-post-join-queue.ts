// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/matching/tickets
//  Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS] → [WORKER] → [POSTGRES] →
//             [REDIS]
//  Bảng       match_tickets, match_sessions, match_daily_quotas, users
//  Redis      matching:queue:{matchType}:{region}:{ageBand} (sorted set) ·
//             matching:shards:active (set) · pub/sub theo user cho event
//             match.confirmed
//  Liên quan  Economy (trừ Diamond), Notification, signaling-gateway (đẩy event cho
//             client). Tiếp theo: client gọi calling/match-sessions/:id/join (chưa
//             chép).
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// [CONTROLLER] matching.controller.ts · L46–72
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
}

// [SERVICE] matching.service.ts · L89–202
async joinQueue(
  user: AuthenticatedUser,
  dto: JoinQueueDto,
  idempotencyKey: string,
  quotaContext: GuestQuotaRequestContext = { ip: 'unknown' },
): Promise<MatchTicket> {
  const genderPreference = dto.genderPreference ?? GenderPreference.Any;
  const useDiamond = dto.useDiamond ?? false;
  const prefixedKey = joinIdempotencyKey(user.userId, idempotencyKey);

  let ticket: MatchTicket;
  try {
    // [POSTGRES] Ghi DB trong 1 transaction, Redis chỉ thêm SAU commit: Postgres là
    // nguồn sự thật của ticket, Redis chỉ là hàng đợi dẫn xuất.
    ticket = await this.dataSource.transaction(async (manager) => {
      const authorization = await this.guestQuota.authorize(
        manager,
        user.userId,
        quotaContext,
      );
      const profile = authorization.user;
      if (profile.status === UserStatus.Banned) {
        throw new DomainException(
          MatchingErrors.USER_BANNED,
          'Tài khoản bị khoá, không thể vào hàng đợi',
          HttpStatus.FORBIDDEN,
        );
      }

      // Nhận ra retry (cùng Idempotency-Key) TRƯỚC khi trừ quota, để retry không tốn
      // thêm lượt hay Diamond.
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

      const quota = await this.guestQuota.consumeForMatch(
        manager,
        authorization,
        dto.matchType,
        useDiamond,
      );
      // Trừ Diamond CÙNG transaction với insert ticket: insert lỗi thì tiền rollback
      // theo.
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
    // [POSTGRES] Mỗi user chỉ 1 vé đang chờ được chặn bằng unique index (xem bên dưới)
    // chứ không check-rồi-insert, vì 2 request đồng thời sẽ cùng lọt qua check. Dính
    // unique thì kiểm retry TRƯỚC, vì cả 2 unique có thể cùng dính; không phải retry
    // thì 409.
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

  // [REDIS] Redis lỗi ở bước này thì ticket "mồ côi" trong DB: client retry cùng key sẽ
  // đẩy lại vào Redis, sweeper expire là chốt chặn cuối.
  await this.ensureEnqueued(ticket);
  return ticket;
}

// [SERVICE] guest-match-quota.service.ts · L48–81, L132–202
async authorize(
  manager: EntityManager,
  userId: string,
  context: GuestQuotaRequestContext,
): Promise<GuestQuotaAuthorization> {
  // [POSTGRES] Khóa dòng user (FOR UPDATE) và đọc isGuest tươi từ DB: JWT cũ sau khi
  // nâng cấp tài khoản không né được giới hạn của guest.
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
}

async consumeForMatch(
  manager: EntityManager,
  authorization: GuestQuotaAuthorization,
  matchType: MatchType,
  useDiamond: boolean,
): Promise<MatchQuotaDecision> {
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
  // [POSTGRES] Đếm lượt miễn phí an toàn khi đồng thời: INSERT … ON CONFLICT DO NOTHING
  // tạo dòng đếm nếu chưa có, rồi SELECT … FOR UPDATE khóa dòng đó trước khi kiểm và
  // cộng 1, nên 2 request cùng lúc không cùng lọt qua giới hạn.
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
}

// [POSTGRES] 1752200000000-matching-core.ts · L34–38
await queryRunner.query(`
  CREATE UNIQUE INDEX uq_match_tickets_active_user
    ON match_tickets(user_id)
    WHERE status IN ('queued','matched')
`);

// [SERVICE] matching.service.ts · L616–627
async ensureEnqueued(ticket: MatchTicket): Promise<void> {
  const shard = shardKeyOfTicket(ticket);
  // [REDIS] NX: chỉ thêm nếu chưa có, để không đè score đã được boost (speed-up).
  await this.queue.enqueue(
    shard,
    String(ticketScore(ticket)),
    ticket.id,
    'NX',
  );
  // SADD sau ZADD: matcher có thể vừa SREM shard rỗng giữa 2 lệnh, SADD sau cùng khôi
  // phục lại shard.
  await this.queue.markActive(shard);
  this.matcherWakeup.notify();
}

// [REDIS] matching-redis.provider.ts · L21, L24–30, L32–36, L43–52
// Worker chỉ quét set này thay vì quét cả keyspace Redis.
export const MATCHING_ACTIVE_SHARDS_KEY = 'matching:shards:active';

export function matchingShardKey(
  matchType: string,
  region: string,
  ageBand: number,
): string {
  return `matching:queue:${matchType}:${region}:${ageBand}`;
}

export function shardKeyOfTicket(
  ticket: Pick<MatchTicket, 'matchType' | 'region' | 'ageBand'>,
): string {
  return matchingShardKey(ticket.matchType, ticket.region, ticket.ageBand);
}

export function ticketScore(
  ticket: Pick<
    MatchTicket,
    'enqueuedAt' | 'priorityBoostMs' | 'trustPenaltyMs'
  >,
): number {
  return (
    ticket.enqueuedAt.getTime() + ticket.trustPenaltyMs - ticket.priorityBoostMs
  );
}

// [REDIS] redis-matching-queue.adapter.ts · L12–19, L25–32, L34–36, L38–40, L42–44, L46–48
async enqueue(
  shard: string,
  score: string,
  ticketId: string,
  mode: 'NX' | 'XX',
): Promise<void> {
  await this.redis.zadd(shard, mode, score, ticketId);
}

async popMin(shard: string, count: number): Promise<Array<[string, string]>> {
  const values = await this.redis.zpopmin(shard, count);
  const entries: Array<[string, string]> = [];
  for (let index = 0; index + 1 < values.length; index += 2) {
    entries.push([values[index], values[index + 1]]);
  }
  return entries;
}

async listActiveShards(): Promise<string[]> {
  return this.redis.smembers(this.activeShardsKey);
}

async markActive(shard: string): Promise<void> {
  await this.redis.sadd(this.activeShardsKey, shard);
}

async unmarkActive(shard: string): Promise<void> {
  await this.redis.srem(this.activeShardsKey, shard);
}

async hasEntries(shard: string): Promise<boolean> {
  return (await this.redis.zcard(shard)) > 0;
}

// [WORKER] matcher-worker.service.ts · L172–191, L193–222, L228–392, L395–398
// Chạy khi có enqueue (gom tín hiệu liên tiếp thành 1 batch) và theo interval dự phòng
// thưa; nhiều pod chạy song song vẫn an toàn nhờ ZPOPMIN nguyên tử.
async runOnce(): Promise<number> {
  return this.job.runExclusive(async () => {
    return withSpan(
      'litmatch.matching',
      'matching.matcher.tick',
      async (span) => {
        let matched = 0;
        for (const shard of await this.queue.listActiveShards()) {
          matched += await this.drainShard(shard);
        }
        span.setAttribute('matching.matched_pairs', matched);
        return matched;
      },
    );
  }, 0);
}

private async drainShard(shard: string): Promise<number> {
  const batchSize = this.config.getOrThrow('MATCHING_MATCHER_BATCH_SIZE', {
    infer: true,
  });
  let matched = 0;
  for (let i = 0; i < batchSize; i++) {
    // [REDIS] ZPOPMIN lấy 2 vé điểm thấp nhất (điểm nhỏ = ghép trước) trong 1 lệnh
    // nguyên tử. Redis đơn luồng nên không cần Lua, và 2 matcher không bao giờ lấy
    // trùng 1 vé.
    const popped = await this.queue.popMin(shard, 2);
    if (popped.length === 0) {
      await this.cleanupShardIfEmpty(shard);
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
    if (outcome === 'matched') matched += 1;
    // Cặp hợp lệ nhưng không ghép được (block/report, giới tính): dừng shard này, nếu
    // không sẽ pop lại đúng cặp đó và lặp vô hạn. Họ được thử lại khi có người thứ 3
    // vào hoặc sweeper expire.
    if (outcome === 'requeued_pair') break;
  }
  return matched;
}

private async tryPair(
  shard: string,
  a: PoppedTicket,
  b: PoppedTicket,
): Promise<PairOutcome> {
  const result = await this.dataSource.transaction(async (manager) => {
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
    // tính: shard nhân 3 và preference "any" phải quét nhiều shard. Đánh đổi: cặp không
    // khớp ở đầu shard chặn shard đó tới khi có người điểm thấp hơn vào hoặc sweeper
    // expire.
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
    // [REDIS] Realtime (pub/sub) làm SAU commit và là best-effort: client vẫn còn poll
    // GET /matching/tickets/:id.
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
}

private async cleanupShardIfEmpty(shard: string): Promise<void> {
  await this.queue.unmarkActive(shard);
  // [REDIS] Kiểm lại sau SREM: nếu có enqueue chen giữa thì khôi phục shard, không nuốt
  // mất.
  if (await this.queue.hasEntries(shard)) await this.queue.markActive(shard);
}

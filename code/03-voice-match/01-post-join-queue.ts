// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/matching/tickets
//  Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → matching.controller.ts → matching.service.ts → Redis →
//             matcher-worker.service.ts → PostgreSQL
//  Bảng       match_tickets, match_sessions, match_daily_quotas, users
//  Redis      matching:queue:{matchType}:{region}:{ageBand} (sorted set) ·
//             matching:shards:active (set) · pub/sub theo user cho event
//             match.confirmed
//  Liên quan  Economy (trừ Diamond), Notification, signaling-gateway (đẩy event cho
//             client). Tiếp theo: client gọi calling/match-sessions/:id/join (chưa
//             chép).
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// matching.controller.ts · L46–72
/**
 * In:    user từ JWT, body { matchType, genderPreference?, useDiamond? }, header
 *        Idempotency-Key, X-Guest-Device-Token (chỉ guest)
 * Out:   TicketDto (status = queued)
 */
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

// matching.service.ts · L89–202, L616–627
/**
 * In:    user, dto { matchType, genderPreference?, useDiamond? }, idempotencyKey
 * Out:   MatchTicket (status = queued)
 *
 * Cơ chế và vì sao:
 * - DB trước, Redis sau. Postgres là nguồn sự thật của ticket, Redis chỉ là hàng đợi
 *   dẫn xuất, nên ghi DB trong 1 transaction rồi mới ZADD. Redis lỗi thì ticket nằm
 *   "mồ côi" trong DB: client retry cùng Idempotency-Key sẽ đẩy lại vào Redis,
 *   sweeper expire là chốt chặn cuối.
 * - Idempotency: key lưu ở cột unique của DB. Retry được nhận ra TRƯỚC khi trừ quota,
 *   nên không tốn lượt hay Diamond lần 2.
 * - 1 user chỉ 1 ticket đang chờ: chặn bằng partial unique index, không "check rồi
 *   insert" vì 2 request đồng thời sẽ cùng lọt qua bước check. Vi phạm thì 409.
 * - Lượt trả bằng Diamond (useDiamond): trừ tiền CÙNG transaction với insert ticket,
 *   insert lỗi thì tiền được rollback.
 * - authorize khóa dòng user (FOR UPDATE) và đọc isGuest tươi từ DB, nên JWT cũ sau
 *   khi nâng cấp tài khoản không né được giới hạn của guest.
 */
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

  await this.ensureEnqueued(ticket);
  return ticket;
}

/**
 * In:    ticket
 * Out:   void
 * Lưu ý: SADD đặt SAU ZADD vì matcher có thể vừa SREM shard rỗng giữa hai lệnh; SADD
 *        sau cùng khôi phục lại shard.
 */
async ensureEnqueued(ticket: MatchTicket): Promise<void> {
  const shard = shardKeyOfTicket(ticket);
  await this.queue.enqueue(
    shard,
    String(ticketScore(ticket)),
    ticket.id,
    'NX',
  );
  await this.queue.markActive(shard);
  this.matcherWakeup.notify();
}

// matching-redis.provider.ts · L21, L24–30, L32–36, L43–52
/**
 * Set các shard đang có người chờ: worker chỉ quét set này, không quét cả keyspace
 * Redis.
 */
export const MATCHING_ACTIVE_SHARDS_KEY = 'matching:shards:active';

/**
 * Mỗi shard là 1 sorted set riêng theo (matchType, region, ageBand): chỉ ghép người
 * cùng loại, cùng vùng, cùng nhóm tuổi.
 */
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

/**
 * In:    ticket
 * Out:   score trong sorted set, nhỏ hơn được ghép trước
 * Score = thời điểm vào hàng + phạt trust − boost speed-up.
 */
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

// redis-matching-queue.adapter.ts · L12–19, L25–32, L34–36, L38–40, L42–44, L46–48
/**
 * ZADD shard NX|XX score ticketId. NX chỉ thêm nếu chưa có, nên không đè score đã
 * được boost.
 */
async enqueue(
  shard: string,
  score: string,
  ticketId: string,
  mode: 'NX' | 'XX',
): Promise<void> {
  await this.redis.zadd(shard, mode, score, ticketId);
}

/** ZPOPMIN: lấy n ticket điểm thấp nhất khỏi shard trong 1 lệnh nguyên tử. */
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

// matcher-worker.service.ts · L172–191, L193–222, L228–255, L260–270, L272–296, L328–356, L358–365, L371–384, L390–392, L395–398
/**
 * In:    không có (job nền, không gắn với request nào)
 * Out:   số cặp ghép được trong lượt này
 * Lưu ý: chạy khi có enqueue (gom các tín hiệu liên tiếp thành 1 batch) và theo
 *        interval dự phòng thưa. Nhiều pod chạy song song vẫn an toàn nhờ ZPOPMIN
 *        nguyên tử.
 */
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

/**
 * In:    shard
 * Out:   số cặp ghép được
 *
 * Cơ chế và vì sao:
 * - ZPOPMIN shard 2 lấy 2 ticket điểm thấp nhất trong 1 lệnh nguyên tử (Redis đơn
 *   luồng nên không cần Lua), vì thế 2 matcher không bao giờ lấy trùng một ticket.
 * - Chỉ còn 1 ticket: trả lại với score cũ rồi chờ người tiếp theo.
 * - Cặp hợp lệ nhưng không ghép được (block/report hoặc giới tính không khớp): dừng
 *   shard này trong lượt hiện tại, nếu không sẽ pop lại đúng cặp đó và lặp vô hạn. Họ
 *   được thử lại khi có ứng viên thứ 3, hoặc sweeper expire khi quá hạn chờ.
 */
private async drainShard(shard: string): Promise<number> {
  const batchSize = this.config.getOrThrow('MATCHING_MATCHER_BATCH_SIZE', {
    infer: true,
  });
  let matched = 0;
  for (let i = 0; i < batchSize; i++) {
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
    if (outcome === 'requeued_pair') break;
  }
  return matched;
}

/**
 * In:    shard, 2 ticket vừa pop khỏi Redis (id + score)
 * Out:   "matched" | "requeued_pair" | "dropped"
 *
 * Cơ chế và vì sao:
 * - Redis không phải nguồn sự thật, nên verify lại trong 1 transaction Postgres: khóa
 *   cả 2 ticket FOR UPDATE theo thứ tự id cố định để không deadlock với writer khác.
 * - Điều kiện ghép: cả 2 còn queued, user còn active, khác người, giới tính khớp 2
 *   chiều (đọc tươi từ users trong chính transaction này), không block/report nhau.
 * - Đủ điều kiện: tạo MatchSession đã confirmed và chuyển 2 ticket sang confirmed
 *   trong cùng transaction.
 * - Không ghép được: ticket còn hợp lệ được đẩy lại Redis với score gốc (không mất
 *   lượt chờ); ticket hỏng mà vẫn queued thì expire.
 * - Giới tính lọc ở bước verify chứ không chia shard theo giới tính: chia shard sẽ
 *   nhân số shard lên 3 và preference "any" phải quét nhiều shard. Đánh đổi: cặp
 *   không khớp ở đầu shard chặn shard đó tới khi có người điểm thấp hơn vào hoặc
 *   sweeper expire.
 * - Redis (requeue) và realtime chỉ làm SAU khi transaction commit. Realtime là
 *   best-effort, client vẫn còn poll GET /matching/tickets/:id.
 */
private async tryPair(
  shard: string,
  a: PoppedTicket,
  b: PoppedTicket,
): Promise<PairOutcome> {
  const result = await this.dataSource.transaction(async (manager) => {
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
      // … tạo thông báo MatchConfirmed (cùng transaction) rồi return { kind: 'matched', matchedPair, … }
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

  if (result.requeue.length > 0) {
    for (const r of result.requeue) {
      await this.queue.enqueue(shard, String(r.score), r.id, 'NX');
    }
    await this.queue.markActive(shard);
  }
  if (result.matchedPair) {
    // … ghi metric thời gian chờ
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
    // … gửi push thông báo
  }
  return result.kind;
}

/**
 * In:    shard
 * Out:   void
 * Lưu ý: bỏ shard rỗng khỏi set active; sau SREM phải kiểm lại để không nuốt một
 *        enqueue chen giữa.
 */
private async cleanupShardIfEmpty(shard: string): Promise<void> {
  await this.queue.unmarkActive(shard);
  if (await this.queue.hasEntries(shard)) await this.queue.markActive(shard);
}

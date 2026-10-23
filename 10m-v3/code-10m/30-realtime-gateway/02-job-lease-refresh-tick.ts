//  JOB LeaseRefresher.tick (mỗi giây, một timer cho cả pod)
//  Mỗi giây gom các lease đến hạn của cả pod thành vài pipeline, thử cấp lại lease Redis báo mất, nhả lease cho socket đã ngắt, ngắt socket chỉ khi không còn chứng minh được lease.
//  Step 1: Một tick (mỗi giây) cho TOÀN BỘ socket của pod; không bao giờ chạy chồng nhau
//  Step 2: Chọn các lease đã đến hạn gia hạn (tối đa maxBatch)
//  Step 3: Gia hạn cả lô bằng pipeline EVALSHA theo shard
//  Step 4: Lease "lost" chưa chắc là hết quyền: thử CẤP LẠI trước khi ngắt socket
//  Step 5: Áp kết quả cho từng lease: ok thì gia hạn, lost hoặc hết biên an toàn thì ngắt, error thì thử lại dày hơn
//  Step 6: Socket đã ngắt trong lúc chờ nhưng lô vẫn cấp/gia hạn: nhả lease để không còn slot ma
//  Step 7: Lần thử kế tiếp không được muộn hơn mốc ngắt

// [GATEWAY] LeaseRefresher.tick · apps/signaling-gateway/src/app/lease-refresher.ts
export class LeaseRefresher {
  private readonly entries = new Map<string, LeaseEntry>();
  private readonly tickMs: number;
  private readonly maxBatch: number;
  private readonly safetyMarginMs: number;
  private readonly now: () => number;
  private readonly random: () => number;
  private ticking = false;
  // ConnectionQuotaService
  private readonly logger = new Logger(ConnectionQuotaService.name);
  private readonly clients: QuotaRedis[];
  private readonly topologyClient: Redis;
  private readonly topologyFingerprint: string;
  readonly leaseMs: number;
  readonly maxConnections: number;
  constructor(
    private readonly options: LeaseRefresherOptions,
    // ConnectionQuotaService
    config: ConfigService<SignalingEnv, true>,
  ) {
    // LeaseRefresher
    this.tickMs = options.tickMs ?? DEFAULT_TICK_MS;
    this.maxBatch = options.maxBatch ?? DEFAULT_MAX_BATCH;
    this.safetyMarginMs = Math.min(
      MAX_SAFETY_MARGIN_MS,
      Math.max(2 * this.tickMs, Math.floor(options.leaseMs / 10)),
    );
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    // ConnectionQuotaService
    this.leaseMs = config.getOrThrow('WS_CONNECTION_LEASE_MS', {
      infer: true,
    });
    this.maxConnections = config.getOrThrow('WS_MAX_CONNECTIONS_PER_USER', {
      infer: true,
    });
    const coordinatorUrl = config.getOrThrow('REDIS_URL', { infer: true });
    const urls = parseRealtimeRedisUrls(
      config.get('REALTIME_REDIS_URLS', { infer: true }),
      coordinatorUrl,
    );
    this.clients = urls.map((url, shard) => this.createClient(url, shard));
    this.topologyClient =
      this.clients[urls.indexOf(coordinatorUrl)] ??
      new Redis(coordinatorUrl, signalingRedisClientOptions());
    if (!this.clients.includes(this.topologyClient as QuotaRedis)) {
      this.topologyClient.on('error', (error) => {
        this.logger.warn(
          `Quota topology coordinator unavailable: ${String(error)}`,
        );
      });
    }
    const placements = urls.map((value) => {
      const url = new URL(value);
      url.searchParams.delete('username');
      url.searchParams.delete('password');
      url.searchParams.sort();
      return [
        url.protocol,
        url.hostname,
        url.port || '6379',
        url.pathname && url.pathname !== '/' ? url.pathname : '/0',
        url.search,
      ];
    });
    this.topologyFingerprint = createHash('sha256')
      .update(JSON.stringify(placements))
      .digest('hex');
  }

  // Step 1: Một tick (mỗi giây) cho TOÀN BỘ socket của pod; không bao giờ chạy chồng nhau
  async tick(): Promise<void> {
    if (this.ticking) return;
    this.ticking = true;
    try {
      const startedAt = this.now();
      const due: LeaseEntry[] = [];
      for (const entry of this.entries.values()) {

        // Step 2: Chọn các lease đã đến hạn gia hạn (tối đa maxBatch)
        if (entry.nextRefreshAt > startedAt) continue;
        due.push(entry);
        if (due.length >= this.maxBatch) break;
      }
      if (due.length === 0) return;

      let outcomes: RefreshOutcome[];
      try {

        // Step 3: Gia hạn cả lô bằng pipeline EVALSHA theo shard
        // Một timer + vài pipeline thay cho một setInterval và một lệnh Redis mỗi socket. Ở
        // 50.000 socket/pod đó là 50.000 timer và 50.000 round trip mỗi chu kỳ; pha gia hạn được
        // rải đều (jitter) để không dồn thành đợt sau một lần reconnect hàng loạt.
        outcomes = await this.options.quota.refreshMany(
          due.map(({ userId, leaseId }) => ({ userId, leaseId })),
        );
      } catch {
        outcomes = due.map(() => 'error');
      }

      // Step 4: Lease "lost" chưa chắc là hết quyền: thử CẤP LẠI trước khi ngắt socket
      const reacquired = await this.recoverLost(due, outcomes);

      // [GATEWAY] LeaseRefresher.recoverLost
      const reacquired = new Set<number>();
      // Redis restart hoặc failover mất dữ liệu làm MỌI lease biến mất cùng lúc: ngắt hết socket
      // khi đó là biến sự cố Redis thành reconnect storm. Cấp lại idempotent theo leaseId và kiểm
      // trần nên chỉ cấp lại khi slot còn trống; hết slot thì vẫn lost và socket bị ngắt.
      const reacquire = this.options.quota.reacquireMany;
      if (!reacquire) return reacquired;
      const lostIndexes: number[] = [];
      due.forEach((entry, index) => {
        if (
          outcomes[index] === 'lost' &&
          this.entries.get(entry.leaseId) === entry
        ) {
          lostIndexes.push(index);
        }
      });
      if (lostIndexes.length === 0) return reacquired;

      let recovered: RefreshOutcome[];
      try {
        recovered = await reacquire.call(
          this.options.quota,
          lostIndexes.map((index) => ({
            userId: due[index].userId,
            leaseId: due[index].leaseId,
          })),
        );
      } catch {
        recovered = lostIndexes.map(() => 'error');
      }
      lostIndexes.forEach((index, position) => {
        const outcome = recovered[position] ?? 'error';
        outcomes[index] = outcome;
        if (outcome === 'ok') reacquired.add(index);
      });
      return reacquired;

      // Step 5: Áp kết quả cho từng lease: ok thì gia hạn, lost hoặc hết biên an toàn thì ngắt,
      // error thì thử lại dày hơn
      this.apply(due, outcomes, reacquired, startedAt);

      // [GATEWAY] LeaseRefresher.apply
      const finishedAt = this.now();
      const counts: Record<LeaseOutcomeKind, number> = {
        ok: 0,
        lost: 0,
        error: 0,
        reacquired: 0,
      };
      const expired: LeaseEntry[] = [];
      const orphaned: LeaseRef[] = [];
      due.forEach((entry, index) => {
        if (this.entries.get(entry.leaseId) !== entry) {

          // Step 6: Socket đã ngắt trong lúc chờ nhưng lô vẫn cấp/gia hạn: nhả lease để không còn
          // slot ma
          // Socket ngắt và gateway đã release TRƯỚC khi lô của nó chạy mà lô vẫn cấp một lease
          // không ai giữ: slot ma + presence online giả tới hết TTL. Timeout cũng có thể che một
          // lần cấp thành công; release lease đã gỡ là idempotent kể cả khi thao tác lỗi chỉ là
          // một lần gia hạn.
          if (reacquired.has(index) || outcomes[index] === 'error') {
            orphaned.push({ userId: entry.userId, leaseId: entry.leaseId });
          }
          return;
        }
        const outcome = outcomes[index] ?? 'error';
        counts[reacquired.has(index) ? 'reacquired' : outcome] += 1;
        if (outcome === 'ok') {
          entry.validUntil = startedAt + this.options.leaseMs;
          entry.nextRefreshAt =
            finishedAt + this.jittered(this.options.refreshMs);
          return;
        }
        if (
          outcome === 'lost' ||
          finishedAt >= entry.validUntil - this.safetyMarginMs
        ) {
          expired.push(entry);
          return;
        }

        // Step 7: Lần thử kế tiếp không được muộn hơn mốc ngắt
        // Lỗi Redis KHÔNG ngắt ngay: thử lại dày hơn và chỉ ngắt khi lần gia hạn thành công cuối
        // đã đủ leaseMs (trừ biên an toàn). Lần thử kế tiếp không được muộn hơn mốc ngắt, nếu
        // không socket có thể sống thêm tới một chu kỳ sau khi Redis đã có thể cấp lại slot của
        // nó.
        entry.nextRefreshAt = Math.min(
          finishedAt +
            this.jittered(Math.min(this.options.refreshMs, MAX_ERROR_RETRY_MS)),
          entry.validUntil - this.safetyMarginMs,
        );
      });
      for (const outcome of ['ok', 'reacquired', 'lost', 'error'] as const) {
        if (counts[outcome] > 0)
          this.options.onOutcomes?.(outcome, counts[outcome]);
      }
      this.releaseOrphans(orphaned);

      // [GATEWAY] LeaseRefresher.releaseOrphans
      const leases = orphaned;
      const release = this.options.quota.releaseMany;
      if (leases.length === 0 || !release) return;
      void release
        .call(this.options.quota, leases)
        .catch((error: unknown) => this.report(error));

      for (const entry of expired) this.expire(entry);

      // [GATEWAY] LeaseRefresher.expire
      if (this.entries.get(entry.leaseId) !== entry) return;
      this.entries.delete(entry.leaseId);
      try {
        entry.onExpired();
      } catch (error) {
        this.report(error);
      }
    } finally {
      this.ticking = false;
    }
  }
}

// [REDIS] ConnectionQuotaService.refreshMany · apps/signaling-gateway/src/app/connection-quota.service.ts
@Injectable()
export class ConnectionQuotaService
  implements OnModuleInit, OnApplicationShutdown
{
  async refreshMany(leases: readonly LeaseRef[]): Promise<RefreshOutcome[]> {
    const results = await this.pipelineByShard(leases, (pipeline, lease) => {
      pipeline.quotaRefresh(
        this.key(lease.userId),
        realtimePresenceKey(lease.userId),
        lease.leaseId,
        this.leaseMs,
      );
    });

    // [REDIS] ConnectionQuotaService.pipelineByShard
    const enqueue = (pipeline, lease) => {
      pipeline.quotaRefresh(
        this.key(lease.userId),
        realtimePresenceKey(lease.userId),
        lease.leaseId,
        this.leaseMs,
      );
    };
    // `fill` để mảng không có "lỗ": Array.prototype.map bỏ qua ô trống nên lease không nhận được
    // kết quả sẽ không bao giờ được ánh xạ thành error.
    const results = new Array<[Error | null, unknown] | undefined>(
      leases.length,
    ).fill(undefined);
    const indexesByShard = new Map<number, number[]>();
    leases.forEach((lease, index) => {
      const shard = realtimeShardIndex(lease.userId, this.clients.length);
      const indexes = indexesByShard.get(shard);
      if (indexes) indexes.push(index);
      else indexesByShard.set(shard, [index]);
    });
    await Promise.all(
      [...indexesByShard].map(async ([shard, indexes]) => {
        for (
          let offset = 0;
          offset < indexes.length;
          offset += PIPELINE_CHUNK
        ) {
          const chunk = indexes.slice(offset, offset + PIPELINE_CHUNK);
          try {
            const pipeline = this.clients[
              shard
            ].pipeline() as unknown as QuotaPipeline;
            for (const index of chunk) enqueue(pipeline, leases[index]);
            const executed = await pipeline.exec();
            executed?.forEach((entry, position) => {
              results[chunk[position]] = entry;
            });
          } catch {
          }
        }
      }),
    );
    return results;

    return results.map((result): RefreshOutcome => {
      if (result === undefined || result[0]) return 'error';
      return result[1] === 1 ? 'ok' : 'lost';
    });
  }
}

// [REDIS] ConnectionQuotaService.reacquireMany
@Injectable()
export class ConnectionQuotaService
  implements OnModuleInit, OnApplicationShutdown
{
  async reacquireMany(leases: readonly LeaseRef[]): Promise<RefreshOutcome[]> {
    const results = await this.pipelineByShard(leases, (pipeline, lease) => {
      pipeline.quotaAcquire(
        this.key(lease.userId),
        realtimePresenceKey(lease.userId),
        lease.leaseId,
        this.leaseMs,
        this.maxConnections,
      );
    });
    return results.map((result): RefreshOutcome => {
      if (result === undefined || result[0]) return 'error';
      return result[1] === 1 ? 'ok' : 'lost';
    });
  }
}

// [REDIS] ConnectionQuotaService.releaseMany
@Injectable()
export class ConnectionQuotaService
  implements OnModuleInit, OnApplicationShutdown
{
  async releaseMany(leases: readonly LeaseRef[]): Promise<void> {
    await this.pipelineByShard(leases, (pipeline, lease) => {
      pipeline.quotaRelease(
        this.key(lease.userId),
        realtimePresenceKey(lease.userId),
        lease.leaseId,
      );
    });
  }
}

// [REDIS] connection-quota.service.ts
const REFRESH_SCRIPT = `
local now = redis.call('TIME')
local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[1], now_ms + tonumber(ARGV[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`;

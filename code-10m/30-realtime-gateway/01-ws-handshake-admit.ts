//  WS socket.io: handshake middleware → connect
//  Handshake xác thực JWT, xin lease slot (tối đa N socket/user) trên Redis và SUBSCRIBE kênh của chính user; chỉ khi xong hết mới đăng ký gia hạn lease.
//  Step 1: Middleware handshake: mọi thứ xong TRƯỚC khi socket được coi là đã kết nối
//  Step 2: Xác thực JWT trong handshake.auth.token
//  Step 3: Xin lease slot của user trên Redis (vượt trần thì từ chối)
//  Step 4: SUBSCRIBE kênh Redis của riêng user này TRƯỚC khi handshake xong
//  Step 5: Subscribe lỗi: trả lease ngay vì socket sẽ không bao giờ connect
//  Step 6: Script Lua nguyên tử: dọn lease hết hạn, kiểm trần, ghi lease + presence cùng TTL
//  Step 7: Socket đầu tiên của user trên pod mới thật sự SUBSCRIBE; các socket sau chỉ tăng bộ đếm
//  Step 8: Sự kiện connect (handshake đã xong): đăng ký lease với bộ gia hạn và vào room của user
//  Step 9: Hết hạn lease mà không gia hạn được thì ĐÓNG transport để client tự reconnect có backoff

// [GATEWAY] SignalingGateway.admit · apps/signaling-gateway/src/app/signaling.gateway.ts
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private readonly logger = new Logger(SignalingGateway.name);
  private fanout?: RealtimeFanout;
  private refresher?: LeaseRefresher;
  private readonly activeLeases = new Map<
    string,
    { userId: string; client: Socket }
  >();
  private shuttingDown = false;
  private readonly previousJwtSecret: string | undefined;
  // ConnectionQuotaService
  private readonly clients: QuotaRedis[];
  readonly leaseMs: number;
  readonly maxConnections: number;
  // RealtimeFanout
  private readonly shards: FanoutShard[];
  constructor(
    private readonly config: ConfigService<SignalingEnv, true>,
    private readonly connectionQuota: ConnectionQuotaService,
    private readonly metrics: SignalingMetrics,
    // ConnectionQuotaService
    config: ConfigService<SignalingEnv, true>,
    // RealtimeFanout
    private readonly options: RealtimeFanoutOptions,
  ) {
    // SignalingGateway
    this.previousJwtSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // ConnectionQuotaService
    this.leaseMs = config.getOrThrow('WS_CONNECTION_LEASE_MS', {
      infer: true,
    });
    this.maxConnections = config.getOrThrow('WS_MAX_CONNECTIONS_PER_USER', {
      infer: true,
    });
    const urls = parseRealtimeRedisUrls(
      config.get('REALTIME_REDIS_URLS', { infer: true }),
      config.getOrThrow('REDIS_URL', { infer: true }),
    );
    this.clients = urls.map((url, shard) => this.createClient(url, shard));
    // RealtimeFanout
    const createClient =
      options.createClient ??
      ((url: string) =>
        new Redis(url, {
          ...signalingRedisClientOptions(),
          autoResubscribe: false,
        }));
    this.shards = options.urls.map((url, index) => {
      const shard: FanoutShard = {
        index,
        redis: createClient(url),
        users: new Map(),
        ready: false,
        dirty: false,
        reconciling: undefined,
        retryTimer: undefined,
      };
      this.attach(shard);
      return shard;
    });
  }

  // Step 1: Middleware handshake: mọi thứ xong TRƯỚC khi socket được coi là đã kết nối
  async admit(client: Socket): Promise<void> {
    if (this.shuttingDown) throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);

    // Step 2: Xác thực JWT trong handshake.auth.token
    // Handshake fail closed: JWT sai/thiếu thì ném Unauthorized (client thử làm mới phiên rồi kết
    // nối lại), quota hoặc Redis lỗi thì ném CONNECTION_QUOTA_UNAVAILABLE — client reconnect có
    // backoff, không "mở cửa" cho socket chưa được admit.
    await this.authenticate(client);
    const userId = (client.data as SocketData).userId;
    if (!userId) throw new Error(RealtimeConnectionErrors.Unauthorized);

    // Step 3: Xin lease slot của user trên Redis (vượt trần thì từ chối)
    if (!(await this.admitConnection(client, userId))) {
      throw new Error(CONNECTION_LIMIT_ERROR);
    }
    try {

      // Step 4: SUBSCRIBE kênh Redis của riêng user này TRƯỚC khi handshake xong
      await this.admitFanout(client, userId);
    } catch (error) {

      // Step 5: Subscribe lỗi: trả lease ngay vì socket sẽ không bao giờ connect
      this.abortQuotaAdmission(client);
      throw error;
    }
  }
}

// [GATEWAY] SignalingGateway.admitConnection
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private async admitConnection(
    client: Socket,
    userId: string,
  ): Promise<boolean> {
    const leaseId = randomUUID();
    const data = client.data as SocketData;
    let transportClosed = client.conn.readyState === 'closed';
    const cleanup = () => {
      transportClosed = true;
      if (data.quotaLeaseId !== leaseId) return;
      data.quotaLeaseId = undefined;
      void this.connectionQuota.release(userId, leaseId).catch(() => {
      });
    };
    client.conn.once('close', cleanup);

    let admitted: boolean;
    try {

      // Step 6: Script Lua nguyên tử: dọn lease hết hạn, kiểm trần, ghi lease + presence cùng TTL
      // [REDIS] Lỗi/timeout ở đây có thể xảy ra SAU khi script đã chạy phía Redis (lease đã được
      // cấp mà ta không biết) nên luôn thử release; nếu Redis quá tệ thì TTL của lease là lưới an
      // toàn cuối.
      admitted = await this.connectionQuota.acquire(userId, leaseId);
    } catch (error) {
      client.conn.off('close', cleanup);
      void this.connectionQuota.release(userId, leaseId).catch(() => {
      });
      captureSentryException(error, 'signaling-connection-quota');
      throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR, { cause: error });
    }
    if (!admitted) {
      client.conn.off('close', cleanup);
      return false;
    }
    data.quotaLeaseId = leaseId;
    data.quotaPreconnectCleanup = cleanup;
    if (
      transportClosed ||
      client.conn.readyState === 'closed' ||
      this.shuttingDown
    ) {
      cleanup();
      throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
    }
    return true;
  }
}

// [REDIS] ConnectionQuotaService.acquire · apps/signaling-gateway/src/app/connection-quota.service.ts
@Injectable()
export class ConnectionQuotaService
  implements OnModuleInit, OnApplicationShutdown
{
  async acquire(userId: string, leaseId: string): Promise<boolean> {
    const result = await this.clientFor(userId).quotaAcquire(
      this.key(userId),
      realtimePresenceKey(userId),
      leaseId,
      this.leaseMs,
      this.maxConnections,
    );
    return result === 1;
  }
}

// [GATEWAY] SignalingGateway.abortQuotaAdmission · apps/signaling-gateway/src/app/signaling.gateway.ts
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private abortQuotaAdmission(client: Socket): void {
    const data = client.data as SocketData;
    const cleanup = data.quotaPreconnectCleanup;
    if (!cleanup) return;
    client.conn.off('close', cleanup);
    data.quotaPreconnectCleanup = undefined;
    cleanup();
  }
}

// [GATEWAY] SignalingGateway.admitFanout
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private async admitFanout(client: Socket, userId: string): Promise<void> {
    const data = client.data as SocketData;
    const fanout = this.fanout;
    if (!fanout) throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
    // Đặt cờ TRƯỚC khi await: transport đóng trong lúc chờ Redis vẫn thấy cờ và hoàn tác được;
    // mỗi socket giữ quyền sở hữu đúng một lần đếm để đóng giữa chừng không làm giảm đếm hai lần
    // (mất subscription của socket khác cùng user).
    data.fanoutUserId = userId;
    const cleanup = () => this.releaseFanout(client);
    data.fanoutPreconnectCleanup = cleanup;
    client.conn.once('close', cleanup);
    try {

      // Step 7: Socket đầu tiên của user trên pod mới thật sự SUBSCRIBE; các socket sau chỉ tăng
      // bộ đếm
      await fanout.subscribeUser(userId);
    } catch (error) {
      client.conn.off('close', cleanup);
      data.fanoutPreconnectCleanup = undefined;
      this.releaseFanout(client);
      captureSentryException(error, 'signaling-fanout-subscribe');
      throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR, { cause: error });
    }
    if (
      !data.fanoutUserId ||
      client.conn.readyState === 'closed' ||
      this.shuttingDown
    ) {
      client.conn.off('close', cleanup);
      data.fanoutPreconnectCleanup = undefined;
      this.releaseFanout(client);
      throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
    }
  }
}

// [REDIS] RealtimeFanout.subscribeUser · apps/signaling-gateway/src/app/realtime-fanout.ts
export class RealtimeFanout {
  async subscribeUser(userId: string): Promise<void> {
    if (this.options.mode !== 'user') return;
    const shard = this.shardFor(userId);
    let entry = shard.users.get(userId);
    if (entry) {
      entry.count += 1;
      // Promise cũ đã resolve từ trước một lần Redis mất kết nối: subscription thật có thể không
      // còn. SUBSCRIBE idempotent nên gửi lại; reject khi connection còn đứt (offline queue tắt)
      // và handshake fail closed đúng như socket đầu tiên.
      if (entry.failed || !shard.ready || shard.redis.status !== 'ready') {
        this.requestSubscription(shard, userId, entry);
      }
    } else {
      entry = { count: 1, ready: Promise.resolve(), failed: false };
      shard.users.set(userId, entry);
      this.requestSubscription(shard, userId, entry);
    }
    await entry.ready;
  }
}

// [REDIS] RealtimeFanout.requestSubscription
export class RealtimeFanout {
  private requestSubscription(
    shard: FanoutShard,
    userId: string,
    entry: UserSubscription,
  ): void {
    // Ghi kết quả vào entry; chỉ lần gửi MỚI NHẤT được tính là trạng thái của entry. Promise đã
    // reject không bao giờ được await lại như "đã sẵn sàng".
    const attempt = shard.redis
      .subscribe(realtimeUserChannel(userId))
      .then(() => undefined);
    entry.ready = attempt;
    entry.failed = false;
    attempt.catch(() => {
      if (entry.ready === attempt) entry.failed = true;
    });
  }
}

// [GATEWAY] SignalingGateway.handleConnection · apps/signaling-gateway/src/app/signaling.gateway.ts
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{

  // Step 8: Sự kiện connect (handshake đã xong): đăng ký lease với bộ gia hạn và vào room của
  // user
  handleConnection(client: Socket): void {
    const data = client.data as SocketData;
    const userId = data.userId;
    if (!userId) {
      client.disconnect(true);
      return;
    }
    if (data.quotaPreconnectCleanup) {
      client.conn.off('close', data.quotaPreconnectCleanup);
      data.quotaPreconnectCleanup = undefined;
    }
    if (data.fanoutPreconnectCleanup) {
      client.conn.off('close', data.fanoutPreconnectCleanup);
      data.fanoutPreconnectCleanup = undefined;
    }
    if (!this.trackLease(client, userId)) return;
    void client.join(userRoom(userId));
    data.connectionCounted = true;
    this.metrics.connectionOpened();
    this.logger.debug(`User ${userId} connected: ${client.id}`);
  }
}

// [GATEWAY] SignalingGateway.trackLease
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private trackLease(client: Socket, userId: string): boolean {
    const leaseId = (client.data as SocketData).quotaLeaseId;
    if (!leaseId) {
      client.disconnect(true);
      return false;
    }
    this.activeLeases.set(leaseId, { userId, client });

    // Step 9: Hết hạn lease mà không gia hạn được thì ĐÓNG transport để client tự reconnect có
    // backoff
    // Lease hết hạn mà không gia hạn được: ĐÓNG transport thay vì client.disconnect(true).
    // disconnect gửi DISCONNECT cuối cùng của namespace nên client không tự kết nối lại; đóng
    // transport thì Socket.IO tự reconnect với backoff của client.
    this.refresher?.add(leaseId, userId, () => client.conn.close(true));
    return true;
  }
}

// [REDIS] connection-quota.service.ts · apps/signaling-gateway/src/app/connection-quota.service.ts
const ACQUIRE_SCRIPT = `
local now = redis.call('TIME')
local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
local expires_at = now_ms + tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], expires_at, ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  redis.call('ZADD', KEYS[2], expires_at, ARGV[1])
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
  return 1
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('ZADD', KEYS[1], expires_at, ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], expires_at, ARGV[1])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`;

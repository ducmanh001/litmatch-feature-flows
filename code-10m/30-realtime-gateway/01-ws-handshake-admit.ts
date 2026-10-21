//  WS socket.io: handshake middleware → connect
//  Giữ chỗ admission (trần socket và trần handshake, không hàng chờ), xác thực JWT, xin lease slot (tối đa N socket/user) trên Redis và SUBSCRIBE kênh của chính user; chỉ khi xong hết mới đăng ký gia hạn lease.
//  Step 1: Middleware handshake: mọi thứ xong TRƯỚC khi socket được coi là đã kết nối
//  Step 2: Giữ chỗ (một slot kết nối + một slot handshake) TRƯỚC mọi việc nặng; hết chỗ thì từ chối ngay, không xếp hàng
//  Step 3: Xác thực JWT trong handshake.auth.token
//  Step 4: Xin lease slot của user trên Redis (vượt trần thì từ chối)
//  Step 5: Script Lua nguyên tử: dọn lease hết hạn, kiểm trần, ghi lease + presence cùng TTL
//  Step 6: SUBSCRIBE kênh Redis của riêng user này TRƯỚC khi handshake xong
//  Step 7: Socket đầu tiên của user trên pod mới thật sự SUBSCRIBE; các socket sau chỉ tăng bộ đếm
//  Step 8: Subscribe lỗi: trả lease ngay vì socket sẽ không bao giờ connect
//  Step 9: Chỉ nhả slot handshake khi chuỗi JWT/Redis bất đồng bộ đã xong, kể cả khi transport đã đóng giữa chừng
//  Step 10: Sự kiện connect (handshake đã xong): đăng ký lease với bộ gia hạn và vào room của user
//  Step 11: Hết hạn lease mà không gia hạn được thì ĐÓNG transport để client tự reconnect có backoff

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
  private readonly admissionBudget: ConnectionAdmissionBudget;
  private readonly admittedClients = new Set<Socket>();
  private readonly outboundMaxPackets: number;
  private readonly outboundMaxBytes: number;
  private readonly outboundTotal: OutboundTotalBudget;
  private readonly previousJwtSecret: string | undefined;
  // ConnectionAdmissionBudget
  private readonly reservations = new Set<ConnectionAdmission>();
  private readonly handshakes = new Set<ConnectionAdmission>();
  private closed = false;
  // ConnectionQuotaService
  private readonly logger = new Logger(ConnectionQuotaService.name);
  private readonly clients: QuotaRedis[];
  private readonly topologyClient: Redis;
  private readonly topologyFingerprint: string;
  readonly leaseMs: number;
  readonly maxConnections: number;
  // RealtimeFanout
  private readonly shards: FanoutShard[];
  constructor(
    private readonly config: ConfigService<SignalingEnv, true>,
    private readonly connectionQuota: ConnectionQuotaService,
    private readonly metrics: SignalingMetrics,
    // ConnectionAdmissionBudget
    private readonly maxConnections: number,
    private readonly maxHandshakes: number,
    // ConnectionQuotaService
    config: ConfigService<SignalingEnv, true>,
    // RealtimeFanout
    private readonly options: RealtimeFanoutOptions,
  ) {
    // SignalingGateway
    this.admissionBudget = new ConnectionAdmissionBudget(
      config.get('WS_MAX_CONNECTIONS_PER_PROCESS', { infer: true }) ?? 2_000,
      config.get('WS_MAX_CONCURRENT_HANDSHAKES', { infer: true }) ?? 64,
    );
    this.previousJwtSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    this.outboundMaxPackets =
      config.get('WS_OUTBOUND_MAX_PACKETS', { infer: true }) ?? 128;
    this.outboundMaxBytes =
      config.get('WS_OUTBOUND_MAX_BYTES', { infer: true }) ?? 1_048_576;
    this.outboundTotal = new OutboundTotalBudget(
      config.get('WS_OUTBOUND_TOTAL_MAX_PACKETS', { infer: true }) ?? 16_384,
      config.get('WS_OUTBOUND_TOTAL_MAX_BYTES', { infer: true }) ?? 134_217_728,
      (result) => {
        this.metrics.outboundPressure(result);
        if (result === 'evicted') this.metrics.slowConsumer();
      },
    );
    this.metrics.bindOutboundGauges(this.outboundTotal);
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

    // Step 2: Giữ chỗ (một slot kết nối + một slot handshake) TRƯỚC mọi việc nặng; hết chỗ thì từ
    // chối ngay, không xếp hàng
    const admission = this.admissionBudget.reserve();

    // [GATEWAY] ConnectionAdmissionBudget.reserve · apps/signaling-gateway/src/app/connection-admission-budget.ts
    if (
      // Hai trần hữu hạn và KHÔNG có hàng chờ: socket đã xác thực cộng socket đang chờ tối đa
      // WS_MAX_CONNECTIONS_PER_PROCESS (mặc định 2.000) và handshake bất đồng bộ tối đa
      // WS_MAX_CONCURRENT_HANDSHAKES (mặc định 64). Hết chỗ thì từ chối ngay bằng lỗi có thể thử
      // lại CONNECTION_QUOTA_UNAVAILABLE để client giữ backoff có jitter và REST dự phòng. Hai số
      // này là điểm khởi đầu thận trọng, chưa phải năng lực đã đo.
      this.closed ||
      this.connections >= this.maxConnections ||
      this.pendingHandshakes >= this.maxHandshakes
    ) {
      return undefined;
    }

    const reservations = this.reservations;
    const handshakes = this.handshakes;
    const admission: ConnectionAdmission = {
      get active() {
        return reservations.has(admission);
      },
      releaseConnection() {
        reservations.delete(admission);
      },
      finishHandshake() {
        handshakes.delete(admission);
      },
    };
    reservations.add(admission);
    handshakes.add(admission);
    return admission;

    if (!admission) throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
    const data = client.data as SocketData;
    const cleanup = () => {
      admission.releaseConnection();
      this.admittedClients.delete(client);
      client.conn.off('close', cleanup);
      if (data.capacityCleanup === cleanup) data.capacityCleanup = undefined;
    };
    data.capacityCleanup = cleanup;
    this.admittedClients.add(client);
    // Slot kết nối chỉ được nhả khi transport đóng hoặc handshake bị từ chối, qua đúng một hàm
    // cleanup tự gỡ listener của chính nó; nhờ vậy socket chưa bao giờ connect (handshake chưa
    // xong) vẫn không để lại slot.
    client.conn.once('close', cleanup);
    try {
      if (client.conn.readyState === 'closed') {
        throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
      }

      // Step 3: Xác thực JWT trong handshake.auth.token
      // Handshake fail closed: JWT sai/thiếu thì ném Unauthorized (client thử làm mới phiên rồi
      // kết nối lại), quota hoặc Redis lỗi thì ném CONNECTION_QUOTA_UNAVAILABLE — client
      // reconnect có backoff, không "mở cửa" cho socket chưa được admit.
      await this.authenticate(client);
      if (!admission.active || this.shuttingDown) {
        throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
      }
      const userId = data.userId;
      if (!userId) throw new Error(RealtimeConnectionErrors.Unauthorized);

      // Step 4: Xin lease slot của user trên Redis (vượt trần thì từ chối)

      // [GATEWAY] SignalingGateway.admitConnection
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

        // Step 5: Script Lua nguyên tử: dọn lease hết hạn, kiểm trần, ghi lease + presence cùng
        // TTL
        // [REDIS] Lỗi/timeout ở đây có thể xảy ra SAU khi script đã chạy phía Redis (lease đã
        // được cấp mà ta không biết) nên luôn thử release; nếu Redis quá tệ thì TTL của lease là
        // lưới an toàn cuối.
        admitted = await this.connectionQuota.acquire(userId, leaseId);

        // [REDIS] ConnectionQuotaService.acquire · apps/signaling-gateway/src/app/connection-quota.service.ts
        const result = await this.clientFor(userId).quotaAcquire(
          this.key(userId),
          realtimePresenceKey(userId),
          leaseId,
          this.leaseMs,
          this.maxConnections,
        );
        return result === 1;
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

      if (!(await this.admitConnection(client, userId))) {
        throw new Error(CONNECTION_LIMIT_ERROR);
      }

      // Step 6: SUBSCRIBE kênh Redis của riêng user này TRƯỚC khi handshake xong
      await this.admitFanout(client, userId);

      // [GATEWAY] SignalingGateway.admitFanout
      const data = client.data as SocketData;
      const fanout = this.fanout;
      if (!fanout) throw new Error(CONNECTION_QUOTA_UNAVAILABLE_ERROR);
      // Đặt cờ TRƯỚC khi await: transport đóng trong lúc chờ Redis vẫn thấy cờ và hoàn tác được;
      // mỗi socket giữ quyền sở hữu đúng một lần đếm để đóng giữa chừng không làm giảm đếm hai
      // lần (mất subscription của socket khác cùng user).
      data.fanoutUserId = userId;
      const cleanup = () => this.releaseFanout(client);
      data.fanoutPreconnectCleanup = cleanup;
      client.conn.once('close', cleanup);
      try {

        // Step 7: Socket đầu tiên của user trên pod mới thật sự SUBSCRIBE; các socket sau chỉ
        // tăng bộ đếm
        await fanout.subscribeUser(userId);

        // [REDIS] RealtimeFanout.subscribeUser · apps/signaling-gateway/src/app/realtime-fanout.ts
        if (this.options.mode !== 'user') return;
        const shard = this.shardFor(userId);
        let entry = shard.users.get(userId);
        if (entry) {
          entry.count += 1;
          // Promise cũ đã resolve từ trước một lần Redis mất kết nối: subscription thật có thể
          // không còn. SUBSCRIBE idempotent nên gửi lại; reject khi connection còn đứt (offline
          // queue tắt) và handshake fail closed đúng như socket đầu tiên.
          if (entry.failed || !shard.ready || shard.redis.status !== 'ready') {
            this.requestSubscription(shard, userId, entry);

            // [REDIS] RealtimeFanout.requestSubscription
            // Ghi kết quả vào entry; chỉ lần gửi MỚI NHẤT được tính là trạng thái của entry.
            // Promise đã reject không bao giờ được await lại như "đã sẵn sàng".
            const attempt = shard.redis
              .subscribe(realtimeUserChannel(userId))
              .then(() => undefined);
            entry.ready = attempt;
            entry.failed = false;
            attempt.catch(() => {
              if (entry.ready === attempt) entry.failed = true;
            });
          }
        } else {
          entry = { count: 1, ready: Promise.resolve(), failed: false };
          shard.users.set(userId, entry);
          this.requestSubscription(shard, userId, entry);
        }
        await entry.ready;
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
    } catch (error) {

      // Step 8: Subscribe lỗi: trả lease ngay vì socket sẽ không bao giờ connect
      this.abortQuotaAdmission(client);

      // [GATEWAY] SignalingGateway.abortQuotaAdmission
      const data = client.data as SocketData;
      const cleanup = data.quotaPreconnectCleanup;
      if (!cleanup) return;
      client.conn.off('close', cleanup);
      data.quotaPreconnectCleanup = undefined;
      cleanup();

      cleanup();
      throw error;
    } finally {

      // Step 9: Chỉ nhả slot handshake khi chuỗi JWT/Redis bất đồng bộ đã xong, kể cả khi
      // transport đã đóng giữa chừng
      // Transport đóng không được nhả slot handshake khi việc JWT/Redis bất đồng bộ còn đang
      // chạy: nếu nhả sớm, client reconnect liên tục sẽ lách trần handshake đồng thời trong lúc
      // việc cũ vẫn chiếm Redis.
      admission.finishHandshake();
    }
  }
}

// [GATEWAY] SignalingGateway.handleConnection
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{

  // Step 10: Sự kiện connect (handshake đã xong): đăng ký lease với bộ gia hạn và vào room của
  // user
  handleConnection(client: Socket): void {
    if (this.shuttingDown || client.conn.readyState === 'closed') {
      this.abortQuotaAdmission(client);
      this.releaseFanout(client);
      this.releaseCapacity(client);
      client.conn.close(true);
      return;
    }
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

    // [GATEWAY] SignalingGateway.trackLease
    const leaseId = (client.data as SocketData).quotaLeaseId;
    if (!leaseId) {
      client.disconnect(true);
      return false;
    }
    this.activeLeases.set(leaseId, { userId, client });

    // Step 11: Hết hạn lease mà không gia hạn được thì ĐÓNG transport để client tự reconnect có
    // backoff
    // Lease hết hạn mà không gia hạn được: ĐÓNG transport thay vì client.disconnect(true).
    // disconnect gửi DISCONNECT cuối cùng của namespace nên client không tự kết nối lại; đóng
    // transport thì Socket.IO tự reconnect với backoff của client.
    this.refresher?.add(leaseId, userId, () => client.conn.close(true));
    return true;

    if (!this.trackLease(client, userId)) return;
    void client.join(userRoom(userId));
    data.connectionCounted = true;
    this.metrics.connectionOpened();
    this.logger.debug(`User ${userId} connected: ${client.id}`);
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

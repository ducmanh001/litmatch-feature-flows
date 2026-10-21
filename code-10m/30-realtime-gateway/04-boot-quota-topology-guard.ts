//  BOOT ConnectionQuotaService.onModuleInit (lúc gateway khởi động)
//  Mỗi gateway tính fingerprint của danh sách shard Redis có thứ tự và đăng ký nó một lần trên Redis điều phối; fingerprint khác thì từ chối khởi động để rolling update không tạo hai quota độc lập cho cùng một user.
//  Step 1: Khởi động gateway: đăng ký topology song song với việc chờ một shard quota sẵn sàng
//  Step 2: Đăng ký fingerprint topology trên Redis điều phối (chờ kết nối tới Redis điều phối trước)
//  Step 3: Script Lua nguyên tử: chưa có thì ghi, có rồi thì so sánh
//  Step 4: Fingerprint khác: từ chối khởi động thay vì để hai quota cùng tồn tại

// [GATEWAY] ConnectionQuotaService.onModuleInit · apps/signaling-gateway/src/app/connection-quota.service.ts
@Injectable()
export class ConnectionQuotaService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly logger = new Logger(ConnectionQuotaService.name);
  private readonly clients: QuotaRedis[];
  private readonly topologyClient: Redis;
  private readonly topologyFingerprint: string;
  readonly leaseMs: number;
  readonly maxConnections: number;
  constructor(
    config: ConfigService<SignalingEnv, true>,
  ) {
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

  // Step 1: Khởi động gateway: đăng ký topology song song với việc chờ một shard quota sẵn sàng
  async onModuleInit(): Promise<void> {
    try {
      await Promise.all([
        this.registerTopology(),
        // Một shard chết lúc khởi động không được chặn cả pod (rolling restart đúng lúc một shard
        // failover sẽ crash-loop): user của shard đó fail closed lúc handshake, user của các
        // shard còn lại vẫn được phục vụ. Chỉ khi KHÔNG shard nào lên được mới là lỗi khởi động.
        Promise.any(this.clients.map((client) => this.waitReady(client))),
      ]);

      // [GATEWAY] ConnectionQuotaService.registerTopology
      // Step 2: Đăng ký fingerprint topology trên Redis điều phối (chờ kết nối tới Redis điều
      // phối trước)
      await this.waitReady(this.topologyClient);

      // Step 3: Script Lua nguyên tử: chưa có thì ghi, có rồi thì so sánh
      // Fingerprint gồm thứ tự, endpoint, database và mọi query option của URL trừ
      // username/password, vì ioredis cho phép ?db=, ?host=, ?keyPrefix= đổi chỗ đặt dữ liệu; đổi
      // mật khẩu thì không đổi fingerprint.
      const registered = await this.topologyClient.eval(
        REGISTER_TOPOLOGY_SCRIPT,
        1,
        TOPOLOGY_KEY,
        this.topologyFingerprint,
      );

      // Step 4: Fingerprint khác: từ chối khởi động thay vì để hai quota cùng tồn tại
      // Đổi số hoặc thứ tự shard làm modulo remap: gateway cũ và mới cùng chạy sẽ cấp hai quota
      // độc lập cho cùng một user. Từ chối khởi động; chuyển topology phải theo thủ tục drain
      // toàn bộ gateway rồi mới xoá key.
      if (registered !== 1) {
        throw new Error(
          'Realtime quota topology changed: drain all gateways, wait for connection leases to expire, then perform the documented topology cutover',
        );
      }
    } catch (error) {
      throw error instanceof AggregateError ? error.errors[0] : error;
    }
  }
}

// [REDIS] connection-quota.service.ts
const REGISTER_TOPOLOGY_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return current == ARGV[1] and 1 or 0
`;

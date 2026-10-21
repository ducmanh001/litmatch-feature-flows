//  GET /health/ready
//  Gộp các probe song song, cache kết quả 1 giây và chỉ giữ tối đa một SQL probe chưa xong, để kubelet thăm dò dày hoặc DB chậm không làm đầy pool.
//  Step 1: Endpoint readiness: 503 khi dependency chưa sẵn sàng
//  Step 2: Kết quả còn mới (1 giây): dùng lại, không kiểm tra lại
//  Step 3: Đã có một lượt kiểm tra đang chạy: các lời gọi đồng thời chờ chung kết quả
//  Step 4: Chỉ một SQL probe chưa xong tại một thời điểm
//  Step 5: Redis PING có deadline 1 giây
//  Step 6: Ít nhất một dependency down thì trả 503 để pod bị rút khỏi Service

// [CONTROLLER] HealthController.ready · app/health.controller.ts
@Controller('health')
@SkipRequestConcurrency()
@SkipThrottle()
export class HealthController {
  // ReadinessService
  private readonly redis: Redis;
  private inFlight: Promise<ReadinessResult> | undefined;
  private cached: { expiresAt: number; result: ReadinessResult } | undefined;
  private postgresProbe: Promise<'up' | 'down'> | undefined;
  constructor(
    private readonly readiness: ReadinessService,
    // ReadinessService
    @InjectDataSource() private readonly dataSource: DataSource,
    config: ConfigService<CoreApiEnv, true>,
  ) {
    // ReadinessService
    this.redis = new Redis(config.getOrThrow('REDIS_URL', { infer: true }), {
      lazyConnect: true,
      connectTimeout: 1_000,
      commandTimeout: 1_000,
      maxRetriesPerRequest: 0,
      enableOfflineQueue: false,
    });
    this.redis.on('error', () => undefined);
  }

  @Public()
  @Get('ready')

  // Step 1: Endpoint readiness: 503 khi dependency chưa sẵn sàng
  async ready(
    @Res({ passthrough: true }) response: Response,
  ): Promise<ReadinessResult> {
    const result = await this.readiness.check();

    // [SERVICE] ReadinessService.check · app/readiness.service.ts
    // Step 2: Kết quả còn mới (1 giây): dùng lại, không kiểm tra lại
    if (this.cached && Date.now() < this.cached.expiresAt) {
      return Promise.resolve(this.cached.result);
    }

    // Step 3: Đã có một lượt kiểm tra đang chạy: các lời gọi đồng thời chờ chung kết quả
    if (this.inFlight) return this.inFlight;
    // Probe bỏ qua throttle chung của request nên phải tự gộp và cache cục bộ: nhiều kubelet hoặc
    // proxy cùng thăm dò một lúc (thường chung địa chỉ IP khi số pod tăng) chỉ tạo một lượt kiểm
    // tra, và không bị vòng lặp 429.
    this.inFlight = this.probe()
      .then((result) => {
        this.cached = { result, expiresAt: Date.now() + PROBE_CACHE_MS };
        return result;
      })
      .finally(() => {
        this.inFlight = undefined;
      });

    // [SERVICE] ReadinessService.probe
    const [postgres, redis] = await Promise.all([
      this.checkPostgres(),
      this.checkRedis(),
    ]);

    // [POSTGRES] ReadinessService.checkPostgres
    try {

      // Step 4: Chỉ một SQL probe chưa xong tại một thời điểm
      // Deadline HTTP không huỷ được SQL đang chạy: giữ lại chính truy vấn đó cho tới khi nó
      // xong, để các probe quá hạn liên tiếp không mở thêm truy vấn và làm đầy pool kết nối.
      this.postgresProbe ??= this.dataSource
        .query('SELECT 1')
        .then(
          () => 'up' as const,
          () => 'down' as const,
        )
        .finally(() => {
          this.postgresProbe = undefined;
        });
      return await this.withTimeout(this.postgresProbe, PROBE_TIMEOUT_MS);

      // [REDIS] ReadinessService.withTimeout
      const promise = this.postgresProbe;
      const timeoutMs = PROBE_TIMEOUT_MS;
      let timeout: NodeJS.Timeout | undefined;
      const deadline = new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error('readiness timeout')),
          timeoutMs,
        );
        timeout.unref();
      });

      try {
        return await Promise.race([promise, deadline]);
      } finally {
        if (timeout) clearTimeout(timeout);
      }
    } catch {
      return 'down';
    }

    // [REDIS] ReadinessService.checkRedis
    try {
      if (this.redis.status === 'wait') await this.redis.connect();

      // Step 5: Redis PING có deadline 1 giây
      // Kết nối readiness không có hàng đợi offline và không thử lại lệnh (enableOfflineQueue:
      // false, maxRetriesPerRequest: 0), nên khi Redis mất kết nối probe thất bại ngay và hiện
      // thành down thay vì treo tới hết deadline.
      await this.withTimeout(this.redis.ping(), PROBE_TIMEOUT_MS);
      return 'up';
    } catch {
      return 'down';
    }

    const ready = postgres === 'up' && redis === 'up';

    return {
      status: ready ? 'ok' : 'unavailable',
      checks: { postgres, redis },
    };

    return this.inFlight;

    // Step 6: Ít nhất một dependency down thì trả 503 để pod bị rút khỏi Service
    // 503 để kubelet và load balancer ngừng gửi traffic vào pod này; chỉ khi cả Postgres lẫn
    // Redis đều up pod mới được coi là ready.
    if (result.status !== 'ok') response.status(HttpStatus.SERVICE_UNAVAILABLE);
    return result;
  }
}

//  POST /api/v1/economy/vip/purchase
//  Trừ Diamond và gia hạn VIP trong một transaction của sổ kép: idempotent theo key, khóa ví FOR UPDATE, ghi outbox.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, planId trong body, Idempotency-Key từ header)
//  Step 2: Đọc giá và số ngày của gói từ catalog server
//  Step 3: Dựng bút toán Nợ ví / Có system_revenue, gọi LedgerService.record
//  Step 4: Kiểm sổ kép: mỗi currency tổng Nợ phải bằng tổng Có
//  Step 5: Băm request; key đã có thì trả kết quả cũ, không mở transaction
//  Step 6: Trong transaction, chèn giao dịch (trùng key là replay)
//  Step 7: Tạo hoặc lấy tài khoản sổ, khóa ví FOR UPDATE theo thứ tự userId
//  Step 8: Ghi bút toán, trừ số dư (kiểm đủ tiền) và ghi outbox
//  Step 9: Phần việc riêng của VIP cùng transaction: gia hạn, ghi outbox
//  Step 10: Thua race cùng key: đọc lại dòng của bên thắng, trả như replay
//  Step 11: Service trả hạng và hạn VIP (replay thì không gia hạn lần 2)
//  Step 12: Worker nền: khóa lô outbox chưa gửi, gửi sang Kafka

// [CONTROLLER] EconomyController.purchaseVip · economy/economy.controller.ts
@Controller('economy')
export class EconomyController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // OutboxRelayService
  private readonly logger = new Logger(OutboxRelayService.name);
  private readonly job = new ManagedInterval();
  // KafkaEventBusAdapter
  private readonly logger = new Logger(KafkaEventBusAdapter.name);
  private readonly kafka: Kafka;
  private producer: Producer | null = null;
  private producerConnect: Promise<Producer> | null = null;
  private readonly publishTotal;
  private readonly publishFailureTotal;
  private readonly consumerFailureTotal;
  private readonly deadLetterTotal;
  // ManagedInterval
  private running = false;
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // EconomyService
    @InjectRepository(Wallet) private readonly walletRepo: Repository<Wallet>,
    @InjectRepository(VipPlan) private readonly planRepo: Repository<VipPlan>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
    // OutboxRelayService
    private readonly config: ConfigService<CoreApiEnv, true>,
    // KafkaEventBusAdapter
    @Inject(METRICS_METER) meter: Meter,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // LedgerService
    super();
    // KafkaEventBusAdapter
    super();
    const brokers = this.config
      .getOrThrow('KAFKA_BROKERS', { infer: true })
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean);
    this.kafka = new Kafka({
      clientId: EVENT_BUS_CLIENT_ID,
      brokers,
      connectionTimeout: this.config.getOrThrow(
        'EVENT_BUS_KAFKA_REQUEST_TIMEOUT_MS',
        {
          infer: true,
        },
      ),
      requestTimeout: this.config.getOrThrow(
        'EVENT_BUS_KAFKA_REQUEST_TIMEOUT_MS',
        {
          infer: true,
        },
      ),
      retry: {
        retries: this.config.getOrThrow('EVENT_BUS_KAFKA_RETRIES', {
          infer: true,
        }),
      },
    });

    this.publishTotal = meter.createCounter('event_bus_publish_total', {
      description: 'Events sent to the broker by outcome and topic.',
    });
    this.publishFailureTotal = meter.createCounter(
      'event_bus_publish_failure_total',
      {
        description: 'Event publish failures handed back to the outbox relay.',
      },
    );
    this.consumerFailureTotal = meter.createCounter(
      'event_bus_consumer_failure_total',
      {
        description:
          'Consumer handler failures before retry or dead-letter handling.',
      },
    );
    this.deadLetterTotal = meter.createCounter('event_bus_dead_letter_total', {
      description: 'Events moved to a broker dead-letter topic.',
    });
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, planId trong body,
  // Idempotency-Key từ header)

  // [CONTROLLER] Bootstrap request configuration · main.ts
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
      bufferLogs: true,
      rawBody: true,
      bodyParser: false,
    });

  // [CONTROLLER] app.useBodyParser
  app.useBodyParser('json');

  // [CONTROLLER] app.useBodyParser
  app.useBodyParser('urlencoded', { extended: true });

  // [CONTROLLER] app.useBodyParser
  app.useBodyParser('json', { type: 'application/webhook+json' });

  // [CONTROLLER] app.set
  app.set(
      'trust proxy',
      config.getOrThrow('HTTP_TRUST_PROXY_HOPS', { infer: true }),
    );

  // [CONTROLLER] app.use
  app.use(helmet());

  // [CONTROLLER] app.use
  app.use(cookieParser());

  // [CONTROLLER] app.use
  app.use(createHttpMetricsMiddleware(app.get<Meter>(METRICS_METER)));

  // [SERVICE] createHttpMetricsMiddleware · libs/observability/src/lib/http-metrics.ts
  const meter = app.get<Meter>(METRICS_METER);
  const histogram = meter.createHistogram('http_request_duration_seconds', {
    description:
      'Thời gian xử lý HTTP request tính bằng giây, theo method/route/status_code',
    unit: 's',
    advice: { explicitBucketBoundaries: DEFAULT_BUCKETS_SECONDS },
  });

  return function httpMetricsMiddleware(
    req: Request,
    res: Response,
    next: NextFunction,
  ): void {
    const startedAt = process.hrtime.bigint();
    res.on('finish', () => {
      const elapsedSeconds =
        Number(process.hrtime.bigint() - startedAt) / 1_000_000_000;
      histogram.record(elapsedSeconds, {
        method: req.method,
        route: req.route?.path ? String(req.route.path) : 'unmatched',
        status_code: String(res.statusCode),
      });
    });
    next();
  };

  // [CONTROLLER] Bootstrap request configuration · main.ts
  const corsOrigins = parseCorsOrigins(
      config.getOrThrow('CORS_ORIGINS', { infer: true }),
    );

  // [SERVICE] parseCorsOrigins · common/cors/cors-origins.ts
  const raw = config.getOrThrow('CORS_ORIGINS', { infer: true });
  const origins = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  for (const origin of origins) {
    let parsed: URL;
    try {
      parsed = new URL(origin);
    } catch {
      throw new Error(
        `CORS_ORIGINS có origin không hợp lệ: "${origin}" (phải dạng http(s)://host[:port])`,
      );
    }
    if (!/^https?:$/.test(parsed.protocol)) {
      throw new Error(`CORS_ORIGINS chỉ chấp nhận http/https: "${origin}"`);
    }
    if (parsed.pathname !== '/' || parsed.search !== '' || parsed.hash !== '') {
      throw new Error(
        `CORS_ORIGINS phải là origin thuần (không path/query/hash): "${origin}"`,
      );
    }
  }

  return origins;

  // [CONTROLLER] app.enableCors · main.ts
  app.enableCors({
      origin: corsOrigins.length > 0 ? corsOrigins : false,
      credentials: true,
    });

  // [CONTROLLER] app.useGlobalPipes
  app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );

  // [CONTROLLER] app.useGlobalInterceptors
  app.useGlobalInterceptors(new ClassSerializerInterceptor(app.get(Reflector)));

  // [CONTROLLER] ThrottlerModule configuration · app/app.module.ts
  ThrottlerModule.forRootAsync({
        inject: [ConfigService],
        useFactory: (config: ConfigService<CoreApiEnv, true>) => {
          const sharedStorage =
            config.getOrThrow('THROTTLE_STORAGE', { infer: true }) === 'redis';
          if (
            sharedStorage &&
            config.getOrThrow('NODE_ENV', { infer: true }) === 'production' &&
            config.getOrThrow('HTTP_TRUST_PROXY_HOPS', { infer: true }) === 0
          ) {
            new Logger('ThrottlerConfig').warn(
              'THROTTLE_STORAGE=redis nhưng HTTP_TRUST_PROXY_HOPS=0: nếu có ingress/LB phía trước thì req.ip là IP proxy và mọi người dùng chung một bucket rate limit. Đặt HTTP_TRUST_PROXY_HOPS đúng số proxy tin cậy.',
            );
          }
          return {
            throttlers: [
              {
                ttl:
                  config.getOrThrow('THROTTLE_TTL_SECONDS', { infer: true }) *
                  1000,
                limit: config.getOrThrow('THROTTLE_LIMIT', { infer: true }),
              },
            ],
            ...(sharedStorage
              ? {
                  storage: new RedisThrottlerStorage(
                    createCoreRedisClient(
                      config.getOrThrow('REDIS_URL', { infer: true }),
                    ),
                  ),
                }
              : {}),
          };

          // [REDIS] createCoreRedisClient · common/redis/core-redis-client.ts
          const redisUrl = config.getOrThrow('REDIS_URL', { infer: true });
          const existing = sharedClientsByUrl.get(redisUrl);
          if (existing && existing.client.status !== 'end') {
            existing.references += 1;
            return existing.client;
          }

          const client = new Redis(redisUrl, {
            connectTimeout: 1_000,
            commandTimeout: 1_000,
            maxRetriesPerRequest: 1,
            enableOfflineQueue: false,
            retryStrategy: coreRedisReconnectDelay,
          });
          client.on('error', () => undefined);
          const entry = { client, references: 1 };
          sharedClientsByUrl.set(redisUrl, entry);
          sharedEntryByClient.set(client, entry);
          return client;
        },
      })

  // [CONTROLLER] RedisThrottlerStorage.increment · common/throttle/redis-throttler-storage.ts
  if (this.now() < this.redisRetryAt) {
    return this.fallback.increment(
      key,
      ttl,
      limit,
      blockDuration,
      throttlerName,
    );
  }

  try {
    const slot = `throttle:{${throttlerName}:${key}}`;
    const [totalHits, windowTtlMs, blocked, blockTtlMs] =
      (await this.redis.eval(
        THROTTLE_INCREMENT_LUA,
        2,
        `${slot}:hits`,
        `${slot}:block`,
        String(ttl),
        String(limit),
        String(blockDuration),
      )) as ThrottleEvalResult;
    return {
      totalHits,
      timeToExpire: Math.ceil(windowTtlMs / 1000),
      isBlocked: blocked === 1,
      timeToBlockExpire: Math.ceil(blockTtlMs / 1000),
    };
  } catch (error) {
    this.redisRetryAt = this.now() + REDIS_RETRY_AFTER_MS;
    this.warnRedisUnavailable(error);

    // [REDIS] RedisThrottlerStorage.warnRedisUnavailable
    const now = this.now();
    if (now - this.lastWarnAt < WARN_INTERVAL_MS) return;
    this.lastWarnAt = now;
    this.logger.warn(
      `Redis throttler storage lỗi, dùng bộ đếm trong bộ nhớ của pod này tạm thời: ${String(error)}`,
    );

    return this.fallback.increment(
      key,
      ttl,
      limit,
      blockDuration,
      throttlerName,
    );
  }

  // [CONTROLLER] APP_GUARD: ThrottlerGuard · app/app.module.ts
  { provide: APP_GUARD, useClass: ThrottlerGuard }

  // [CONTROLLER] APP_GUARD: JwtAuthGuard
  { provide: APP_GUARD, useClass: JwtAuthGuard }

  // [CONTROLLER] JwtAuthGuard.canActivate · common/guards/jwt-auth.guard.ts
  const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
    context.getHandler(),
    context.getClass(),
  ]);
  if (isPublic) return true;

  const req = context
    .switchToHttp()
    .getRequest<Request & { user?: AuthenticatedUser }>();
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith('Bearer ')) {
    throw new UnauthorizedException('Thiếu access token');
  }

  try {
    const payload = await verifyJwtWithRotation<AccessTokenPayload>(
      this.jwtService,
      authHeader.slice('Bearer '.length),
      this.previousSecret,
    );

    // [SERVICE] verifyJwtWithRotation · common/guards/jwt-rotation.ts
    const jwtService = this.jwtService;
    const token = authHeader.slice('Bearer '.length);
    const previousSecret = this.previousSecret;
    try {
      return await jwtService.verifyAsync<T>(token);
    } catch (error) {

      // [SERVICE] isInvalidSignature
      return (
        error instanceof Error &&
        error.name === 'JsonWebTokenError' &&
        error.message === 'invalid signature'
      );

      if (!previousSecret || !isInvalidSignature(error)) throw error;
      return jwtService.verifyAsync<T>(token, { secret: previousSecret });
    }

    req.user = {
      userId: payload.sub,
      isGuest: payload.isGuest === true,
      role: payload.role,
    };
    return true;
  } catch {
    throw new UnauthorizedException(
      'Access token không hợp lệ hoặc đã hết hạn',
    );
  }

  // [CONTROLLER] APP_GUARD: RolesGuard · app/app.module.ts
  { provide: APP_GUARD, useClass: RolesGuard }

  // [CONTROLLER] RolesGuard.canActivate · common/guards/roles.guard.ts
  const requiredRoles = this.reflector.getAllAndOverride<Role[] | undefined>(
    ROLES_KEY,
    [context.getHandler(), context.getClass()],
  );
  if (requiredRoles === undefined) return true;

  const req = context
    .switchToHttp()
    .getRequest<Request & { user?: AuthenticatedUser }>();
  if (req.user === undefined || !requiredRoles.includes(req.user.role)) {
    throw new DomainException(
      CommonErrors.FORBIDDEN,
      'Không đủ quyền truy cập',
      HttpStatus.FORBIDDEN,
    );
  }
  return true;

  // [CONTROLLER] APP_INTERCEPTOR: ResponseEnvelopeInterceptor · app/app.module.ts
  { provide: APP_INTERCEPTOR, useClass: ResponseEnvelopeInterceptor }

  // [CONTROLLER] ResponseEnvelopeInterceptor.intercept · common/interceptors/response-envelope.interceptor.ts
  const request = context
    .switchToHttp()
    .getRequest<{ originalUrl?: string; url?: string }>();
  markRuntimeActivity(request.originalUrl ?? request.url ?? '');

  // [SERVICE] markRuntimeActivity · common/runtime/runtime-activity.ts
  const path = request.originalUrl ?? request.url ?? '';
  // now = Date.now()

  // [SERVICE] isProbePath
  const pathname = path.split('?', 1)[0] ?? path;
  return (
    pathname === '/health' ||
    pathname.startsWith('/health/') ||
    pathname.endsWith('/health') ||
    pathname.includes('/health/') ||
    pathname === '/metrics' ||
    pathname.endsWith('/metrics') ||
    pathname.startsWith('/swagger')
  );

  if (isProbePath(path)) return;
  lastMeaningfulRequestAt = now;
  meaningfulRequestCount += 1;

  return next.handle().pipe(
    map((value) => {
      if (value && typeof value === 'object' && 'data' in (value as object)) {
        return value as unknown as ApiResponse<T>;
      }
      return { data: value };
    }),
  );

  // [CONTROLLER] APP_FILTER: GlobalExceptionFilter · app/app.module.ts
  { provide: APP_FILTER, useClass: GlobalExceptionFilter }

  // [CONTROLLER] GlobalExceptionFilter.catch · common/filters/global-exception.filter.ts
  const ctx = host.switchToHttp();
  const res = ctx.getResponse<Response>();
  const req = ctx.getRequest<Request & { id?: string }>();
  const traceId = req.id ?? 'unknown';
  const locale = resolveApiLocale(req.headers['accept-language']);

  // [SERVICE] resolveApiLocale · common/i18n/error-message.localizer.ts
  const acceptLanguage = req.headers['accept-language'];
  const value = Array.isArray(acceptLanguage)
    ? acceptLanguage.join(',')
    : acceptLanguage;
  if (value === undefined) return DEFAULT_LOCALE;

  for (const item of value.split(',')) {
    const language = item.trim().split(';', 1)[0]?.toLowerCase();
    if (language === 'en' || language?.startsWith('en-')) return 'en';
    if (language === 'vi' || language?.startsWith('vi-')) return 'vi';
  }
  return DEFAULT_LOCALE;

  let status: number;
  let code: string;
  let message: string;
  let details: Record<string, unknown> | undefined;

  if (exception instanceof DomainException) {
    status = exception.httpStatus;
    code = exception.code;
    message = exception.message;
    details = exception.details;
  } else if (exception instanceof HttpException) {
    status = exception.getStatus();
    const body = exception.getResponse();
    message =
      typeof body === 'string'
        ? body
        : ((body as { message?: string | string[] }).message?.toString() ??
          exception.message);
    code = this.mapHttpStatusToCode(status);

    // [SERVICE] GlobalExceptionFilter.mapHttpStatusToCode
    switch (status) {
      case HttpStatus.UNAUTHORIZED:
        return CommonErrors.UNAUTHORIZED;
      case HttpStatus.FORBIDDEN:
        return CommonErrors.FORBIDDEN;
      case HttpStatus.NOT_FOUND:
        return CommonErrors.ROUTE_NOT_FOUND;
      case HttpStatus.TOO_MANY_REQUESTS:
        return CommonErrors.RATE_LIMITED;
      case HttpStatus.BAD_REQUEST:
        return CommonErrors.VALIDATION_FAILED;
      default:
        return CommonErrors.INTERNAL_ERROR;
    }

    if (status === HttpStatus.BAD_REQUEST && typeof body === 'object') {
      const raw = (body as { message?: string | string[] }).message;
      if (Array.isArray(raw)) {
        code = CommonErrors.VALIDATION_FAILED;
        message = 'Dữ liệu đầu vào không hợp lệ';
        details = { violations: raw };
      }
    }
  } else {
    status = HttpStatus.INTERNAL_SERVER_ERROR;
    code = CommonErrors.INTERNAL_ERROR;
    message = 'Lỗi hệ thống, thử lại sau';
    this.logger.error(
      {
        traceId,
        err: exception instanceof Error ? exception.stack : String(exception),
      },
      'Unhandled exception',
    );
    captureSentryException(exception, traceId);

    // [SERVICE] captureSentryException · libs/observability/src/lib/sentry.ts
    Sentry.withScope((scope) => {
      scope.setTag('trace_id', traceId);
      Sentry.captureException(exception);
    });
  }

  const payload: ApiErrorBody = {
    error: {
      code,
      message: localizeErrorMessage(code, locale, message),
      traceId,
      ...(details ? { details } : {}),
    },
  };

  // [SERVICE] localizeErrorMessage · common/i18n/error-message.localizer.ts
  const vietnameseMessage = message;
  if (locale === 'vi') return vietnameseMessage;
  return EN_ERROR_MESSAGES[code] ?? 'The request could not be completed.';

  res.status(status).json(payload);

  @Post('vip/purchase')
  @HttpCode(HttpStatus.OK)
  purchaseVip(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Body() dto: PurchaseVipDto,
    @IdempotencyKey() idempotencyKey: string,

    // [CONTROLLER] IdempotencyKey · common/decorators/idempotency-key.decorator.ts
    export const IdempotencyKey = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): string => {
        const request = ctx
          .switchToHttp()
          .getRequest<{ headers: Record<string, unknown> }>();
        return extractIdempotencyKey(request.headers);

        // [SERVICE] extractIdempotencyKey
        const headers = request.headers;
        const raw = headers[IDEMPOTENCY_KEY_HEADER];
        const key = Array.isArray(raw) ? raw[0] : raw;
        if (typeof key !== 'string' || key.trim() === '') {
          throw new DomainException(
            CommonErrors.IDEMPOTENCY_KEY_MISSING,
            'Thiếu header Idempotency-Key',
            HttpStatus.BAD_REQUEST,
          );
        }
        if (key.length > MAX_KEY_LENGTH) {
          throw new DomainException(
            CommonErrors.IDEMPOTENCY_KEY_INVALID,
            `Idempotency-Key dài quá ${MAX_KEY_LENGTH} ký tự`,
            HttpStatus.BAD_REQUEST,
          );
        }
        return key;
      },
    );
  ): Promise<VipPurchaseResultDto> {
    return this.economyService.purchaseVip(
      user.userId,
      dto.planId,
      idempotencyKey,
    );

    // [SERVICE] EconomyService.purchaseVip · economy/economy.service.ts
    const userId = user.userId;
    const planId = dto.planId;

    // Step 2: Đọc giá và số ngày của gói từ catalog server
    // Giá và số ngày đọc từ catalog server; client chỉ gửi planId, không gửi giá.
    const plan = await this.planRepo.findOneBy({ id: planId, active: true });
    if (!plan) {
      throw new DomainException(
        EconomyErrors.VIP_PLAN_UNKNOWN,
        `Gói VIP ${planId} không tồn tại`,
        HttpStatus.NOT_FOUND,
      );
    }

    let newExpiry = new Date();

    // Step 3: Dựng bút toán Nợ ví / Có system_revenue, gọi LedgerService.record
    const result = await this.ledger.record({
      type: TransactionType.VipPurchase,
      idempotencyKey: `vip:${userId}:${idempotencyKey}`,
      actorUserId: userId,
      // Snapshot giá và thời hạn đã áp dụng; đổi catalog sau này không diễn giải lại giao dịch
      // cũ.
      metadata: {
        planId,
        tier: plan.tier,
        days: plan.days,
        priceDiamond: plan.priceDiamond,
      },
      entries: [
        {
          accountKind: LedgerAccountKind.UserWallet,
          userId,
          direction: LedgerDirection.Debit,
          amount: BigInt(plan.priceDiamond),
          currency: LedgerCurrency.Diamond,
        },
        {
          accountKind: LedgerAccountKind.SystemRevenue,
          direction: LedgerDirection.Credit,
          amount: BigInt(plan.priceDiamond),
          currency: LedgerCurrency.Diamond,
        },
      ],
      // Chạy ở cuối recordInManager, khi ví đã khóa FOR UPDATE: gia hạn VIP và outbox nằm CÙNG
      // transaction với bút toán, nên lỗi thì rollback cả tiền.
      withinTransaction: async (manager) => {
        const wallet = await manager
          .getRepository(Wallet)
          .findOneByOrFail({ userId });
        const base =
          wallet.vipExpiresAt && wallet.vipExpiresAt > new Date()
            ? wallet.vipExpiresAt
            : new Date();
        // Gia hạn cộng dồn: còn hạn thì cộng thêm vào hạn cũ, hết hạn thì tính từ bây giờ.
        newExpiry = new Date(base.getTime() + plan.days * 24 * 3600 * 1000);
        await manager.update(
          Wallet,
          { userId },
          { vipTier: plan.tier, vipExpiresAt: newExpiry },
        );
        await manager.save(
          manager.create(OutboxEvent, {
            topic: ECONOMY_EVENTS_TOPIC,
            eventType: 'economy.vip.purchased',
            payload: {
              version: 1,
              userId,
              planId,
              tier: plan.tier,
              vipExpiresAt: newExpiry.toISOString(),
            },
          }),
        );
      },
    });

    // [SERVICE] LedgerService.record · economy/services/ledger.service.ts
    // input = đối số 1 của lời gọi ở trên

    // Step 4: Kiểm sổ kép: mỗi currency tổng Nợ phải bằng tổng Có
    this.validateInput(input);

    // [SERVICE] LedgerService.validateInput
    this.validateEntries(input.entries);

    // [SERVICE] LedgerService.validateEntries
    const entries = input.entries;
    if (entries.length < 2)
      throw new Error('Giao dịch double-entry cần tối thiểu 2 bút toán');
    const sums = new Map<string, bigint>();
    for (const e of entries) {
      if (e.amount <= 0n)
        throw new Error('Bút toán phải có amount nguyên dương');
      if (USER_ACCOUNT_KINDS.has(e.accountKind) && !e.userId) {
        throw new Error(`Tài khoản ${e.accountKind} bắt buộc có userId`);
      }
      const delta =
        e.direction === LedgerDirection.Debit ? e.amount : -e.amount;
      sums.set(e.currency, (sums.get(e.currency) ?? 0n) + delta);
    }
    // Sổ kép: tổng Nợ phải bằng tổng Có, kiểm theo TỪNG currency và trước khi ghi bất cứ thứ gì.
    for (const [currency, sum] of sums) {
      if (sum !== 0n) {
        throw new Error(
          `Tổng Nợ != tổng Có cho currency ${currency} — vi phạm bất biến double-entry`,
        );
      }
    }

    if (input.type === TransactionType.Adjustment && !input.actorUserId) {
      throw new Error(
        'Transaction type=adjustment bắt buộc có actorUserId (ai thực hiện sửa sai) để audit',
      );
    }

    // Step 5: Băm request; key đã có thì trả kết quả cũ, không mở transaction
    const requestHash = this.hashRequest(input);

    // [SERVICE] LedgerService.hashRequest
    // request_hash gồm loại giao dịch, người thực hiện và toàn bộ bút toán: cùng key mà nội dung
    // khác thì bị 409, không được coi là replay.
    const normalized = {
      type: input.type,
      actorUserId: input.actorUserId ?? null,
      reversalOf: input.reversalOf ?? null,
      entries: input.entries.map((e) => ({
        k: e.accountKind,
        u: e.userId ?? null,
        d: e.direction,
        a: e.amount.toString(),
        c: e.currency,
      })),
    };
    return createHash('sha256')
      .update(JSON.stringify(normalized))
      .digest('hex');

    // Đường nhanh cho retry: key đã có thì kiểm tra cùng nội dung rồi trả kết quả cũ, không mở
    // transaction.
    const existing = await this.dataSource
      .getRepository(LedgerTransaction)
      .findOneBy({ idempotencyKey: input.idempotencyKey });
    if (existing) {
      try {
        const transaction = this.assertSameRequest(existing, requestHash);

        // [SERVICE] LedgerService.assertSameRequest
        if (existing.requestHash !== requestHash) {
          throw new DomainException(
            EconomyErrors.IDEMPOTENCY_CONFLICT,
            'Idempotency key đã dùng cho 1 request khác nội dung',
            HttpStatus.CONFLICT,
          );
        }
        return existing;

        this.metrics.record(input.type, 'replayed');
        return { transaction, replayed: true };
      } catch (err) {
        this.metrics.record(input.type, 'failed');
        throw err;
      }
    }

    try {
      const result = await this.dataSource.transaction((manager) =>
        this.recordInManager(manager, input),
      );

      // [SERVICE] LedgerService.recordInManager
      this.validateInput(input);
      const requestHash = this.hashRequest(input);
      const existing = await manager.findOne(LedgerTransaction, {
        where: { idempotencyKey: input.idempotencyKey },
      });
      if (existing) {
        return {
          transaction: this.assertSameRequest(existing, requestHash),
          replayed: true,
        };
      }

      // Step 6: Trong transaction, chèn giao dịch (trùng key là replay)
      // [POSTGRES] ON CONFLICT DO NOTHING thay vì bắt lỗi unique, vì lỗi đó làm abort cả
      // transaction của caller. Postgres chờ bên thắng commit; không chèn được dòng mới thì đây
      // là replay, không ghi bút toán.
      const insertResult = await manager
        .createQueryBuilder()
        .insert()
        .into(LedgerTransaction)
        .values({
          type: input.type,
          status: TransactionStatus.Completed,
          idempotencyKey: input.idempotencyKey,
          requestHash,
          actorUserId: input.actorUserId ?? null,
          reversalOf: input.reversalOf ?? null,
          metadata: (input.metadata ?? {}) as never,
        })
        .orIgnore()
        .execute();

      const insertedId =
        insertResult.identifiers[0]?.id ?? insertResult.raw[0]?.id;

      const txn = await manager.findOneByOrFail(LedgerTransaction, {
        idempotencyKey: input.idempotencyKey,
      });
      if (!insertedId)
        return {
          transaction: this.assertSameRequest(txn, requestHash),
          replayed: true,
        };

      // Step 7: Tạo hoặc lấy tài khoản sổ, khóa ví FOR UPDATE theo thứ tự userId
      const accounts: LedgerAccount[] = [];
      for (const entry of input.entries)
        accounts.push(await this.resolveAccount(manager, entry));

        // [SERVICE] LedgerService.resolveAccount
        const isUserAccount = USER_ACCOUNT_KINDS.has(entry.accountKind);
        const where = {
          kind: entry.accountKind,
          userId: isUserAccount ? (entry.userId as string) : IsNull(),
          currency: entry.currency,
        };
        const found = await manager.getRepository(LedgerAccount).findOneBy(where);
        if (found) return found;
        // Tạo tài khoản kiểu ON CONFLICT DO NOTHING, KHÔNG insert-rồi-bắt-lỗi 23505: lỗi đó làm
        // abort cả transaction nên nhánh bắt-lỗi không bao giờ chạy được, bên thua sẽ chết 500.
        await manager.query(
          `INSERT INTO ledger_accounts (kind, user_id, currency) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
          [
            entry.accountKind,
            isUserAccount ? (entry.userId as string) : null,
            entry.currency,
          ],
        );
        return manager.getRepository(LedgerAccount).findOneByOrFail(where);

      // Khóa ví theo thứ tự userId đã sắp xếp để tránh deadlock khi một giao dịch chạm nhiều ví.
      const userIds = [
        ...new Set(
          input.entries.filter((e) => e.userId).map((e) => e.userId as string),
        ),
      ].sort();
      const wallets = new Map<string, Wallet>();
      for (const userId of userIds)
        wallets.set(userId, await this.lockWallet(manager, userId));

        // [SERVICE] LedgerService.lockWallet
        // [POSTGRES] Ví tạo khi cần rồi khóa FOR UPDATE: đây là điểm tuần tự hóa theo từng user,
        // 2 giao dịch cùng ví không chạy song song.
        await manager.query(
          `INSERT INTO wallets (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`,
          [userId],
        );
        const wallet = await manager
          .getRepository(Wallet)
          .createQueryBuilder('w')
          .setLock('pessimistic_write')
          .where('w.user_id = :userId', { userId })
          .getOne();
        if (!wallet) throw new Error(`Không lock được ví của user ${userId}`);
        return wallet;

      // Step 8: Ghi bút toán, trừ số dư (kiểm đủ tiền) và ghi outbox
      await manager.save(
        input.entries.map((entry, i) =>
          manager.create(LedgerEntry, {
            transactionId: txn.id,
            accountId: accounts[i].id,
            direction: entry.direction,
            amount: entry.amount.toString(),
            currency: entry.currency,
          }),
        ),
      );
      await this.applyWalletDeltas(
        manager,
        txn,
        input.entries,
        wallets,
        input.outboxEventTypeOverride,
      );

      // [SERVICE] LedgerService.applyWalletDeltas
      // entries = input.entries
      const outboxEventTypeOverride = input.outboxEventTypeOverride;
      for (const [userId, wallet] of wallets) {
        let balanceDelta = 0n;
        let earningsDelta = 0n;
        for (const e of entries) {
          if (e.userId !== userId) continue;
          const signed =
            e.direction === LedgerDirection.Credit ? e.amount : -e.amount;
          if (e.accountKind === LedgerAccountKind.UserWallet)
            balanceDelta += signed;
          if (e.accountKind === LedgerAccountKind.UserEarnings)
            earningsDelta += signed;
        }

        const newBalance = BigInt(wallet.balance) + balanceDelta;
        const newEarnings = BigInt(wallet.earnings) + earningsDelta;
        // Chỉ reversal hoặc adjustment (refund, chargeback, sửa sai) mới được đẩy số dư âm = user
        // nợ diamond; giao dịch tiêu tiền thường thì không.
        const mayGoNegative =
          txn.type === TransactionType.Reversal ||
          txn.type === TransactionType.Adjustment;
        // Kiểm số dư SAU khi đã khóa ví, đúng lúc trừ tiền; không tin số dư đọc ở đầu luồng.
        if (newBalance < 0n && !mayGoNegative) {
          throw new DomainException(
            EconomyErrors.WALLET_INSUFFICIENT_BALANCE,
            'Không đủ diamond',
            HttpStatus.UNPROCESSABLE_ENTITY,
            { required: (-newBalance).toString() },
          );
        }
        if (newEarnings < 0n) {
          throw new DomainException(
            EconomyErrors.WALLET_INSUFFICIENT_BALANCE,
            'Không đủ điểm quy đổi',
            HttpStatus.UNPROCESSABLE_ENTITY,
          );
        }

        wallet.balance = newBalance.toString();
        wallet.earnings = newEarnings.toString();
        await manager.save(wallet);

        // Outbox: event ghi CÙNG transaction với số dư, relay gửi Kafka sau. Nhờ vậy không có
        // chuyện đổi số dư mà mất event, hay ngược lại.
        if (balanceDelta !== 0n) {
          await manager.save(
            manager.create(OutboxEvent, {
              topic: ECONOMY_EVENTS_TOPIC,
              eventType:
                outboxEventTypeOverride ??
                (balanceDelta > 0n
                  ? 'economy.diamond.credited'
                  : 'economy.diamond.debited'),
              payload: {
                version: 1,
                transactionId: txn.id,
                transactionType: txn.type,
                userId,
                amount: (balanceDelta > 0n
                  ? balanceDelta
                  : -balanceDelta
                ).toString(),
              },
            }),
          );
        }
      }

      // Step 9: Phần việc riêng của VIP cùng transaction: gia hạn, ghi outbox
      // Phần việc riêng của từng luồng (gia hạn VIP, lưu receipt...) chạy CÙNG transaction, sau
      // khi ví đã khóa và số dư đã cập nhật.
      await input.withinTransaction?.(manager, txn);
      return { transaction: txn, replayed: false };

      this.metrics.record(input.type, result.replayed ? 'replayed' : 'success');
      return result;
    } catch (err) {

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      // [SERVICE] violatedConstraint
      const constraint = UQ_TRANSACTIONS_IDEMPOTENCY_KEY;
      const e = err as {
        driverError?: { constraint?: string };
        constraint?: string;
        message?: string;
      };
      const name = e.driverError?.constraint ?? e.constraint;
      return name === constraint || (e.message?.includes(constraint) ?? false);

      if (
        isUniqueViolation(err) &&
        violatedConstraint(err, UQ_TRANSACTIONS_IDEMPOTENCY_KEY)
      ) {

        // Step 10: Thua race cùng key: đọc lại dòng của bên thắng, trả như replay
        // [POSTGRES] Kiểm trước chỉ là đường nhanh; nguồn chân lý là unique idempotency_key ở DB.
        // 2 request cùng key chạy song song thì bên thua đọc lại dòng của bên thắng và trả như
        // replay.
        const winner = await this.dataSource
          .getRepository(LedgerTransaction)
          .findOneByOrFail({ idempotencyKey: input.idempotencyKey });
        try {
          const transaction = this.assertSameRequest(winner, requestHash);
          this.metrics.record(input.type, 'replayed');
          return { transaction, replayed: true };
        } catch (assertErr) {
          this.metrics.record(input.type, 'failed');
          throw assertErr;
        }
      }
      this.metrics.record(input.type, 'failed');
      throw err;
    }

    // Step 11: Service trả hạng và hạn VIP (replay thì không gia hạn lần 2)
    // Replay (cùng key) không gia hạn lần 2: chỉ trả trạng thái VIP hiện tại.
    if (result.replayed) {
      const wallet = await this.walletRepo.findOneByOrFail({ userId });
      return {
        transactionId: result.transaction.id,
        tier: plan.tier,
        vipExpiresAt: wallet.vipExpiresAt ?? newExpiry,
        replayed: true,
      };
    }
    return {
      transactionId: result.transaction.id,
      tier: plan.tier,
      vipExpiresAt: newExpiry,
      replayed: false,
    };
  }
}

// [WORKER] OutboxRelayService.flushOnce · economy/jobs/outbox-relay.service.ts
@Injectable()
export class OutboxRelayService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  async flushOnce(): Promise<number> {
    return this.job.runExclusive(async () => {
      return await this.dataSource.transaction(async (manager) => {

        // Step 12: Worker nền: khóa lô outbox chưa gửi, gửi sang Kafka
        // [POSTGRES] FOR UPDATE SKIP LOCKED: nhiều pod chạy relay song song không dẫm nhau, pod
        // nào đã khóa lô nào thì pod khác bỏ qua lô đó.
        const events: OutboxEvent[] = await manager
          .getRepository(OutboxEvent)
          .createQueryBuilder('e')
          .setLock('pessimistic_write')
          .setOnLocked('skip_locked')
          .where('e.published_at IS NULL')
          .andWhere('e.dead_lettered_at IS NULL')
          .orderBy('e.created_at', 'ASC')
          .limit(BATCH_SIZE)
          .getMany();
        if (events.length === 0) return 0;

        for (const event of events) {
          try {
            // [KAFKA] Gửi xong mới đánh dấu published_at (lưu ở cuối vòng). Process chết giữa
            // chừng thì event được gửi lại ở vòng sau: giao kiểu at-least-once, consumer phải
            // chịu được trùng.
            await this.eventBus.publish(toEventEnvelope(event));

            // [WORKER] toEventEnvelope
            return {
              id: event.id,
              topic: event.topic,
              type: event.eventType,
              version: eventVersion(event.payload),
              key: orderingKey(event),
              payload: event.payload,
            };

            // [WORKER] eventVersion
            const payload = event.payload;
            const version = payload['version'];
            return typeof version === 'number' && Number.isInteger(version)
              ? version
              : DEFAULT_EVENT_VERSION;

            // [WORKER] orderingKey
            for (const field of ORDERING_KEY_FIELDS) {
              const value = event.payload[field];
              if (typeof value === 'string' && value.length > 0) return value;
            }
            return event.id;

            // [KAFKA] KafkaEventBusAdapter.publish · common/events/kafka-event-bus.adapter.ts
            // event = toEventEnvelope(event)
            try {
              const producer = await this.getProducer();

              // [KAFKA] KafkaEventBusAdapter.getProducer
              if (this.producer !== null) return this.producer;
              if (this.producerConnect !== null) return this.producerConnect;

              const producer = this.kafka.producer({
                idempotent: true,
                maxInFlightRequests: 1,
                retry: {
                  retries: this.config.getOrThrow('EVENT_BUS_KAFKA_RETRIES', {
                    infer: true,
                  }),
                },
              });
              this.producerConnect = producer
                .connect()
                .then(() => {
                  this.producer = producer;
                  return producer;
                })
                .catch((error: unknown) => {
                  this.producerConnect = null;
                  throw error;
                });
              return this.producerConnect;

              await producer.send({
                topic: event.topic,
                messages: [
                  {
                    key: event.key,
                    value: JSON.stringify(event),
                  },
                ],
              });
              this.publishTotal.add(1, {
                topic: event.topic,
                type: event.type,
                outcome: 'success',
              });
            } catch (error) {
              this.publishFailureTotal.add(1, { topic: event.topic, type: event.type });
              this.logger.error(
                { eventId: event.id, topic: event.topic, err: errorMessage(error) },
                'Event publish failed; caller must retain the outbox row',
              );

              // [KAFKA] errorMessage
              const message = error instanceof Error ? error.message : String(error);
              return message.slice(0, MAX_ERROR_LENGTH);

              throw error;
            }

            event.publishedAt = new Date();
            event.lastError = null;
          } catch (err) {
            event.attempts += 1;
            event.lastError = stringifyError(err);

            // [SERVICE] stringifyError
            const error = err;
            const message = error instanceof Error ? error.message : String(error);
            return message.slice(0, MAX_ERROR_LENGTH);

            this.logger.error(
              {
                eventId: event.id,
                topic: event.topic,
                attempts: event.attempts,
                maxAttempts: this.maxAttempts(),
                err: event.lastError,
              },
              'Outbox event publish failed',
            );

            // [WORKER] OutboxRelayService.maxAttempts
            return this.config.getOrThrow('ECONOMY_OUTBOX_MAX_ATTEMPTS', {
              infer: true,
            });

            if (event.attempts >= this.maxAttempts()) {
              await this.deadLetter(event);

              // [WORKER] OutboxRelayService.deadLetter
              try {
                await this.eventBus.publishDeadLetter(
                  toEventEnvelope(event),
                  event.lastError ?? 'unknown publish failure',
                  event.attempts,
                );

                // [KAFKA] KafkaEventBusAdapter.publishDeadLetter · common/events/kafka-event-bus.adapter.ts
                // event = toEventEnvelope(event)
                // error = event.lastError ?? 'unknown publish failure'
                const attempts = event.attempts;
                const dlqEvent: EventEnvelope = {
                  ...event,
                  topic: deadLetterTopic(event.topic),
                  payload: {
                    original: event.payload,
                    deadLetter: { error, attempts },
                  },
                };

                // [SERVICE] deadLetterTopic · common/events/events.constants.ts
                const topic = event.topic;
                return `${topic}${EVENT_BUS_DEAD_LETTER_SUFFIX}`;

                await this.publish(dlqEvent);
                this.deadLetterTotal.add(1, { topic: event.topic, type: event.type });

                this.logger.error(
                  {
                    eventId: event.id,
                    topic: event.topic,
                    attempts: event.attempts,
                    deadLetterTopic: `${event.topic}.DLQ`,
                  },
                  'Outbox event moved to dead-letter topic',
                );
              } catch (deadLetterError) {
                this.logger.error(
                  {
                    eventId: event.id,
                    topic: event.topic,
                    attempts: event.attempts,
                    err: stringifyError(deadLetterError),
                  },
                  'Outbox dead-letter publish failed; retaining terminal row for replay',
                );
              } finally {
                // Dù gửi dead-letter được hay không (Kafka vẫn hỏng) vẫn đánh dấu terminal để
                // khỏi lặp vô hạn; dòng còn giữ lại là nguồn để vận hành replay.
                event.deadLetteredAt = new Date();
              }
            }
          }
        }
        await manager.save(events);
        return events.filter((e) => e.publishedAt).length;
      });
    }, 0);

    // [SERVICE] ManagedInterval.runExclusive · common/scheduling/managed-interval.ts
    // task = đối số 1 của lời gọi ở trên
    const busyResult = 0;
    if (this.running) return busyResult;
    this.running = true;
    try {
      return await task();
    } finally {
      this.running = false;
    }
  }
}

// [POSTGRES] CREATE TABLE transactions · database/migrations/1752000000000-economy-ledger.ts
await queryRunner.query(`
  CREATE TABLE transactions (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type             varchar(32)  NOT NULL,
    status           varchar(16)  NOT NULL DEFAULT 'completed',
    idempotency_key  varchar(255) NOT NULL,
    request_hash     char(64)     NOT NULL,
    actor_user_id    uuid         NULL REFERENCES users(id),
    reversal_of      uuid         NULL REFERENCES transactions(id),
    metadata         jsonb        NOT NULL DEFAULT '{}',
    created_at       timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_transactions_idempotency_key UNIQUE (idempotency_key)
  )
`);

// [POSTGRES] CREATE TABLE wallets
await queryRunner.query(`
  CREATE TABLE wallets (
    user_id         uuid PRIMARY KEY REFERENCES users(id),
    -- balance CÓ THỂ âm sau refund/chargeback (user nợ diamond) — không đặt CHECK >= 0
    balance         bigint      NOT NULL DEFAULT 0,
    -- earnings (PTS) chỉ được cộng ở giai đoạn này (chưa có luồng tiêu) → vẫn giữ CHECK >= 0
    earnings        bigint      NOT NULL DEFAULT 0 CHECK (earnings >= 0),
    vip_tier        varchar(16) NULL,
    vip_expires_at  timestamptz NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
  )
`);

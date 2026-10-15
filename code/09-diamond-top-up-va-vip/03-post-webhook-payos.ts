//  POST /api/v1/economy/webhooks/payos
//  payOS báo thanh toán thành công: kiểm HMAC, đối chiếu snapshot của đơn, rồi cộng Diamond đúng một lần.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận webhook công khai của payOS (tối đa 120 lần/phút)
//  Step 2: Kiểm chữ ký HMAC trước mọi truy vấn hoặc ghi
//  Step 3: Dựng chuỗi chuẩn từ data (sắp key tăng dần) rồi ký HMAC-SHA256
//  Step 4: So chữ ký tính được với chữ ký payOS gửi (timingSafeEqual)
//  Step 5: Giao event đã xác thực cho PayosService để cộng Diamond
//  Step 6: Tìm đơn theo orderCode, đối chiếu snapshot (lệch thì bỏ qua)
//  Step 7: Ghi sổ Nợ system_iap / Có ví user, idempotent theo order_code
//  Step 8: Cùng transaction: khóa dòng đơn, đối chiếu lại snapshot
//  Step 9: Đánh dấu đơn paid và nối với giao dịch vừa ghi
//  Step 10: Trả { received: true } cho payOS

// [CONTROLLER] EconomyWebhooksController.payos · economy/webhooks/economy-webhooks.controller.ts
@Public()
@Controller('economy/webhooks')
export class EconomyWebhooksController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // PayosHttpClientAdapter
    private readonly config: ConfigService<CoreApiEnv, true>,
    // PayosService
    @InjectRepository(PayosPaymentOrder)
    private readonly orderRepo: Repository<PayosPaymentOrder>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // PayosHttpClientAdapter
    super();
    // LedgerService
    super();
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận webhook công khai của payOS (tối đa 120 lần/phút)

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

  // [CONTROLLER] @Public · economy/webhooks/economy-webhooks.controller.ts
  @Public()

  // [SERVICE] Public · common/decorators/public.decorator.ts
  return SetMetadata(IS_PUBLIC_KEY, true);

  // [CONTROLLER] Public
  export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);

  @Post('payos')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 120, ttl: minutes(1) } })
  async payos(@Body() dto: PayosWebhookDto): Promise<{ received: boolean }> {

    // Step 2: Kiểm chữ ký HMAC trước mọi truy vấn hoặc ghi
    // Endpoint công khai nhưng HMAC được kiểm TRƯỚC mọi truy vấn hoặc ghi: sai chữ ký thì 401 và
    // chưa chạm DB.
    const event = this.payosClient.verifyWebhook({
      code: dto.code,
      success: dto.success,
      data: dto.data,
      signature: dto.signature,
      ...(dto.desc ? { desc: dto.desc } : {}),
    });

    // [EXTERNAL] PayosHttpClientAdapter.verifyWebhook · economy/ports/payos-client.ts
    // payload = { code: dto.code, success: dto.success, data: dto.data, signature: dto.signature,
    // ...(dto.desc ? { desc: dto.desc } : {}), }
    const { checksumKey } = this.credentials();

    // [SERVICE] PayosHttpClientAdapter.credentials
    const clientId = this.config.getOrThrow('PAYOS_CLIENT_ID', { infer: true });
    const apiKey = this.config.getOrThrow('PAYOS_API_KEY', { infer: true });
    const checksumKey = this.config.getOrThrow('PAYOS_CHECKSUM_KEY', {
      infer: true,
    });
    if (!clientId || !apiKey || !checksumKey) {
      throw new ServiceUnavailableException(
        EconomyErrors.PAYOS_DISABLED,
        'Nạp diamond qua payOS chưa được cấu hình',
      );
    }
    return { clientId, apiKey, checksumKey };

    const signature = payload['signature'];
    const data = payload['data'];

    // [EXTERNAL] PayosHttpClientAdapter.matchesSignature
    // data = data as Record<string, unknown>
    const expected = this.sign(data, checksumKey);

    // [EXTERNAL] PayosHttpClientAdapter.sign
    // Step 3: Dựng chuỗi chuẩn từ data (sắp key tăng dần) rồi ký HMAC-SHA256
    // Định dạng chuẩn của payOS: sắp key tăng dần, không URI-encode giá trị. Sai định dạng thì
    // HMAC không khớp.
    const canonical = Object.keys(data)
      .sort()
      .map((key) => `${key}=${this.stringifyValue(data[key])}`)
      .join('&');

    // [EXTERNAL] PayosHttpClientAdapter.stringifyValue
    // value = data[key]
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);

    return createHmac('sha256', checksumKey).update(canonical).digest('hex');

    // Step 4: So chữ ký tính được với chữ ký payOS gửi (timingSafeEqual)
    const left = Buffer.from(expected, 'hex');
    const right = Buffer.from(signature, 'hex');
    // So chữ ký bằng timingSafeEqual (thời gian so sánh không phụ thuộc nội dung) và kiểm độ dài
    // trước, vì hàm này ném lỗi khi 2 buffer khác độ dài.
    return left.length === right.length && timingSafeEqual(left, right);

    if (
      typeof signature !== 'string' ||
      !data ||
      typeof data !== 'object' ||
      Array.isArray(data) ||
      !this.matchesSignature(
        data as Record<string, unknown>,
        signature,
        checksumKey,
      )
    ) {
      throw new DomainException(
        EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
        'Webhook payOS có chữ ký không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }
    const event = data as Record<string, unknown>;
    const orderCode = event['orderCode'];
    const amount = event['amount'];
    const currency = event['currency'];
    const paymentLinkId = event['paymentLinkId'];
    const paymentCode = event['code'];

    // [EXTERNAL] PayosHttpClientAdapter.isIntegerLike
    // value = orderCode
    return (
      (typeof value === 'number' && Number.isSafeInteger(value)) ||
      (typeof value === 'string' && /^\d+$/.test(value))
    );

    if (
      !this.isIntegerLike(orderCode) ||
      !this.isIntegerLike(amount) ||
      typeof currency !== 'string' ||
      typeof paymentLinkId !== 'string' ||
      paymentLinkId.length === 0 ||
      typeof paymentCode !== 'string'
    ) {
      throw new DomainException(
        EconomyErrors.PAYOS_WEBHOOK_INVALID,
        'Webhook payOS thiếu dữ liệu thanh toán bắt buộc',
        HttpStatus.BAD_REQUEST,
      );
    }
    return {
      // payOS chỉ ký object data; code và success ở ngoài sửa được mà chữ ký vẫn đúng. Vì vậy
      // quyết định credit chỉ dựa vào code nằm trong data đã verify.
      code: paymentCode,
      success: paymentCode === '00',
      orderCode: String(orderCode),
      amount: String(amount),
      currency,
      paymentLinkId,
    };

    // Step 5: Giao event đã xác thực cho PayosService để cộng Diamond
    await this.payosService.creditVerifiedWebhook(event);

    // [SERVICE] PayosService.creditVerifiedWebhook · economy/services/payos.service.ts
    // Step 6: Tìm đơn theo orderCode, đối chiếu snapshot (lệch thì bỏ qua)
    const order = await this.orderRepo.findOneBy({
      orderCode: event.orderCode,
    });
    // Không khớp snapshot của đơn (orderCode, số tiền, VND, mã thành công, paymentLinkId) thì
    // KHÔNG ghi sổ, trả false chứ không ném lỗi.

    // [SERVICE] PayosService.matchesPaidOrder
    // Mọi trường tiền đều so với snapshot lúc tạo đơn, không tin số trong webhook. Đơn đã paid
    // thì không còn khớp nên không ghi sổ lần hai.
    return (
      order.status !== PayosPaymentOrderStatus.Paid &&
      event.success &&
      event.code === PAYOS_SUCCESS_CODE &&
      event.orderCode === order.orderCode &&
      event.amount === order.amountVnd &&
      event.currency === order.currency &&
      event.paymentLinkId === order.paymentLinkId
    );

    if (!order || !this.matchesPaidOrder(order, event)) return false;
    try {

      // Step 7: Ghi sổ Nợ system_iap / Có ví user, idempotent theo order_code
      const result = await this.ledger.record({
        type: TransactionType.PayosTopup,
        // Key do server sinh từ order_code, không lấy từ client: webhook gửi lại hay chạy song
        // song cũng chỉ cộng Diamond một lần.
        idempotencyKey: `payos-credit:${order.orderCode}`,
        actorUserId: order.userId,
        metadata: {
          provider: 'payos',
          orderId: order.id,
          orderCode: order.orderCode,
          packageId: order.packageId,
          amountVnd: order.amountVnd,
          diamonds: order.diamonds,
          paymentLinkId: order.paymentLinkId,
        },
        entries: [
          {
            accountKind: LedgerAccountKind.SystemIap,
            direction: LedgerDirection.Debit,
            amount: BigInt(order.diamonds),
            currency: LedgerCurrency.Diamond,
          },
          {
            accountKind: LedgerAccountKind.UserWallet,
            userId: order.userId,
            direction: LedgerDirection.Credit,
            amount: BigInt(order.diamonds),
            currency: LedgerCurrency.Diamond,
          },
        ],
        withinTransaction: async (manager, txn) => {

          // Step 8: Cùng transaction: khóa dòng đơn, đối chiếu lại snapshot
          // [POSTGRES] Khóa dòng đơn FOR UPDATE rồi đối chiếu lại snapshot ngay trong transaction
          // ghi sổ; lệch thì ném lỗi để rollback cả bút toán.
          const locked = await this.lockOrder(manager, order.id);

          // [SERVICE] PayosService.lockOrder
          const orderId = order.id;
          return manager
            .getRepository(PayosPaymentOrder)
            .createQueryBuilder('payos_order')
            .setLock('pessimistic_write')
            .where('payos_order.id = :orderId', { orderId })
            .getOneOrFail();

          if (!this.matchesPaidOrder(locked, event))
            this.throwWebhookMismatch();

            // [SERVICE] PayosService.throwWebhookMismatch
            throw new DomainException(
              EconomyErrors.PAYOS_WEBHOOK_MISMATCH,
              'Webhook payOS không khớp snapshot đơn hàng',
              HttpStatus.BAD_REQUEST,
            );

          // Step 9: Đánh dấu đơn paid và nối với giao dịch vừa ghi
          // Không lọc theo status: webhook hợp lệ đến trễ sau khi đơn bị coi là hết hạn vẫn phải
          // nối đơn với bút toán nguyên tử, không để ví đã cộng mà đơn còn expired.
          await manager.update(
            PayosPaymentOrder,
            { id: locked.id },
            {
              status: PayosPaymentOrderStatus.Paid,
              transactionId: txn.id,
            },
          );
        },
      });

      // [SERVICE] LedgerService.record · economy/services/ledger.service.ts
      // input = đối số 1 của lời gọi ở trên
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

      const requestHash = this.hashRequest(input);

      // [SERVICE] LedgerService.hashRequest
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
          await manager.query(
            `INSERT INTO ledger_accounts (kind, user_id, currency) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
            [
              entry.accountKind,
              isUserAccount ? (entry.userId as string) : null,
              entry.currency,
            ],
          );
          return manager.getRepository(LedgerAccount).findOneByOrFail(where);

        const userIds = [
          ...new Set(
            input.entries.filter((e) => e.userId).map((e) => e.userId as string),
          ),
        ].sort();
        const wallets = new Map<string, Wallet>();
        for (const userId of userIds)
          wallets.set(userId, await this.lockWallet(manager, userId));

          // [SERVICE] LedgerService.lockWallet
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
          const mayGoNegative =
            txn.type === TransactionType.Reversal ||
            txn.type === TransactionType.Adjustment;
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

      return !result.replayed;
    } catch (err) {
      if (
        err instanceof DomainException &&
        err.code === EconomyErrors.PAYOS_WEBHOOK_MISMATCH
      ) {
        return false;
      }
      throw err;
    }

    // Step 10: Trả { received: true } cho payOS
    return { received: true };
  }
}

// [POSTGRES] CREATE TABLE payos_payment_orders · database/migrations/1756300000000-payos-diamond.ts
// Unique transaction_id: mỗi đơn chỉ nối được với đúng một transaction credit. order_code lấy từ
// sequence 16 chữ số để vẫn nằm trong số nguyên an toàn của JavaScript (payOS nhận number).
await queryRunner.query(`
  CREATE TABLE payos_payment_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id),
    package_id varchar(64) NOT NULL REFERENCES payos_packages(package_id),
    amount_vnd bigint NOT NULL CHECK (amount_vnd > 0),
    diamonds bigint NOT NULL CHECK (diamonds > 0),
    currency varchar(3) NOT NULL DEFAULT 'VND' CHECK (currency = 'VND'),
    order_code bigint NOT NULL DEFAULT nextval('payos_order_code_seq'),
    idempotency_key varchar(255) NOT NULL,
    request_hash char(64) NOT NULL,
    status varchar(32) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'cancelled', 'expired')),
    payment_link_id varchar(128) NULL,
    checkout_url text NULL,
    qr_code text NULL,
    expires_at timestamptz NOT NULL,
    transaction_id uuid NULL REFERENCES transactions(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_payos_payment_orders_order_code UNIQUE (order_code),
    CONSTRAINT uq_payos_payment_orders_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_payos_payment_orders_payment_link_id UNIQUE (payment_link_id),
    CONSTRAINT uq_payos_payment_orders_transaction_id UNIQUE (transaction_id)
  )
`);

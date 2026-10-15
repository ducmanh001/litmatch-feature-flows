//  POST /api/v1/economy/iap/verify
//  Xác thực receipt với Apple hoặc Google ở server, rồi cộng Diamond đúng một lần theo id giao dịch của store.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận receipt từ app (user từ JWT; tối đa 10 lần/phút)
//  Step 2: Tra catalog để ra số Diamond (client không gửi số lượng)
//  Step 3: Hỏi Apple hoặc Google xác thực receipt ở server
//  Step 4: Apple: gọi production trước, receipt sandbox thì gọi lại sandbox
//  Step 5: Apple: khớp productId và transactionId trong receipt
//  Step 6: Google: hỏi Publisher API, chỉ nhận purchaseState = purchased
//  Step 7: Ghi sổ Nợ system_iap / Có ví user, key theo id giao dịch store
//  Step 8: Lưu iap_receipts cùng transaction (unique chặn credit đôi)
//  Step 9: Trả transactionId, số Diamond và cờ replayed

// [CONTROLLER] EconomyController.verifyIap · economy/economy.controller.ts
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
  // DevIapVerifier
  private readonly logger = new Logger(DevIapVerifier.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // EconomyService
    @InjectRepository(IapProduct)
    private readonly productRepo: Repository<IapProduct>,
    // AppleReceiptApiAdapter
    private readonly config: ConfigService<CoreApiEnv, true>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // DevIapVerifier
    super();
    // AppleReceiptApiAdapter
    super();
    // LedgerService
    super();
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận receipt từ app (user từ JWT; tối đa 10 lần/phút)

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

  @Post('iap/verify')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 10, ttl: minutes(1) } })
  verifyIap(@CurrentUser() user: AuthenticatedUser, @Body() dto: VerifyIapDto) {

  // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
  export const CurrentUser = createParamDecorator(
    (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
      const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
      return req.user;
    },
  );

    return this.economyService.creditFromIap(
      user.userId,
      dto.provider,
      dto.payload,
      dto.productId,
    );

    // [SERVICE] EconomyService.creditFromIap · economy/economy.service.ts
    const userId = user.userId;
    const provider = dto.provider;
    // payload = dto.payload
    const productId = dto.productId;

    // Step 2: Tra catalog để ra số Diamond (client không gửi số lượng)
    // Số Diamond lấy từ catalog server theo productId và provider; client không gửi số lượng.
    const product = await this.productRepo.findOneBy({
      productId,
      provider,
      active: true,
    });
    if (!product) {
      throw new DomainException(
        EconomyErrors.IAP_PRODUCT_UNKNOWN,
        `Product ${productId} không tồn tại`,
        HttpStatus.BAD_REQUEST,
      );
    }

    // Step 3: Hỏi Apple hoặc Google xác thực receipt ở server
    // [EXTERNAL] Hỏi store ở SERVER trước mọi ghi sổ. Bản verifier chọn theo env: production
    // thiếu credential dùng bản "disabled", từ chối ngay khi chưa đụng ledger.
    const verified = await this.iapVerifier.verify(
      provider,
      payload,
      productId,
    );

    // [EXTERNAL] DisabledIapVerifier.verify · economy/ports/iap-verifier.ts
    void provider;
    void payload;
    void productId;
    throw new DomainException(
      EconomyErrors.IAP_DISABLED,
      'Nạp kim cương qua cửa hàng chưa khả dụng trên môi trường này',
      HttpStatus.FORBIDDEN,
    );

    // [EXTERNAL] DevIapVerifier.verify
    const devId = payload['devTransactionId'];
    if (typeof devId !== 'string' || devId.length < 4) {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        'devTransactionId không hợp lệ',
        HttpStatus.BAD_REQUEST,
      );
    }
    this.logger.warn(
      `[DEV-ONLY IAP] chấp nhận receipt giả ${provider}:${devId}`,
    );
    return { providerTransactionId: devId };

    // [EXTERNAL] StoreIapVerifierAdapter.verify · economy/clients/store-iap-verifier.adapter.ts
    if (provider === IapProvider.Apple) {
      const receiptData = payload['receiptData'];
      if (typeof receiptData !== 'string' || receiptData.length === 0) {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          'Thiếu receiptData',
          HttpStatus.BAD_REQUEST,
        );
      }
      const rawTransactionId = payload['transactionId'];
      if (
        rawTransactionId !== undefined &&
        typeof rawTransactionId !== 'string'
      ) {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          'transactionId không hợp lệ',
          HttpStatus.BAD_REQUEST,
        );
      }
      const transactionId =
        typeof rawTransactionId === 'string' && rawTransactionId.length > 0
          ? rawTransactionId
          : undefined;
      return this.apple.verify({
        receiptData,
        productId,
        ...(transactionId ? { transactionId } : {}),
      });

      // [EXTERNAL] AppleReceiptApiAdapter.verify · economy/clients/apple-receipt-api.adapter.ts
      const input = {
        receiptData,
        productId,
        ...(transactionId ? { transactionId } : {}),
      };
      const sharedSecret = this.config.getOrThrow('ECONOMY_APPLE_SHARED_SECRET', {
        infer: true,
      });

      // Step 4: Apple: gọi production trước, receipt sandbox thì gọi lại sandbox
      let body = await this.postVerify(
        APPLE_VERIFY_RECEIPT_URL,
        input.receiptData,
        sharedSecret,
      );

      // [EXTERNAL] AppleReceiptApiAdapter.postVerify
      const url = APPLE_VERIFY_RECEIPT_URL;
      // receiptData = input.receiptData
      let response: Response;
      try {
        response = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            'receipt-data': receiptData,
            password: sharedSecret,
          }),
          signal: storeApiAbortSignal(this.config),
        });

        // [SERVICE] storeApiAbortSignal · economy/clients/store-api-http.ts
        const config = this.config;
        return AbortSignal.timeout(
          config.getOrThrow('ECONOMY_STORE_HTTP_TIMEOUT_MS', { infer: true }),
        );
      } catch (error) {
        // Timeout hay lỗi mạng của store là lỗi dependency (503), không được đổi thành "receipt
        // sai" của client.

        // [SERVICE] isAbortError · economy/clients/store-api-http.ts
        return (
          error instanceof DOMException &&
          (error.name === 'AbortError' || error.name === 'TimeoutError')
        );

        if (isAbortError(error))
          throw storeProviderUnavailable('Apple Store', 'timeout');

          // [SERVICE] storeProviderUnavailable · economy/clients/store-api-http.ts
          // provider = 'Apple Store'
          const reason = 'timeout';
          return new ServiceUnavailableException(`${provider} ${reason}`);

        throw storeProviderUnavailable('Apple Store', 'không kết nối được');
      }
      if (!response.ok) {
        throw storeProviderUnavailable(
          'Apple Store',
          `trả HTTP ${response.status}`,
        );
      }
      try {
        return (await response.json()) as AppleReceiptResponse;
      } catch {
        throw storeProviderUnavailable(
          'Apple Store',
          'trả response không hợp lệ',
        );
      }

      // Quy tắc của Apple: gọi production trước; receipt sandbox (status 21007) thì gọi lại
      // endpoint sandbox.
      if (body.status === APPLE_STATUS_SANDBOX_RECEIPT) {
        body = await this.postVerify(
          APPLE_VERIFY_RECEIPT_SANDBOX_URL,
          input.receiptData,
          sharedSecret,
        );
      }
      if (body.status !== APPLE_STATUS_OK) {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          `Apple từ chối receipt (status ${body.status ?? 'unknown'})`,
          HttpStatus.BAD_REQUEST,
        );
      }

      const purchases = (body.receipt?.in_app ?? []) as ApplePurchase[];
      const matching = purchases.filter(
        (purchase) =>
          purchase.product_id === input.productId &&
          typeof purchase.transaction_id === 'string' &&
          purchase.transaction_id.length > 0,
      );

      // Step 5: Apple: khớp productId và transactionId trong receipt
      // Khớp cả productId lẫn transactionId. Receipt có nhiều giao dịch cùng product mà thiếu
      // transactionId thì từ chối, để không credit nhầm.
      const match = input.transactionId
        ? matching.find(
            (purchase) => purchase.transaction_id === input.transactionId,
          )
        : matching.length === 1
          ? matching[0]
          : undefined;
      if (!match || typeof match.transaction_id !== 'string') {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          input.transactionId
            ? 'Receipt không chứa transactionId/product này'
            : matching.length > 1
              ? 'Receipt consumable chứa nhiều giao dịch; cần transactionId cụ thể'
              : 'Receipt không chứa product này',
          HttpStatus.BAD_REQUEST,
        );
      }
      return { providerTransactionId: match.transaction_id };
    }

    const purchaseToken = payload['purchaseToken'];
    if (typeof purchaseToken !== 'string' || purchaseToken.length === 0) {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        'Thiếu purchaseToken',
        HttpStatus.BAD_REQUEST,
      );
    }
    return this.google.verify(productId, purchaseToken);

    // [EXTERNAL] GooglePlayReceiptApiAdapter.verify · economy/clients/google-play-receipt-api.adapter.ts
    // Step 6: Google: hỏi Publisher API, chỉ nhận purchaseState = purchased
    let response: Response;
    try {
      const packageName = this.config.getOrThrow(
        'ECONOMY_GOOGLE_PACKAGE_NAME',
        {
          infer: true,
        },
      );
      const accessToken = await getGoogleServiceAccountAccessToken(
        this.config,
        ANDROID_PUBLISHER_SCOPE,
      );

      // [SERVICE] getGoogleServiceAccountAccessToken · economy/clients/google-service-account.ts
      // config = this.config
      const scope = ANDROID_PUBLISHER_SCOPE;
      const email = config.getOrThrow('ECONOMY_GOOGLE_SA_EMAIL', { infer: true });
      const privateKeyPem = config
        .getOrThrow('ECONOMY_GOOGLE_SA_PRIVATE_KEY', { infer: true })
        .replace(/\\n/g, '\n');
      const key = await importPKCS8(privateKeyPem, 'RS256');
      const assertion = await new SignJWT({ scope })
        .setProtectedHeader({ alg: 'RS256' })
        .setIssuer(email)
        .setAudience(GOOGLE_OAUTH_TOKEN_URL)
        .setIssuedAt()
        .setExpirationTime('5m')
        .sign(key);

      const res = await fetch(GOOGLE_OAUTH_TOKEN_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
          assertion,
        }),
        signal: storeApiAbortSignal(config),
      });
      if (!res.ok) throw new Error(`Google OAuth lỗi ${res.status}`);
      const body = (await res.json()) as { access_token?: unknown };
      if (typeof body.access_token !== 'string' || body.access_token === '') {
        throw new Error('Google OAuth response thiếu access_token');
      }
      return body.access_token;

      response = await fetch(
        `${ANDROID_PUBLISHER_API_BASE}/applications/${encodeURIComponent(packageName)}/purchases/products/${encodeURIComponent(productId)}/tokens/${encodeURIComponent(purchaseToken)}`,
        {
          headers: { Authorization: `Bearer ${accessToken}` },
          signal: storeApiAbortSignal(this.config),
        },
      );
    } catch (error) {
      if (error instanceof DomainException) throw error;
      if (isAbortError(error))
        throw storeProviderUnavailable('Google Play', 'timeout');
      throw storeProviderUnavailable('Google Play', 'không kết nối được');
    }

    if (!response.ok) {
      // Store trả 4xx nghĩa là purchase token bị từ chối (lỗi của client); mọi lỗi khác là lỗi
      // dependency (503).
      if (response.status >= 400 && response.status < 500) {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          `Google từ chối purchase token (${response.status})`,
          HttpStatus.BAD_REQUEST,
        );
      }
      throw storeProviderUnavailable(
        'Google Play',
        `trả HTTP ${response.status}`,
      );
    }

    // Chỉ chấp nhận purchaseState = purchased (0); giao dịch đang chờ hay đã hủy chưa được cộng
    // Diamond. orderId của Google làm providerTransactionId.
    let body: { purchaseState?: number; orderId?: string };
    try {
      body = (await response.json()) as {
        purchaseState?: number;
        orderId?: string;
      };
    } catch {
      throw storeProviderUnavailable(
        'Google Play',
        'trả response không hợp lệ',
      );
    }
    if (
      body.purchaseState !== GOOGLE_PURCHASE_STATE_PURCHASED ||
      !body.orderId
    ) {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        'Purchase chưa ở trạng thái purchased',
        HttpStatus.BAD_REQUEST,
      );
    }
    return { providerTransactionId: body.orderId };

    const diamonds = BigInt(product.diamonds);

    // Step 7: Ghi sổ Nợ system_iap / Có ví user, key theo id giao dịch store
    const result = await this.ledger.record({
      type: TransactionType.IapPurchase,
      // Key do server sinh từ id giao dịch của store, không lấy từ header client: gửi lại receipt
      // bao nhiêu lần cũng chỉ credit một lần. Key không chứa userId, nên user khác gửi lại
      // receipt này sẽ lệch request_hash và bị 409.
      idempotencyKey: `iap:${provider}:${verified.providerTransactionId}`,
      actorUserId: userId,
      metadata: {
        provider,
        productId,
        providerTransactionId: verified.providerTransactionId,
        diamonds: product.diamonds,
      },
      entries: [
        {
          accountKind: LedgerAccountKind.SystemIap,
          direction: LedgerDirection.Debit,
          amount: diamonds,
          currency: LedgerCurrency.Diamond,
        },
        {
          accountKind: LedgerAccountKind.UserWallet,
          userId,
          direction: LedgerDirection.Credit,
          amount: diamonds,
          currency: LedgerCurrency.Diamond,
        },
      ],

      // Step 8: Lưu iap_receipts cùng transaction (unique chặn credit đôi)
      // [POSTGRES] Lưu iap_receipts CÙNG transaction với bút toán; unique (provider,
      // provider_transaction_id) chặn credit đôi ở DB. transactionId bắt buộc vì job đối soát coi
      // receipt thiếu transaction là lệch.
      withinTransaction: async (manager, txn) => {
        await manager.save(
          manager.create(IapReceipt, {
            provider,
            providerTransactionId: verified.providerTransactionId,
            userId,
            productId,
            transactionId: txn.id,
            rawPayload: payload,
          }),
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

    // Step 9: Trả transactionId, số Diamond và cờ replayed
    return {
      transactionId: result.transaction.id,
      diamonds: diamonds.toString(),
      replayed: result.replayed,
    };
  }
}

// [POSTGRES] CREATE TABLE iap_receipts · database/migrations/1752000000000-economy-ledger.ts
await queryRunner.query(`
  CREATE TABLE iap_receipts (
    id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                 varchar(16)  NOT NULL,
    provider_transaction_id  varchar(255) NOT NULL,
    user_id                  uuid         NOT NULL REFERENCES users(id),
    product_id               varchar(128) NOT NULL,
    status                   varchar(16)  NOT NULL DEFAULT 'credited',
    transaction_id           uuid         NULL REFERENCES transactions(id),
    raw_payload              jsonb        NOT NULL DEFAULT '{}',
    created_at               timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_iap_receipts_provider_txn UNIQUE (provider, provider_transaction_id)
  )
`);

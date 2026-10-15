//  POST /api/v1/economy/webhooks/apple
//  Apple báo REFUND hoặc REVOKE: kiểm chữ ký JWS, rồi đảo bút toán của lần nạp gốc; số dư có thể âm.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận webhook công khai của Apple (tối đa 60 lần/phút)
//  Step 2: Kiểm chữ ký JWS trước khi tin bất kỳ trường nào
//  Step 3: Xác thực chuỗi x5c lên Apple Root CA G3, rồi verify ES256
//  Step 4: Đối chiếu bundleId, chỉ xử lý REFUND và REVOKE
//  Step 5: Giao RefundService tìm receipt gốc và hoàn tiền
//  Step 6: Tìm receipt gốc; không rõ hoặc đã hoàn thì dừng
//  Step 7: Nhờ LedgerService.reverse đảo bút toán của giao dịch gốc
//  Step 8: Ghi giao dịch đảo vào sổ; số dư ví được phép âm
//  Step 9: Đánh dấu giao dịch gốc reversed (UPDATE có điều kiện)
//  Step 10: Gốc đã bị đảo trước đó: coi là idempotent, nối receipt
//  Step 11: Luôn ACK 200 cho Apple sau khi đã verify

// [CONTROLLER] EconomyWebhooksController.appleNotification · economy/webhooks/economy-webhooks.controller.ts
@Public()
@Controller('economy/webhooks')
export class EconomyWebhooksController {
  private readonly logger = new Logger(EconomyWebhooksController.name);
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // DevAppleNotificationVerifier
  private readonly logger = new Logger(DevAppleNotificationVerifier.name);
  // RefundService
  private readonly logger = new Logger(RefundService.name);
  constructor(
    private readonly config: ConfigService<CoreApiEnv, true>,
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // RefundService
    @InjectRepository(IapReceipt)
    private readonly receiptRepo: Repository<IapReceipt>,
    @InjectRepository(LedgerTransaction)
    private readonly transactionRepo: Repository<LedgerTransaction>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // DevAppleNotificationVerifier
    super();
    // LedgerService
    super();
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận webhook công khai của Apple (tối đa 60 lần/phút)

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

  @Post('apple')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 60, ttl: minutes(1) } })
  async appleNotification(
    @Body() dto: AppleServerNotificationDto,
  ): Promise<{ received: boolean }> {

    // Step 2: Kiểm chữ ký JWS trước khi tin bất kỳ trường nào
    // Chữ ký JWS được verify TRƯỚC khi tin bất kỳ trường nào; payload chưa verify không được
    // dùng.
    const payload = await this.appleVerifier.verify(dto.signedPayload);

    // [SERVICE] DevAppleNotificationVerifier.verify · economy/ports/notification-verifier.ts
    const signedPayload = dto.signedPayload;
    this.logger.warn(
      '[DEV-ONLY] chấp nhận Apple notification KHÔNG verify chữ ký',
    );
    return decodeJwt(signedPayload) as unknown as AppleNotificationPayload;

    // [EXTERNAL] StoreAppleNotificationVerifier.verify
    // signedPayload = dto.signedPayload
    const header = decodeProtectedHeader(signedPayload) as { x5c?: string[] };
    if (!header.x5c || header.x5c.length === 0) {
      throw new DomainException(
        EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
        'Thiếu x5c trong JWS header',
        HttpStatus.UNAUTHORIZED,
      );
    }

    // Step 3: Xác thực chuỗi x5c lên Apple Root CA G3, rồi verify ES256
    // Chuỗi chứng chỉ x5c trong header phải dẫn lên Apple Root CA G3 (nạp từ env, không hardcode)
    // và mọi chứng chỉ còn hạn.
    const certs = header.x5c.map(
      (b64) => new X509Certificate(Buffer.from(b64, 'base64')),
    );
    const trustedRootPem = this.config
      .getOrThrow('ECONOMY_APPLE_ROOT_CA_PEM', { infer: true })
      .replace(/\\n/g, '\n');
    const trustedRoot = new X509Certificate(trustedRootPem);

    // [EXTERNAL] StoreAppleNotificationVerifier.chainIsTrusted
    const now = new Date();
    if (
      certs.some(
        (c) => now < new Date(c.validFrom) || now > new Date(c.validTo),
      )
    )
      return false;
    for (let i = 0; i < certs.length - 1; i++) {
      if (
        !certs[i].checkIssued(certs[i + 1]) ||
        !certs[i].verify(certs[i + 1].publicKey)
      )
        return false;
    }
    const last = certs[certs.length - 1];
    if (last.fingerprint256 === trustedRoot.fingerprint256) return true;
    return last.checkIssued(trustedRoot) && last.verify(trustedRoot.publicKey);

    if (!this.chainIsTrusted(certs, trustedRoot)) {
      throw new DomainException(
        EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
        'Chain chứng chỉ Apple không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }

    // Khóa cứng ES256, không đọc alg từ header vì đó là dữ liệu CHƯA xác thực (chống algorithm
    // confusion).
    const leafPublicKey = await importX509(certs[0].toString(), 'ES256');
    const { payload } = await jwtVerify(signedPayload, leafPublicKey, {
      algorithms: ['ES256'],
    });
    return payload as unknown as AppleNotificationPayload;

    // Step 4: Đối chiếu bundleId, chỉ xử lý REFUND và REVOKE
    // Chain hợp lệ chỉ chứng minh "Apple ký", không chứng minh "ký cho app này" (mọi app dùng
    // chung chuỗi chứng chỉ), nên phải đối chiếu bundleId. Sai bundleId vẫn ACK 200 để store
    // không retry dồn dập.
    const expectedBundleId = this.config.getOrThrow('ECONOMY_APPLE_BUNDLE_ID', {
      infer: true,
    });
    if (expectedBundleId && payload.data.bundleId !== expectedBundleId) {
      this.logger.warn(
        `Apple notification bundleId lạ: ${payload.data.bundleId}`,
      );
      return { received: true };
    }

    if (
      !APPLE_REFUND_NOTIFICATION_TYPES.has(payload.notificationType) ||
      !payload.data.signedTransactionInfo
    ) {
      return { received: true };
    }
    // signedTransactionInfo lồng bên trong đã nằm trong phạm vi chữ ký của payload ngoài, nên chỉ
    // cần decode, không verify lần nữa.
    const txnInfo = this.appleVerifier.decodeTransactionInfo(
      payload.data.signedTransactionInfo,
    );

    // [SERVICE] DevAppleNotificationVerifier.decodeTransactionInfo · economy/ports/notification-verifier.ts
    const signedTransactionInfo = payload.data.signedTransactionInfo;
    return decodeJwt(signedTransactionInfo) as unknown as AppleTransactionInfo;

    // [EXTERNAL] StoreAppleNotificationVerifier.decodeTransactionInfo
    // signedTransactionInfo = payload.data.signedTransactionInfo
    return decodeJwt(signedTransactionInfo) as unknown as AppleTransactionInfo;

    // Step 5: Giao RefundService tìm receipt gốc và hoàn tiền
    await this.refundService.refundIapPurchase(
      IapProvider.Apple,
      txnInfo.transactionId,
      `apple:${payload.notificationType}${payload.subtype ? `:${payload.subtype}` : ''}`,
    );

    // [SERVICE] RefundService.refundIapPurchase · economy/services/refund.service.ts
    const provider = IapProvider.Apple;
    const providerTransactionId = txnInfo.transactionId;
    const reason =
      `apple:${payload.notificationType}${payload.subtype ? `:${payload.subtype}` : ''}`;

    // Step 6: Tìm receipt gốc; không rõ hoặc đã hoàn thì dừng
    const receipt = await this.receiptRepo.findOneBy({
      provider,
      providerTransactionId,
    });
    // Receipt không rõ: có thể của app khác, hoặc refund đến gần như tức thời khi credit chưa xử
    // lý xong. Trả unknown_receipt, job quét định kỳ sẽ bắt lại; không phải lỗi cần retry gấp.
    if (!receipt) {
      this.logger.warn(
        `Refund cho receipt không rõ: ${provider}:${providerTransactionId}`,
      );
      return { outcome: 'unknown_receipt' };
    }
    // Đã hoàn thì bỏ qua: webhook retry hay job quét trùng cũng chỉ hoàn một lần.
    if (receipt.status === IapReceiptStatus.Refunded) {
      return {
        outcome: 'already_refunded',
        transactionId: receipt.refundTransactionId ?? undefined,
      };
    }
    if (!receipt.transactionId) {
      this.logger.error(
        `Receipt ${receipt.id} đã credited nhưng không có transactionId — dữ liệu lệch, cần điều tra`,
      );
      return { outcome: 'receipt_not_credited' };
    }

    try {

      // Step 7: Nhờ LedgerService.reverse đảo bút toán của giao dịch gốc
      const result = await this.ledger.reverse(
        receipt.transactionId,
        `refund:${provider}:${providerTransactionId}`,
        reason,
        {
          // actor = null: hoàn tiền tự động từ store, không phải hành động của user.
          actorUserId: null,
          outboxEventTypeOverride: 'economy.diamond.refunded',
          // [POSTGRES] Đánh dấu receipt refunded CÙNG transaction với bút toán đảo, nên không có
          // trạng thái ví đã bị đảo mà receipt vẫn credited.
          withinTransaction: async (manager, reversalTxn) => {
            await manager.update(
              IapReceipt,
              { id: receipt.id },
              {
                status: IapReceiptStatus.Refunded,
                refundTransactionId: reversalTxn.id,
              },
            );
          },
        },
      );

      // [SERVICE] LedgerService.reverse · economy/services/ledger.service.ts
      const originalTransactionId = receipt.transactionId;
      const idempotencyKey = `refund:${provider}:${providerTransactionId}`;
      // opts = đối số 4 của lời gọi ở trên
      const original = await this.dataSource
        .getRepository(LedgerTransaction)
        .findOneBy({ id: originalTransactionId });
      if (!original) {
        throw new DomainException(
          EconomyErrors.TRANSACTION_NOT_FOUND,
          'Không tìm thấy giao dịch gốc',
          HttpStatus.NOT_FOUND,
        );
      }
      if (original.status === TransactionStatus.Reversed) {
        throw new DomainException(
          EconomyErrors.TRANSACTION_ALREADY_REVERSED,
          'Giao dịch đã được hoàn trước đó',
          HttpStatus.CONFLICT,
        );
      }

      const entries = await this.dataSource
        .getRepository(LedgerEntry)
        .findBy({ transactionId: original.id });
      const accounts = await this.dataSource
        .getRepository(LedgerAccount)
        .findBy(entries.map((e) => ({ id: e.accountId })));
      const accountById = new Map(accounts.map((a) => [a.id, a]));

      // Step 8: Ghi giao dịch đảo vào sổ; số dư ví được phép âm
      return this.record({
        // Reversal được phép đẩy số dư âm = user nợ diamond (đã tiêu rồi mới bị hoàn). Không kẹp
        // về 0 vì sẽ phá sổ kép; nợ được trừ dần khi user nạp tiếp.
        type: TransactionType.Reversal,
        idempotencyKey,
        actorUserId:
          opts.actorUserId !== undefined
            ? (opts.actorUserId ?? undefined)
            : (original.actorUserId ?? undefined),
        reversalOf: original.id,
        metadata: { reason, originalType: original.type },
        outboxEventTypeOverride: opts.outboxEventTypeOverride,
        entries: entries.map((e) => {
          const account = accountById.get(e.accountId);
          if (!account)
            throw new Error(
              `Ledger account ${e.accountId} biến mất — dữ liệu hỏng`,
            );
          return {
            accountKind: account.kind,
            userId: account.userId ?? undefined,
            // Đảo chiều từng bút toán của giao dịch gốc, không sửa hay xóa giao dịch gốc: trigger
            // ở DB chặn UPDATE và DELETE trên ledger_entries.
            direction:
              e.direction === LedgerDirection.Debit
                ? LedgerDirection.Credit
                : LedgerDirection.Debit,
            amount: BigInt(e.amount),
            currency: e.currency,
          };
        }),
        withinTransaction: async (manager, reversalTxn) => {

          // Step 9: Đánh dấu giao dịch gốc reversed (UPDATE có điều kiện)
          // [POSTGRES] UPDATE có điều kiện status = completed: 2 request cùng đảo thì chỉ 1 bên
          // cập nhật được dòng; bên thua ném lỗi và rollback bút toán đảo của mình.
          const marked = await manager.update(
            LedgerTransaction,
            { id: original.id, status: TransactionStatus.Completed },
            { status: TransactionStatus.Reversed },
          );
          if (!marked.affected) {
            throw new DomainException(
              EconomyErrors.TRANSACTION_ALREADY_REVERSED,
              'Giao dịch đã được hoàn trước đó',
              HttpStatus.CONFLICT,
            );
          }
          await opts.withinTransaction?.(manager, reversalTxn);
        },
      });

      // [SERVICE] LedgerService.record
      // input = đối số 1 của lời gọi ở trên
      this.validateInput(input);

      // [SERVICE] LedgerService.validateInput
      this.validateEntries(input.entries);

      // [SERVICE] LedgerService.validateEntries
      // entries = input.entries
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

      return { outcome: 'refunded', transactionId: result.transaction.id };
    } catch (err) {
      if (
        err instanceof DomainException &&
        err.code === EconomyErrors.TRANSACTION_ALREADY_REVERSED
      ) {

        // Step 10: Gốc đã bị đảo trước đó: coi là idempotent, nối receipt
        // Giao dịch gốc đã bị đảo trước (vd admin) nhưng receipt chưa kịp cập nhật: coi là
        // idempotent, tìm đúng giao dịch đã đảo để giữ vết kiểm toán.
        const existingReversal = await this.transactionRepo.findOneBy({
          reversalOf: receipt.transactionId,
        });
        await this.receiptRepo.update(
          { id: receipt.id },
          {
            status: IapReceiptStatus.Refunded,
            refundTransactionId: existingReversal?.id ?? null,
          },
        );
        return {
          outcome: 'already_refunded',
          transactionId: existingReversal?.id,
        };
      }
      throw err;
    }

    // Step 11: Luôn ACK 200 cho Apple sau khi đã verify
    return { received: true };
  }
}

// [POSTGRES] CREATE TABLE wallets · database/migrations/1752000000000-economy-ledger.ts
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

// [POSTGRES] 1752000000000-economy-ledger.ts
await queryRunner.query(`
  CREATE TRIGGER trg_ledger_entries_append_only
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_ledger_entry_mutation()
`);

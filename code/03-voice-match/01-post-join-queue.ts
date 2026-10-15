//  POST /api/v1/matching/tickets
//  Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request vào hàng đợi (user, body, Idempotency-Key, guest)
//  Step 2: Mở transaction DB; khóa dòng user, kiểm guest và tài khoản bị khóa
//  Step 3: Đã có ticket cùng Idempotency-Key thì trả lại luôn, không trừ quota
//  Step 4: Trừ lượt miễn phí theo ngày (khóa dòng quota) hoặc trả Diamond
//  Step 5: Ghi ticket queued vào Postgres (unique index: mỗi user 1 vé đang chờ)
//  Step 6: Dính unique: retry cùng key thì trả vé cũ, còn lại 409
//  Step 7: Sau commit, đẩy ticket vào Redis (ensureEnqueued: ZADD NX, đánh dấu shard active)
//  Step 8: Trả ticket về controller (bọc thành TicketDto kèm giá speed-up)
//  Step 9: Worker nền: ZPOPMIN lấy 2 vé điểm thấp nhất của shard (nguyên tử)
//  Step 10: Khóa 2 vé FOR UPDATE, verify lại trong DB (giới tính, canPair)
//  Step 11: Ghép được: tạo MatchSession, cập nhật 2 vé, tạo thông báo
//  Step 12: Sau commit: vé không ghép được về lại Redis, hoặc phát match.confirmed

// [CONTROLLER] MatchingController.joinQueue · matching/matching.controller.ts
@Controller('matching')
export class MatchingController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // MatcherWakeup
  private readonly listeners = new Set<MatcherWakeListener>();
  // MatcherWorkerService
  private readonly logger = new Logger(MatcherWorkerService.name);
  private readonly job = new ManagedInterval();
  // NotificationService
  private readonly logger = new Logger(NotificationService.name);
  // ApnsPushNotificationAdapter
  private readonly transport: ApnsPushTransport;
  // DevPushNotificationAdapter
  private readonly logger = new Logger(DevPushNotificationAdapter.name);
  // FcmPushNotificationAdapter
  private readonly transport: FcmPushTransport;
  // FcmHttpTransport
  private accessToken: { value: string; expiresAt: number } | undefined;
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
    // MatchingService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(MatchTicket)
    private readonly ticketRepo: Repository<MatchTicket>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // GuestDeviceTokenService
    private readonly jwt: JwtService,
    // EconomyService
    @InjectRepository(Wallet) private readonly walletRepo: Repository<Wallet>,
    // RedisMatchingQueue
    private readonly activeShardsKey: string,
    // WebPushSubscriptionService
    @InjectRepository(WebPushSubscription)
    private readonly subscriptionRepo: Repository<WebPushSubscription>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // ApnsPushNotificationAdapter
    super();
    this.transport = transport ?? new ApnsHttp2Transport(this.config);
    // DevPushNotificationAdapter
    super();
    // FcmPushNotificationAdapter
    super();
    this.transport = transport ?? new FcmHttpTransport(this.config);
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request vào hàng đợi (user, body, Idempotency-Key,
  // guest)

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

  @Post('tickets')
  @Throttle({ default: { limit: 10, ttl: minutes(1) } })
  async joinQueue(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Body() dto: JoinQueueDto,
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

    // [SERVICE] MatchingService.joinQueue · matching/matching.service.ts
    const quotaContext = {
      deviceToken: guestDeviceToken,
      ip: req.ip || req.socket.remoteAddress || 'unknown',
    };
    const genderPreference = dto.genderPreference ?? GenderPreference.Any;
    const useDiamond = dto.useDiamond ?? false;
    const prefixedKey = joinIdempotencyKey(user.userId, idempotencyKey);

    // [SERVICE] joinIdempotencyKey · matching/matching.constants.ts
    const userId = user.userId;
    return `matching:join:${userId}:${idempotencyKey}`;

    let ticket: MatchTicket;
    try {

      // Step 2: Mở transaction DB; khóa dòng user, kiểm guest và tài khoản bị khóa
      // [POSTGRES] Ghi DB trong 1 transaction, Redis chỉ thêm SAU commit: Postgres là nguồn sự
      // thật của ticket, Redis chỉ là hàng đợi dẫn xuất.
      ticket = await this.dataSource.transaction(async (manager) => {
        const authorization = await this.guestQuota.authorize(
          manager,
          user.userId,
          quotaContext,
        );

        // [SERVICE] GuestMatchQuotaService.authorize · matching/services/guest-match-quota.service.ts
        // userId = user.userId
        const context = quotaContext;
        // [POSTGRES] Khóa dòng user (FOR UPDATE) và đọc isGuest tươi từ DB: JWT cũ sau khi nâng
        // cấp tài khoản không né được giới hạn của guest.
        const user = await manager.findOneOrFail(User, {
          where: { id: userId },
          lock: { mode: 'pessimistic_write' },
        });
        const quotaDate = new Date().toISOString().slice(0, 10);
        if (!user.isGuest) return { user, quotaDate, keyHashes: [] };

        if (!context.deviceToken) {
          throw new DomainException(
            MatchingErrors.GUEST_DEVICE_TOKEN_REQUIRED,
            'Guest phải gửi X-Guest-Device-Token được cấp lúc đăng nhập',
            HttpStatus.UNAUTHORIZED,
          );
        }
        const deviceKey = await this.deviceTokens.verifyForUser(
          context.deviceToken,
          userId,
        );

        // [SERVICE] GuestDeviceTokenService.verifyForUser · auth/services/guest-device-token.service.ts
        // token = context.deviceToken
        try {
          const payload = await this.jwt.verifyAsync<GuestDeviceTokenPayload>(
            token,
            {
              secret: this.secret(),
              algorithms: ['HS256'],
              audience: 'litmatch-matching',
              issuer: 'litmatch-core-api',
            },
          );

          // [SERVICE] GuestDeviceTokenService.secret
          return this.config.getOrThrow('AUTH_GUEST_DEVICE_TOKEN_SECRET', {
            infer: true,
          });

          if (
            payload.purpose !== 'guest-device' ||
            payload.sub !== userId ||
            !/^[a-f0-9]{64}$/.test(payload.deviceKey)
          ) {
            throw new Error('guest device token binding invalid');
          }
          return payload.deviceKey;
        } catch {
          throw new DomainException(
            AuthErrors.GUEST_DEVICE_TOKEN_INVALID,
            'Guest device token không hợp lệ hoặc đã hết hạn',
            HttpStatus.UNAUTHORIZED,
          );
        }

        const keyHashes = [
          this.keyHash('user', userId),
          this.keyHash('device', deviceKey),
          this.keyHash('network', this.normalizeNetwork(context.ip)),
        ];

        // [SERVICE] GuestMatchQuotaService.normalizeNetwork
        // value = context.ip
        const normalized = value.trim().toLowerCase();
        return normalized.startsWith('::ffff:') ? normalized.slice(7) : normalized;

        // [SERVICE] GuestMatchQuotaService.keyHash
        const kind = 'user';
        // value = userId
        return createHmac(
          'sha256',
          this.config.getOrThrow('MATCHING_GUEST_QUOTA_PEPPER', { infer: true }),
        )
          .update(`${kind}:${value}`)
          .digest('hex');

        return {
          user,
          quotaDate,
          keyHashes: keyHashes.sort(),
        };

        const profile = authorization.user;
        if (profile.status === UserStatus.Banned) {
          throw new DomainException(
            MatchingErrors.USER_BANNED,
            'Tài khoản bị khoá, không thể vào hàng đợi',
            HttpStatus.FORBIDDEN,
          );
        }

        // Step 3: Đã có ticket cùng Idempotency-Key thì trả lại luôn, không trừ quota
        // Nhận ra retry (cùng Idempotency-Key) TRƯỚC khi trừ quota, để retry không tốn thêm lượt
        // hay Diamond.
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

          // [SERVICE] MatchingService.assertJoinReplay
          // ticket = replay
          // userId = user.userId
          const matchType = dto.matchType;
          if (
            ticket.userId === userId &&
            ticket.matchType === matchType &&
            ticket.genderPreference === genderPreference &&
            ticket.paidDiamond === useDiamond
          )
            return;
          throw new DomainException(
            MatchingErrors.TICKET_IDEMPOTENCY_CONFLICT,
            'Idempotency-Key đã dùng cho 1 request khác nội dung',
            HttpStatus.CONFLICT,
          );

          return replay;
        }

        // Step 4: Trừ lượt miễn phí theo ngày (khóa dòng quota) hoặc trả Diamond
        const quota = await this.guestQuota.consumeForMatch(
          manager,
          authorization,
          dto.matchType,
          useDiamond,
        );

        // [SERVICE] GuestMatchQuotaService.consumeForMatch · matching/services/guest-match-quota.service.ts
        // matchType = dto.matchType
        const guestLimit = this.config.getOrThrow('MATCHING_GUEST_DAILY_LIMIT', {
          infer: true,
        });
        if (useDiamond)
          return {
            quotaDate: authorization.quotaDate,
            freeLimit: guestLimit,
            tier: null,
            paidDiamond: true,
          };

        if (authorization.keyHashes.length > 0) {
          await this.consume(manager, authorization);

          // [SERVICE] GuestMatchQuotaService.consume
          if (authorization.keyHashes.length === 0) return;
          const { quotaDate, keyHashes } = authorization;
          await manager
            .createQueryBuilder()
            .insert()
            .into(GuestMatchQuota)
            .values(keyHashes.map((keyHash) => ({ quotaDate, keyHash, count: 0 })))
            .orIgnore()
            .execute();

          const rows = await manager
            .createQueryBuilder(GuestMatchQuota, 'q')
            .setLock('pessimistic_write')
            .where('q.quotaDate = :quotaDate', { quotaDate })
            .andWhere('q.keyHash IN (:...keyHashes)', { keyHashes })
            .orderBy('q.keyHash', 'ASC')
            .getMany();
          const limit = this.config.getOrThrow('MATCHING_GUEST_DAILY_LIMIT', {
            infer: true,
          });
          if (
            rows.length !== keyHashes.length ||
            rows.some((row) => row.count >= limit)
          ) {
            throw new DomainException(
              MatchingErrors.GUEST_DAILY_QUOTA_EXCEEDED,
              `Guest đã hết ${limit} lượt match miễn phí trong ngày UTC`,
              HttpStatus.TOO_MANY_REQUESTS,
              { limit, quotaDate },
            );
          }
          await manager
            .createQueryBuilder()
            .update(GuestMatchQuota)
            .set({ count: () => 'count + 1', updatedAt: () => 'CURRENT_TIMESTAMP' })
            .where('quota_date = :quotaDate', { quotaDate })
            .andWhere('key_hash IN (:...keyHashes)', { keyHashes })
            .execute();

          return {
            quotaDate: authorization.quotaDate,
            freeLimit: guestLimit,
            tier: null,
            paidDiamond: false,
          };
        }

        const tier = await this.economy.getActiveVipTier(
          authorization.user.id,
          manager,
        );

        // [SERVICE] EconomyService.getActiveVipTier · economy/economy.service.ts
        // userId = authorization.user.id
        const wallet = manager
          ? await manager.getRepository(Wallet).findOne({
              where: { userId },
              lock: { mode: 'pessimistic_write' },
            })
          : await this.walletRepo.findOneBy({ userId });
        return wallet?.activeVipTier ?? null;

        const freeLimit = this.dailyLimit(tier, matchType);

        // [SERVICE] GuestMatchQuotaService.dailyLimit
        if (tier === VipTier.Svip) {
          return matchType === MatchType.Soul
            ? this.config.getOrThrow('MATCHING_SVIP_DAILY_SOUL_LIMIT', {
                infer: true,
              })
            : this.config.getOrThrow('MATCHING_SVIP_DAILY_VOICE_LIMIT', {
                infer: true,
              });
        }
        if (tier === VipTier.Vip) {
          return matchType === MatchType.Soul
            ? this.config.getOrThrow('MATCHING_VIP_DAILY_SOUL_LIMIT', {
                infer: true,
              })
            : this.config.getOrThrow('MATCHING_VIP_DAILY_VOICE_LIMIT', {
                infer: true,
              });
        }
        return matchType === MatchType.Soul
          ? this.config.getOrThrow('MATCHING_DAILY_SOUL_LIMIT', { infer: true })
          : this.config.getOrThrow('MATCHING_DAILY_VOICE_LIMIT', { infer: true });

        // [POSTGRES] Đếm lượt miễn phí an toàn khi đồng thời: INSERT … ON CONFLICT DO NOTHING tạo
        // dòng đếm nếu chưa có, rồi SELECT … FOR UPDATE khóa dòng đó trước khi kiểm và cộng 1,
        // nên 2 request cùng lúc không cùng lọt qua giới hạn.
        const quotaDate = authorization.quotaDate;
        await manager
          .createQueryBuilder()
          .insert()
          .into(MatchDailyQuota)
          .values({
            userId: authorization.user.id,
            quotaDate,
            matchType,
            count: 0,
          })
          .orIgnore()
          .execute();
        const row = await manager
          .getRepository(MatchDailyQuota)
          .createQueryBuilder('q')
          .setLock('pessimistic_write')
          .where('q.user_id = :userId', { userId: authorization.user.id })
          .andWhere('q.quota_date = :quotaDate', { quotaDate })
          .andWhere('q.match_type = :matchType', { matchType })
          .getOneOrFail();
        if (row.count >= freeLimit) {
          throw new DomainException(
            MatchingErrors.DAILY_QUOTA_EXCEEDED,
            `Đã hết ${freeLimit} lượt ${matchType} miễn phí trong ngày UTC`,
            HttpStatus.TOO_MANY_REQUESTS,
            { limit: freeLimit, quotaDate, matchType },
          );
        }
        await manager
          .createQueryBuilder()
          .update(MatchDailyQuota)
          .set({ count: () => 'count + 1', updatedAt: () => 'CURRENT_TIMESTAMP' })
          .where('user_id = :userId', { userId: authorization.user.id })
          .andWhere('quota_date = :quotaDate', { quotaDate })
          .andWhere('match_type = :matchType', { matchType })
          .execute();
        return { quotaDate, freeLimit, tier, paidDiamond: false };

        // Trừ Diamond CÙNG transaction với insert ticket: insert lỗi thì tiền rollback theo.
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

          // [SERVICE] MatchingService.getExtraMatchPriceDiamond
          // matchType = dto.matchType
          return matchType === MatchType.Soul
            ? this.config.getOrThrow('MATCHING_EXTRA_SOUL_PRICE_DIAMOND', {
                infer: true,
              })
            : this.config.getOrThrow('MATCHING_EXTRA_VOICE_PRICE_DIAMOND', {
                infer: true,
              });

          // [SERVICE] EconomyService.spendDiamondInTransaction · economy/economy.service.ts
          // userId = user.userId
          const type = TransactionType.MatchingExtraMatch;
          const amountDiamond = this.getExtraMatchPriceDiamond(dto.matchType);
          // idempotencyKey = `matching:extra-match:${user.userId}:${idempotencyKey}`
          const metadata = {
            matchType: dto.matchType,
            quotaDate: quota.quotaDate,
            freeLimit: quota.freeLimit,
          };
          if (!Number.isSafeInteger(amountDiamond) || amountDiamond <= 0) {
            throw new Error(
              `spendDiamondInTransaction: amountDiamond phải là số nguyên dương, nhận ${amountDiamond}`,
            );
          }
          const result = await this.ledger.recordInManager(manager, {
            type,
            idempotencyKey,
            actorUserId: userId,
            metadata,
            entries: [
              {
                accountKind: LedgerAccountKind.UserWallet,
                userId,
                direction: LedgerDirection.Debit,
                amount: BigInt(amountDiamond),
                currency: LedgerCurrency.Diamond,
              },
              {
                accountKind: LedgerAccountKind.SystemRevenue,
                direction: LedgerDirection.Credit,
                amount: BigInt(amountDiamond),
                currency: LedgerCurrency.Diamond,
              },
            ],
          });

          // [SERVICE] LedgerService.recordInManager · economy/services/ledger.service.ts
          // input = đối số 2 của lời gọi ở trên
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

          const existing = await manager.findOne(LedgerTransaction, {
            where: { idempotencyKey: input.idempotencyKey },
          });
          if (existing) {
            return {
              transaction: this.assertSameRequest(existing, requestHash),
              replayed: true,
            };

            // [SERVICE] LedgerService.assertSameRequest
            if (existing.requestHash !== requestHash) {
              throw new DomainException(
                EconomyErrors.IDEMPOTENCY_CONFLICT,
                'Idempotency key đã dùng cho 1 request khác nội dung',
                HttpStatus.CONFLICT,
              );
            }
            return existing;
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

          return { transactionId: result.transaction.id, replayed: result.replayed };
        }

        // Step 5: Ghi ticket queued vào Postgres (unique index: mỗi user 1 vé đang chờ)
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

        // [SERVICE] MatchingService.ageBandOf
        const birthDate = profile.birthDate;
        const bandSize = this.config.getOrThrow('MATCHING_AGE_BAND_SIZE', {
          infer: true,
        });
        return ageBandOf(birthDate, bandSize);

        // [SERVICE] ageBandOf · matching/matching.constants.ts
        if (!birthDate) return UNKNOWN_AGE_BAND;
        const birth = new Date(birthDate);
        if (Number.isNaN(birth.getTime())) return UNKNOWN_AGE_BAND;
        const now = new Date();
        let age = now.getFullYear() - birth.getFullYear();
        const beforeBirthday =
          now.getMonth() < birth.getMonth() ||
          (now.getMonth() === birth.getMonth() && now.getDate() < birth.getDate());
        if (beforeBirthday) age -= 1;
        if (age < 0) return UNKNOWN_AGE_BAND;
        return Math.floor(age / bandSize);

        // [SERVICE] MatchingService.trustPenaltyMsOf · matching/matching.service.ts
        const trustScore = profile.trustScore;
        const perPoint = this.config.getOrThrow(
          'MATCHING_TRUST_PENALTY_MS_PER_POINT',
          { infer: true },
        );
        const maxMs = this.config.getOrThrow('MATCHING_TRUST_PENALTY_MAX_MS', {
          infer: true,
        });
        return trustPenaltyMsOf(trustScore, perPoint, maxMs);

        // [SERVICE] trustPenaltyMsOf · matching/matching.constants.ts
        return Math.min(maxMs, Math.max(0, 100 - trustScore) * perPoint);
      });
    } catch (err) {

      // Step 6: Dính unique: retry cùng key thì trả vé cũ, còn lại 409
      // [POSTGRES] Mỗi user chỉ 1 vé đang chờ được chặn bằng unique index (xem bên dưới) chứ
      // không check-rồi-insert, vì 2 request đồng thời sẽ cùng lọt qua check. Dính unique thì
      // kiểm retry TRƯỚC, vì cả 2 unique có thể cùng dính; không phải retry thì 409.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

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

      // [SERVICE] violatedConstraint · database/postgres-errors.ts
      const constraint = UQ_ACTIVE_USER;
      const e = err as {
        driverError?: { constraint?: string };
        constraint?: string;
        message?: string;
      };
      const name = e.driverError?.constraint ?? e.constraint;
      return name === constraint || (e.message?.includes(constraint) ?? false);

      if (violatedConstraint(err, UQ_ACTIVE_USER)) {
        throw new DomainException(
          MatchingErrors.TICKET_ALREADY_QUEUED,
          'Đã có 1 ticket đang chờ/đang ghép — 1 user chỉ ở trong 1 queue tại 1 thời điểm (docs/06)',
          HttpStatus.CONFLICT,
        );
      }
      throw err;
    }

    // Step 7: Sau commit, đẩy ticket vào Redis (ensureEnqueued: ZADD NX, đánh dấu shard active)
    // [REDIS] Redis lỗi ở bước này thì ticket "mồ côi" trong DB: client retry cùng key sẽ đẩy lại
    // vào Redis, sweeper expire là chốt chặn cuối.
    await this.ensureEnqueued(ticket);

    // [SERVICE] MatchingService.ensureEnqueued
    const shard = shardKeyOfTicket(ticket);

    // [REDIS] shardKeyOfTicket · matching/redis/matching-redis.provider.ts
    return matchingShardKey(ticket.matchType, ticket.region, ticket.ageBand);

    // [REDIS] matchingShardKey
    // matchType = ticket.matchType
    const region = ticket.region;
    const ageBand = ticket.ageBand;
    return `matching:queue:${matchType}:${region}:${ageBand}`;

    // [REDIS] NX: chỉ thêm nếu chưa có, để không đè score đã được boost (speed-up).
    await this.queue.enqueue(
      shard,
      String(ticketScore(ticket)),
      ticket.id,
      'NX',
    );

    // [REDIS] RedisMatchingQueue.enqueue · matching/redis/redis-matching-queue.adapter.ts
    const score = String(
      ticket.enqueuedAt.getTime() + ticket.trustPenaltyMs - ticket.priorityBoostMs,
    );
    const ticketId = ticket.id;
    const mode = 'NX';
    await this.redis.zadd(shard, mode, score, ticketId);

    // SADD sau ZADD: matcher có thể vừa SREM shard rỗng giữa 2 lệnh, SADD sau cùng khôi phục lại
    // shard.
    await this.queue.markActive(shard);

    // [REDIS] RedisMatchingQueue.markActive · matching/redis/redis-matching-queue.adapter.ts
    await this.redis.sadd(this.activeShardsKey, shard);

    this.matcherWakeup.notify();

    // [SERVICE] MatcherWakeup.notify · matching/matcher-wakeup.ts
    for (const listener of this.listeners) listener();

    // Step 8: Trả ticket về controller (bọc thành TicketDto kèm giá speed-up)
    return ticket;

    // [SERVICE] MatchingService.getSpeedupPriceDiamondForUser
    // userId = user.userId
    const tier = await this.economy.getActiveVipTier(userId);
    if (tier === VipTier.Svip)
      return this.config.getOrThrow('MATCHING_SVIP_SPEEDUP_PRICE_DIAMOND', {
        infer: true,
      });
    if (tier === VipTier.Vip)
      return this.config.getOrThrow('MATCHING_VIP_SPEEDUP_PRICE_DIAMOND', {
        infer: true,
      });
    return this.getSpeedupPriceDiamond();

    // [SERVICE] MatchingService.getSpeedupPriceDiamond
    return this.config.getOrThrow('MATCHING_SPEEDUP_PRICE_DIAMOND', {
      infer: true,
    });
  }
}

// [WORKER] MatcherWorkerService.runOnce · matching/jobs/matcher-worker.service.ts
@Injectable()
export class MatcherWorkerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  // Chạy khi có enqueue (gom tín hiệu liên tiếp thành 1 batch) và theo interval dự phòng thưa;
  // nhiều pod chạy song song vẫn an toàn nhờ ZPOPMIN nguyên tử.
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {
      return withSpan(
        'litmatch.matching',
        'matching.matcher.tick',
        async (span) => {
          let matched = 0;

          // [REDIS] RedisMatchingQueue.listActiveShards · matching/redis/redis-matching-queue.adapter.ts
          return this.redis.smembers(this.activeShardsKey);

          for (const shard of await this.queue.listActiveShards()) {
            matched += await this.drainShard(shard);

            // [WORKER] MatcherWorkerService.drainShard
            const batchSize = this.config.getOrThrow('MATCHING_MATCHER_BATCH_SIZE', {
              infer: true,
            });
            let matched = 0;
            for (let i = 0; i < batchSize; i++) {

              // Step 9: Worker nền: ZPOPMIN lấy 2 vé điểm thấp nhất của shard (nguyên tử)
              // [REDIS] ZPOPMIN lấy 2 vé điểm thấp nhất (điểm nhỏ = ghép trước) trong 1 lệnh
              // nguyên tử. Redis đơn luồng nên không cần Lua, và 2 matcher không bao giờ lấy
              // trùng 1 vé.
              const popped = await this.queue.popMin(shard, 2);

              // [REDIS] RedisMatchingQueue.popMin · matching/redis/redis-matching-queue.adapter.ts
              const count = 2;
              const values = await this.redis.zpopmin(shard, count);
              const entries: Array<[string, string]> = [];
              for (let index = 0; index + 1 < values.length; index += 2) {
                entries.push([values[index], values[index + 1]]);
              }
              return entries;

              if (popped.length === 0) {
                await this.cleanupShardIfEmpty(shard);

                // [WORKER] MatcherWorkerService.cleanupShardIfEmpty
                await this.queue.unmarkActive(shard);

                // [REDIS] RedisMatchingQueue.unmarkActive · matching/redis/redis-matching-queue.adapter.ts
                await this.redis.srem(this.activeShardsKey, shard);

                // [REDIS] RedisMatchingQueue.hasEntries
                // Kiểm lại sau SREM: nếu có enqueue chen giữa thì khôi phục shard, không nuốt
                // mất.
                return (await this.redis.zcard(shard)) > 0;

                if (await this.queue.hasEntries(shard)) await this.queue.markActive(shard);

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

              // [WORKER] MatcherWorkerService.tryPair
              const a = { id: popped[0][0], score: Number(popped[0][1]) };
              const b = { id: popped[1][0], score: Number(popped[1][1]) };
              const result = await this.dataSource.transaction(async (manager) => {

                // Step 10: Khóa 2 vé FOR UPDATE, verify lại trong DB (giới tính, canPair)
                // [POSTGRES] Redis không phải nguồn sự thật: khóa 2 vé FOR UPDATE rồi verify lại
                // trong DB. Khóa theo thứ tự id cố định để không deadlock với writer khác.
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

                // Giới tính lọc ở bước verify (đọc tươi từ users) chứ không chia shard theo giới
                // tính: shard nhân 3 và preference "any" phải quét nhiều shard. Đánh đổi: cặp
                // không khớp ở đầu shard chặn shard đó tới khi có người điểm thấp hơn vào hoặc
                // sweeper expire.
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

                // [SERVICE] genderPreferenceAllows · matching/entities/match-ticket.entity.ts
                const pref = ta.genderPreference;
                const partnerGender = userById.get(tb.userId)?.gender;
                return pref === GenderPreference.Any || (pref as string) === partnerGender;

                if (
                  aValid &&
                  bValid &&
                  ta.userId !== tb.userId &&
                  genderOk &&
                  (await this.interactionPolicy.canPair(ta.userId, tb.userId))
                ) {
                  const confirmedAt = new Date();

                  // Step 11: Ghép được: tạo MatchSession, cập nhật 2 vé, tạo thông báo
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
                  const matchNotifications = await Promise.all(
                    [ta, tb].map((ticket) =>
                      this.notificationService.createWithManager(manager, {
                        userId: ticket.userId,
                        type: NotificationType.MatchConfirmed,
                        payload: { sessionId: session.id, matchType: ticket.matchType },
                      }),
                    ),
                  );

                  // [SERVICE] NotificationService.createWithManager · notification/notification.service.ts
                  const input = {
                    userId: ticket.userId,
                    type: NotificationType.MatchConfirmed,
                    payload: { sessionId: session.id, matchType: ticket.matchType },
                  };
                  return manager.save(
                    manager.create(Notification, { ...input, readAt: null }),
                  );

                  const matchedAt = Date.now();
                  return {
                    kind: 'matched' as const,
                    requeue: [] as PoppedTicket[],
                    matchedPair: [
                      {
                        userId: ta.userId,
                        ticketId: ta.id,
                        sessionId: session.id,
                        matchType: ta.matchType,
                        waitSeconds: (matchedAt - ta.enqueuedAt.getTime()) / 1000,
                      },
                      {
                        userId: tb.userId,
                        ticketId: tb.id,
                        sessionId: session.id,
                        matchType: tb.matchType,
                        waitSeconds: (matchedAt - tb.enqueuedAt.getTime()) / 1000,
                      },
                    ],
                    matchNotifications,
                  };
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

              // Step 12: Sau commit: vé không ghép được về lại Redis, hoặc phát match.confirmed
              // [REDIS] Không ghép được thì trả vé còn hợp lệ về Redis với score gốc (không mất
              // lượt chờ); vé hỏng mà vẫn queued đã bị expire ở trên. Việc này làm SAU commit.
              if (result.requeue.length > 0) {
                for (const r of result.requeue) {
                  await this.queue.enqueue(shard, String(r.score), r.id, 'NX');
                }
                await this.queue.markActive(shard);
              }
              if (result.matchedPair) {
                for (const { matchType, waitSeconds } of result.matchedPair) {
                  this.metrics.observeMatched(matchType, waitSeconds);
                }
                // [REDIS] Realtime (pub/sub) làm SAU commit và là best-effort: client vẫn còn
                // poll GET /matching/tickets/:id.
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

                    // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
                    const publisher = this.realtime;
                    const logger = this.logger;
                    try {
                      await publisher.publish(
                        realtimeUserChannel(userId),
                        JSON.stringify(envelope),
                      );

                      // [REDIS] RedisRealtimePublisher.publish · common/realtime/redis-realtime-publisher.adapter.ts
                      const channel = `${REALTIME_USER_CHANNEL_PREFIX}${userId}`;
                      const payload = JSON.stringify(envelope);
                      return this.redis.publish(channel, payload);
                    } catch (err) {
                      logger.warn(
                        `Publish realtime '${envelope.event}' cho user ${userId} thất bại (client còn polling fallback): ${
                          err instanceof Error ? err.message : String(err)
                        }`,
                      );
                    }
                  }),
                );
                await Promise.all(
                  result.matchNotifications.map((n) =>
                    this.notificationService.sendPush(n),
                  ),
                );

                // [SERVICE] NotificationService.sendPush · notification/notification.service.ts
                const notification = n;
                try {
                  const result = await this.pushPort.send({
                    notificationId: notification.id,
                    recipientId: notification.userId,
                    type: notification.type,
                    ...(deviceToken ? { deviceToken } : {}),
                  });

                  // [SERVICE] ApnsPushNotificationAdapter.send · common/platform/apns-push-notification.adapter.ts
                  const message = {
                    notificationId: notification.id,
                    recipientId: notification.userId,
                    type: notification.type,
                    ...(deviceToken ? { deviceToken } : {}),
                  };
                  if (message.deviceToken === undefined || message.deviceToken === '') {
                    return { status: 'skipped' };
                  }
                  try {
                    await this.transport.send({
                      token: message.deviceToken,
                      type: message.type,
                    });

                    // [SERVICE] ApnsHttp2Transport.send
                    // message = { token: message.deviceToken, type: message.type, }
                    const host =
                      this.config.getOrThrow('NOTIFICATION_APNS_ENVIRONMENT', {
                        infer: true,
                      }) === 'production'
                        ? APNS_PRODUCTION_HOST
                        : APNS_SANDBOX_HOST;
                    const token = await this.createProviderToken();

                    // [SERVICE] ApnsHttp2Transport.createProviderToken
                    const key = await importPKCS8(
                      this.config
                        .getOrThrow('NOTIFICATION_APNS_PRIVATE_KEY', { infer: true })
                        .replace(/\\n/g, '\n'),
                      'ES256',
                    );
                    return new SignJWT({})
                      .setProtectedHeader({
                        alg: 'ES256',
                        kid: this.config.getOrThrow('NOTIFICATION_APNS_KEY_ID', {
                          infer: true,
                        }),
                        typ: 'JWT',
                      })
                      .setIssuer(
                        this.config.getOrThrow('NOTIFICATION_APNS_TEAM_ID', { infer: true }),
                      )
                      .setIssuedAt()
                      .setExpirationTime('1h')
                      .sign(key);

                    const body = JSON.stringify({
                      aps: { alert: { title: 'Litmatch', body: 'Bạn có thông báo mới' } },
                      notification_type: message.type,
                    });
                    const url = new URL(host);

                    await new Promise<void>((resolve, reject) => {
                      const client = connect(url);
                      const timeout = setTimeout(
                        () => {
                          client.destroy();
                          reject(new Error('APNs push provider timed out'));
                        },
                        this.config.getOrThrow('NOTIFICATION_PUSH_HTTP_TIMEOUT_MS', {
                          infer: true,
                        }),
                      );
                      let settled = false;
                      const finish = (error?: Error) => {
                        if (settled) return;
                        settled = true;
                        clearTimeout(timeout);
                        client.close();
                        if (error) reject(error);
                        else resolve();
                      };

                      client.once('error', () => finish(new Error('APNs connection failed')));
                      const request = client.request({
                        ':method': 'POST',
                        ':path': `/3/device/${encodeURIComponent(message.token)}`,
                        authorization: `bearer ${token}`,
                        'apns-topic': this.config.getOrThrow('NOTIFICATION_APNS_BUNDLE_ID', {
                          infer: true,
                        }),
                        'apns-push-type': 'alert',
                        'content-type': 'application/json',
                        'content-length': Buffer.byteLength(body),
                      });
                      let status = 500;
                      request.once('response', (headers) => {
                        status = Number(headers[':status'] ?? 500);
                      });
                      request.on('data', () => undefined);
                      request.once('error', () => finish(new Error('APNs request failed')));
                      request.once('end', () => {
                        finish(
                          status >= 200 && status < 300
                            ? undefined
                            : new Error('APNs push provider rejected the request'),
                        );
                      });
                      request.end(body);
                    });

                    return { status: 'delivered' };
                  } catch {
                    return { status: 'failed' };
                  }

                  // [SERVICE] DisabledPushNotificationAdapter.send · common/platform/dev-platform-adapters.ts
                  // message = { notificationId: notification.id, recipientId:
                  // notification.userId, type: notification.type, ...(deviceToken ? { deviceToken
                  // } : {}), }
                  void message;
                  return { status: 'skipped' };

                  // [SERVICE] DevPushNotificationAdapter.send
                  // message = { notificationId: notification.id, recipientId:
                  // notification.userId, type: notification.type, ...(deviceToken ? { deviceToken
                  // } : {}), }
                  void message;
                  this.logger.debug('[dev-push] delivery skipped');
                  return { status: 'skipped' };

                  // [SERVICE] FcmPushNotificationAdapter.send · common/platform/fcm-push-notification.adapter.ts
                  // message = { notificationId: notification.id, recipientId:
                  // notification.userId, type: notification.type, ...(deviceToken ? { deviceToken
                  // } : {}), }
                  if (message.deviceToken === undefined || message.deviceToken === '') {
                    return { status: 'skipped' };
                  }
                  try {
                    await this.transport.send({
                      token: message.deviceToken,
                      type: message.type,
                    });

                    // [SERVICE] FcmHttpTransport.send
                    // message = { token: message.deviceToken, type: message.type, }
                    const projectId = this.config.getOrThrow('NOTIFICATION_FCM_PROJECT_ID', {
                      infer: true,
                    });
                    const accessToken = await this.getAccessToken();

                    // [SERVICE] FcmHttpTransport.getAccessToken
                    if (
                      this.accessToken !== undefined &&
                      this.accessToken.expiresAt > Date.now() + 60_000
                    ) {
                      return this.accessToken.value;
                    }

                    const email = this.config.getOrThrow('NOTIFICATION_FCM_CLIENT_EMAIL', {
                      infer: true,
                    });
                    const privateKeyPem = this.config
                      .getOrThrow('NOTIFICATION_FCM_PRIVATE_KEY', { infer: true })
                      .replace(/\\n/g, '\n');
                    const key = await importPKCS8(privateKeyPem, 'RS256');
                    const assertion = await new SignJWT({ scope: FCM_MESSAGING_SCOPE })
                      .setProtectedHeader({ alg: 'RS256', typ: 'JWT' })
                      .setIssuer(email)
                      .setAudience(FCM_OAUTH_TOKEN_URL)
                      .setIssuedAt()
                      .setExpirationTime('5m')
                      .sign(key);
                    const response = await fetch(FCM_OAUTH_TOKEN_URL, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                      body: new URLSearchParams({
                        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                        assertion,
                      }),
                      signal: AbortSignal.timeout(
                        this.config.getOrThrow('NOTIFICATION_PUSH_HTTP_TIMEOUT_MS', {
                          infer: true,
                        }),
                      ),
                    });
                    if (!response.ok)
                      throw new Error('FCM OAuth provider rejected the request');
                    const body = (await response.json()) as {
                      access_token?: unknown;
                      expires_in?: unknown;
                    };
                    if (typeof body.access_token !== 'string' || body.access_token === '') {
                      throw new Error('FCM OAuth response did not include an access token');
                    }
                    const expiresIn =
                      typeof body.expires_in === 'number' && body.expires_in > 0
                        ? body.expires_in
                        : 3600;
                    this.accessToken = {
                      value: body.access_token,
                      expiresAt: Date.now() + expiresIn * 1000,
                    };
                    return body.access_token;

                    const response = await fetch(
                      `${FCM_API_BASE_URL}/${encodeURIComponent(projectId)}/messages:send`,
                      {
                        method: 'POST',
                        headers: {
                          Authorization: `Bearer ${accessToken}`,
                          'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                          message: {
                            token: message.token,
                            data: { notification_type: message.type },
                          },
                        }),
                        signal: AbortSignal.timeout(
                          this.config.getOrThrow('NOTIFICATION_PUSH_HTTP_TIMEOUT_MS', {
                            infer: true,
                          }),
                        ),
                      },
                    );
                    if (!response.ok) {
                      throw new Error('FCM push provider rejected the request');
                    }

                    return { status: 'delivered' };
                  } catch {
                    return { status: 'failed' };
                  }

                  if (result?.status === 'failed') {
                    this.logger.warn('Push provider failed; in-app notification retained');
                  }
                } catch {
                  this.logger.warn('Push provider failed; in-app notification retained');
                }

                if (
                  this.webPushSubscriptions === undefined ||
                  this.webPushPort === undefined
                )
                  return;
                const webPushPort = this.webPushPort;
                const webPushSubscriptions = this.webPushSubscriptions;
                try {
                  const subscriptions = await webPushSubscriptions.listForUser(
                    notification.userId,
                  );

                  // [SERVICE] WebPushSubscriptionService.listForUser · notification/services/web-push-subscription.service.ts
                  const userId = notification.userId;
                  return this.subscriptionRepo.find({ where: { userId } });

                  await Promise.all(
                    subscriptions.map(async (subscription) => {
                      const result = await webPushPort.send({
                        notificationId: notification.id,
                        recipientId: notification.userId,
                        type: notification.type,
                        payload: notification.payload,
                        subscription: {
                          endpoint: subscription.endpoint,
                          keys: { p256dh: subscription.p256dh, auth: subscription.auth },
                        },
                      });

                      // [SERVICE] WebPushNotificationAdapter.send · common/platform/web-push-notification.adapter.ts
                      // message = đối số 1 của lời gọi ở trên
                      if (!this.config.getOrThrow('WEB_PUSH_ENABLED', { infer: true })) {
                        return { status: 'skipped' };
                      }
                      try {
                        await webpush.sendNotification(
                          message.subscription,
                          JSON.stringify({
                            notificationId: message.notificationId,
                            type: message.type,
                            payload: message.payload,
                          }),
                          { TTL: 3600 },
                        );
                        return { status: 'delivered' };
                      } catch (error) {
                        const statusCode =
                          typeof error === 'object' && error !== null && 'statusCode' in error
                            ? (error as { statusCode?: unknown }).statusCode
                            : undefined;
                        return {
                          status: 'failed',
                          removeSubscription: statusCode === 404 || statusCode === 410,
                        };
                      }

                      if (result.removeSubscription)
                        await webPushSubscriptions.removeById(subscription.id);

                        // [SERVICE] WebPushSubscriptionService.removeById · notification/services/web-push-subscription.service.ts
                        const id = subscription.id;
                        await this.subscriptionRepo.delete({ id });
                    }),
                  );
                } catch {
                  this.logger.warn(
                    'Web push provider failed; in-app notification retained',
                  );
                }
              }
              return result.kind;

              if (outcome === 'matched') matched += 1;
              // Cặp hợp lệ nhưng không ghép được (block/report, giới tính): dừng shard này, nếu
              // không sẽ pop lại đúng cặp đó và lặp vô hạn. Họ được thử lại khi có người thứ 3
              // vào hoặc sweeper expire.
              if (outcome === 'requeued_pair') break;
            }
            return matched;
          }
          span.setAttribute('matching.matched_pairs', matched);
          return matched;
        },
      );

      // [SERVICE] withSpan · libs/observability/src/lib/traced.ts
      const tracerName = 'litmatch.matching';
      const spanName = 'matching.matcher.tick';
      const fn = async (span) => {
        let matched = 0;
        for (const shard of await this.queue.listActiveShards()) {
          matched += await this.drainShard(shard);
        }
        span.setAttribute('matching.matched_pairs', matched);
        return matched;
      };
      const tracer = trace.getTracer(tracerName);
      return tracer.startActiveSpan(spanName, async (span) => {
        try {
          const result = await fn(span);
          span.setStatus({ code: SpanStatusCode.OK });
          return result;
        } catch (err) {
          span.recordException(err as Error);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: err instanceof Error ? err.message : String(err),
          });
          throw err;
        } finally {
          span.end();
        }
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

// [POSTGRES] CREATE UNIQUE INDEX uq_match_tickets_active_user · database/migrations/1752200000000-matching-core.ts
await queryRunner.query(`
  CREATE UNIQUE INDEX uq_match_tickets_active_user
    ON match_tickets(user_id)
    WHERE status IN ('queued','matched')
`);

// [REDIS] matching-redis.provider.ts · matching/redis/matching-redis.provider.ts
// Worker chỉ quét set này thay vì quét cả keyspace Redis.
export const MATCHING_ACTIVE_SHARDS_KEY = 'matching:shards:active';

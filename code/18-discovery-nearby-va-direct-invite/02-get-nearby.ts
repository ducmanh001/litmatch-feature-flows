//  GET /api/v1/discovery/nearby
//  Trả danh sách người gần vị trí của mình dưới dạng nhóm khoảng cách, có 3 lớp chống dò vị trí.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận bộ lọc (giới tính, tuổi) và cursor từ query
//  Step 2: Kiểm hạn mức truy vấn nearby của user qua Redis (429 nếu vượt)
//  Step 3: Kiểm user đã bật hiển thị Nearby (opt-in)
//  Step 4: Lấy vị trí của mình, vị trí quá hạn coi như chưa có
//  Step 5: Lấy tập người cần ẩn (block 2 chiều, report)
//  Step 6: Lấy ứng viên: lọc hộp giới hạn trong SQL, haversine ở app
//  Step 7: Mỗi ứng viên: haversine + jitter, rồi sắp xếp và lọc theo cursor
//  Step 8: Cắt trang theo limit và dựng nextCursor
//  Step 9: Trả trang thẻ người dùng kèm nhóm khoảng cách, không trả km hay tọa độ

// [CONTROLLER] DiscoveryController.nearby · discovery/discovery.controller.ts
@Controller('discovery')
export class DiscoveryController {
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
    // NearbyService
    @InjectRepository(UserLocation)
    private readonly locationRepo: Repository<UserLocation>,
    @InjectRepository(DiscoverySetting)
    private readonly settingRepo: Repository<DiscoverySetting>,
    @Inject(DISCOVERY_REDIS) private readonly redis: Redis,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // SafetyService
    @InjectRepository(Report) private readonly reportRepo: Repository<Report>,
    @InjectRepository(Block) private readonly blockRepo: Repository<Block>,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
    // PrivacySettingsService
    @InjectRepository(PrivacySetting)
    private readonly settingRepo: Repository<PrivacySetting>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận bộ lọc (giới tính, tuổi) và cursor từ query

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

  @Get('nearby')
  async nearby(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Query() query: NearbyQueryDto,
  ): Promise<NearbyCardsPageDto> {
    return NearbyCardsPageDto.from(
      await this.nearbyService.listNearby(user, query),
    );

    // [SERVICE] NearbyService.listNearby · discovery/nearby.service.ts
    // Step 2: Kiểm hạn mức truy vấn nearby của user qua Redis (429 nếu vượt)
    // [REDIS] Hạn mức truy vấn nearby theo user (lớp 3 chống trilateration): chặn dò quét vị trí
    // bằng nhiều truy vấn.
    const allowed = await checkRateLimit(
      this.redis,
      nearbyQueryCountKey(user.userId),
      this.config.getOrThrow('DISCOVERY_NEARBY_QUERY_RATE_LIMIT_PER_HOUR', {
        infer: true,
      }),
      RATE_LIMIT_WINDOW_SECONDS,
    );

    // [REDIS] checkRateLimit · common/redis/rate-limit.ts
    const redis = this.redis;
    const key = `discovery:nearby-query:count:${user.userId}`;
    const max = this.config.getOrThrow('DISCOVERY_NEARBY_QUERY_RATE_LIMIT_PER_HOUR', {
      infer: true,
    });
    const windowSeconds = RATE_LIMIT_WINDOW_SECONDS;
    const result = await new RedisRateLimitAdapter(redis).consume({
      key,
      limit: max,
      windowSeconds,
    });

    // [REDIS] RedisRateLimitAdapter.consume · common/redis/redis-rate-limit.adapter.ts
    // request = { key, limit: max, windowSeconds, }
    validateConsumeRequest(request);

    // [REDIS] validateConsumeRequest
    if (!request.key) throw new Error('Rate-limit key is required');
    if (!Number.isSafeInteger(request.limit) || request.limit < 1) {
      throw new Error('Rate-limit limit must be a positive integer');
    }
    if (
      !Number.isSafeInteger(request.windowSeconds) ||
      request.windowSeconds < 1
    ) {
      throw new Error('Rate-limit windowSeconds must be a positive integer');
    }

    const reservationKey = `${request.key}${RATE_LIMIT_RESERVATION_SUFFIX}${
      request.operationId ?? randomUUID()
    }`;
    const windowKey = `${request.key}${RATE_LIMIT_WINDOW_SUFFIX}`;
    const result = (await this.redis.eval(
      RATE_LIMIT_CONSUME_LUA,
      3,
      request.key,
      reservationKey,
      windowKey,
      String(request.limit),
      String(request.windowSeconds),
      randomUUID(),
    )) as RedisEvalResult;
    const [status] = Array.isArray(result) ? result : [result];

    if (Number(status) <= 0) {
      return { allowed: false, deduplicated: false };
    }

    return {
      allowed: true,
      deduplicated: Number(status) === 2,
      reservation: { rateLimitKey: request.key, reservationKey, windowKey },
    };

    return result.allowed;

    if (!allowed) {
      throw new DomainException(
        DiscoveryErrors.NEARBY_RATE_LIMITED,
        'Truy vấn nearby quá nhanh, thử lại sau',
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }

    // Step 3: Kiểm user đã bật hiển thị Nearby (opt-in)
    const mySetting = await this.settingRepo.findOne({
      where: { userId: user.userId },
    });
    // Có đi có lại (reciprocity): chưa bật hiển thị Nearby của chính mình thì không được xem
    // người khác.
    if (!mySetting?.nearbyVisible) {
      throw new DomainException(
        DiscoveryErrors.NEARBY_NOT_OPTED_IN,
        'Bạn cần bật hiển thị Nearby trước khi xem người khác',
        HttpStatus.FORBIDDEN,
      );
    }

    // Step 4: Lấy vị trí của mình, vị trí quá hạn coi như chưa có
    const myLocation = await this.locationRepo.findOne({
      where: { userId: user.userId },
    });
    const freshnessHours = this.config.getOrThrow(
      'DISCOVERY_LOCATION_FRESHNESS_HOURS',
      { infer: true },
    );
    // Vị trí quá hạn coi như không có: vị trí cũ tự biến mất khỏi kết quả, không cần cron dọn
    // bảng.

    // [SERVICE] NearbyService.isFresh
    const updatedAt = myLocation.updatedAt;
    const ageMs = Date.now() - updatedAt.getTime();
    return ageMs <= freshnessHours * 60 * 60 * 1000;

    if (!myLocation || !this.isFresh(myLocation.updatedAt, freshnessHours)) {
      throw new DomainException(
        DiscoveryErrors.NEARBY_LOCATION_MISSING,
        'Bạn chưa cập nhật vị trí (hoặc vị trí đã quá hạn)',
        HttpStatus.CONFLICT,
      );
    }

    const cursor = this.decodeNearbyCursor(query.cursor);

    // [SERVICE] NearbyService.decodeNearbyCursor
    // cursor = query.cursor
    if (!cursor) return undefined;
    const pos = decodeCursor<{ distanceKm?: unknown; userId?: unknown }>(
      cursor,
    );

    // [SERVICE] decodeCursor · libs/common-dtos/src/lib/cursor-pagination.ts
    try {
      const parsed: unknown = JSON.parse(
        Buffer.from(cursor, 'base64url').toString('utf8'),
      );
      return typeof parsed === 'object' &&
        parsed !== null &&
        !Array.isArray(parsed)
        ? (parsed as T)
        : null;
    } catch {
      return null;
    }

    if (
      !pos ||
      typeof pos.distanceKm !== 'number' ||
      typeof pos.userId !== 'string'
    ) {
      throw new DomainException(
        DiscoveryErrors.NEARBY_CURSOR_INVALID,
        'Cursor không hợp lệ',
        HttpStatus.BAD_REQUEST,
      );
    }
    return { distanceKm: pos.distanceKm, userId: pos.userId };

    const radiusKm = this.config.getOrThrow('DISCOVERY_NEARBY_RADIUS_KM', {
      infer: true,
    });

    // Step 5: Lấy tập người cần ẩn (block 2 chiều, report)
    // Loại người đã block hoặc report với mình (2 chiều); report ẩn vĩnh viễn vì bảng reports là
    // append-only.
    const hiddenUserIds = await this.safetyService.getHiddenUserIds(
      user.userId,
    );

    // [SERVICE] SafetyService.getHiddenUserIds · safety/safety.service.ts
    const userId = user.userId;
    const [blockedIds, reportedRows] = await Promise.all([
      this.getBlockedUserIds(userId),
      this.reportRepo.query(
        `
        SELECT DISTINCT other_id FROM (
          SELECT target_user_id AS other_id FROM reports WHERE reporter_user_id = $1
          UNION
          SELECT reporter_user_id AS other_id FROM reports WHERE target_user_id = $1
        ) pairs
        `,
        [userId],
      ),
    ]);

    // [SERVICE] SafetyService.getBlockedUserIds
    const rows = await this.blockRepo.query(
      `
      SELECT other_id FROM (
        SELECT DISTINCT ON (blocked_user_id) blocked_user_id AS other_id, action
        FROM blocks WHERE blocker_user_id = $1
        ORDER BY blocked_user_id, created_at DESC
      ) latest WHERE action = $2
      UNION
      SELECT other_id FROM (
        SELECT DISTINCT ON (blocker_user_id) blocker_user_id AS other_id, action
        FROM blocks WHERE blocked_user_id = $1
        ORDER BY blocker_user_id, created_at DESC
      ) latest WHERE action = $2
      `,
      [userId, BlockAction.Blocked],
    );
    return (rows as Array<{ other_id: string }>).map((r) => r.other_id);

    const reportedIds = (reportedRows as Array<{ other_id: string }>).map(
      (r) => r.other_id,
    );
    return Array.from(new Set([...blockedIds, ...reportedIds]));

    // Step 6: Lấy ứng viên: lọc hộp giới hạn trong SQL, haversine ở app
    // Spatial MVP: lọc hộp giới hạn bằng btree rồi haversine ở app (không PostGIS), tối đa
    // DISCOVERY_NEARBY_CANDIDATE_CAP ứng viên; sort và cursor cũng làm ở app.
    const candidates = await this.queryCandidates(
      myLocation,
      radiusKm,
      [user.userId, ...hiddenUserIds],
      freshnessHours,
    );

    // [SERVICE] NearbyService.queryCandidates
    const excludeUserIds = [user.userId, ...hiddenUserIds];
    // [POSTGRES] Hộp giới hạn xấp xỉ từ bán kính km (1 độ vĩ ≈ 111.32 km, độ kinh co lại theo cos
    // vĩ độ) chỉ là prefilter; khoảng cách thật tính lại bằng haversine ở bước lọc cuối.
    const latDeltaDeg = radiusKm / 111.32;
    const lonDeltaDeg =
      radiusKm /
      (111.32 *
        Math.max(0.1, Math.cos((myLocation.latQuantized * Math.PI) / 180)));
    const cap = this.config.getOrThrow('DISCOVERY_NEARBY_CANDIDATE_CAP', {
      infer: true,
    });

    const rows: Array<{
      user_id: string;
      lat_quantized: string;
      lon_quantized: string;
      updated_at: Date;
    }> = await this.locationRepo.query(
      `SELECT ul.user_id, ul.lat_quantized, ul.lon_quantized, ul.updated_at
       FROM user_locations ul
       JOIN discovery_settings ds ON ds.user_id = ul.user_id
       LEFT JOIN user_privacy_settings ups ON ups.user_id = ul.user_id
       WHERE ds.nearby_visible = true
         AND COALESCE(ups.hide_profile, false) = false
         AND ul.updated_at > now() - make_interval(hours => $1::int)
         AND ul.lat_quantized BETWEEN $2 AND $3
         AND ul.lon_quantized BETWEEN $4 AND $5
         AND ul.user_id != ALL($6::uuid[])
       LIMIT $7`,
      [
        freshnessHours,
        myLocation.latQuantized - latDeltaDeg,
        myLocation.latQuantized + latDeltaDeg,
        myLocation.lonQuantized - lonDeltaDeg,
        myLocation.lonQuantized + lonDeltaDeg,
        excludeUserIds,
        cap,
      ],
    );

    return rows
      .map((r) => {
        const loc = new UserLocation();
        loc.userId = r.user_id;
        loc.latQuantized = Number(r.lat_quantized);
        loc.lonQuantized = Number(r.lon_quantized);
        loc.updatedAt = r.updated_at;
        return loc;
      })
      .filter(
        (loc) =>
          haversineDistanceKm(
            myLocation.latQuantized,
            myLocation.lonQuantized,
            loc.latQuantized,
            loc.lonQuantized,
          ) <= radiusKm,
      );

    // [SERVICE] haversineDistanceKm · discovery/nearby.constants.ts
    const lat1 = myLocation.latQuantized;
    const lon1 = myLocation.lonQuantized;
    const lat2 = loc.latQuantized;
    const lon2 = loc.lonQuantized;
    const earthRadiusKm = 6371;
    const dLat = ((lat2 - lat1) * Math.PI) / 180;
    const dLon = ((lon2 - lon1) * Math.PI) / 180;
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos((lat1 * Math.PI) / 180) *
        Math.cos((lat2 * Math.PI) / 180) *
        Math.sin(dLon / 2) ** 2;
    return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    if (candidates.length === 0) {
      return { items: [], meta: { nextCursor: null } };
    }

    const users = await this.userService.findActiveByIds(
      candidates.map((c) => c.userId),
      {
        gender: query.gender,
        ageMin: query.ageMin,
        ageMax: query.ageMax,
        excludeGuests: !this.config.getOrThrow('DISCOVERY_GUEST_VISIBLE', {
          infer: true,
        }),
      },
    );

    // [SERVICE] UserService.findActiveByIds · user/user.service.ts
    const userIds = candidates.map((c) => c.userId);
    const filter = {
      gender: query.gender,
      ageMin: query.ageMin,
      ageMax: query.ageMax,
      excludeGuests: !this.config.getOrThrow('DISCOVERY_GUEST_VISIBLE', {
        infer: true,
      }),
    };
    if (userIds.length === 0) return [];

    const qb = this.userRepo
      .createQueryBuilder('u')
      .where('u.status = :status', { status: UserStatus.Active })
      .andWhere(
        'NOT EXISTS (SELECT 1 FROM user_privacy_settings ups WHERE ups.user_id = u.id AND ups.hide_profile = true)',
      )
      .andWhere('u.id IN (:...userIds)', { userIds });

    if (filter.gender) {
      qb.andWhere('u.gender = :gender', { gender: filter.gender });
    }
    if (filter.ageMin !== undefined) {
      qb.andWhere('u.birthDate IS NOT NULL AND u.birthDate <= :maxBirthDate', {
        maxBirthDate: this.ageCutoffDate(filter.ageMin),
      });

      // [SERVICE] UserService.ageCutoffDate
      const years = filter.ageMin;
      const now = new Date();
      const cutoff = new Date(
        now.getFullYear() - years,
        now.getMonth(),
        now.getDate(),
      );
      return cutoff.toISOString().slice(0, 10);
    }
    if (filter.ageMax !== undefined) {
      qb.andWhere('u.birthDate IS NOT NULL AND u.birthDate > :minBirthDate', {
        minBirthDate: this.ageCutoffDate(filter.ageMax + 1),
      });
    }
    if (filter.excludeGuests) {
      qb.andWhere('u.isGuest = false');
    }

    return qb.getMany();

    const userById = new Map(users.map((u) => [u.id, u]));
    const privacyByUserId = await this.privacySettings.findForUsers(
      users.map((u) => u.id),
    );

    // [SERVICE] PrivacySettingsService.findForUsers · user/services/privacy-settings.service.ts
    // userIds = users.map((u) => u.id)
    const uniqueIds = [...new Set(userIds)];
    if (uniqueIds.length === 0) return new Map();
    const rows = await this.settingRepo.find({
      where: { userId: In(uniqueIds) },
    });
    const settings = new Map<string, PrivacySettingsDto>();
    for (const userId of uniqueIds) {
      settings.set(userId, PrivacySettingsDto.from());
    }
    for (const row of rows) {
      settings.set(row.userId, PrivacySettingsDto.from(row));
    }
    return settings;

    const forDate = todayUtc();

    // [SERVICE] todayUtc · common/date/utc-date.ts
    return new Date().toISOString().slice(0, 10);

    const distanceBucketsCsv = this.config.getOrThrow(
      'DISCOVERY_DISTANCE_BUCKETS_KM',
      { infer: true },
    );
    const jitterMaxKm = radiusKm * 0.1;

    // Step 7: Mỗi ứng viên: haversine + jitter, rồi sắp xếp và lọc theo cursor
    const rows = candidates
      .filter((c) => userById.has(c.userId))
      .map((c) => {
        const rawDistanceKm = haversineDistanceKm(
          myLocation.latQuantized,
          myLocation.lonQuantized,
          c.latQuantized,
          c.lonQuantized,
        );
        // Lớp 2 chống trilateration: cộng jitter tất định theo cặp và ngày vào khoảng cách trước
        // khi tính nhóm.
        const jitter = nearbyJitterKm(
          user.userId,
          c.userId,
          forDate,
          jitterMaxKm,
        );

        // [SERVICE] nearbyJitterKm · discovery/nearby.constants.ts
        const userIdA = user.userId;
        const userIdB = c.userId;
        const maxJitterKm = jitterMaxKm;
        const [low, high] = canonicalPair(userIdA, userIdB);

        // [SERVICE] canonicalPair · common/entities/canonical-pair.ts
        // a = userIdA
        const b = userIdB;
        return a < b ? [a, b] : [b, a];

        // Seed theo cặp (canonical) và ngày UTC: cùng cặp cùng ngày luôn cùng jitter, nên truy
        // vấn lặp trong ngày không lộ thêm; sang ngày khác đổi, nên không cộng dồn nhiều ngày để
        // suy ra vị trí thật.
        const seed = fnv1aHash(`${low}:${high}:${forDate}`);

        // [SERVICE] fnv1aHash · common/hash/fnv1a.ts
        const input = `${low}:${high}:${forDate}`;
        let hash = 2166136261;
        for (let i = 0; i < input.length; i++) {
          hash ^= input.charCodeAt(i);
          hash = Math.imul(hash, 16777619) >>> 0;
        }
        return hash >>> 0;

        return (seed % 1000) * (maxJitterKm / 1000) - maxJitterKm / 2;

        const jitteredDistanceKm = Math.max(0, rawDistanceKm + jitter);
        const targetPrivacy = privacyByUserId.get(c.userId);
        return {
          userId: c.userId,
          jitteredDistanceKm,
          user: userById.get(c.userId) as User,
          showDistance: targetPrivacy?.showDistance ?? true,
        };
      })
      .sort(
        (a, b) =>
          a.jitteredDistanceKm - b.jitteredDistanceKm ||
          (a.userId < b.userId ? -1 : a.userId > b.userId ? 1 : 0),
      )
      // Cursor giữ khoảng cách đã jitter; nhờ jitter cố định theo cặp-theo-ngày nên thứ tự giữa
      // các trang không bị xáo.
      .filter((r) => !cursor || this.isAfterCursor(r, cursor));

    // [SERVICE] NearbyService.isAfterCursor
    // row = r
    if (row.jitteredDistanceKm !== cursor.distanceKm) {
      return row.jitteredDistanceKm > cursor.distanceKm;
    }
    return row.userId > cursor.userId;

    // Step 8: Cắt trang theo limit và dựng nextCursor
    const page = buildCursorPage(rows, query.limit, (last) => ({
      distanceKm: last.jitteredDistanceKm,
      userId: last.userId,
    }));

    // [SERVICE] buildCursorPage · libs/common-dtos/src/lib/cursor-pagination.ts
    const limit = query.limit;
    const toCursorPayload = (last) => ({
      distanceKm: last.jitteredDistanceKm,
      userId: last.userId,
    });
    const hasMore = rows.length > limit;
    const items = hasMore ? rows.slice(0, limit) : rows;
    const last = items[items.length - 1];
    return {
      items,
      meta: {
        nextCursor: hasMore && last ? encodeCursor(toCursorPayload(last)) : null,
      },
    };

    // [SERVICE] encodeCursor
    // payload = toCursorPayload(last)
    return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');

    // Step 9: Trả trang thẻ người dùng kèm nhóm khoảng cách, không trả km hay tọa độ
    return {
      items: page.items.map((r) => ({
        user: r.user,
        // Chỉ trả nhóm khoảng cách, không bao giờ trả km hay tọa độ chính xác; người tắt
        // showDistance thì null.
        distanceBucket: r.showDistance
          ? computeDistanceBucket(r.jitteredDistanceKm, distanceBucketsCsv)
          : null,
      })),
      meta: page.meta,
    };

    // [SERVICE] computeDistanceBucket · discovery/nearby.constants.ts
    const distanceKm = r.jitteredDistanceKm;
    const boundariesCsv = distanceBucketsCsv;
    const boundaries = boundariesCsv
      .split(',')
      .map((s) => Number(s.trim()))
      .filter((n) => Number.isFinite(n))
      .sort((a, b) => a - b);

    if (boundaries.length === 0 || distanceKm < boundaries[0]) {
      return `<${boundaries[0] ?? 0}km`;
    }
    for (let i = boundaries.length - 1; i >= 0; i--) {
      if (distanceKm >= boundaries[i]) {
        const upper = i + 1 < boundaries.length ? boundaries[i + 1] : null;
        return upper !== null
          ? `${boundaries[i]}-${upper}km`
          : `${boundaries[i]}km+`;
      }
    }
    return `<${boundaries[0]}km`;
  }
}

// [POSTGRES] CREATE TABLE user_locations · database/migrations/1754600000000-discovery-nearby.ts
await queryRunner.query(`
  CREATE TABLE user_locations (
    user_id       uuid PRIMARY KEY REFERENCES users(id),
    lat_quantized double precision NOT NULL,
    lon_quantized double precision NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_user_locations_lat CHECK (lat_quantized BETWEEN -90 AND 90),
    CONSTRAINT chk_user_locations_lon CHECK (lon_quantized BETWEEN -180 AND 180)
  )
`);

await queryRunner.query(`
  CREATE INDEX idx_user_locations_lat_lon ON user_locations (lat_quantized, lon_quantized)
`);

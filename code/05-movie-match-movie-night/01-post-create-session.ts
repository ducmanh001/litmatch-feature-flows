//  POST /api/v1/movie-match/sessions
//  Tạo phiên xem video chung giữa 2 người bạn, idempotent theo cặp, mỗi user chỉ có 1 phiên đang xem.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, friendUserId và videoUrl trong body)
//  Step 2: Kiểm URL video theo whitelist host
//  Step 3: Kiểm tra 2 người đã là bạn (lỗi chung 404, không lộ quan hệ)
//  Step 4: Chuẩn hóa cặp (low, high), cặp đang xem chung thì trả phiên cũ
//  Step 5: Trong transaction, ghi phiên rồi 2 participant (PK user_id chặn trùng)
//  Step 6: Thua race: đọc lại để phân biệt replay với user ở phiên khác
//  Step 7: Báo movie.session.started qua Redis cho cả 2 người
//  Step 8: Controller trả phiên theo góc nhìn của người tạo

// [CONTROLLER] MovieMatchController.createSession · movie-match/movie-match.controller.ts
@Controller('movie-match')
export class MovieMatchController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // MovieMatchService
  private readonly logger = new Logger(MovieMatchService.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // MovieMatchService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(MovieSession)
    private readonly sessionRepo: Repository<MovieSession>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(MOVIE_MATCH_REDIS) private readonly redis: Redis,
    // FriendService
    @InjectRepository(Friendship)
    private readonly friendshipRepo: Repository<Friendship>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, friendUserId và videoUrl trong
  // body)

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

  @Post('sessions')
  @HttpCode(HttpStatus.OK)
  async createSession(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Body() dto: CreateMovieSessionDto,
  ): Promise<MovieSessionDto> {
    const session = await this.movieMatchService.createSession(
      user.userId,
      dto.friendUserId,
      dto.videoUrl,
    );

    // [SERVICE] MovieMatchService.createSession · movie-match/movie-match.service.ts
    const userId = user.userId;
    const friendUserId = dto.friendUserId;
    const videoUrl = dto.videoUrl;

    // Step 2: Kiểm URL video theo whitelist host
    this.assertValidVideoUrl(videoUrl);

    // [SERVICE] MovieMatchService.assertValidVideoUrl
    const maxLength = this.config.getOrThrow('MOVIE_MATCH_URL_MAX_LENGTH', {
      infer: true,
    });
    if (videoUrl.length > maxLength) {
      throw new DomainException(
        MovieMatchErrors.INVALID_VIDEO_URL,
        `videoUrl dài quá ${maxLength} ký tự`,
        HttpStatus.UNPROCESSABLE_ENTITY,
        { maxLength },
      );
    }

    let url: URL;
    try {
      url = new URL(videoUrl);
    } catch {
      throw new DomainException(
        MovieMatchErrors.INVALID_VIDEO_URL,
        'videoUrl không hợp lệ',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }
    if (url.protocol !== 'https:' && url.protocol !== 'http:') {
      throw new DomainException(
        MovieMatchErrors.INVALID_VIDEO_URL,
        'videoUrl phải dùng http/https',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    const hostname = url.hostname.toLowerCase();
    const allowedHosts = this.config
      .getOrThrow('MOVIE_MATCH_ALLOWED_VIDEO_HOSTS', { infer: true })
      .split(',')
      .map((h) => h.trim().toLowerCase())
      .filter((h) => h.length > 0);
    // Khớp hostname CHÍNH XÁC hoặc subdomain thật (host hoặc .host), không substring/regex: chặn
    // bypass kiểu youtube.com.evil.com.
    const allowed = allowedHosts.some(
      (host) => hostname === host || hostname.endsWith(`.${host}`),
    );
    if (!allowed) {
      throw new DomainException(
        MovieMatchErrors.INVALID_VIDEO_URL,
        'Domain video không nằm trong danh sách được phép',
        HttpStatus.UNPROCESSABLE_ENTITY,
        { allowedHosts },
      );
    }

    // Step 3: Kiểm tra 2 người đã là bạn (lỗi chung 404, không lộ quan hệ)
    if (userId === friendUserId) {
      throw new DomainException(
        MovieMatchErrors.NOT_FRIEND,
        'Không thể xem chung với chính mình',
        HttpStatus.NOT_FOUND,
      );
    }
    // Tự xem với mình, không phải bạn, không tồn tại đều trả cùng 404 NOT_FRIEND: không để lộ
    // quan hệ (oracle).

    // [SERVICE] FriendService.areFriends · friend/friend.service.ts
    const userAId = userId;
    const userBId = friendUserId;
    if (userAId === userBId) return false;
    const [userLowId, userHighId] = canonicalPair(userAId, userBId);

    // [SERVICE] canonicalPair · common/entities/canonical-pair.ts
    const a = userAId;
    const b = userBId;
    return a < b ? [a, b] : [b, a];

    return this.friendshipRepo.exists({ where: { userLowId, userHighId } });

    if (!(await this.friendService.areFriends(userId, friendUserId))) {
      throw new DomainException(
        MovieMatchErrors.NOT_FRIEND,
        'Không phải bạn của bạn',
        HttpStatus.NOT_FOUND,
      );
    }

    // Step 4: Chuẩn hóa cặp (low, high), cặp đang xem chung thì trả phiên cũ
    const [userLowId, userHighId] = canonicalPair(userId, friendUserId);

    // Idempotent theo cặp: đã active đúng cặp này thì trả lại phiên cũ. Active với CẶP KHÁC thì
    // 409, không tự ý kết thúc phiên cũ thay user.
    // Chỉ tính phiên mode friend: 2 người bạn đang tình cờ ghép ẩn danh với nhau KHÔNG được coi
    // là replay của phiên xem chung bạn bè (flow và quyền riêng tư khác nhau).
    const existing = await this.findActiveByPair(userLowId, userHighId);

    // [SERVICE] MovieMatchService.findActiveByPair
    return this.sessionRepo.findOneBy({
      userLowId,
      userHighId,
      status: MovieSessionStatus.Active,
      mode: MovieSessionMode.Friend,
    });

    if (existing) return existing;

    const now = new Date();
    let created: MovieSession;
    try {

      // Step 5: Trong transaction, ghi phiên rồi 2 participant (PK user_id chặn trùng)
      // [POSTGRES] Phiên và 2 dòng participant ghi trong 1 transaction: insert participant lỗi
      // thì rollback cả phiên, không mồ côi dòng.
      created = await this.dataSource.transaction(async (manager) => {
        const session = await manager.save(
          manager.create(MovieSession, {
            userLowId,
            userHighId,
            videoUrl,
            positionSeconds: 0,
            isPlaying: false,
            positionUpdatedAt: now,
            status: MovieSessionStatus.Active,
          }),
        );
        // PK user_id chặn user đang active ở phiên KHÁC, dù là low hay high ở đó. 2 unique index
        // đơn cột trên user_low_id/user_high_id không đủ: mỗi index chỉ soi 1 cột nên không thấy
        // xung đột chéo (low ở A, high ở B).
        await manager.insert(MovieSessionActiveParticipant, [
          { userId: userLowId, sessionId: session.id },
          { userId: userHighId, sessionId: session.id },
        ]);
        return session;
      });
    } catch (err) {

      // Step 6: Thua race: đọc lại để phân biệt replay với user ở phiên khác
      // Thua race: đọc lại để phân biệt "đúng cặp" (replay, trả lại phiên) với "user đã active ở
      // cặp khác" (409, không đoán ý user).

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (!isUniqueViolation(err)) throw err;
      const raced = await this.findActiveByPair(userLowId, userHighId);
      if (raced) return raced;
      throw new DomainException(
        MovieMatchErrors.ALREADY_ACTIVE,
        'Đang có phiên xem chung khác đang active',
        HttpStatus.CONFLICT,
      );
    }

    // Step 7: Báo movie.session.started qua Redis cho cả 2 người
    const envelope: RealtimeEnvelope<MovieSessionStartedEventData> = {
      event: RealtimeEvents.MovieSessionStarted,
      data: {
        sessionId: created.id,
        videoUrl: created.videoUrl,
        initiatorUserId: userId,
      },
    };
    // [REDIS] Chỉ bắn khi vừa tạo phiên mới (các nhánh replay đã return). Best-effort: client còn
    // poll GET /sessions/:id.
    await Promise.all(
      [userLowId, userHighId].map((uid) =>
        publishRealtimeEvent(this.redis, this.logger, uid, envelope),
      ),
    );

    // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
    const publisher = this.redis;
    const logger = this.logger;
    // userId = uid
    try {
      await publisher.publish(
        realtimeUserChannel(userId),
        JSON.stringify(envelope),
      );

      // [REDIS] RedisRealtimePublisher.publish · common/realtime/redis-realtime-publisher.adapter.ts
      const channel = `${REALTIME_USER_CHANNEL_PREFIX}${userId}`;
      // payload = JSON.stringify(envelope)
      return this.redis.publish(channel, payload);
    } catch (err) {
      logger.warn(
        `Publish realtime '${envelope.event}' cho user ${userId} thất bại (client còn polling fallback): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }

    return created;

    // Step 8: Controller trả phiên theo góc nhìn của người tạo
    return MovieSessionDto.from(session, user.userId);
  }
}

// [POSTGRES] CREATE TABLE movie_session_active_participants · database/migrations/1753200000000-movie-match.ts
await queryRunner.query(`
  CREATE TABLE movie_session_active_participants (
    user_id    uuid PRIMARY KEY REFERENCES users(id),
    session_id uuid NOT NULL REFERENCES movie_sessions(id)
  )
`);

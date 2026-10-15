//  POST /api/v1/soul-match/sessions/:id/rating
//  Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL, verdict trong body)
//  Step 2: Kiểm tra user thuộc phòng và còn trong cửa sổ đánh giá
//  Step 3: Trong transaction, khóa dòng session FOR UPDATE
//  Step 4: Đã đánh giá rồi: cùng verdict thì trả kết quả cũ, khác thì 409
//  Step 5: Ghi rating (unique session + người đánh giá)
//  Step 6: Cả 2 cùng like: chụp lịch sử chat ẩn danh theo seq
//  Step 7: Nhờ Friend tạo Friendship và Conversation, cùng transaction
//  Step 8: Chép lịch sử chat ẩn danh sang Conversation
//  Step 9: Double-submit song song: đọc lại rồi replay hoặc 409
//  Step 10: Sau commit, báo soul.matched qua Redis cho cả 2 người
//  Step 11: Controller trả verdict và cờ matched

// [CONTROLLER] SoulMatchController.rate · soul-match/soul-match.controller.ts
@Controller('soul-match')
export class SoulMatchController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // SoulMatchService
  private readonly logger = new Logger(SoulMatchService.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // SoulMatchService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(SoulMatchRating)
    private readonly ratingRepo: Repository<SoulMatchRating>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(SOUL_MATCH_REDIS) private readonly redis: Redis,
    // FriendService
    @InjectRepository(Friendship)
    private readonly friendshipRepo: Repository<Friendship>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL, verdict trong
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

  @Post('sessions/:id/rating')
  @HttpCode(HttpStatus.OK)
  async rate(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Param('id', ParseUUIDPipe) id: string,

    // [CONTROLLER] ParseUUIDPipe configuration
    @Param('id', ParseUUIDPipe)

    @Body() dto: RateSessionDto,
  ): Promise<RatingResultDto> {
    const result = await this.soulMatchService.rate(user, id, dto);

    // [SERVICE] SoulMatchService.rate · soul-match/soul-match.service.ts
    const sessionId = id;

    // Step 2: Kiểm tra user thuộc phòng và còn trong cửa sổ đánh giá
    const room = await this.getRoomForMember(user, sessionId);

    // [SERVICE] SoulMatchService.getRoomForMember
    const session = await this.matchingService.findSessionById(sessionId);

    // [SERVICE] MatchingService.findSessionById · matching/matching.service.ts
    return this.dataSource
      .getRepository(MatchSession)
      .findOneBy({ id: sessionId });

    if (
      !session ||
      (session.userAId !== user.userId && session.userBId !== user.userId)
    ) {
      throw new DomainException(
        SoulMatchErrors.SESSION_NOT_FOUND,
        'Không tìm thấy session',
        HttpStatus.NOT_FOUND,
      );
    }
    if (
      session.matchType !== MatchType.Soul ||
      ![MatchSessionStatus.Confirmed, MatchSessionStatus.Ended].includes(
        session.status,
      )
    ) {
      throw new DomainException(
        SoulMatchErrors.CHAT_NOT_OPEN,
        'Phòng chat chỉ mở khi session soul đã đủ 2 bên xác nhận',
        HttpStatus.CONFLICT,
      );
    }
    if (!session.confirmedAAt || !session.confirmedBAt) {
      throw new Error(
        `Session ${session.id} confirmed nhưng thiếu confirmedAAt/confirmedBAt — dữ liệu hỏng`,
      );
    }

    const confirmedAt = new Date(
      Math.max(session.confirmedAAt.getTime(), session.confirmedBAt.getTime()),
    );
    const chatSeconds = this.config.getOrThrow('SOUL_CHAT_DURATION_SECONDS', {
      infer: true,
    });
    const ratingSeconds = this.config.getOrThrow('SOUL_RATING_WINDOW_SECONDS', {
      infer: true,
    });
    const chatEndsAt = new Date(confirmedAt.getTime() + chatSeconds * 1000);
    const ratingEndsAt = new Date(chatEndsAt.getTime() + ratingSeconds * 1000);
    const matched = await this.friendService.areFriends(
      session.userAId,
      session.userBId,
    );

    // [SERVICE] FriendService.areFriends · friend/friend.service.ts
    const userAId = session.userAId;
    const userBId = session.userBId;
    if (userAId === userBId) return false;
    const [userLowId, userHighId] = canonicalPair(userAId, userBId);

    // [SERVICE] canonicalPair · common/entities/canonical-pair.ts
    const a = userAId;
    const b = userBId;
    return a < b ? [a, b] : [b, a];

    return this.friendshipRepo.exists({ where: { userLowId, userHighId } });

    if (session.status === MatchSessionStatus.Ended) {
      return {
        session,
        chatEndsAt,
        ratingEndsAt,
        phase: SoulRoomPhase.Closed,
        matched,
      };
    }
    const now = Date.now();
    const phase =
      matched || now < chatEndsAt.getTime()
        ? SoulRoomPhase.Chatting
        : now < ratingEndsAt.getTime()
          ? SoulRoomPhase.Rating
          : SoulRoomPhase.Closed;
    return { session, chatEndsAt, ratingEndsAt, phase, matched };

    if (room.phase === SoulRoomPhase.Closed) {
      throw new DomainException(
        SoulMatchErrors.RATING_NOT_OPEN,
        'Hết cửa sổ đánh giá',
        HttpStatus.CONFLICT,
      );
    }
    const partnerId = this.partnerIdOf(room.session, user.userId);

    // [SERVICE] SoulMatchService.partnerIdOf
    // session = room.session
    const userId = user.userId;
    return session.userAId === userId ? session.userBId : session.userAId;

    let result: RateResult;
    let newlyMatched = false;
    try {
      result = await this.dataSource.transaction(async (manager) => {

        // Step 3: Trong transaction, khóa dòng session FOR UPDATE
        // [POSTGRES] Khóa dòng MatchSession FOR UPDATE TRƯỚC khi ghi rating: 2 lượt like chạy
        // tuần tự. Không khóa thì mỗi bên đều thấy "bên kia chưa like" (READ COMMITTED) và không
        // ai tạo Friendship. Chỉ khóa, không ghi.
        await manager.findOne(MatchSession, {
          where: { id: sessionId },
          lock: { mode: 'pessimistic_write' },
        });

        const existing = await manager.findOneBy(SoulMatchRating, {
          sessionId,
          raterUserId: user.userId,
        });

        // Step 4: Đã đánh giá rồi: cùng verdict thì trả kết quả cũ, khác thì 409
        // Rating bất biến: gửi lại cùng verdict thì trả kết quả cũ, khác verdict thì 409. Cho đổi
        // ý sẽ mở đường "like thử xem bên kia like chưa rồi đổi lại".
        if (existing) {
          this.assertSameVerdict(existing.verdict, dto.verdict);

          // [SERVICE] SoulMatchService.assertSameVerdict
          // existing = existing.verdict
          const incoming = dto.verdict;
          if (existing !== incoming) {
            throw new DomainException(
              SoulMatchErrors.RATING_CONFLICT,
              `Đã đánh giá '${existing}' — verdict không đổi được`,
              HttpStatus.CONFLICT,
            );
          }

          return {
            verdict: existing.verdict,
            matched: await this.friendService.areFriends(
              user.userId,
              partnerId,
            ),
          };
        }

        // Step 5: Ghi rating (unique session + người đánh giá)
        await manager.save(
          manager.create(SoulMatchRating, {
            sessionId,
            raterUserId: user.userId,
            verdict: dto.verdict,
          }),
        );

        let matched = await this.friendService.areFriends(
          user.userId,
          partnerId,
        );
        if (dto.verdict === SoulMatchVerdict.Like) {
          const partnerRating = await manager.findOneBy(SoulMatchRating, {
            sessionId,
            raterUserId: partnerId,
          });
          if (partnerRating?.verdict === SoulMatchVerdict.Like) {

            // Step 6: Cả 2 cùng like: chụp lịch sử chat ẩn danh theo seq
            // Đủ 2 like thì chụp toàn bộ lịch sử chat ẩn danh theo seq để chép sang Friend Chat.
            // Key import ổn định theo (session, message) nên chạy lại không nhân đôi; dòng gốc
            // vẫn giữ cho T&S.
            const anonymousMessages = await manager.find(SoulChatMessage, {
              where: { sessionId },
              order: { seq: 'ASC' },
            });
            const messageSeeds: FriendMessageSeed[] = anonymousMessages.map(
              (message) => ({
                senderUserId: message.senderUserId,
                content: message.content,
                createdAt: message.createdAt,
                idempotencyKey: friendHistoryImportKey(sessionId, message.id),
              }),
            );

            // [SERVICE] friendHistoryImportKey · soul-match/soul-match.constants.ts
            const messageId = message.id;
            return `friend:import:soul:${sessionId}:${messageId}`;

            // Step 7: Nhờ Friend tạo Friendship và Conversation, cùng transaction
            // Friendship, Conversation và lịch sử chat nằm CÙNG transaction với rating: hoặc có
            // đủ cả, hoặc không có gì.
            const { created } = await this.friendService.ensureFriendship(
              manager,
              user.userId,
              partnerId,
              FriendshipSource.SoulMatch,
              messageSeeds,
            );

            // [SERVICE] FriendService.ensureFriendship · friend/friend.service.ts
            // userAId = user.userId
            // userBId = partnerId
            const source = FriendshipSource.SoulMatch;
            if (userAId === userBId) {
              throw new Error(
                `Không thể tạo friendship với chính mình (${userAId}) — dữ liệu session hỏng`,
              );
            }
            const [userLowId, userHighId] = canonicalPair(userAId, userBId);
            // [POSTGRES] ON CONFLICT DO NOTHING trên cặp canonical (low < high): 2 like song song
            // hoặc match lại cặp cũ đều không tạo đôi và không lỗi.
            const result = await manager
              .createQueryBuilder()
              .insert()
              .into(Friendship)
              .values({ userLowId, userHighId, source })
              .orIgnore()
              .execute();
            // Conversation luôn được ensure cùng lúc, dù Friendship mới hay replay: bất biến "có
            // Friendship thì có Conversation" không phụ thuộc nhánh nào.
            await manager
              .createQueryBuilder()
              .insert()
              .into(Conversation)
              .values({ userLowId, userHighId, lastMessageAt: null })
              .orIgnore()
              .execute();

            // Step 8: Chép lịch sử chat ẩn danh sang Conversation
            // Chép lịch sử chat ẩn danh: chèn ON CONFLICT DO NOTHING theo idempotency_key nên
            // chạy lại không nhân đôi. Friend chỉ nhận seed trung lập, không import entity của
            // luồng nguồn.
            if (messageSeeds.length > 0) {
              const conversation = await manager.findOne(Conversation, {
                where: { userLowId, userHighId },
              });
              if (!conversation) {
                throw new Error(
                  `Không tìm thấy conversation sau khi ensure friendship ${userLowId}/${userHighId}`,
                );
              }

              await manager
                .createQueryBuilder()
                .insert()
                .into(Message)
                .values(
                  messageSeeds.map((message) => ({
                    conversationId: conversation.id,
                    senderUserId: message.senderUserId,
                    content: message.content,
                    idempotencyKey: message.idempotencyKey,
                    createdAt: message.createdAt,
                  })),
                )
                .orIgnore()
                .execute();

              // Không làm lùi mốc sắp xếp của conversation nếu đây là lần chép lại của một cặp đã
              // chat trước đó.
              const latestMessageAt = messageSeeds.reduce(
                (latest, message) =>
                  message.createdAt > latest ? message.createdAt : latest,
                messageSeeds[0].createdAt,
              );
              await manager
                .createQueryBuilder()
                .update(Conversation)
                .set({ lastMessageAt: latestMessageAt })
                .where('id = :conversationId', { conversationId: conversation.id })
                .andWhere(
                  '(last_message_at IS NULL OR last_message_at < :latestMessageAt)',
                  { latestMessageAt },
                )
                .execute();
            }
            return { created: result.raw.length > 0 };

            newlyMatched = created;
            matched = true;
          }
        }
        return { verdict: dto.verdict, matched };
      });
    } catch (err) {

      // Step 9: Double-submit song song: đọc lại rồi replay hoặc 409
      // Double-submit song song của CÙNG user: bên thua unique (session, rater) thì đọc lại rồi
      // replay hoặc 409.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (!isUniqueViolation(err)) throw err;
      const existing = await this.ratingRepo.findOneBy({
        sessionId,
        raterUserId: user.userId,
      });
      if (!existing) throw err;
      this.assertSameVerdict(existing.verdict, dto.verdict);
      return {
        verdict: existing.verdict,
        matched: await this.friendService.areFriends(user.userId, partnerId),
      };
    }

    // Step 10: Sau commit, báo soul.matched qua Redis cho cả 2 người
    // [REDIS] soul.matched phát SAU khi commit, best-effort, và chỉ khi Friendship MỚI được tạo;
    // replay không bắn lại.
    if (newlyMatched) {
      const envelope: RealtimeEnvelope<SoulMatchedEventData> = {
        event: RealtimeEvents.SoulMatched,
        data: { sessionId },
      };
      await Promise.all(
        [user.userId, partnerId].map((uid) =>
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
    }
    return result;

    // Step 11: Controller trả verdict và cờ matched
    return RatingResultDto.from(result.verdict, result.matched);
  }
}

// [POSTGRES] CREATE TABLE soul_match_ratings · database/migrations/1752400000000-soul-match.ts
// Unique (session_id, rater_user_id) chính là idempotency tự nhiên của rating, không cần
// Idempotency-Key.
await queryRunner.query(`
  CREATE TABLE soul_match_ratings (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id    uuid        NOT NULL REFERENCES match_sessions(id),
    rater_user_id uuid        NOT NULL REFERENCES users(id),
    verdict       varchar(8)  NOT NULL CHECK (verdict IN ('rude','boring','like')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_soul_match_ratings_session_rater UNIQUE (session_id, rater_user_id)
  )
`);

// [POSTGRES] CREATE TABLE friendships
await queryRunner.query(`
  CREATE TABLE friendships (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_low_id  uuid        NOT NULL REFERENCES users(id),
    user_high_id uuid        NOT NULL REFERENCES users(id),
    source       varchar(16) NOT NULL CHECK (source IN ('soul_match','voice_match')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_friendships_pair UNIQUE (user_low_id, user_high_id),
    CONSTRAINT chk_friendships_canonical CHECK (user_low_id < user_high_id)
  )
`);

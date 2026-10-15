//  POST /api/v1/soul-match/sessions/:id/messages
//  Gửi tin nhắn trong phòng ẩn danh: giờ chat tính ở server, idempotent theo key, realtime riêng cho từng người nhận.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL, Idempotency-Key từ header)
//  Step 2: Lấy phòng chat và tính phase theo giờ server
//  Step 3: Chặn nếu phòng không còn ở phase chatting
//  Step 4: Kiểm tra người gửi chưa bị khóa tài khoản
//  Step 5: Gắn user vào Idempotency-Key của client
//  Step 6: Trong transaction, khóa dòng session FOR UPDATE
//  Step 7: Kiểm lại hạn chat sau khi đã có khóa
//  Step 8: Ghi tin nhắn (unique idempotency_key chặn nhân đôi)
//  Step 9: Phát soul.message qua Redis, payload riêng cho từng người
//  Step 10: Trùng key: cùng nội dung thì trả tin cũ, khác thì 409
//  Step 11: Controller trả tin nhắn với vai trò me hoặc partner

// [CONTROLLER] SoulMatchController.sendMessage · soul-match/soul-match.controller.ts
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
    @InjectRepository(SoulChatMessage)
    private readonly messageRepo: Repository<SoulChatMessage>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(SOUL_MATCH_REDIS) private readonly redis: Redis,
    // FriendService
    @InjectRepository(Friendship)
    private readonly friendshipRepo: Repository<Friendship>,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL,
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

  @Post('sessions/:id/messages')
  @Throttle({ default: { limit: 60, ttl: minutes(1) } })
  async sendMessage(
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

    @Body() dto: SendSoulMessageDto,
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
  ): Promise<SoulMessageDto> {
    const message = await this.soulMatchService.sendMessage(
      user,
      id,
      dto,
      idempotencyKey,
    );

    // [SERVICE] SoulMatchService.sendMessage · soul-match/soul-match.service.ts
    const sessionId = id;

    // Step 2: Lấy phòng chat và tính phase theo giờ server
    // Phase chat tính từ giờ SERVER (lúc cả 2 xác nhận cộng thời lượng chat), không bao giờ tin
    // đồng hồ của client.
    const room = await this.getRoomForMember(user, sessionId);

    // [SERVICE] SoulMatchService.getRoomForMember
    // Không tồn tại hoặc không phải thành viên đều trả 404: không làm oracle dò sessionId.
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
    // Phòng không có cột trạng thái riêng: hạn chat và hạn rating chỉ được tính khi đọc, từ lúc
    // cả 2 xác nhận cộng thời lượng cấu hình.
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
    // Đã là bạn thì phòng ở chatting mãi: Friendship là nguồn sự thật, hạn cũ chỉ còn là dữ liệu
    // lịch sử.
    const phase =
      matched || now < chatEndsAt.getTime()
        ? SoulRoomPhase.Chatting
        : now < ratingEndsAt.getTime()
          ? SoulRoomPhase.Rating
          : SoulRoomPhase.Closed;
    return { session, chatEndsAt, ratingEndsAt, phase, matched };

    // Step 3: Chặn nếu phòng không còn ở phase chatting
    if (room.phase !== SoulRoomPhase.Chatting) {
      throw new DomainException(
        SoulMatchErrors.CHAT_NOT_OPEN,
        'Hết giờ chat — phòng đã chuyển sang đánh giá hoặc đã đóng',
        HttpStatus.CONFLICT,
        { phase: room.phase },
      );
    }

    // Step 4: Kiểm tra người gửi chưa bị khóa tài khoản
    // Kiểm lại trạng thái ban đúng lúc gửi: bị khóa giữa chừng phải cắt ngay.
    const profile = await this.userService.getByIdOrThrow(user.userId);

    // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
    // id = user.userId
    const user = await this.userRepo.findOneBy({ id });
    if (!user) {
      throw new DomainException(
        UserErrors.PROFILE_NOT_FOUND,
        'Không tìm thấy user',
        HttpStatus.NOT_FOUND,
      );
    }
    return user;

    if (profile.status === UserStatus.Banned) {
      throw new DomainException(
        SoulMatchErrors.USER_BANNED,
        'Tài khoản bị khoá',
        HttpStatus.FORBIDDEN,
      );
    }

    const maxLength = this.config.getOrThrow('SOUL_CHAT_MESSAGE_MAX_LENGTH', {
      infer: true,
    });
    if (dto.content.length > maxLength) {
      throw new DomainException(
        SoulMatchErrors.MESSAGE_TOO_LONG,
        `Message dài quá ${maxLength} ký tự`,
        HttpStatus.UNPROCESSABLE_ENTITY,
        { maxLength },
      );
    }

    // Step 5: Gắn user vào Idempotency-Key của client
    // Key của client được gắn prefix soul:msg:{userId}: unique ở DB, retry không nhân đôi tin
    // nhắn.
    const prefixedKey = messageIdempotencyKey(user.userId, idempotencyKey);

    // [SERVICE] messageIdempotencyKey · soul-match/soul-match.constants.ts
    const userId = user.userId;
    // key = idempotencyKey
    return `soul:msg:${userId}:${key}`;

    try {
      const message = await this.dataSource.transaction(async (manager) => {

        // Step 6: Trong transaction, khóa dòng session FOR UPDATE
        // [POSTGRES] Khóa dòng MatchSession FOR UPDATE, cùng khóa với lúc rating chụp lịch sử
        // chat để import sang Friend Chat: một tin nhắn không thể lọt vào giữa bước chụp và bước
        // chuyển.
        const lockedSession = await manager.findOne(MatchSession, {
          where: { id: sessionId },
          lock: { mode: 'pessimistic_write' },
        });
        if (
          !lockedSession ||
          (lockedSession.userAId !== user.userId &&
            lockedSession.userBId !== user.userId)
        ) {
          throw new DomainException(
            SoulMatchErrors.SESSION_NOT_FOUND,
            'Không tìm thấy session',
            HttpStatus.NOT_FOUND,
          );
        }

        // Step 7: Kiểm lại hạn chat sau khi đã có khóa
        // Kiểm lại hạn chat sau khi có khóa: phase tính ở đầu hàm có thể đã hết hạn trong lúc chờ
        // khóa. Đã là bạn (matched) thì chat không còn hết hạn.
        if (!room.matched && Date.now() >= room.chatEndsAt.getTime()) {
          throw new DomainException(
            SoulMatchErrors.CHAT_NOT_OPEN,
            'Hết giờ chat — phòng đã chuyển sang đánh giá hoặc đã đóng',
            HttpStatus.CONFLICT,
            { phase: SoulRoomPhase.Rating },
          );
        }

        // Step 8: Ghi tin nhắn (unique idempotency_key chặn nhân đôi)
        return manager.save(
          manager.create(SoulChatMessage, {
            sessionId,
            senderUserId: user.userId,
            content: dto.content,
            idempotencyKey: prefixedKey,
          }),
        );
      });

      // Step 9: Phát soul.message qua Redis, payload riêng cho từng người
      // [REDIS] Realtime phát SAU khi lưu, best-effort. Mỗi người nhận một payload riêng:
      // senderRole (me hoặc partner) tính ngay ở đây để gateway không phải biết gì về ẩn danh.
      const partnerId = this.partnerIdOf(room.session, user.userId);

      // [SERVICE] SoulMatchService.partnerIdOf
      // session = room.session
      // userId = user.userId
      return session.userAId === userId ? session.userBId : session.userAId;

      await Promise.all(
        (
          [
            [user.userId, 'me'],
            [partnerId, 'partner'],
          ] as const
        ).map(([recipientId, senderRole]) => {
          const envelope: RealtimeEnvelope<SoulMessageEventData> = {
            event: RealtimeEvents.SoulMessage,
            data: {
              sessionId,
              messageId: message.id,
              senderRole,
              content: message.content,
              sentAt: message.createdAt.toISOString(),
            },
          };
          return publishRealtimeEvent(
            this.redis,
            this.logger,
            recipientId,
            envelope,
          );

          // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
          const publisher = this.redis;
          const logger = this.logger;
          // userId = recipientId
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
        }),
      );
      return message;
    } catch (err) {

      // Step 10: Trùng key: cùng nội dung thì trả tin cũ, khác thì 409
      // Trùng Idempotency-Key: cùng phòng và cùng nội dung thì trả lại tin cũ (replay, không phát
      // realtime lại); khác nội dung thì 409.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (!isUniqueViolation(err)) throw err;
      const existing = await this.messageRepo.findOneBy({
        idempotencyKey: prefixedKey,
      });
      if (
        existing &&
        existing.sessionId === sessionId &&
        existing.content === dto.content
      ) {
        return existing;
      }
      throw new DomainException(
        SoulMatchErrors.MESSAGE_IDEMPOTENCY_CONFLICT,
        'Idempotency-Key đã dùng cho 1 message khác nội dung',
        HttpStatus.CONFLICT,
      );
    }

    // Step 11: Controller trả tin nhắn với vai trò me hoặc partner
    return SoulMessageDto.from(message, user.userId);
  }
}

// [POSTGRES] CREATE TABLE soul_chat_messages · database/migrations/1752400000000-soul-match.ts
// Unique idempotency_key là chốt chặn thật chống nhân đôi tin nhắn. seq bigint IDENTITY là thứ tự
// do DB cấp cho cursor: dùng created_at sẽ trùng mili-giây làm phân trang trùng hoặc mất dòng.
await queryRunner.query(`
  CREATE TABLE soul_chat_messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seq             bigint GENERATED ALWAYS AS IDENTITY,
    session_id      uuid         NOT NULL REFERENCES match_sessions(id),
    sender_user_id  uuid         NOT NULL REFERENCES users(id),
    content         text         NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_soul_chat_messages_idempotency_key UNIQUE (idempotency_key)
  )
`);

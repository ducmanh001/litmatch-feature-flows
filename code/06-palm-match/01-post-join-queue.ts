//  POST /api/v1/palm-match/queue
//  Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user lấy từ JWT)
//  Step 2: Trong transaction, lấy khóa advisory để chọn cặp tuần tự
//  Step 3: Đóng session đã quá hạn của chính user (hết hạn kiểu lười)
//  Step 4: Còn session chưa dismiss thì trả lại state đó, không xếp hàng
//  Step 5: Xóa entry chờ quá hạn, rồi xếp user vào hàng đợi (PK user_id)
//  Step 6: Lấy tối đa 20 người chờ lâu nhất (FIFO)
//  Step 7: Duyệt ứng viên: bỏ người đã có session, kiểm block/report
//  Step 8: Chưa có ứng viên hợp lệ: user ở lại hàng đợi, trả queued
//  Step 9: Có cặp: tạo session snapshot (seed tất định)
//  Step 10: Rút cả 2 khỏi hàng đợi, ghi 2 participant (PK user_id)
//  Step 11: Trả trạng thái đã ghép theo góc nhìn của user

// [CONTROLLER] PalmMatchController.joinQueue · palm-match/palm-match.controller.ts
@Controller('palm-match')
export class PalmMatchController {
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
    // PalmMatchService
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly dataSource: DataSource,
    // SafetyService
    @InjectRepository(Report) private readonly reportRepo: Repository<Report>,
    @InjectRepository(Block) private readonly blockRepo: Repository<Block>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user lấy từ JWT)

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

  @Post('queue')
  @HttpCode(HttpStatus.OK)
  async joinQueue(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );
  ): Promise<PalmMatchStateDto> {
    return this.palmMatchService.joinQueue(user.userId);

    // [SERVICE] PalmMatchService.joinQueue · palm-match/palm-match.service.ts
    const userId = user.userId;
    return this.dataSource.transaction(async (manager) => {

      // Step 2: Trong transaction, lấy khóa advisory để chọn cặp tuần tự
      // [POSTGRES] Khóa advisory theo transaction tuần tự hóa phần chọn cặp: không có 2 người
      // cùng ghép với 1 ứng viên. Đánh đổi: 1 khóa toàn cục nên ghép tuần tự trên cả hệ thống, đủ
      // cho tải pilot; khi cần thì chuyển hàng đợi sang Redis shard như Matching.
      await this.lockMatcher(manager);

      // [SERVICE] PalmMatchService.lockMatcher
      await manager.query('SELECT pg_advisory_xact_lock(hashtext($1))', [
        PALM_MATCH_ADVISORY_LOCK_KEY,
      ]);

      // Step 3: Đóng session đã quá hạn của chính user (hết hạn kiểu lười)
      // Hết hạn kiểu lười: module không có job quét, mỗi lần user vào hoặc hỏi trạng thái mới
      // đóng session đã quá deadline của chính họ.
      await this.expireActiveForUser(manager, userId);

      // [SERVICE] PalmMatchService.expireActiveForUser
      const active = await manager.findOneBy(PalmMatchActiveParticipant, {
        userId,
      });
      if (!active) return;
      const session = await manager.findOne(PalmMatchSession, {
        where: { id: active.sessionId },
        lock: { mode: 'pessimistic_write' },
      });
      if (session) await this.expireSession(manager, session);

      // [SERVICE] PalmMatchService.expireSession
      if (
        session.status === PalmMatchSessionStatus.Active &&
        session.expiresAt.getTime() <= Date.now()
      ) {
        this.completeSession(session, PalmMatchOutcome.Expired);

        // [SERVICE] PalmMatchService.completeSession
        const outcome = PalmMatchOutcome.Expired;
        session.status = PalmMatchSessionStatus.Completed;
        session.outcome = outcome;
        session.closedAt = new Date();

        await manager.save(session);
      }

      // Step 4: Còn session chưa dismiss thì trả lại state đó, không xếp hàng
      // Còn trỏ tới 1 session (đang chơi, hoặc đã xong nhưng chưa dismiss) thì trả lại state đó,
      // không xếp hàng thêm.
      const activeState = await this.activeState(manager, userId);

      // [SERVICE] PalmMatchService.activeState
      const active = await manager.findOneBy(PalmMatchActiveParticipant, {
        userId,
      });
      if (!active) return null;
      const session = await manager.findOneBy(PalmMatchSession, {
        id: active.sessionId,
      });
      if (!session) {
        await manager.delete(PalmMatchActiveParticipant, { userId });
        return null;
      }
      return this.sessionView(session, userId);

      // [SERVICE] PalmMatchService.sessionView
      const isLow = session.userLowId === userId;
      const partnerUserId = isLow ? session.userHighId : session.userLowId;
      const myFlippedAt = isLow ? session.lowFlippedAt : session.highFlippedAt;
      const opponentFlippedAt = isLow
        ? session.highFlippedAt
        : session.lowFlippedAt;
      const bothFlipped = Boolean(myFlippedAt && opponentFlippedAt);
      const mySignKey = isLow ? session.lowSign : session.highSign;
      const opponentSignKey = isLow ? session.highSign : session.lowSign;
      const myRating = isLow ? session.lowRating : session.highRating;

      return {
        state:
          session.status === PalmMatchSessionStatus.Completed
            ? PalmMatchClientState.Completed
            : PalmMatchClientState.Active,
        sessionId: session.id,
        expiresAt: session.expiresAt.toISOString(),
        myFlipped: Boolean(myFlippedAt),
        opponentFlipped: Boolean(opponentFlippedAt),
        ...(myFlippedAt ? { mySign: this.signByKey(mySignKey) } : {}),
        ...(opponentFlippedAt
          ? { opponentSign: this.signByKey(opponentSignKey) }
          : {}),
        ...(bothFlipped
          ? {
              compatibilityPercent: session.compatibilityPercent,
              fortune: session.fortune,
            }
          : {}),
        ...(myRating ? { myRating } : {}),
        ...(session.outcome ? { outcome: session.outcome } : {}),
        ...(session.outcome === PalmMatchOutcome.Matched
          ? { partnerUserId }
          : {}),
      };

      // [SERVICE] PalmMatchService.signByKey
      const key = mySignKey;
      const sign = PALM_ZODIAC_SIGNS.find((entry) => entry.key === key);
      if (!sign)
        throw new Error(`Palm Match session có sign không hợp lệ: ${key}`);
      return sign;

      if (activeState) return activeState;

      const queueMaxWaitSeconds = this.config.getOrThrow(
        'PALM_MATCH_QUEUE_MAX_WAIT_SECONDS',
        { infer: true },
      );
      const queueCutoff = new Date(Date.now() - queueMaxWaitSeconds * 1000);

      // Step 5: Xóa entry chờ quá hạn, rồi xếp user vào hàng đợi (PK user_id)
      // Xóa entry chờ quá hạn ngay lúc ghép, để không ghép với người đã bỏ đi.
      await manager.query(
        'DELETE FROM palm_match_queue_entries WHERE queued_at < $1',
        [queueCutoff],
      );
      // INSERT ... ON CONFLICT DO NOTHING: PK user_id làm enqueue idempotent, 2 tab hoặc retry
      // không xếp hàng 2 lần.
      await manager
        .createQueryBuilder()
        .insert()
        .into(PalmMatchQueueEntry)
        .values({ userId })
        .orIgnore()
        .execute();

      // Step 6: Lấy tối đa 20 người chờ lâu nhất (FIFO)
      // FIFO: người chờ lâu nhất trước, user_id làm tie-break cho thứ tự ổn định; chỉ xét tối đa
      // 20 người.
      const candidates = (await manager.query(
        `SELECT user_id FROM palm_match_queue_entries
         WHERE user_id <> $1 ORDER BY queued_at ASC, user_id ASC LIMIT 20`,
        [userId],
      )) as Array<{ user_id: string }>;

      let partnerId: string | undefined;

      // Step 7: Duyệt ứng viên: bỏ người đã có session, kiểm block/report
      for (const candidate of candidates) {
        const candidateActive = await manager.findOneBy(
          PalmMatchActiveParticipant,
          { userId: candidate.user_id },
        );
        // Ứng viên đã có session active thì entry hàng đợi của họ là thừa: xóa rồi xét người kế.
        if (candidateActive) {
          await manager.delete(PalmMatchQueueEntry, {
            userId: candidate.user_id,
          });
          continue;
        }
        // Kiểm lại block/report ĐÚNG LÚC ghép, không tin state ở thời điểm vào hàng đợi.

        // [SERVICE] SafetyService.canPair · safety/safety.service.ts
        const userAId = userId;
        const userBId = candidate.user_id;
        const activeBlock =
          (await this.isBlocked(userAId, userBId)) ||
          (await this.isBlocked(userBId, userAId));

        // [SERVICE] SafetyService.isBlocked
        const blockerUserId = userAId;
        const targetUserId = userBId;
        return (
          (await this.latestAction(blockerUserId, targetUserId)) ===
          BlockAction.Blocked
        );

        // [SERVICE] SafetyService.latestAction
        const blockedUserId = targetUserId;
        const latest = await this.blockRepo.findOne({
          where: { blockerUserId, blockedUserId },
          order: { createdAt: 'DESC' },
        });
        return latest?.action ?? null;

        if (activeBlock) return false;

        const cooldownDays = this.config.getOrThrow(
          'SAFETY_REMATCH_COOLDOWN_DAYS',
          { infer: true },
        );
        const cutoff = new Date(Date.now() - cooldownDays * DAY_MS);

        const recentReport = await this.reportRepo.exists({
          where: [
            {
              reporterUserId: userAId,
              targetUserId: userBId,
              createdAt: MoreThanOrEqual(cutoff),
            },
            {
              reporterUserId: userBId,
              targetUserId: userAId,
              createdAt: MoreThanOrEqual(cutoff),
            },
          ],
        });
        if (recentReport) return false;

        const recentBlock = await this.blockRepo.exists({
          where: [
            {
              blockerUserId: userAId,
              blockedUserId: userBId,
              createdAt: MoreThanOrEqual(cutoff),
            },
            {
              blockerUserId: userBId,
              blockedUserId: userAId,
              createdAt: MoreThanOrEqual(cutoff),
            },
          ],
        });
        return !recentBlock;

        if (await this.safetyService.canPair(userId, candidate.user_id)) {
          partnerId = candidate.user_id;
          break;
        }
      }

      // Step 8: Chưa có ứng viên hợp lệ: user ở lại hàng đợi, trả queued
      if (!partnerId) {
        const queued = await manager.findOneByOrFail(PalmMatchQueueEntry, {
          userId,
        });
        return {
          state: PalmMatchClientState.Queued,
          queuedAt: queued.queuedAt.toISOString(),
        };
      }

      // Step 9: Có cặp: tạo session snapshot (seed tất định)
      const session = await this.createSessionSnapshot(
        manager,
        userId,
        partnerId,
      );

      // [SERVICE] PalmMatchService.createSessionSnapshot
      // userAId = userId
      // userBId = partnerId
      const templates = await manager.find(PalmReadingTemplate, {
        where: { category: PalmMatchCategory.Love, isActive: true },
        order: { id: 'ASC' },
      });
      if (templates.length === 0) {
        throw new DomainException(
          PalmMatchErrors.CATEGORY_EMPTY,
          'Không có nội dung Palm Match đang hoạt động',
          HttpStatus.CONFLICT,
        );
      }

      const id = randomUUID();
      const [userLowId, userHighId] = canonicalPair(userAId, userBId);

      // [SERVICE] canonicalPair · common/entities/canonical-pair.ts
      const a = userAId;
      const b = userBId;
      return a < b ? [a, b] : [b, a];

      // Seed tất định (FNV-1a) từ id session và id từng user: cùng session luôn ra cùng kết quả,
      // session mới ra kết quả khác. Chỉ để giải trí, không dùng cho bảo mật.
      const lowSeed = fnv1aHash(`${id}:${userLowId}:sign`);

      // [SERVICE] fnv1aHash · common/hash/fnv1a.ts
      const input = `${id}:${userLowId}:sign`;
      let hash = 2166136261;
      for (let i = 0; i < input.length; i++) {
        hash ^= input.charCodeAt(i);
        hash = Math.imul(hash, 16777619) >>> 0;
      }
      return hash >>> 0;

      const highSeed = fnv1aHash(`${id}:${userHighId}:sign`);
      const resultSeed = fnv1aHash(`${id}:${userLowId}:${userHighId}:result`);
      const durationSeconds = this.config.getOrThrow(
        'PALM_MATCH_SESSION_DURATION_SECONDS',
        { infer: true },
      );
      const template = templates[resultSeed % templates.length];

      // Snapshot: cung hoàng đạo, % hợp và lời bói chốt MỘT lần lúc tạo session, nên reload hay
      // retry không quay ra kết quả khác.
      return manager.save(
        manager.create(PalmMatchSession, {
          id,
          userLowId,
          userHighId,
          lowSign: PALM_ZODIAC_SIGNS[lowSeed % PALM_ZODIAC_SIGNS.length].key,
          highSign: PALM_ZODIAC_SIGNS[highSeed % PALM_ZODIAC_SIGNS.length].key,
          compatibilityPercent: 60 + (resultSeed % 40),
          fortune: template.content.split('{name}').join('Người ấy'),
          lowFlippedAt: null,
          highFlippedAt: null,
          lowRating: null,
          highRating: null,
          status: PalmMatchSessionStatus.Active,
          outcome: null,
          expiresAt: new Date(Date.now() + durationSeconds * 1000),
          closedAt: null,
        }),
      );

      // Step 10: Rút cả 2 khỏi hàng đợi, ghi 2 participant (PK user_id)
      // Cùng transaction: ghép xong thì rút cả 2 khỏi hàng đợi và ghi 2 participant. PK
      // participant là chốt DB cuối cho "1 user 1 session" dù logic phía trên đổi sau này.
      await manager.delete(PalmMatchQueueEntry, [userId, partnerId]);
      await manager.insert(PalmMatchActiveParticipant, [
        { userId, sessionId: session.id },
        { userId: partnerId, sessionId: session.id },
      ]);

      // Step 11: Trả trạng thái đã ghép theo góc nhìn của user
      return this.sessionView(session, userId);
    });
  }
}

// [POSTGRES] CREATE TABLE palm_match_queue_entries · database/migrations/1755500000000-palm-match-session.ts
await queryRunner.query(`
  CREATE TABLE palm_match_queue_entries (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    queued_at timestamptz NOT NULL DEFAULT now()
  )
`);

await queryRunner.query(`
  CREATE INDEX idx_palm_match_queue_order ON palm_match_queue_entries (queued_at, user_id)
`);

await queryRunner.query(`
  CREATE TABLE palm_match_active_participants (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    session_id uuid NOT NULL REFERENCES palm_match_sessions(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
  )
`);

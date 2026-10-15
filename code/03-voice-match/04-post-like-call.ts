//  POST /api/v1/calling/calls/:id/like
//  Ghi lượt thích bất biến; khi cả 2 cùng thích thì tạo bạn bè và phòng chat trong cùng transaction.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận yêu cầu thích (id call từ URL, user từ JWT)
//  Step 2: Kiểm user thuộc call (không phải thành viên thì 404)
//  Step 3: Mở transaction DB, khóa dòng CallSession để 2 lượt thích chạy tuần tự
//  Step 4: Chỉ cho thích khi call đang active hoặc đã ended
//  Step 5: Ghi lượt thích bất biến của mình (bấm lại không ghi thêm)
//  Step 6: Cả 2 đã thích thì tạo Friendship và Conversation, cùng transaction
//  Step 7: Trả { liked, matched, friendUserId } về controller
//  Step 8: Dính unique: coi như đã thích, đọc lại trạng thái bạn bè

// [CONTROLLER] CallingController.likeCall · calling/calling.controller.ts
@Controller('calling')
export class CallingController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // CallingService
  private readonly logger = new Logger(CallingService.name);
  // LivekitSdkClient
  private readonly apiKey: string;
  private readonly apiSecret: string;
  private readonly roomService: RoomServiceClient;
  private readonly receiver: WebhookReceiver;
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // CallingService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(CallSession)
    private readonly callRepo: Repository<CallSession>,
    @Inject(CALLING_REDIS) private readonly redis: Redis,
    // FriendService
    @InjectRepository(Friendship)
    private readonly friendshipRepo: Repository<Friendship>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // LivekitSdkClient
    this.apiKey = config.getOrThrow('LIVEKIT_API_KEY', { infer: true });
    this.apiSecret = config.getOrThrow('LIVEKIT_API_SECRET', { infer: true });

    const apiUrl = config.getOrThrow('LIVEKIT_API_URL', { infer: true });
    const wsUrl = config.getOrThrow('LIVEKIT_URL', { infer: true });
    this.roomService = new RoomServiceClient(
      apiUrl || wsUrl.replace(/^ws/, 'http'),
      this.apiKey,
      this.apiSecret,
    );
    this.receiver = new WebhookReceiver(this.apiKey, this.apiSecret);
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận yêu cầu thích (id call từ URL, user từ JWT)

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

  @Post('calls/:id/like')
  @HttpCode(HttpStatus.OK)
  async likeCall(
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
  ): Promise<VoiceMatchLikeDto> {
    return await this.callingService.likeCall(user, id);

    // [SERVICE] CallingService.likeCall · calling/calling.service.ts
    const callId = id;

    // Step 2: Kiểm user thuộc call (không phải thành viên thì 404)
    // Không phải thành viên thì trả 404 giống như "không tồn tại", để không ai dò được callId của
    // người khác.
    await this.getCall(user, callId);

    // [SERVICE] CallingService.getCall
    let call = await this.callRepo.findOneBy({ id: callId });
    if (
      !call ||
      (call.userAId !== user.userId && call.userBId !== user.userId)
    ) {
      throw new DomainException(
        CallingErrors.CALL_NOT_FOUND,
        'Không tìm thấy call',
        HttpStatus.NOT_FOUND,
      );
    }
    call = await this.reconcileCallWithLivekit(call, user.userId);

    // [SERVICE] CallingService.reconcileCallWithLivekit
    const userId = user.userId;
    let identities: string[];
    try {
      identities = await this.livekit.listParticipantIdentities(call.roomName);

      // [EXTERNAL] LivekitRoomAdapter.listParticipantIdentities · calling/clients/livekit-room.adapter.ts
      const roomName = call.roomName;
      return this.client.listParticipantIdentities(roomName);

      // [EXTERNAL] LivekitSdkClient.listParticipantIdentities · common/livekit/livekit-sdk.client.ts
      const participants = await this.roomService.listParticipants(roomName);
      return participants.map((participant) => participant.identity);
    } catch (err) {
      this.logger.debug(
        `Không đối soát được participant của ${call.roomName}: ${err instanceof Error ? err.message : String(err)}`,
      );
      return call;
    }

    if (call.status === CallSessionStatus.Pending) {
      const memberIds = identities.filter(
        (identity) => identity === call.userAId || identity === call.userBId,
      );
      if (memberIds.length === 0) return call;
      for (const identity of memberIds) {
        await this.markJoined(call.id, identity);

        // [SERVICE] CallingService.markJoined
        // callId = call.id
        await this.dataSource.transaction(async (manager) => {
          const call = await manager.findOne(CallSession, {
            where: { id: callId },
            lock: { mode: 'pessimistic_write' },
          });
          if (!call || call.status === CallSessionStatus.Ended) return;

          const now = new Date();
          if (identity === call.userAId) call.joinedAAt = call.joinedAAt ?? now;
          else if (identity === call.userBId)
            call.joinedBAt = call.joinedBAt ?? now;
          else return;

          if (
            call.joinedAAt &&
            call.joinedBAt &&
            call.status === CallSessionStatus.Pending
          ) {
            call.status = CallSessionStatus.Active;
            call.startedAt = call.startedAt ?? now;
          }

          if (call.status === CallSessionStatus.Active) {
            if (identity === call.userAId) call.disconnectedAAt = null;
            if (identity === call.userBId) call.disconnectedBAt = null;

            if (
              call.reconnectStartedAt &&
              call.disconnectedAAt === null &&
              call.disconnectedBAt === null &&
              call.startedAt
            ) {
              const pausedMs = Math.max(
                0,
                now.getTime() - call.reconnectStartedAt.getTime(),
              );
              call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
              call.reconnectStartedAt = null;
            }
          }
          await manager.save(call);
        });
      }
      return (await this.callRepo.findOneBy({ id: call.id })) ?? call;
    }

    if (call.status === CallSessionStatus.Active) {
      const partnerId = call.userAId === userId ? call.userBId : call.userAId;
      if (!identities.includes(partnerId)) {
        const ended = await this.endById(call.id, CallEndReason.Completed);

        // [SERVICE] CallingService.endById
        // callId = call.id
        const reason = CallEndReason.Completed;
        const result = await this.dataSource.transaction(async (manager) => {
          const call = await manager.findOne(CallSession, {
            where: { id: callId },
            lock: { mode: 'pessimistic_write' },
          });
          if (!call) return { call: null, justEnded: false };
          if (call.status === CallSessionStatus.Ended) {
            return { call, justEnded: false };
          }
          call.status = CallSessionStatus.Ended;
          call.endReason = reason;
          call.endedAt = new Date();
          if (call.startedAt && call.reconnectStartedAt) {
            const pausedMs = Math.max(
              0,
              call.endedAt.getTime() - call.reconnectStartedAt.getTime(),
            );
            call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
          }
          call.durationSeconds = call.startedAt
            ? Math.max(
                0,
                Math.round(
                  (call.endedAt.getTime() - call.startedAt.getTime()) / 1000,
                ),
              )
            : 0;
          return { call: await manager.save(call), justEnded: true };
        });

        if (result.justEnded && result.call) {
          this.metrics.recordEnded(result.call.endReason ?? reason);
          await this.cleanupEndedCall(result.call);

          // [SERVICE] CallingService.cleanupEndedCall
          // call = result.call
          if (call.callKind !== CallKind.Friend && call.matchSessionId)
            await this.matchingService.endVoiceSessionForCall(
              call.matchSessionId,
              call.userAId,
              call.userBId,
            );

            // [SERVICE] MatchingService.endVoiceSessionForCall · matching/matching.service.ts
            const sessionId = call.matchSessionId;
            const userAId = call.userAId;
            const userBId = call.userBId;
            await this.dataSource.transaction(async (manager) => {
              const session = await manager.findOne(MatchSession, {
                where: { id: sessionId },
                lock: { mode: 'pessimistic_write' },
              });
              if (
                !session ||
                session.matchType !== MatchType.Voice ||
                session.status === MatchSessionStatus.Ended
              ) {
                return;
              }
              const samePair =
                (session.userAId === userAId && session.userBId === userBId) ||
                (session.userAId === userBId && session.userBId === userAId);
              if (!samePair) {
                throw new Error(
                  `Call members không khớp Voice Match session ${sessionId} — dữ liệu hỏng`,
                );
              }
              if (session.status !== MatchSessionStatus.Confirmed) return;
              session.status = MatchSessionStatus.Ended;
              session.endedAt = new Date();
              await manager.save(session);
            });

          await this.livekit.deleteRoom(call.roomName).catch((err) => {
            this.logger.debug(
              `deleteRoom ${call.roomName} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
            );
          });

          // [EXTERNAL] LivekitRoomAdapter.deleteRoom · calling/clients/livekit-room.adapter.ts
          // roomName = call.roomName
          return this.client.deleteRoom(roomName);

          // [EXTERNAL] LivekitSdkClient.deleteRoom · common/livekit/livekit-sdk.client.ts
          await this.roomService.deleteRoom(roomName);

          const envelope: RealtimeEnvelope<CallEndedEventData> = {
            event: RealtimeEvents.CallEnded,
            data: {
              callId: call.id,
              matchSessionId: call.matchSessionId,
              reason: call.endReason ?? CallEndReason.Completed,
              durationSeconds: call.durationSeconds ?? 0,
            },
          };
          await Promise.all(
            [call.userAId, call.userBId].map((uid) =>
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

        return ended.call ?? call;
      }
    }
    return call;

    return call;

    try {

      // Step 3: Mở transaction DB, khóa dòng CallSession để 2 lượt thích chạy tuần tự
      return await this.dataSource.transaction(async (manager) => {
        const call = await manager.findOne(CallSession, {
          where: { id: callId },
          // [POSTGRES] Khóa dòng CallSession FOR UPDATE: lượt thích của 2 người chạy tuần tự.
          // Không khóa thì mỗi người đều thấy "bên kia chưa thích" và không ai tạo Friendship.
          lock: { mode: 'pessimistic_write' },
        });
        if (
          !call ||
          (call.userAId !== user.userId && call.userBId !== user.userId)
        ) {
          throw new DomainException(
            CallingErrors.CALL_NOT_FOUND,
            'Không tìm thấy call',
            HttpStatus.NOT_FOUND,
          );
        }

        // Step 4: Chỉ cho thích khi call đang active hoặc đã ended
        // Cho thích cả khi call đang active, không bắt user phải end mới bấm tim được. Call
        // pending chưa có cuộc nói chuyện thật nên bị chặn.
        if (
          call.callKind === CallKind.Friend ||
          (call.status !== CallSessionStatus.Active &&
            call.status !== CallSessionStatus.Ended)
        ) {
          throw new DomainException(
            CallingErrors.CALL_NOT_ENDED,
            'Chỉ có thể yêu thích khi cuộc gọi đã kết nối hoặc đã kết thúc',
            HttpStatus.CONFLICT,
          );
        }
        const partnerId =
          call.userAId === user.userId ? call.userBId : call.userAId;

        // Step 5: Ghi lượt thích bất biến của mình (bấm lại không ghi thêm)
        // Lượt thích bất biến: bấm lại không ghi thêm dòng. Unique (call_id, rater_user_id) ở DB
        // là chốt chặn cuối cho double tap và retry.
        const existing = await manager.findOneBy(VoiceMatchReaction, {
          callId,
          raterUserId: user.userId,
        });
        if (!existing) {
          await manager.save(
            manager.create(VoiceMatchReaction, {
              callId,
              raterUserId: user.userId,
            }),
          );
        }
        const partnerLiked = await manager.existsBy(VoiceMatchReaction, {
          callId,
          raterUserId: partnerId,
        });

        // Step 6: Cả 2 đã thích thì tạo Friendship và Conversation, cùng transaction
        // [POSTGRES] Chỉ khi cả 2 đã thích mới tạo Friendship và Conversation, ngay trong
        // transaction đã khóa. ensureFriendship dùng ON CONFLICT DO NOTHING nên gọi lại không tạo
        // đôi, và không có Friendship thiếu Conversation.
        if (partnerLiked) {
          await this.friendService.ensureFriendship(
            manager,
            user.userId,
            partnerId,
            FriendshipSource.VoiceMatch,
          );

          // [SERVICE] FriendService.ensureFriendship · friend/friend.service.ts
          // userAId = user.userId
          // userBId = partnerId
          const source = FriendshipSource.VoiceMatch;
          const messageSeeds = [];
          if (userAId === userBId) {
            throw new Error(
              `Không thể tạo friendship với chính mình (${userAId}) — dữ liệu session hỏng`,
            );
          }
          const [userLowId, userHighId] = canonicalPair(userAId, userBId);

          // [SERVICE] canonicalPair · common/entities/canonical-pair.ts
          const a = userAId;
          const b = userBId;
          return a < b ? [a, b] : [b, a];

          const result = await manager
            .createQueryBuilder()
            .insert()
            .into(Friendship)
            .values({ userLowId, userHighId, source })
            .orIgnore()
            .execute();
          await manager
            .createQueryBuilder()
            .insert()
            .into(Conversation)
            .values({ userLowId, userHighId, lastMessageAt: null })
            .orIgnore()
            .execute();

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
        }
        const matched =
          partnerLiked ||
          (await this.friendService.areFriends(user.userId, partnerId));

        // [SERVICE] FriendService.areFriends · friend/friend.service.ts
        // userAId = user.userId
        // userBId = partnerId
        if (userAId === userBId) return false;
        const [userLowId, userHighId] = canonicalPair(userAId, userBId);
        return this.friendshipRepo.exists({ where: { userLowId, userHighId } });

        // Step 7: Trả { liked, matched, friendUserId } về controller
        return {
          liked: true,
          matched,
          friendUserId: matched ? partnerId : null,
        };
      });
    } catch (err) {

      // Step 8: Dính unique: coi như đã thích, đọc lại trạng thái bạn bè
      // Nếu unique vẫn bị đụng (chốt chặn cuối) thì coi như lượt thích đã được ghi: đọc lại trạng
      // thái bạn bè rồi trả như bình thường, không báo lỗi.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (!isUniqueViolation(err)) throw err;
      const call = await this.getCall(user, callId);
      const partnerId =
        call.userAId === user.userId ? call.userBId : call.userAId;
      const matched = await this.friendService.areFriends(
        user.userId,
        partnerId,
      );
      return {
        liked: true,
        matched,
        friendUserId: matched ? partnerId : null,
      };
    }
  }
}

// [POSTGRES] CREATE TABLE voice_match_reactions · database/migrations/1756000000000-voice-match-completion.ts
await queryRunner.query(`
  CREATE TABLE voice_match_reactions (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    call_id      uuid NOT NULL REFERENCES call_sessions(id),
    rater_user_id uuid NOT NULL REFERENCES users(id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_voice_match_reactions_call_rater UNIQUE (call_id, rater_user_id)
  );
`);

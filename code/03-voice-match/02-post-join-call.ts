//  POST /api/v1/calling/match-sessions/:matchSessionId/join
//  Mở phòng gọi cho cặp đã ghép: tạo CallSession duy nhất rồi cấp token LiveKit riêng cho từng người.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận yêu cầu vào phòng (giới hạn 20 lần/phút, user từ JWT)
//  Step 2: Kiểm user thuộc session (không phải thành viên thì 404)
//  Step 3: Lấy CallSession hiện có; session đã kết thúc thì từ chối
//  Step 4: Chưa có call thì tạo CallSession pending (roomName do server đặt)
//  Step 5: 2 người join cùng lúc: unique match_session_id, bên thua đọc lại call
//  Step 6: Đọc lại session; đã kết thúc giữa chừng thì đóng luôn call vừa tạo
//  Step 7: Ký token LiveKit riêng cho user (identity = userId)
//  Step 8: Trả call, token và URL LiveKit về controller
//  Step 9: Controller trả call, token, URL LiveKit và số giây gọi miễn phí

// [CONTROLLER] CallingController.join · calling/calling.controller.ts
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
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(CALLING_REDIS) private readonly redis: Redis,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
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
  // decorator, validate rồi gọi xử lý: Nhận yêu cầu vào phòng (giới hạn 20 lần/phút, user từ JWT)

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
    // token = authHeader.slice('Bearer '.length)
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

  @Post('match-sessions/:matchSessionId/join')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 20, ttl: minutes(1) } })
  async join(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Param('matchSessionId', ParseUUIDPipe) matchSessionId: string,

    // [CONTROLLER] ParseUUIDPipe configuration
    @Param('matchSessionId', ParseUUIDPipe)
  ): Promise<JoinCallDto> {
    const { call, token, livekitUrl } = await this.callingService.joinCall(
      user,
      matchSessionId,
    );

    // [SERVICE] CallingService.joinCall · calling/calling.service.ts
    // Step 2: Kiểm user thuộc session (không phải thành viên thì 404)
    // Không phải thành viên thì trả 404 giống như "không tồn tại", để không ai dò được sessionId
    // của người khác.
    const session = await this.matchingService.findSessionById(matchSessionId);

    // [SERVICE] MatchingService.findSessionById · matching/matching.service.ts
    const sessionId = matchSessionId;
    return this.dataSource
      .getRepository(MatchSession)
      .findOneBy({ id: sessionId });

    if (
      !session ||
      (session.userAId !== user.userId && session.userBId !== user.userId)
    ) {
      throw new DomainException(
        CallingErrors.SESSION_NOT_FOUND,
        'Không tìm thấy session',
        HttpStatus.NOT_FOUND,
      );
    }

    // Step 3: Lấy CallSession hiện có; session đã kết thúc thì từ chối
    let call = await this.callRepo.findOneBy({ matchSessionId });
    if (
      session.matchType !== MatchType.Voice ||
      session.status !== MatchSessionStatus.Confirmed
    ) {
      // Sau timeout hoặc webhook, session đã terminal cùng với call. Re-join call cũ vẫn trả
      // CALL_ENDED để client có một nhánh xử lý dứt khoát; dòng pending mồ côi từ race cũ được
      // đóng luôn, không chờ ticker.
      if (call?.status === CallSessionStatus.Ended) {
        throw new DomainException(
          CallingErrors.CALL_ENDED,
          'Call đã kết thúc',
          HttpStatus.CONFLICT,
          { reason: call.endReason },
        );
      }
      if (call && session.matchType === MatchType.Voice) {
        await this.endById(call.id, CallEndReason.Completed);

        // [SERVICE] CallingService.endById
        const callId = call.id;
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
            // sessionId = call.matchSessionId
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
          const roomName = call.roomName;
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
          const userId = uid;
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
      }
      throw new DomainException(
        CallingErrors.SESSION_NOT_CALLABLE,
        'Chỉ mở phòng call cho voice session đã đủ 2 bên xác nhận',
        HttpStatus.CONFLICT,
      );
    }

    // Step 4: Chưa có call thì tạo CallSession pending (roomName do server đặt)
    if (!call) {
      // Sinh id trước để roomName = call-{id} do server đặt; client không bao giờ tự chọn phòng.
      const id = randomUUID();
      try {
        call = await this.callRepo.save(
          this.callRepo.create({
            id,
            matchSessionId,
            roomName: callRoomName(id),
            userAId: session.userAId,
            userBId: session.userBId,
            status: CallSessionStatus.Pending,
            callKind: CallKind.VoiceMatch,
          }),
        );

        // [SERVICE] callRoomName · calling/entities/call-session.entity.ts
        // callId = id
        return `call-${callId}`;
      } catch (err) {

        // Step 5: 2 người join cùng lúc: unique match_session_id, bên thua đọc lại call
        // [POSTGRES] 2 bên join gần như cùng lúc đều thấy chưa có call và cùng insert: unique
        // match_session_id (xem bên dưới) chỉ cho 1 dòng, bên thua lấy lại dòng của bên kia.

        // [SERVICE] isUniqueViolation · database/postgres-errors.ts
        const e = err as { code?: string; driverError?: { code?: string } };
        return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

        if (!isUniqueViolation(err)) throw err;
        call = await this.callRepo.findOneByOrFail({ matchSessionId });
      }
    }

    // Step 6: Đọc lại session; đã kết thúc giữa chừng thì đóng luôn call vừa tạo
    // Đọc lại session sau khi tạo call: bên kia có thể vừa bấm back hoặc đóng tab và kết thúc
    // session. Khi đó đóng luôn call vừa tạo, không để dòng pending mồ côi chờ ticker hết hạn.
    const currentSession =
      await this.matchingService.findSessionById(matchSessionId);
    if (
      !currentSession ||
      currentSession.matchType !== MatchType.Voice ||
      currentSession.status !== MatchSessionStatus.Confirmed ||
      (currentSession.userAId !== user.userId &&
        currentSession.userBId !== user.userId)
    ) {
      await this.endById(call.id, CallEndReason.Completed);
      throw new DomainException(
        CallingErrors.CALL_ENDED,
        'Phiên Voice Match đã kết thúc',
        HttpStatus.CONFLICT,
      );
    }
    if (call.status === CallSessionStatus.Ended) {
      throw new DomainException(
        CallingErrors.CALL_ENDED,
        'Call đã kết thúc',
        HttpStatus.CONFLICT,
        { reason: call.endReason },
      );
    }

    // Step 7: Ký token LiveKit riêng cho user (identity = userId)
    // [EXTERNAL] Token chỉ cấp cho thành viên của call; mỗi lần join (kể cả join lại sau rớt
    // mạng) là một token mới. identity = userId từ JWT, phòng do server đặt.
    // Token là JWT ký ngay tại core-api bằng API key và secret của LiveKit, không gọi mạng; thời
    // hạn theo CALLING_TOKEN_TTL_SECONDS.
    const token = await this.livekit.mintJoinToken(
      call.roomName,
      user.userId,
      this.config.getOrThrow('CALLING_TOKEN_TTL_SECONDS', { infer: true }),
    );

    // [EXTERNAL] LivekitRoomAdapter.mintJoinToken · calling/clients/livekit-room.adapter.ts
    // roomName = call.roomName
    const identity = user.userId;
    const ttlSeconds = this.config.getOrThrow(
      'CALLING_TOKEN_TTL_SECONDS',
      { infer: true },
    );
    return this.client.mintJoinToken(roomName, identity, ttlSeconds, {
      canPublish: true,
    });

    // [EXTERNAL] LivekitSdkClient.mintJoinToken · common/livekit/livekit-sdk.client.ts
    const grant = {
      canPublish: true,
    };
    const token = new AccessToken(this.apiKey, this.apiSecret, {
      identity,
      ttl: ttlSeconds,
    });
    token.addGrant({
      room: roomName,
      roomJoin: true,
      canPublish: grant.canPublish,
      canSubscribe: true,
      ...(grant.canPublishData === undefined
        ? {}
        : { canPublishData: grant.canPublishData }),
    });
    return token.toJwt();

    // Step 8: Trả call, token và URL LiveKit về controller
    return {
      call,
      token,
      // URL LiveKit chọn theo region của userA: luôn dùng region của userA để cả 2 bên và mọi lần
      // join lại nhận cùng một URL. Region lệch chỉ cảnh báo, vì mọi URL trong map thuộc cùng một
      // cụm LiveKit nên call không vỡ.
      livekitUrl: await this.resolveCallLivekitUrl(call),
    };

    // [SERVICE] CallingService.resolveCallLivekitUrl
    const defaultUrl = this.config.getOrThrow('LIVEKIT_URL', { infer: true });
    const regionUrls = this.config.getOrThrow('LIVEKIT_REGION_URLS', {
      infer: true,
    });

    // [EXTERNAL] hasLivekitRegionUrls · common/livekit/livekit-url.ts
    const regionUrlsRaw = regionUrls;
    return Object.keys(parseLivekitRegionUrls(regionUrlsRaw)).length > 0;

    // [EXTERNAL] parseLivekitRegionUrls
    // raw = regionUrlsRaw
    if (raw.trim() === '') return {};
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error(
        'LIVEKIT_REGION_URLS phải là JSON hợp lệ dạng {"REGION": "wss://..."}',
      );
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(
        'LIVEKIT_REGION_URLS phải là JSON object map region → URL, không phải mảng/scalar',
      );
    }
    const map: Record<string, string> = {};
    for (const [region, url] of Object.entries(parsed)) {
      if (region.trim() === '') {
        throw new Error('LIVEKIT_REGION_URLS có key region rỗng');
      }
      if (typeof url !== 'string' || !/^wss?:\/\/./.test(url)) {
        throw new Error(
          `LIVEKIT_REGION_URLS["${region}"] phải là URL ws:// hoặc wss://`,
        );
      }
      map[region] = url;
    }
    return map;

    if (!hasLivekitRegionUrls(regionUrls)) return defaultUrl;

    try {
      const [userA, userB] = await Promise.all([
        this.userService.getByIdOrThrow(call.userAId),
        this.userService.getByIdOrThrow(call.userBId),
      ]);

      // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
      // id = call.userAId
      const user = await this.userRepo.findOneBy({ id });
      if (!user) {
        throw new DomainException(
          UserErrors.PROFILE_NOT_FOUND,
          'Không tìm thấy user',
          HttpStatus.NOT_FOUND,
        );
      }
      return user;

      if ((userA.region ?? null) !== (userB.region ?? null)) {
        this.logger.warn(
          `Region lệch giữa 2 bên call ${call.id} (userA=${userA.region ?? 'null'}, userB=${userB.region ?? 'null'}) — dùng region userA theo quy tắc tất định`,
        );
      }
      return resolveLivekitUrl(regionUrls, defaultUrl, userA.region);

      // [EXTERNAL] resolveLivekitUrl · common/livekit/livekit-url.ts
      // regionUrlsRaw = regionUrls
      // region = userA.region
      if (!region) return defaultUrl;
      return parseLivekitRegionUrls(regionUrlsRaw)[region] ?? defaultUrl;
    } catch (err) {
      this.logger.warn(
        `Không resolve được region cho call ${call.id} — fallback LIVEKIT_URL: ${err instanceof Error ? err.message : String(err)}`,
      );
      return defaultUrl;
    }

    // Step 9: Controller trả call, token, URL LiveKit và số giây gọi miễn phí
    return JoinCallDto.from(
      call,
      token,
      livekitUrl,
      this.callingService.getFreeCallSeconds(),
    );

    // [SERVICE] CallingService.getFreeCallSeconds · calling/calling.service.ts
    return this.config.getOrThrow('CALLING_FREE_CALL_SECONDS', { infer: true });
  }
}

// [POSTGRES] call-session.entity.ts · calling/entities/call-session.entity.ts
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })

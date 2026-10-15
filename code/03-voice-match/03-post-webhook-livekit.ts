//  POST /api/v1/calling/webhooks/livekit
//  LiveKit báo ai vào hoặc rời phòng, ticker nền canh giờ (reconnect, tối đa 420 giây) rồi kết thúc call.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận webhook công khai của LiveKit (@Public, lấy nguyên văn body)
//  Step 2: Kiểm chữ ký webhook (sai thì 401) rồi mới đọc event
//  Step 3: Chỉ nhận phòng có prefix call-, tìm CallSession theo roomName
//  Step 4: Người vào phòng: khóa dòng, ghi mốc; đủ 2 người thì call sang active
//  Step 5: Người rớt: ghi mốc rớt, mở cửa sổ reconnect, chưa end call
//  Step 6: Sau khi chữ ký hợp lệ luôn trả 200 { ok: true }
//  Step 7: Ticker nền: đăng ký job theo chu kỳ, mỗi tick chỉ 1 pod chạy
//  Step 8: Đóng call pending quá hạn (chưa ai vào đủ)
//  Step 9: End idempotent: khóa dòng, kiểm lại status, chốt thời lượng
//  Step 10: Dọn sau end: chốt MatchSession, xóa phòng, phát call.ended
//  Step 11: Đóng call rớt mạng quá cửa sổ reconnect
//  Step 12: Đóng call Voice Match chạm 420 giây (tính trong SQL)

// [CONTROLLER] LivekitWebhookController.handle · calling/webhooks/livekit-webhook.controller.ts
@Controller('calling/webhooks')
export class LivekitWebhookController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // LivekitSdkClient
  private readonly apiKey: string;
  private readonly apiSecret: string;
  private readonly roomService: RoomServiceClient;
  private readonly receiver: WebhookReceiver;
  // CallingService
  private readonly logger = new Logger(CallingService.name);
  // CallTickerService
  private readonly logger = new Logger(CallTickerService.name);
  private readonly job = new ManagedInterval();
  // ManagedInterval
  private running = false;
  private scheduledRunInFlight = false;
  private clusterRetryAfter = 0;
  private registration:
    { scheduler: SchedulerRegistry; jobName: string } | undefined;
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
    // CallTickerService
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly scheduler: SchedulerRegistry,
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
  // decorator, validate rồi gọi xử lý: Nhận webhook công khai của LiveKit (@Public, lấy nguyên
  // văn body)

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

  // [CONTROLLER] Public · common/decorators/public.decorator.ts
  export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);

  @Public()
  @Post('livekit')
  @HttpCode(HttpStatus.OK)
  async handle(
    @Req() req: RawBodyRequest<Request>,
    @Headers('authorization') authHeader?: string,
  ): Promise<{ ok: true }> {
    // Chữ ký được kiểm trên NGUYÊN VĂN body (không phải JSON đã parse), TRƯỚC khi đọc bất cứ nội
    // dung nào. Sai chữ ký thì 401.
    const rawBody = req.rawBody?.toString('utf8');
    if (!rawBody || !authHeader) {
      throw new DomainException(
        CallingErrors.WEBHOOK_INVALID,
        'Thiếu body hoặc Authorization header',
        HttpStatus.UNAUTHORIZED,
      );
    }
    let event;
    try {

      // Step 2: Kiểm chữ ký webhook (sai thì 401) rồi mới đọc event
      event = await this.livekit.receiveWebhook(rawBody, authHeader);

      // [EXTERNAL] LivekitRoomAdapter.receiveWebhook · calling/clients/livekit-room.adapter.ts
      const event = await this.client.receiveWebhook(rawBody, authHeader);

      // [EXTERNAL] LivekitSdkClient.receiveWebhook · common/livekit/livekit-sdk.client.ts
      // WebhookReceiver của LiveKit SDK kiểm JWT trong header Authorization bằng API key và
      // secret; sai thì ném lỗi, controller đổi thành 401.
      const event = await this.receiver.receive(rawBody, authHeader);
      return {
        event: event.event,
        roomName: event.room?.name ?? null,
        participantIdentity: event.participant?.identity ?? null,
      };

      return {
        event: event.event,
        roomName: event.roomName,
        participantIdentity: event.participantIdentity,
      };
    } catch {
      throw new DomainException(
        CallingErrors.WEBHOOK_INVALID,
        'Chữ ký webhook không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }
    await this.callingService.handleWebhookEvent(event);

    // [SERVICE] CallingService.handleWebhookEvent · calling/calling.service.ts
    // Step 3: Chỉ nhận phòng có prefix call-, tìm CallSession theo roomName
    // Chỉ nhận phòng có prefix call-: đó là prefix cố định để lọc phòng không thuộc calling.
    // Phòng lạ hoặc không có dòng CallSession thì bỏ qua.
    if (!event.roomName?.startsWith('call-')) return;
    const call = await this.callRepo.findOneBy({ roomName: event.roomName });
    if (!call) return;

    switch (event.event) {
      case 'participant_joined':
        if (event.participantIdentity) {
          await this.markJoined(call.id, event.participantIdentity);

          // [SERVICE] CallingService.markJoined
          const callId = call.id;
          const identity = event.participantIdentity;

          // Step 4: Người vào phòng: khóa dòng, ghi mốc; đủ 2 người thì call sang active
          await this.dataSource.transaction(async (manager) => {
            const call = await manager.findOne(CallSession, {
              where: { id: callId },
              // [POSTGRES] Khóa dòng CallSession FOR UPDATE rồi mới đọc và ghi: webhook của 2
              // bên, ticker và nút end cùng chạm một dòng. Có khóa rồi mới kiểm lại status.
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
              // Chỉ khi CẢ 2 đã vào phòng, call mới sang active và đặt startedAt. Đây là mốc tính
              // giới hạn 7 phút của Voice Match; set-if-null nên webhook gửi lại không ghi đè.
              call.startedAt = call.startedAt ?? now;
            }

            if (call.status === CallSessionStatus.Active) {
              // Bên nào quay lại thì gỡ dấu rớt của bên đó. Chỉ khi cả 2 đã quay lại mới chạy
              // tiếp đồng hồ: startedAt dời tiến đúng bằng khoảng gián đoạn nên thời gian rớt
              // mạng không bị tính vào 7 phút.
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
        return;
      // participant_left và room_finished không kết thúc call ngay mà chỉ mở cửa sổ reconnect:
      // mất mạng hay connection abort có thể quay lại. Quá hạn mới end.
      case 'participant_left':
      case 'participant_connection_aborted':
        if (event.participantIdentity) {
          await this.markDisconnected(call.id, event.participantIdentity);

          // [SERVICE] CallingService.markDisconnected
          // callId = call.id
          // identity = event.participantIdentity

          // Step 5: Người rớt: ghi mốc rớt, mở cửa sổ reconnect, chưa end call
          await this.dataSource.transaction(async (manager) => {
            const call = await manager.findOne(CallSession, {
              where: { id: callId },
              lock: { mode: 'pessimistic_write' },
            });
            if (!call || call.status !== CallSessionStatus.Active) return;

            const now = new Date();
            if (identity === call.userAId) call.disconnectedAAt ??= now;
            else if (identity === call.userBId) call.disconnectedBAt ??= now;
            else return;
            // Chỉ ghi mốc rớt đầu tiên (??=): event rớt lặp lại không kéo dài cửa sổ reconnect.
            // Từ mốc này đồng hồ giới hạn tạm dừng.
            call.reconnectStartedAt ??= now;
            await manager.save(call);
          });
        }
        return;
      case 'room_finished':
        await this.markRoomDisconnected(call.id);

        // [SERVICE] CallingService.markRoomDisconnected
        // callId = call.id
        await this.dataSource.transaction(async (manager) => {
          const call = await manager.findOne(CallSession, {
            where: { id: callId },
            lock: { mode: 'pessimistic_write' },
          });
          if (!call || call.status !== CallSessionStatus.Active) return;

          const now = new Date();
          call.reconnectStartedAt ??= now;
          call.disconnectedAAt ??= now;
          call.disconnectedBAt ??= now;
          await manager.save(call);
        });

        return;
      default:
        return;
    }

    // Step 6: Sau khi chữ ký hợp lệ luôn trả 200 { ok: true }
    return { ok: true };
  }
}

// [WORKER] CallTickerService.onApplicationBootstrap · calling/jobs/call-ticker.service.ts
@Injectable()
export class CallTickerService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  onApplicationBootstrap(): void {

    // Step 7: Ticker nền: đăng ký job theo chu kỳ, mỗi tick chỉ 1 pod chạy
    this.job.start(this.scheduler, {
      jobName: TICKER_JOB,
      intervalMs: this.config.getOrThrow('CALLING_TICKER_INTERVAL_MS', {
        infer: true,
      }),
      task: () => this.runOnce(),
      logger: this.logger,
      errorMessage: 'Call ticker tick lỗi',
      // [POSTGRES] Mỗi tick chỉ 1 pod làm nhờ advisory lock của Postgres. Nếu 2 pod vẫn chạy
      // trùng thì vẫn an toàn vì endById khóa dòng và chỉ end khi chưa ended.
      clusterSingleton: { dataSource: this.dataSource },
    });

    // [WORKER] CallTickerService.runOnce
    await this.job.runExclusive(async () => {
      await withSpan('litmatch.calling', 'calling.ticker.tick', async () => {

        // Step 8: Đóng call pending quá hạn (chưa ai vào đủ)
        await this.sweepPending();

        // [WORKER] CallTickerService.sweepPending
        const timeoutSeconds = this.config.getOrThrow(
          'CALLING_PENDING_TIMEOUT_SECONDS',
          { infer: true },
        );
        const cutoff = new Date(Date.now() - timeoutSeconds * 1000);
        const stale = await this.dataSource.getRepository(CallSession).find({
          select: { id: true },
          where: { status: CallSessionStatus.Pending, createdAt: LessThan(cutoff) },
          order: { createdAt: 'ASC', id: 'ASC' },
          // Mỗi tick chỉ lấy id theo lô; phần tồn đọng xử lý dần ở các tick sau. Truy vấn đi theo
          // index (status, created_at).
          take: TICK_BATCH_SIZE,
        });
        for (const call of stale) {
          await this.callingService.endById(call.id, CallEndReason.PendingTimeout);

          // [SERVICE] CallingService.endById · calling/calling.service.ts
          const callId = call.id;
          const reason = CallEndReason.PendingTimeout;

          // Step 9: End idempotent: khóa dòng, kiểm lại status, chốt thời lượng
          const result = await this.dataSource.transaction(async (manager) => {
            const call = await manager.findOne(CallSession, {
              where: { id: callId },
              // [POSTGRES] Khóa dòng rồi kiểm lại status: chốt chặn race end-vs-tick (nút end,
              // webhook, ticker cùng gọi). Đã ended thì trả lại như cũ, không end lần 2.
              lock: { mode: 'pessimistic_write' },
            });
            if (!call) return { call: null, justEnded: false };
            if (call.status === CallSessionStatus.Ended) {
              return { call, justEnded: false };
            }
            call.status = CallSessionStatus.Ended;
            call.endReason = reason;
            call.endedAt = new Date();
            // Thời gian nằm trong cửa sổ reconnect không tính vào thời lượng: dời startedAt tiến
            // lên trước khi chốt duration.
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

          // Chỉ lời gọi thực hiện transition mới dọn phòng và phát realtime, nên retry hay ticker
          // chạy trùng không bắn event đôi.
          if (result.justEnded && result.call) {
            this.metrics.recordEnded(result.call.endReason ?? reason);

            // Step 10: Dọn sau end: chốt MatchSession, xóa phòng, phát call.ended
            await this.cleanupEndedCall(result.call);

            // [SERVICE] CallingService.cleanupEndedCall
            // call = result.call
            // Chốt MatchSession ở Matching TRƯỚC khi phát event, để user nhận call.ended rồi quay
            // lại tìm ghép không bị session cũ giữ lại. Cả webhook lẫn ticker đều đi qua đây,
            // không chỉ nút UI.
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

            // [EXTERNAL] Xóa phòng LiveKit best-effort để không rò tài nguyên. Phòng có thể đã tự
            // đóng (room_finished) nên lỗi ở đây không phải lỗi nghiệp vụ.
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
            // [REDIS] Phát call.ended cho cả 2 người qua kênh realtime:user:{id}. Best-effort:
            // client vẫn poll được GET /calling/calls/:id.
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
              const payload = JSON.stringify(envelope);
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

        // Step 11: Đóng call rớt mạng quá cửa sổ reconnect
        await this.sweepReconnectTimeouts();

        // [WORKER] CallTickerService.sweepReconnectTimeouts
        const reconnectSeconds = this.config.getOrThrow(
          'CALLING_RECONNECT_WINDOW_SECONDS',
          { infer: true },
        );
        const cutoff = new Date(Date.now() - reconnectSeconds * 1000);
        const stale = await this.dataSource.getRepository(CallSession).find({
          select: { id: true },
          where: {
            status: CallSessionStatus.Active,
            // Rớt mạng ngắn không kết thúc call ngay: có CALLING_RECONNECT_WINDOW_SECONDS để quay
            // lại. Quá hạn mà chưa quay lại mới end completed.
            reconnectStartedAt: LessThan(cutoff),
          },
          order: { reconnectStartedAt: 'ASC', id: 'ASC' },
          take: TICK_BATCH_SIZE,
        });
        for (const call of stale) {
          await this.callingService.endById(call.id, CallEndReason.Completed);
        }

        // Step 12: Đóng call Voice Match chạm 420 giây (tính trong SQL)
        await this.processActiveCalls();

        // [WORKER] CallTickerService.processActiveCalls
        const freeSeconds = this.config.getOrThrow('CALLING_FREE_CALL_SECONDS', {
          infer: true,
        });
        const activeQuery = this.dataSource
          .getRepository(CallSession)
          .createQueryBuilder('call')
          .select(['call.id'])
          .where('call.status = :status', { status: CallSessionStatus.Active })
          // Call đang trong cửa sổ reconnect bị loại khỏi đợt kiểm này: đồng hồ 420 giây tạm dừng
          // cho tới khi cả 2 quay lại hoặc quá hạn.
          .andWhere('call.reconnect_started_at IS NULL')
          .andWhere('call.started_at IS NOT NULL')
          .orderBy('call.updated_at', 'ASC')
          .addOrderBy('call.id', 'ASC')
          .take(TICK_BATCH_SIZE);

        activeQuery
          // Chỉ Voice Match bị giới hạn: call bạn bè gọi lâu dài, không đi qua đồng hồ này. Hạn
          // 420 giây tính ngay trong SQL theo đồng hồ của Postgres, từ startedAt đã bù thời gian
          // reconnect; không tin timer của client.
          .andWhere("call.call_kind = 'voice_match'")
          .andWhere(
            'call.started_at <= now() - make_interval(secs => :freeSeconds)',
            { freeSeconds },
          );

        const active = await activeQuery.getMany();
        for (const call of active) {
          await this.callingService
            .endById(call.id, CallEndReason.FreeLimit)
            .catch((err) =>
              this.logger.error(
                { err: `${err}` },
                `Xử lý call active ${call.id} lỗi — thử lại ở tick sau`,
              ),
            );
        }
      });

      // [SERVICE] withSpan · libs/observability/src/lib/traced.ts
      const tracerName = 'litmatch.calling';
      const spanName = 'calling.ticker.tick';
      const fn = async () => {
        await this.sweepPending();
        await this.sweepReconnectTimeouts();
        await this.processActiveCalls();
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
    }, undefined);

    // [SERVICE] ManagedInterval.runExclusive · common/scheduling/managed-interval.ts
    const task = async () => {
      await withSpan('litmatch.calling', 'calling.ticker.tick', async () => {
        await this.sweepPending();
        await this.sweepReconnectTimeouts();
        await this.processActiveCalls();
      });
    };
    const busyResult = undefined;
    if (this.running) return busyResult;
    this.running = true;
    try {
      return await task();
    } finally {
      this.running = false;
    }

    // [SERVICE] ManagedInterval.start
    const scheduler = this.scheduler;
    // options = đối số 2 của lời gọi ở trên
    const interval = setInterval(
      () =>
        void this.runScheduled(options).catch((error: unknown) =>
          options.logger.error({ err: `${error}` }, options.errorMessage),
        ),
      options.intervalMs,
    );

    // [SERVICE] ManagedInterval.runScheduled
    if (this.scheduledRunInFlight) return;

    // [SERVICE] isRuntimeActive · common/runtime/runtime-activity.ts
    const now = Date.now();
    const idleAfterMs = RUNTIME_ACTIVITY_IDLE_AFTER_MS;
    return (
      lastMeaningfulRequestAt !== undefined &&
      now - lastMeaningfulRequestAt <= idleAfterMs
    );

    if (options.skipWhenIdle && !isRuntimeActive()) {
      recordRuntimeBackgroundSkip(options.jobName);

      // [SERVICE] recordRuntimeBackgroundSkip · common/runtime/runtime-activity.ts
      const jobName = options.jobName;
      backgroundSkipCount += 1;
      backgroundSkipsByJob.set(
        jobName,
        (backgroundSkipsByJob.get(jobName) ?? 0) + 1,
      );

      return;
    }
    if (
      options.clusterSingleton !== undefined &&
      Date.now() < this.clusterRetryAfter
    ) {
      return;
    }

    this.scheduledRunInFlight = true;
    try {
      const clusterSingleton = options.clusterSingleton;
      if (clusterSingleton === undefined) {
        await options.task();
        return;
      }
      await this.runClusterSingleton({
        ...options,
        clusterSingleton,
      });

      // [SERVICE] ManagedInterval.runClusterSingleton
      const options = {
        ...options,
        clusterSingleton,
      };
      const retryMs =
        options.clusterSingleton.followerRetryMs ??
        Math.max(options.intervalMs, CLUSTER_FOLLOWER_RETRY_MIN_MS);
      let runner: QueryRunner | undefined;

      try {
        runner = options.clusterSingleton.dataSource.createQueryRunner();
        await runner.connect();
        await runner.startTransaction();
        const rows = (await runner.query(
          `SELECT pg_try_advisory_xact_lock(
                    hashtextextended($1, 0)
                  ) AS acquired`,
          [`litmatch:cluster-job:${options.jobName}`],
        )) as Array<{ acquired: boolean }>;

        if (rows[0]?.acquired !== true) {
          this.clusterRetryAfter = Date.now() + retryMs;
          await runner.rollbackTransaction();
          return;
        }

        this.clusterRetryAfter = 0;
        await options.task();
        await runner.commitTransaction();
      } catch (error) {
        if (runner?.isTransactionActive) {
          await runner.rollbackTransaction().catch(() => undefined);
        }
        this.clusterRetryAfter = Date.now() + retryMs;
        throw error;
      } finally {
        await runner?.release().catch(() => undefined);
      }
    } finally {
      this.scheduledRunInFlight = false;
    }

    scheduler.addInterval(options.jobName, interval);
    this.registration = { scheduler, jobName: options.jobName };
  }
}

// [POSTGRES] call-session.entity.ts · calling/entities/call-session.entity.ts
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })
@Index('idx_call_sessions_status_created', ['status', 'createdAt'])
@Index('idx_call_sessions_active_updated', ['updatedAt', 'id'], {
  where: "status = 'active'",
})
@Index('idx_call_sessions_reconnect_started', ['reconnectStartedAt', 'id'], {
  where: "status = 'active' AND reconnect_started_at IS NOT NULL",
})

//  POST /api/v1/party/rooms
//  Tạo phòng và membership host trong 1 transaction, rồi tạo room trên LiveKit; SFU lỗi thì đóng phòng bù trừ.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, title và category từ body; 5 lần/phút)
//  Step 2: Chốt URL LiveKit theo region của host
//  Step 3: Ghi phòng và membership host trong 1 transaction
//  Step 4: Sau commit, tạo room tường minh trên LiveKit
//  Step 5: SFU lỗi: đóng phòng vừa tạo để bù trừ
//  Step 6: Đóng phòng idempotent: khóa dòng phòng, đã closed thì bỏ qua
//  Step 7: Dọn room trên SFU và báo party.room.closed cho member
//  Step 8: Trả 503 để client thử lại sau
//  Step 9: Ký token publish cho host, trả phòng và URL LiveKit
//  Step 10: Controller trả phòng, membership host, token và URL LiveKit

// [CONTROLLER] PartyRoomController.create · party-room/party-room.controller.ts
@Controller('party/rooms')
export class PartyRoomController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // PartyRoomService
  private readonly logger = new Logger(PartyRoomService.name);
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
    // PartyRoomService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(PARTY_REDIS) private readonly redis: Redis,
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
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, title và category từ body; 5
  // lần/phút)

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

  @Post()
  @Throttle({ default: { limit: 5, ttl: minutes(1) } })
  async create(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Body() body: CreatePartyRoomDto,
  ): Promise<JoinPartyRoomDto> {
    const { room, membership, token, livekitUrl } =
      await this.partyRoomService.createRoom(user, body.title, body.category);

    // [SERVICE] PartyRoomService.createRoom · party-room/party-room.service.ts
    const title = body.title;
    const category = body.category;
    const cleanTitle = title.trim();
    const maxLen = this.config.getOrThrow('PARTY_TITLE_MAX_LENGTH', {
      infer: true,
    });
    if (cleanTitle.length === 0 || cleanTitle.length > maxLen) {
      throw new DomainException(
        PartyRoomErrors.TITLE_TOO_LONG,
        `Title phải từ 1 tới ${maxLen} ký tự`,
        HttpStatus.BAD_REQUEST,
      );
    }

    // Step 2: Chốt URL LiveKit theo region của host
    // Chốt URL LiveKit theo region của HOST (server suy ra từ User, không nhận từ client) trước
    // khi tạo phòng: phòng ghim 1 endpoint trọn đời, cùng triết lý snapshot với speakerLimit.
    const livekitUrl = await this.resolveHostLivekitUrl(user.userId);

    // [SERVICE] PartyRoomService.resolveHostLivekitUrl
    const hostUserId = user.userId;
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
    const host = await this.userService.getByIdOrThrow(hostUserId);

    // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
    const id = hostUserId;
    const user = await this.userRepo.findOneBy({ id });
    if (!user) {
      throw new DomainException(
        UserErrors.PROFILE_NOT_FOUND,
        'Không tìm thấy user',
        HttpStatus.NOT_FOUND,
      );
    }
    return user;

    return resolveLivekitUrl(regionUrls, defaultUrl, host.region);

    // [EXTERNAL] resolveLivekitUrl · common/livekit/livekit-url.ts
    // regionUrlsRaw = regionUrls
    // region = host.region
    if (!region) return defaultUrl;
    return parseLivekitRegionUrls(regionUrlsRaw)[region] ?? defaultUrl;

    let room: PartyRoom;
    let membership: PartyRoomMember;
    try {

      // Step 3: Ghi phòng và membership host trong 1 transaction
      // [POSTGRES] Phòng và membership host ghi ATOMIC trong 1 transaction: không bao giờ có
      // phòng vô chủ ngay từ lúc sinh.
      ({ room, membership } = await this.dataSource.transaction(
        async (manager) => {
          const createdRoom = await manager.save(
            manager.create(PartyRoom, {
              hostUserId: user.userId,
              title: cleanTitle,
              category,
              status: PartyRoomStatus.Active,
              // Chụp giới hạn speaker lúc tạo: đổi config sau này không ảnh hưởng phòng đang
              // sống.
              speakerLimit: this.config.getOrThrow('PARTY_MAX_SPEAKERS', {
                infer: true,
              }),
              livekitUrl,
            }),
          );
          const member = await manager.save(
            manager.create(PartyRoomMember, {
              roomId: createdRoom.id,
              userId: user.userId,
              role: PartyRole.Host,
            }),
          );
          return { room: createdRoom, membership: member };
        },
      ));
    } catch (err) {

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      // [SERVICE] violatedConstraint
      const constraint = UQ_PARTY_MEMBERS_ACTIVE_USER;
      const e = err as {
        driverError?: { constraint?: string };
        constraint?: string;
        message?: string;
      };
      const name = e.driverError?.constraint ?? e.constraint;
      return name === constraint || (e.message?.includes(constraint) ?? false);

      if (
        isUniqueViolation(err) &&
        violatedConstraint(err, UQ_PARTY_MEMBERS_ACTIVE_USER)
      ) {
        // [POSTGRES] Host đang ở phòng active khác thì insert member dính unique index "1 user 1
        // phòng" (xem bên dưới).
        throw new DomainException(
          PartyRoomErrors.ALREADY_IN_ANOTHER_ROOM,
          'Bạn đang ở trong 1 phòng khác — rời phòng đó trước',
          HttpStatus.CONFLICT,
        );
      }
      throw err;
    }

    try {

      // Step 4: Sau commit, tạo room tường minh trên LiveKit
      // [EXTERNAL] Gọi SFU SAU commit (lệnh mạng không nằm trong atomic của Postgres) và tạo room
      // TƯỜNG MINH với maxParticipants, emptyTimeout thay vì để LiveKit tự tạo khi có người vào.
      await this.livekit.createRoom(partyRoomName(room.id), {
        maxParticipants: this.config.getOrThrow('PARTY_MAX_MEMBERS', {
          infer: true,
        }),
        emptyTimeoutSeconds: this.config.getOrThrow(
          'PARTY_EMPTY_ROOM_TIMEOUT_SECONDS',
          { infer: true },
        ),
      });

      // [EXTERNAL] LivekitPartyRoomAdapter.createRoom · party-room/clients/livekit-party-room.adapter.ts
      const roomName = `${PARTY_ROOM_NAME_PREFIX}${room.id}`;
      // opts = đối số 2 của lời gọi ở trên
      return this.client.createRoom(roomName, opts);

      // [EXTERNAL] LivekitSdkClient.createRoom · common/livekit/livekit-sdk.client.ts
      await this.roomService.createRoom({
        name: roomName,
        maxParticipants: opts.maxParticipants,
        emptyTimeout: opts.emptyTimeoutSeconds,
      });
    } catch (err) {
      this.logger.error(
        `Tạo LiveKit room cho phòng ${room.id} thất bại: ${err instanceof Error ? err.message : String(err)}`,
      );

      // Step 5: SFU lỗi: đóng phòng vừa tạo để bù trừ
      // Bù trừ: SFU lỗi thì đóng phòng vừa tạo (lý do error); đóng cũng nhả membership host nên
      // host không bị kẹt trong 1 phòng hỏng.
      await this.closeRoomById(room.id, PartyRoomCloseReason.Error).catch(
        () => undefined,
      );

      // [SERVICE] PartyRoomService.closeRoomById
      const roomId = room.id;
      const reason = PartyRoomCloseReason.Error;
      const result = await this.dataSource.transaction(async (manager) => {

        // Step 6: Đóng phòng idempotent: khóa dòng phòng, đã closed thì bỏ qua
        // [POSTGRES] Đóng phòng idempotent: khóa dòng phòng, đã closed thì no-op, nên endpoint
        // host-leave, webhook, sweeper và bù trừ này gọi trùng vẫn an toàn.
        const room = await manager.findOne(PartyRoom, {
          where: { id: roomId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!room || room.status === PartyRoomStatus.Closed) {
          return { closed: false, memberIds: [] as string[] };
        }
        // `guard` (sweeper dùng, lúc bù trừ này không truyền) chạy TRÊN dòng đã khóa, trong cùng
        // transaction: host kịp nối lại thì guard fail và không đóng nhầm.
        if (guard !== undefined && !guard(room)) {
          return { closed: false, memberIds: [] as string[] };
        }
        const activeMembers = await manager.findBy(PartyRoomMember, {
          roomId,
          leftAt: IsNull(),
        });
        const now = new Date();
        room.status = PartyRoomStatus.Closed;
        room.closeReason = reason;
        room.closedAt = now;
        await manager.save(room);
        await onTransition?.(manager, room);
        if (activeMembers.length > 0) {
          await manager.update(
            PartyRoomMember,
            { roomId, leftAt: IsNull() },
            { leftAt: now, disconnectedAt: null },
          );
        }
        return { closed: true, memberIds: activeMembers.map((m) => m.userId) };
      });

      // Step 7: Dọn room trên SFU và báo party.room.closed cho member
      // Chỉ lời gọi thực hiện transition mới dọn SFU và publish, nên retry không bắn event đôi.
      if (result.closed) {
        await this.livekit.deleteRoom(partyRoomName(roomId)).catch((err) => {
          // Room có thể đã tự đóng (room_finished hoặc empty timeout): không phải lỗi nghiệp vụ.
          this.logger.debug(
            `deleteRoom ${partyRoomName(roomId)} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
          );
        });

        // [EXTERNAL] LivekitPartyRoomAdapter.deleteRoom · party-room/clients/livekit-party-room.adapter.ts
        // roomName = `${PARTY_ROOM_NAME_PREFIX}${roomId}`
        return this.client.deleteRoom(roomName);

        // [EXTERNAL] LivekitSdkClient.deleteRoom · common/livekit/livekit-sdk.client.ts
        await this.roomService.deleteRoom(roomName);

        const envelope: RealtimeEnvelope<PartyRoomClosedEventData> = {
          event: RealtimeEvents.PartyRoomClosed,
          data: { roomId, reason },
        };
        await Promise.all(
          result.memberIds.map((uid) =>
            // [REDIS] Báo party.room.closed cho từng member đã đóng; best-effort, client còn
            // poll.
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

      // Step 8: Trả 503 để client thử lại sau
      // Lỗi hạ tầng (5xx) nên không dùng DomainException (chỉ 4xx); client thử lại sau.
      throw new ServiceUnavailableException(
        PartyRoomErrors.MEDIA_ROOM_CREATE_FAILED,
        'Không tạo được phòng trên media server — thử lại sau',
      );
    }

    // Step 9: Ký token publish cho host, trả phòng và URL LiveKit
    return {
      room,
      membership,
      token: await this.mintToken(room.id, user.userId, PartyRole.Host),
      livekitUrl: this.livekitUrlOf(room),
    };

    // [SERVICE] PartyRoomService.mintToken
    // roomId = room.id
    // userId = user.userId
    const role = PartyRole.Host;
    return this.livekit.mintJoinToken(
      partyRoomName(roomId),
      userId,
      this.config.getOrThrow('PARTY_TOKEN_TTL_SECONDS', { infer: true }),
      { canPublish: role === PartyRole.Host || role === PartyRole.Speaker },
    );

    // [EXTERNAL] LivekitPartyRoomAdapter.mintJoinToken · party-room/clients/livekit-party-room.adapter.ts
    // roomName = `${PARTY_ROOM_NAME_PREFIX}${roomId}`
    const identity = userId;
    const ttlSeconds = this.config.getOrThrow('PARTY_TOKEN_TTL_SECONDS', { infer: true });
    const grants = { canPublish: role === PartyRole.Host || role === PartyRole.Speaker };
    return this.client.mintJoinToken(roomName, identity, ttlSeconds, {
      canPublish: grants.canPublish,
      canPublishData: true,
    });

    // [EXTERNAL] LivekitSdkClient.mintJoinToken · common/livekit/livekit-sdk.client.ts
    const grant = {
      canPublish: grants.canPublish,
      canPublishData: true,
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

    // [SERVICE] PartyRoomService.livekitUrlOf · party-room/party-room.service.ts
    return (
      room.livekitUrl ?? this.config.getOrThrow('LIVEKIT_URL', { infer: true })
    );

    // Step 10: Controller trả phòng, membership host, token và URL LiveKit
    return JoinPartyRoomDto.from(room, membership, token, livekitUrl);
  }
}

// [POSTGRES] 1752700000000-party-room-gift.ts · database/migrations/1752700000000-party-room-gift.ts
await queryRunner.query(
  `CREATE UNIQUE INDEX uq_party_members_active_user
     ON party_room_members(user_id) WHERE left_at IS NULL`,
);

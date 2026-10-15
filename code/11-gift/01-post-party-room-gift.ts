//  POST /api/v1/party/rooms/:roomId/gifts
//  Tặng quà trong Party Room: trừ DIA người tặng, cộng PTS người nhận, ghi GiftEvent trong cùng một transaction.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, roomId từ URL, Idempotency-Key từ header)
//  Step 2: Chặn tự tặng, đọc giá quà từ DB
//  Step 3: Kiểm người tặng và người nhận cùng ở trong phòng
//  Step 4: Tính điểm PTS cho người nhận (guest thì 0)
//  Step 5: Economy dựng 2 chân sổ kép: DIA người tặng, PTS người nhận
//  Step 6: Ledger kiểm sổ kép: mỗi currency tự cân Nợ = Có
//  Step 7: Mở transaction DB, chèn giao dịch idempotent
//  Step 8: Khóa ví người tặng và người nhận theo thứ tự userId
//  Step 9: Cập nhật số dư (DIA) và earnings (PTS), ghi outbox
//  Step 10: Chạy tiếp outbox, GiftEvent, notification trong cùng transaction
//  Step 11: Sau commit, lần đầu mới phát gift.sent qua Redis và push
//  Step 12: Controller trả response (GiftEventDto)

// [CONTROLLER] GiftController.sendGift · gift/gift.controller.ts
@Controller()
export class GiftController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // GiftService
  private readonly logger = new Logger(GiftService.name);
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
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // GiftService
    @InjectRepository(Gift) private readonly giftRepo: Repository<Gift>,
    @InjectRepository(GiftEvent)
    private readonly giftEventRepo: Repository<GiftEvent>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(GIFT_REDIS) private readonly redis: Redis,
    // PartyRoomService
    @InjectRepository(PartyRoom)
    private readonly roomRepo: Repository<PartyRoom>,
    @InjectRepository(PartyRoomMember)
    private readonly memberRepo: Repository<PartyRoomMember>,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
    // WebPushSubscriptionService
    @InjectRepository(WebPushSubscription)
    private readonly subscriptionRepo: Repository<WebPushSubscription>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // LedgerService
    super();
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
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, roomId từ URL, Idempotency-Key
  // từ header)

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

  @Post('party/rooms/:roomId/gifts')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 30, ttl: minutes(1) } })
  async sendGift(
    @CurrentUser() user: AuthenticatedUser,

    // [CONTROLLER] CurrentUser · common/decorators/current-user.decorator.ts
    export const CurrentUser = createParamDecorator(
      (_data: unknown, ctx: ExecutionContext): AuthenticatedUser => {
        const req = ctx.switchToHttp().getRequest<{ user: AuthenticatedUser }>();
        return req.user;
      },
    );

    @Param('roomId', ParseUUIDPipe) roomId: string,

    // [CONTROLLER] ParseUUIDPipe configuration
    @Param('roomId', ParseUUIDPipe)

    @Body() body: SendGiftDto,
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
  ): Promise<GiftEventDto> {
    const { giftEvent, gift, replayed } = await this.giftService.sendGift(
      user,
      roomId,
      body.giftId,
      body.receiverUserId,
      idempotencyKey,
    );

    // [SERVICE] GiftService.sendGift · gift/gift.service.ts
    const giftId = body.giftId;
    const receiverUserId = body.receiverUserId;

    // Step 2: Chặn tự tặng, đọc giá quà từ DB
    if (receiverUserId === user.userId) {
      throw new DomainException(
        GiftErrors.SELF_GIFT_FORBIDDEN,
        'Không tự tặng quà cho chính mình',
        HttpStatus.BAD_REQUEST,
      );
    }

    // Giá đọc từ DB đúng lúc tặng; client chỉ gửi giftId, không bao giờ gửi giá.
    const gift = await this.giftRepo.findOneBy({ id: giftId, active: true });
    if (!gift) {
      throw new DomainException(
        GiftErrors.GIFT_NOT_FOUND,
        'Quà không tồn tại hoặc đã ngừng bán',
        HttpStatus.NOT_FOUND,
      );
    }

    // Step 3: Kiểm người tặng và người nhận cùng ở trong phòng
    // Kiểm thành viên chỉ là luật nghiệp vụ, KHÔNG phải chốt an toàn tiền: tiền vẫn đúng dù có
    // race rời phòng nhờ transaction và idempotency của Economy.
    const { members } =
      await this.partyRoomService.getActiveRoomMembers(roomId);

    // [SERVICE] PartyRoomService.getActiveRoomMembers · party-room/party-room.service.ts
    const { room, members } = await this.getRoom(roomId, false);

    // [SERVICE] PartyRoomService.getRoom
    const includeProfiles = false;
    const room = await this.roomRepo.findOneBy({ id: roomId });
    if (!room) {
      throw new DomainException(
        PartyRoomErrors.ROOM_NOT_FOUND,
        'Không tìm thấy phòng',
        HttpStatus.NOT_FOUND,
      );
    }
    const members =
      room.status === PartyRoomStatus.Active
        ? await this.memberRepo.findBy({ roomId, leftAt: IsNull() })
        : [];
    const profiles = includeProfiles
      ? await this.userService.findByIds(members.map((member) => member.userId))
      : [];

    // [SERVICE] UserService.findByIds · user/user.service.ts
    const ids = members.map((member) => member.userId);
    const uniqueIds = [...new Set(ids)];
    if (uniqueIds.length === 0) return [];
    return this.userRepo.find({ where: { id: In(uniqueIds) } });

    return { room, members, profiles };

    if (room.status !== PartyRoomStatus.Active) {
      throw new DomainException(
        PartyRoomErrors.ROOM_CLOSED,
        'Phòng đã đóng',
        HttpStatus.CONFLICT,
      );
    }
    return { room, members };

    if (!members.some((m) => m.userId === user.userId)) {
      throw new DomainException(
        GiftErrors.SENDER_NOT_IN_ROOM,
        'Bạn không ở trong phòng này',
        HttpStatus.FORBIDDEN,
      );
    }
    if (!members.some((m) => m.userId === receiverUserId)) {
      throw new DomainException(
        GiftErrors.RECEIVER_NOT_IN_ROOM,
        'Người nhận không ở trong phòng này',
        HttpStatus.CONFLICT,
      );
    }

    const receiver = await this.userService.getByIdOrThrow(receiverUserId);

    // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
    const id = receiverUserId;
    const user = await this.userRepo.findOneBy({ id });
    if (!user) {
      throw new DomainException(
        UserErrors.PROFILE_NOT_FOUND,
        'Không tìm thấy user',
        HttpStatus.NOT_FOUND,
      );
    }
    return user;

    const ratePercent = this.config.getOrThrow('GIFT_POINTS_RATE_PERCENT', {
      infer: true,
    });

    // Step 4: Tính điểm PTS cho người nhận (guest thì 0)
    // Người nhận là guest thì chân PTS = 0 (quà vẫn tặng được); còn lại điểm = giá × tỉ lệ%, làm
    // tròn xuống.
    const pointsAwarded = receiver.isGuest
      ? 0
      : Math.floor((gift.priceDiamond * ratePercent) / 100);

    let notification: Notification | undefined;
    const result = await this.economy.sendGift({
      senderUserId: user.userId,
      receiverUserId,
      priceDiamond: gift.priceDiamond,
      pointsAwarded,
      // Key = gift:send:{người tặng}:{key client}, unique ở DB trên transactions: retry không trừ
      // tiền hai lần.
      idempotencyKey: giftSendIdempotencyKey(user.userId, idempotencyKey),
      // Snapshot tỉ lệ quy đổi vào metadata của giao dịch; đổi cấu hình sau này không diễn giải
      // lại giao dịch cũ.
      metadata: {
        giftId: gift.id,
        giftCode: gift.code,
        roomId,
        pointsRatePercent: ratePercent,
        receiverIsGuest: receiver.isGuest,
      },
      // [POSTGRES] GiftEvent và notification ghi CÙNG transaction với các bút toán: bước nào lỗi
      // thì rollback hết, không có chuyện trừ tiền mà không có quà.
      withinTransaction: async (manager, transactionId) => {
        await manager.save(
          manager.create(GiftEvent, {
            giftId: gift.id,
            roomId,
            senderUserId: user.userId,
            receiverUserId,
            priceDiamond: gift.priceDiamond,
            pointsAwarded,
            pointsRatePercent: ratePercent,
            transactionId,
          }),
        );
        notification = await this.notificationService.createWithManager(
          manager,
          {
            userId: receiverUserId,
            type: NotificationType.GiftReceived,
            payload: {
              roomId,
              senderUserId: user.userId,
              giftCode: gift.code,
              priceDiamond: gift.priceDiamond,
            },
          },
        );

        // [SERVICE] NotificationService.createWithManager · notification/notification.service.ts
        // input = đối số 2 của lời gọi ở trên
        return manager.save(
          manager.create(Notification, { ...input, readAt: null }),
        );
      },
    });

    // [SERVICE] EconomyService.sendGift · economy/economy.service.ts
    // input = đối số 1 của lời gọi ở trên
    const {
      senderUserId,
      receiverUserId,
      priceDiamond,
      pointsAwarded,
      idempotencyKey,
      metadata,
    } = input;
    if (!Number.isSafeInteger(priceDiamond) || priceDiamond <= 0) {
      throw new Error(
        `sendGift: priceDiamond phải là số nguyên dương, nhận ${priceDiamond}`,
      );
    }
    if (!Number.isSafeInteger(pointsAwarded) || pointsAwarded < 0) {
      throw new Error(
        `sendGift: pointsAwarded phải là số nguyên >= 0, nhận ${pointsAwarded}`,
      );
    }
    // Bất biến chống nhân đôi giá trị: điểm quy đổi không bao giờ vượt giá quà (tỉ lệ dưới 1:1).
    if (pointsAwarded > priceDiamond) {
      throw new Error(
        `sendGift: pointsAwarded (${pointsAwarded}) không được vượt priceDiamond (${priceDiamond})`,
      );
    }
    if (senderUserId === receiverUserId) {
      throw new Error('sendGift: không tự tặng quà cho chính mình');
    }

    const price = BigInt(priceDiamond);
    const points = BigInt(pointsAwarded);

    // Step 5: Economy dựng 2 chân sổ kép: DIA người tặng, PTS người nhận
    // Hai chân độc lập, mỗi chân tự cân theo currency: DIA từ người tặng vào quỹ quà, PTS do hệ
    // thống mint cho người nhận. Diamond không bao giờ chuyển thẳng user sang user; phần chênh
    // (giá trừ điểm) ở lại system_gift_pool.
    const entries = [
      {
        accountKind: LedgerAccountKind.UserWallet,
        userId: senderUserId,
        direction: LedgerDirection.Debit,
        amount: price,
        currency: LedgerCurrency.Diamond,
      },
      {
        accountKind: LedgerAccountKind.SystemGiftPool,
        direction: LedgerDirection.Credit,
        amount: price,
        currency: LedgerCurrency.Diamond,
      },
      // Chân PTS chỉ có khi điểm lớn hơn 0: bút toán amount 0 bị validateEntries chặn.
      ...(points > 0n
        ? [
            {
              accountKind: LedgerAccountKind.SystemPointsMint,
              direction: LedgerDirection.Debit,
              amount: points,
              currency: LedgerCurrency.Points,
            },
            {
              accountKind: LedgerAccountKind.UserEarnings,
              userId: receiverUserId,
              direction: LedgerDirection.Credit,
              amount: points,
              currency: LedgerCurrency.Points,
            },
          ]
        : []),
    ];

    const result = await this.ledger.record({
      type: TransactionType.GiftSend,
      idempotencyKey,
      actorUserId: senderUserId,
      metadata: { ...metadata, receiverUserId },
      entries,
      // Event economy.gift.sent riêng vì consumer cần người nhận (event mặc định chỉ nói về ví
      // người tặng); rồi chạy tiếp phần việc của module Gift (GiftEvent, notification) trong cùng
      // transaction.
      withinTransaction: async (manager, txn) => {
        await manager.save(
          manager.create(OutboxEvent, {
            topic: ECONOMY_EVENTS_TOPIC,
            eventType: 'economy.gift.sent',
            payload: {
              version: 1,
              transactionId: txn.id,
              senderUserId,
              receiverUserId,
              priceDiamond: price.toString(),
              pointsAwarded: points.toString(),
              ...metadata,
            },
          }),
        );
        await input.withinTransaction?.(manager, txn.id);
      },
    });

    // [SERVICE] LedgerService.record · economy/services/ledger.service.ts
    // input = đối số 1 của lời gọi ở trên

    // Step 6: Ledger kiểm sổ kép: mỗi currency tự cân Nợ = Có
    this.validateInput(input);

    // [SERVICE] LedgerService.validateInput
    this.validateEntries(input.entries);

    // [SERVICE] LedgerService.validateEntries
    // entries = input.entries
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
    // Mỗi currency phải tự cân (tổng Nợ bằng tổng Có): giao dịch 2 currency được tách thành các
    // chân độc lập, DIA và PTS không bù trừ cho nhau.
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

    const existing = await this.dataSource
      .getRepository(LedgerTransaction)
      .findOneBy({ idempotencyKey: input.idempotencyKey });
    if (existing) {
      try {
        const transaction = this.assertSameRequest(existing, requestHash);

        // [SERVICE] LedgerService.assertSameRequest
        if (existing.requestHash !== requestHash) {
          throw new DomainException(
            EconomyErrors.IDEMPOTENCY_CONFLICT,
            'Idempotency key đã dùng cho 1 request khác nội dung',
            HttpStatus.CONFLICT,
          );
        }
        return existing;

        this.metrics.record(input.type, 'replayed');
        return { transaction, replayed: true };
      } catch (err) {
        this.metrics.record(input.type, 'failed');
        throw err;
      }
    }

    try {

      // Step 7: Mở transaction DB, chèn giao dịch idempotent
      const result = await this.dataSource.transaction((manager) =>
        this.recordInManager(manager, input),
      );

      // [SERVICE] LedgerService.recordInManager
      this.validateInput(input);
      const requestHash = this.hashRequest(input);
      const existing = await manager.findOne(LedgerTransaction, {
        where: { idempotencyKey: input.idempotencyKey },
      });
      if (existing) {
        return {
          transaction: this.assertSameRequest(existing, requestHash),
          replayed: true,
        };
      }

      // [POSTGRES] ON CONFLICT DO NOTHING thay vì bắt lỗi unique, vì lỗi đó làm abort cả
      // transaction của caller. Postgres chờ bên thắng commit; không chèn được dòng mới thì đây
      // là replay, không ghi bút toán.
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

      // Step 8: Khóa ví người tặng và người nhận theo thứ tự userId
      // Khóa ví theo thứ tự userId đã sắp xếp để tránh deadlock khi một giao dịch chạm nhiều ví
      // (quà chạm ví người tặng và, khi có chân PTS, cả ví người nhận).
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

      // Step 9: Cập nhật số dư (DIA) và earnings (PTS), ghi outbox
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
        // [POSTGRES] Ví người tặng đổi balance (DIA), ví người nhận đổi earnings (PTS), cùng một
        // vòng lặp trong cùng transaction: không thể trừ DIA mà không cộng PTS.
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
        // Điểm earnings không bao giờ âm (DB cũng có CHECK earnings >= 0), nên hoàn quà sau khi
        // người nhận đã tiêu điểm sẽ bị chặn ở đây; luồng hoàn quà chưa làm.
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

      // Step 10: Chạy tiếp outbox, GiftEvent, notification trong cùng transaction
      await input.withinTransaction?.(manager, txn);
      return { transaction: txn, replayed: false };

      this.metrics.record(input.type, result.replayed ? 'replayed' : 'success');
      return result;
    } catch (err) {

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      // [SERVICE] violatedConstraint
      const constraint = UQ_TRANSACTIONS_IDEMPOTENCY_KEY;
      const e = err as {
        driverError?: { constraint?: string };
        constraint?: string;
        message?: string;
      };
      const name = e.driverError?.constraint ?? e.constraint;
      return name === constraint || (e.message?.includes(constraint) ?? false);

      if (
        isUniqueViolation(err) &&
        violatedConstraint(err, UQ_TRANSACTIONS_IDEMPOTENCY_KEY)
      ) {
        // [POSTGRES] Kiểm trước chỉ là đường nhanh; nguồn chân lý là unique idempotency_key ở DB.
        // 2 request cùng key chạy song song thì bên thua đọc lại dòng của bên thắng và trả như
        // replay.
        const winner = await this.dataSource
          .getRepository(LedgerTransaction)
          .findOneByOrFail({ idempotencyKey: input.idempotencyKey });
        try {
          const transaction = this.assertSameRequest(winner, requestHash);
          this.metrics.record(input.type, 'replayed');
          return { transaction, replayed: true };
        } catch (assertErr) {
          this.metrics.record(input.type, 'failed');
          throw assertErr;
        }
      }
      this.metrics.record(input.type, 'failed');
      throw err;
    }

    return { transactionId: result.transaction.id, replayed: result.replayed };

    // Replay trả lại đúng GiftEvent cũ nhờ unique transaction_id, không tạo event mới.
    const giftEvent = await this.giftEventRepo.findOneByOrFail({
      transactionId: result.transactionId,
    });

    // Step 11: Sau commit, lần đầu mới phát gift.sent qua Redis và push
    // Chỉ lần đầu mới phát realtime và push, và chỉ SAU khi giao dịch tiền đã commit: retry không
    // bắn hiệu ứng lần hai.
    if (!result.replayed) {
      await this.publishGiftSent(roomId, gift, giftEvent);

      // [SERVICE] GiftService.publishGiftSent
      const event = giftEvent;
      const envelope: RealtimeEnvelope<GiftSentEventData> = {
        event: RealtimeEvents.GiftSent,
        data: {
          roomId,
          giftEventId: event.id,
          giftCode: gift.code,
          senderUserId: event.senderUserId,
          receiverUserId: event.receiverUserId,
          priceDiamond: event.priceDiamond,
          pointsAwarded: event.pointsAwarded,
          sentAt: event.createdAt.toISOString(),
        },
      };
      try {
        const { members } =
          await this.partyRoomService.getActiveRoomMembers(roomId);
        // [REDIS] Phát gift.sent cho từng thành viên đang ở phòng qua kênh realtime:user:{id},
        // SAU khi giao dịch commit: client chỉ thấy hiệu ứng khi tiền đã trừ.
        await Promise.all(
          members.map((m) =>
            publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
          ),
        );

        // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
        const publisher = this.redis;
        const logger = this.logger;
        // userId = m.userId
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
      } catch (err) {
        // Phòng vừa đóng giữa chừng thì chỉ mất hiệu ứng realtime, giao dịch quà vẫn hợp lệ.
        this.logger.warn(
          `Fanout gift.sent phòng ${roomId} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
        );
      }

      if (notification) await this.notificationService.sendPush(notification);

      // [SERVICE] NotificationService.sendPush · notification/notification.service.ts
      try {
        const result = await this.pushPort.send({
          notificationId: notification.id,
          recipientId: notification.userId,
          type: notification.type,
          ...(deviceToken ? { deviceToken } : {}),
        });

        // [SERVICE] ApnsPushNotificationAdapter.send · common/platform/apns-push-notification.adapter.ts
        // message = { notificationId: notification.id, recipientId: notification.userId, type:
        // notification.type, ...(deviceToken ? { deviceToken } : {}), }
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
        // message = { notificationId: notification.id, recipientId: notification.userId, type:
        // notification.type, ...(deviceToken ? { deviceToken } : {}), }
        void message;
        return { status: 'skipped' };

        // [SERVICE] DevPushNotificationAdapter.send
        // message = { notificationId: notification.id, recipientId: notification.userId, type:
        // notification.type, ...(deviceToken ? { deviceToken } : {}), }
        void message;
        this.logger.debug('[dev-push] delivery skipped');
        return { status: 'skipped' };

        // [SERVICE] FcmPushNotificationAdapter.send · common/platform/fcm-push-notification.adapter.ts
        // message = { notificationId: notification.id, recipientId: notification.userId, type:
        // notification.type, ...(deviceToken ? { deviceToken } : {}), }
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
        // userId = notification.userId
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
              // id = subscription.id
              await this.subscriptionRepo.delete({ id });
          }),
        );
      } catch {
        this.logger.warn(
          'Web push provider failed; in-app notification retained',
        );
      }
    }
    return { giftEvent, gift, replayed: result.replayed };

    // Step 12: Controller trả response (GiftEventDto)
    return GiftEventDto.from(giftEvent, gift.code, replayed);
  }
}

// [POSTGRES] CREATE TABLE gift_events · database/migrations/1752700000000-party-room-gift.ts
// Các bất biến của Economy cũng được khóa ở DB: unique transaction_id (1 GiftEvent ứng 1 giao
// dịch), không tự tặng, điểm không vượt giá. Đây là bản gốc của bảng; sau đó thêm video_id và
// profile_user_id (CHECK đúng 1 ngữ cảnh).
await queryRunner.query(`
  CREATE TABLE gift_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gift_id             uuid        NOT NULL REFERENCES gifts(id),
    room_id             uuid        NOT NULL REFERENCES party_rooms(id),
    sender_user_id      uuid        NOT NULL REFERENCES users(id),
    receiver_user_id    uuid        NOT NULL REFERENCES users(id),
    price_diamond       int         NOT NULL,
    points_awarded      int         NOT NULL,
    points_rate_percent int         NOT NULL,
    transaction_id      uuid        NOT NULL REFERENCES transactions(id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_gift_events_transaction UNIQUE (transaction_id),
    CONSTRAINT chk_gift_events_no_self_gift CHECK (sender_user_id <> receiver_user_id),
    CONSTRAINT chk_gift_events_points_lte_price CHECK (points_awarded <= price_diamond)
  )
`);

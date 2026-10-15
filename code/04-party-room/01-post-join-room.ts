//  POST /api/v1/party/rooms/:id/join
//  Vào phòng Party làm audience, hoặc vào lại khi rớt mạng, rồi nhận token LiveKit theo role hiện tại (audience không được publish).
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL; tối đa 20 lần/phút)
//  Step 2: Trong transaction, khóa dòng phòng FOR UPDATE
//  Step 3: Join lại khi rớt mạng: xóa mốc rớt, không tạo membership mới
//  Step 4: Đếm member đang ở phòng, so với sức chứa
//  Step 5: Tạo membership audience (unique index chặn đang ở phòng khác)
//  Step 6: Sau commit, báo party.member.joined cho các member còn lại
//  Step 7: Chốt URL LiveKit của phòng, ký token theo role hiện tại
//  Step 8: Controller trả phòng, membership, token và URL LiveKit

// [CONTROLLER] PartyRoomController.join · party-room/party-room.controller.ts
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
    @InjectRepository(PartyRoomMember)
    private readonly memberRepo: Repository<PartyRoomMember>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(PARTY_REDIS) private readonly redis: Redis,
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
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL; tối đa 20
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

  @Post(':id/join')
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

    @Param('id', ParseUUIDPipe) id: string,

    // [CONTROLLER] ParseUUIDPipe configuration
    @Param('id', ParseUUIDPipe)
  ): Promise<JoinPartyRoomDto> {
    const { room, membership, token, livekitUrl } =
      await this.partyRoomService.joinRoom(user, id);

    // [SERVICE] PartyRoomService.joinRoom · party-room/party-room.service.ts
    const roomId = id;
    const { room, membership, isNewJoin, hostReconnected, memberReconnected } =
      await this.dataSource.transaction(async (manager) => {

        // Step 2: Trong transaction, khóa dòng phòng FOR UPDATE
        // [POSTGRES] Khóa dòng phòng (FOR UPDATE) trước mọi việc khác: join, leave, đổi role,
        // close đều xếp hàng qua đúng 1 dòng này, nên việc đếm bên dưới không bị race.
        const lockedRoom = await this.lockActiveRoom(manager, roomId);

        // [SERVICE] PartyRoomService.lockActiveRoom
        const room = await manager.findOne(PartyRoom, {
          where: { id: roomId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!room) {
          throw new DomainException(
            PartyRoomErrors.ROOM_NOT_FOUND,
            'Không tìm thấy phòng',
            HttpStatus.NOT_FOUND,
          );
        }
        if (room.status !== PartyRoomStatus.Active) {
          throw new DomainException(
            PartyRoomErrors.ROOM_CLOSED,
            'Phòng đã đóng',
            HttpStatus.CONFLICT,
          );
        }
        return room;

        const existing = await manager.findOneBy(PartyRoomMember, {
          roomId,
          userId: user.userId,
          leftAt: IsNull(),
        });

        // Step 3: Join lại khi rớt mạng: xóa mốc rớt, không tạo membership mới
        // Rớt mạng rồi join lại là hợp lệ: không tạo membership mới, chỉ xóa mốc rớt và cấp token
        // mới theo role hiện tại trong DB.
        if (existing) {
          // Host nối lại trong lúc chờ thì hủy lịch đóng phòng. Lock CÙNG dòng phòng với
          // grace-check của sweeper nên Postgres tự xếp thứ tự: bên nào lock trước thắng, không
          // đóng nhầm phòng vừa hồi phục.
          const hostReconnected =
            existing.role === PartyRole.Host &&
            lockedRoom.hostDisconnectedAt !== null;
          const memberReconnected =
            existing.role !== PartyRole.Host &&
            existing.disconnectedAt !== null;
          if (hostReconnected) {
            lockedRoom.hostDisconnectedAt = null;
            await manager.save(lockedRoom);
          }
          if (memberReconnected) {
            existing.disconnectedAt = null;
            await manager.save(existing);
          }
          return {
            room: lockedRoom,
            membership: existing,
            isNewJoin: false,
            hostReconnected,
            memberReconnected,
          };
        }

        // Step 4: Đếm member đang ở phòng, so với sức chứa
        // Đếm member DƯỚI lock phòng; DB và SFU (maxParticipants) dùng chung 1 giới hạn từ 1
        // config, SFU là chốt chặn thứ hai.
        const activeCount = await manager.countBy(PartyRoomMember, {
          roomId,
          leftAt: IsNull(),
        });
        if (
          activeCount >=
          this.config.getOrThrow('PARTY_MAX_MEMBERS', { infer: true })
        ) {
          throw new DomainException(
            PartyRoomErrors.ROOM_FULL,
            'Phòng đã đầy',
            HttpStatus.CONFLICT,
          );
        }

        try {

          // Step 5: Tạo membership audience (unique index chặn đang ở phòng khác)
          // [POSTGRES] 1 user chỉ ở 1 phòng active: khóa phòng chỉ xếp hàng trong từng phòng, nên
          // chốt chặn là unique index trên user_id (xem bên dưới), không phải check-rồi-insert.
          // Dính unique = user đang ở phòng khác.
          const member = await manager.save(
            manager.create(PartyRoomMember, {
              roomId,
              userId: user.userId,
              role: PartyRole.Audience,
            }),
          );
          return {
            room: lockedRoom,
            membership: member,
            isNewJoin: true,
            hostReconnected: false,
            memberReconnected: false,
          };
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
            throw new DomainException(
              PartyRoomErrors.ALREADY_IN_ANOTHER_ROOM,
              'Bạn đang ở trong 1 phòng khác — rời phòng đó trước',
              HttpStatus.CONFLICT,
            );
          }
          throw err;
        }
      });

    // Step 6: Sau commit, báo party.member.joined cho các member còn lại
    // [REDIS] Realtime bắn SAU commit và best-effort; join lại (isNewJoin = false) không bắn
    // "joined", và không bắn cho chính người vừa join.
    if (isNewJoin) {
      await this.publishToRoomMembers(
        roomId,
        {
          event: RealtimeEvents.PartyMemberJoined,
          data: {
            roomId,
            userId: user.userId,
            role: membership.role,
          } satisfies PartyMemberJoinedEventData,
        },
        { excludeUserId: user.userId },
      );

      // [SERVICE] PartyRoomService.publishToRoomMembers
      const envelope = {
        event: RealtimeEvents.PartyMemberJoined,
        data: {
          roomId,
          userId: user.userId,
          role: membership.role,
        } satisfies PartyMemberJoinedEventData,
      };
      const opts = { excludeUserId: user.userId };
      const members = await this.memberRepo.findBy({ roomId, leftAt: IsNull() });
      await Promise.all(
        members
          .filter((m) => m.userId !== opts.excludeUserId)
          .map((m) =>
            // [REDIS] Mỗi member nhận event qua kênh riêng realtime:user:{userId}; không có kênh
            // theo phòng nên gateway không chứa logic. Publish lỗi chỉ log, không phá join đã
            // commit.
            publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
          ),
      );

      // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
      const publisher = this.redis;
      const logger = this.logger;
      const userId = m.userId;
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
    if (hostReconnected) {
      await this.publishToRoomMembers(roomId, {
        event: RealtimeEvents.PartyHostReconnected,
        data: { roomId } satisfies PartyHostReconnectedEventData,
      });
    }
    if (memberReconnected) {
      await this.publishToRoomMembers(roomId, {
        event: RealtimeEvents.PartyMemberReconnected,
        data: {
          roomId,
          userId: user.userId,
        } satisfies PartyMemberReconnectedEventData,
      });
    }

    // Step 7: Chốt URL LiveKit của phòng, ký token theo role hiện tại
    return {
      room,
      membership,
      // URL là snapshot của PHÒNG, không phải region người join: mọi người về đúng endpoint đã
      // chốt lúc tạo, kể cả khi khác region với host.
      livekitUrl: this.livekitUrlOf(room),
      token: await this.mintToken(roomId, user.userId, membership.role),
    };

    // [SERVICE] PartyRoomService.livekitUrlOf
    return (
      room.livekitUrl ?? this.config.getOrThrow('LIVEKIT_URL', { infer: true })
    );

    // [SERVICE] PartyRoomService.mintToken
    // userId = user.userId
    const role = membership.role;
    return this.livekit.mintJoinToken(
      partyRoomName(roomId),
      userId,
      this.config.getOrThrow('PARTY_TOKEN_TTL_SECONDS', { infer: true }),
      // Audience canPublish=false NGAY TỪ TOKEN: quyền publish nằm trong JWT (ký bằng API secret
      // LiveKit), nên SFU tự chặn việc tự unmute, không chỉ chặn ở UI và không tin client.
      { canPublish: role === PartyRole.Host || role === PartyRole.Speaker },
    );

    // [EXTERNAL] LivekitPartyRoomAdapter.mintJoinToken · party-room/clients/livekit-party-room.adapter.ts
    const roomName = `${PARTY_ROOM_NAME_PREFIX}${roomId}`;
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

    // Step 8: Controller trả phòng, membership, token và URL LiveKit
    return JoinPartyRoomDto.from(room, membership, token, livekitUrl);
  }
}

// [POSTGRES] 1752700000000-party-room-gift.ts · database/migrations/1752700000000-party-room-gift.ts
await queryRunner.query(
  `CREATE UNIQUE INDEX uq_party_members_active_user
     ON party_room_members(user_id) WHERE left_at IS NULL`,
);

//  POST /api/v1/party/rooms/:id/speaker-invite/accept
//  Audience đồng ý lời mời của host để lên speaker: đếm giới hạn dưới khóa phòng, commit role, rồi mở quyền publish trên LiveKit.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL)
//  Step 2: Trong transaction, khóa dòng phòng FOR UPDATE
//  Step 3: Kiểm tra lời mời đang chờ (đã là speaker thì trả luôn)
//  Step 4: Đếm speaker so với giới hạn, rồi đổi role thành speaker
//  Step 5: Sau commit, gọi LiveKit mở quyền publish cho người này
//  Step 6: LiveKit lỗi: hoàn role cũ bằng transaction mới
//  Step 7: Báo party.role.changed cho mọi member qua Redis
//  Step 8: Service trả membership, controller trả DTO

// [CONTROLLER] PartyRoomController.acceptSpeakerInvite · party-room/party-room.controller.ts
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
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id phòng từ URL)

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

  @Post(':id/speaker-invite/accept')
  @HttpCode(HttpStatus.OK)
  async acceptSpeakerInvite(
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
  ): Promise<PartyRoomMemberDto> {
    return PartyRoomMemberDto.from(
      await this.partyRoomService.acceptSpeakerInvite(user, id),
    );

    // [SERVICE] PartyRoomService.acceptSpeakerInvite · party-room/party-room.service.ts
    const roomId = id;
    const result = await this.dataSource.transaction(
      async (manager): Promise<PartyRoleTransitionResult> => {

        // Step 2: Trong transaction, khóa dòng phòng FOR UPDATE
        // [POSTGRES] Đếm speaker và đổi role dưới CÙNG lock phòng: 2 người accept đồng thời bị
        // xếp hàng, người sau thấy count đã tăng nên bị chặn.
        const room = await this.lockActiveRoom(manager, roomId);

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

        const target = await manager.findOneBy(PartyRoomMember, {
          roomId,
          userId: user.userId,
          leftAt: IsNull(),
        });
        if (!target) {
          throw new DomainException(
            PartyRoomErrors.NOT_A_MEMBER,
            'Bạn không ở trong phòng',
            HttpStatus.FORBIDDEN,
          );
        }

        // Step 3: Kiểm tra lời mời đang chờ (đã là speaker thì trả luôn)
        // Đã là speaker thì trả về luôn: retry không lỗi và không gọi SFU thêm lần nữa.
        if (target.role === PartyRole.Speaker)
          return { member: target, changed: false };
        if (
          target.role !== PartyRole.Audience ||
          !target.speakerInvitePending
        ) {
          // Không có lời mời đang chờ thì từ chối: lời mời là bước đồng ý bắt buộc, vì API đổi
          // role của host chỉ thu quyền (không cấp), nên client không bypass được.
          throw new DomainException(
            PartyRoomErrors.SPEAKER_INVITE_NOT_PENDING,
            'Không có lời mời speaker đang chờ',
            HttpStatus.CONFLICT,
          );
        }

        // Step 4: Đếm speaker so với giới hạn, rồi đổi role thành speaker
        const speakerCount = await manager.countBy(PartyRoomMember, {
          roomId,
          role: PartyRole.Speaker,
          leftAt: IsNull(),
        });
        // So với speakerLimit đã chụp lúc tạo phòng; host không chiếm slot vì chỉ đếm role
        // speaker.
        if (speakerCount >= room.speakerLimit) {
          throw new DomainException(
            PartyRoomErrors.SPEAKER_LIMIT_REACHED,
            `Phòng đã đủ ${room.speakerLimit} speaker`,
            HttpStatus.CONFLICT,
          );
        }
        target.role = PartyRole.Speaker;
        target.speakerInvitePending = false;
        const saved = await manager.save(target);
        return {
          member: saved,
          changed: true,
          previousRole: PartyRole.Audience,
          previousInvitePending: true,
        };
      },
    );
    if (!result.changed) return result.member;

    // Step 5: Sau commit, gọi LiveKit mở quyền publish cho người này
    await this.syncPublishPermissionAfterCommit({
      roomId,
      userId: user.userId,
      canPublish: true,
      expectedRole: PartyRole.Speaker,
      previousRole: result.previousRole,
      previousInvitePending: result.previousInvitePending,
    });

    // [SERVICE] PartyRoomService.syncPublishPermissionAfterCommit
    const input = {
      roomId,
      userId: user.userId,
      canPublish: true,
      expectedRole: PartyRole.Speaker,
      previousRole: result.previousRole,
      previousInvitePending: result.previousInvitePending,
    };
    try {
      // [EXTERNAL] Gọi SFU SAU commit, không giữ lock DB lúc chờ network: nếu gọi khi đang khóa
      // rồi DB rollback thì DB và SFU lệch nhau.
      await this.livekit.updateParticipantPublish(
        partyRoomName(input.roomId),
        input.userId,
        input.canPublish,
      );

      // [EXTERNAL] LivekitPartyRoomAdapter.updateParticipantPublish · party-room/clients/livekit-party-room.adapter.ts
      const roomName = `${PARTY_ROOM_NAME_PREFIX}${input.roomId}`;
      const identity = input.userId;
      const canPublish = input.canPublish;
      return this.client.updateParticipantPublish(roomName, identity, canPublish);

      // [EXTERNAL] LivekitSdkClient.updateParticipantPublish · common/livekit/livekit-sdk.client.ts
      try {
        await this.roomService.updateParticipant(roomName, identity, undefined, {
          canPublish,
          canSubscribe: true,
          canPublishData: true,
        });
        return 'updated';
      } catch (error) {
        // 404 = participant chưa nối SFU: coi như xong, vì không nối thì không publish được; lần
        // join sau token lấy role từ DB.

        // [EXTERNAL] LivekitSdkClient.isNotFound
        return (
          typeof error === 'object' &&
          error !== null &&
          'status' in error &&
          (error as { status: unknown }).status === 404
        );

        if (this.isNotFound(error)) return 'not_connected';
        throw error;
      }

      return;
    } catch (error) {
      let compensated = false;
      try {

        // Step 6: LiveKit lỗi: hoàn role cũ bằng transaction mới
        // [POSTGRES] SFU lỗi thì hoàn role cũ bằng 1 transaction mới (lock phòng lại), vì lệnh
        // SFU đã nằm ngoài transaction đầu.
        compensated = await this.dataSource.transaction(async (manager) => {
          await this.lockActiveRoom(manager, input.roomId);
          const current = await manager.findOneBy(PartyRoomMember, {
            roomId: input.roomId,
            userId: input.userId,
            leftAt: IsNull(),
          });
          // Chỉ hoàn khi role hiện tại vẫn là role vừa đặt; đã có chuyển đổi mới thắng thì không
          // đè lên.
          if (!current || current.role !== input.expectedRole) return false;
          current.role = input.previousRole;
          current.speakerInvitePending = input.previousInvitePending;
          await manager.save(current);
          return true;
        });
      } catch (compensationError) {
        this.logger.error(
          `Không thể rollback role sau khi LiveKit đổi quyền thất bại: ${
            compensationError instanceof Error
              ? compensationError.message
              : String(compensationError)
          }`,
        );
      }

      // [EXTERNAL] DB đã hoàn role thì mới trả grant cũ trên SFU, best-effort (lỗi chỉ log).
      if (compensated) {
        await this.livekit
          .updateParticipantPublish(
            partyRoomName(input.roomId),
            input.userId,
            input.previousRole !== PartyRole.Audience,
          )
          .catch((restoreError) =>
            this.logger.error(
              `Khôi phục grant LiveKit sau lỗi quyền thất bại: ${
                restoreError instanceof Error
                  ? restoreError.message
                  : String(restoreError)
              }`,
            ),
          );
      }

      this.logger.error(
        `Đổi quyền publish LiveKit thất bại: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw new ServiceUnavailableException(
        'PARTY_MEDIA_PERMISSION_UPDATE_FAILED',
      );
    }

    // Step 7: Báo party.role.changed cho mọi member qua Redis
    await this.publishRoleChanged(roomId, result.member);

    // [SERVICE] PartyRoomService.publishRoleChanged
    const member = result.member;
    await this.publishToRoomMembers(roomId, {
      event: RealtimeEvents.PartyRoleChanged,
      data: {
        roomId,
        userId: member.userId,
        role: member.role,
      } satisfies PartyRoleChangedEventData,
    });

    // [SERVICE] PartyRoomService.publishToRoomMembers
    const envelope = {
      event: RealtimeEvents.PartyRoleChanged,
      data: {
        roomId,
        userId: member.userId,
        role: member.role,
      } satisfies PartyRoleChangedEventData,
    };
    const opts = {};
    const members = await this.memberRepo.findBy({ roomId, leftAt: IsNull() });
    await Promise.all(
      members
        .filter((m) => m.userId !== opts.excludeUserId)
        .map((m) =>
          // [REDIS] Mỗi member nhận event qua kênh riêng realtime:user:{userId}; publish lỗi chỉ
          // log, role đã commit không bị ảnh hưởng.
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

    // Step 8: Service trả membership, controller trả DTO
    return result.member;
  }
}

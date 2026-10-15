//  POST /api/v1/matching/invites/:id/accept
//  Người được mời chấp nhận: tạo thẳng 2 ticket và 1 session đã confirmed, bỏ qua hàng đợi, idempotent theo (lời mời, vai trò).
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận accept của người được mời (user từ JWT, id lời mời từ URL)
//  Step 2: Precheck trong transaction riêng, commit xong mới throw
//  Step 3: Khóa dòng lời mời (FOR UPDATE), kiểm đúng người được mời
//  Step 4: Kiểm lại block/report, không ghép được thì ghi Declined
//  Step 5: Precheck thấy đã accept thì trả lại ticket và session cũ
//  Step 6: Mở transaction chính: tạo ticket, session, đổi lời mời
//  Step 7: Khóa lại dòng lời mời; replay giữa chừng thì trả session cũ
//  Step 8: Tạo 2 ticket matched, unique chặn user đang bận và ticket đôi
//  Step 9: Tạo session confirmed, chuyển 2 ticket sang confirmed
//  Step 10: Đổi lời mời sang accepted, lưu session_id
//  Step 11: Sau commit, phát match.confirmed cho cả 2 bên qua Redis
//  Step 12: Controller trả lời mời, session và inviteeTicketId

// [CONTROLLER] InviteController.accept · matching/controllers/invite.controller.ts
@Controller('matching/invites')
export class InviteController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // InviteService
  private readonly logger = new Logger(InviteService.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // InviteService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận accept của người được mời (user từ JWT, id lời mời từ
  // URL)

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

  @Post(':id/accept')
  @HttpCode(HttpStatus.OK)
  async accept(
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
  ): Promise<MatchInviteAcceptedDto> {
    const result = await this.inviteService.acceptInvite(user, id);

    // [SERVICE] InviteService.acceptInvite · matching/services/invite.service.ts
    const inviteId = id;

    // Step 2: Precheck trong transaction riêng, commit xong mới throw
    // Precheck chạy ở transaction RIÊNG và COMMIT xong mới throw: canPair fail thì lời mời phải
    // ghi Declined, nhưng throw trong CÙNG transaction sẽ rollback luôn phần Declined vừa ghi
    // (bắt được qua test thật).
    const precheck = await this.precheckAccept(user, inviteId);

    // [SERVICE] InviteService.precheckAccept
    return this.dataSource.transaction(async (manager) => {

      // Step 3: Khóa dòng lời mời (FOR UPDATE), kiểm đúng người được mời
      const invite = await this.lockInvite(manager, inviteId);

      // [SERVICE] InviteService.lockInvite
      const invite = await manager.findOne(MatchInvite, {
        where: { id: inviteId },
        // [POSTGRES] FOR UPDATE: mọi thao tác trên 1 lời mời (accept, decline, cancel) xếp hàng
        // qua đúng dòng này.
        lock: { mode: 'pessimistic_write' },
      });
      if (!invite) {
        throw new DomainException(
          MatchingErrors.INVITE_NOT_FOUND,
          'Không tìm thấy lời mời',
          HttpStatus.NOT_FOUND,
        );
      }
      return invite;

      if (invite.inviteeUserId !== user.userId) {
        throw new DomainException(
          MatchingErrors.INVITE_FORBIDDEN,
          'Lời mời không thuộc về bạn',
          HttpStatus.FORBIDDEN,
        );
      }

      if (invite.status === MatchInviteStatus.Accepted && invite.sessionId) {
        const session = await manager.findOneByOrFail(MatchSession, {
          id: invite.sessionId,
        });
        const inviteeTicket = await manager.findOneByOrFail(MatchTicket, {
          idempotencyKey: inviteAcceptIdempotencyKey(invite.id, 'invitee'),
        });

        // [SERVICE] inviteAcceptIdempotencyKey · matching/matching.constants.ts
        // inviteId = invite.id
        const role = 'invitee';
        return `matching:invite-accept:${inviteId}:${role}`;

        return {
          kind: 'already_accepted' as const,
          invite,
          session,
          inviteeTicketId: inviteeTicket.id,
        };
      }

      // Lazy-expire: sweeper có thể chưa kịp chạy, nên kiểm lại hạn TẠI THỜI ĐIỂM hành động.
      this.assertNotExpired(invite);

      // [SERVICE] InviteService.assertNotExpired
      if (
        invite.status === MatchInviteStatus.Pending &&
        invite.expiresAt.getTime() <= Date.now()
      ) {
        invite.status = MatchInviteStatus.Expired;
        throw new DomainException(
          MatchingErrors.INVITE_INVALID_TRANSITION,
          'Lời mời đã hết hạn',
          HttpStatus.CONFLICT,
        );
      }

      this.assertTransition(invite, MatchInviteStatus.Accepted);

      // [SERVICE] InviteService.assertTransition
      const to = MatchInviteStatus.Accepted;
      if (!MATCH_INVITE_TRANSITIONS[invite.status].includes(to)) {
        throw new DomainException(
          MatchingErrors.INVITE_INVALID_TRANSITION,
          `Không thể chuyển invite từ '${invite.status}' sang '${to}'`,
          HttpStatus.CONFLICT,
        );
      }

      // Step 4: Kiểm lại block/report, không ghép được thì ghi Declined
      // Kiểm lại block/report TẠI THỜI ĐIỂM accept, không chỉ lúc mời: block có thể phát sinh SAU
      // khi mời, TRƯỚC khi accept.
      const canPair = await this.interactionPolicy.canPair(
        invite.inviterUserId,
        invite.inviteeUserId,
      );
      if (!canPair) {
        // Không ghép được thì lời mời tự chuyển declined: ghi và COMMIT ngay ở đây, trả "blocked"
        // để tầng gọi throw NGOÀI transaction.
        invite.status = MatchInviteStatus.Declined;
        invite.respondedAt = new Date();
        await manager.save(invite);
        return { kind: 'blocked' as const };
      }
      return { kind: 'proceed' as const };
    });

    // Step 5: Precheck thấy đã accept thì trả lại ticket và session cũ
    // Accept lặp lại (retry mạng) trả lại đúng ticket và session cũ, không lỗi, không tạo đôi.
    if (precheck.kind === 'already_accepted') {
      return {
        invite: precheck.invite,
        session: precheck.session,
        inviteeTicketId: precheck.inviteeTicketId,
      };
    }
    if (precheck.kind === 'blocked') {
      throw new DomainException(
        MatchingErrors.INVITE_TARGET_UNAVAILABLE,
        'Không thể ghép với người này',
        HttpStatus.FORBIDDEN,
      );
    }

    let matchedPair: Array<{
      userId: string;
      ticketId: string;
      sessionId: string;
    }> | null = null;

    // Step 6: Mở transaction chính: tạo ticket, session, đổi lời mời
    // [POSTGRES] Tạo ticket, session và đổi lời mời sang accepted trong 1 transaction: lỗi ở đâu
    // cũng rollback hết, lời mời còn pending để thử lại.
    const result = await this.dataSource.transaction(async (manager) => {

      // Step 7: Khóa lại dòng lời mời; replay giữa chừng thì trả session cũ
      // Khóa dòng lời mời (FOR UPDATE) trước khi đổi gì: 2 accept đồng thời xếp hàng, bên sau
      // thấy lời mời đã accepted và trả sớm ở nhánh dưới, nên không bao giờ insert ticket lần 2.
      const invite = await this.lockInvite(manager, inviteId);
      if (invite.inviteeUserId !== user.userId) {
        throw new DomainException(
          MatchingErrors.INVITE_FORBIDDEN,
          'Lời mời không thuộc về bạn',
          HttpStatus.FORBIDDEN,
        );
      }

      // Replay xảy ra giữa lúc precheck và đây: trả lại session cũ.
      if (invite.status === MatchInviteStatus.Accepted && invite.sessionId) {
        const session = await manager.findOneByOrFail(MatchSession, {
          id: invite.sessionId,
        });
        const inviteeTicket = await manager.findOneByOrFail(MatchTicket, {
          idempotencyKey: inviteAcceptIdempotencyKey(invite.id, 'invitee'),
        });
        return { invite, session, inviteeTicketId: inviteeTicket.id };
      }

      this.assertNotExpired(invite);
      this.assertTransition(invite, MatchInviteStatus.Accepted);

      // Chỉ kiểm 2 bên còn Active, KHÔNG kiểm gender preference như auto-match: người được mời đã
      // chủ động chấp nhận đúng người này (consent trực tiếp).
      const users = await manager.find(User, {
        where: { id: In([invite.inviterUserId, invite.inviteeUserId]) },
      });
      const userById = new Map(users.map((u) => [u.id, u]));
      const inviter = userById.get(invite.inviterUserId);
      const invitee = userById.get(invite.inviteeUserId);
      if (
        !inviter ||
        !invitee ||
        inviter.status !== UserStatus.Active ||
        invitee.status !== UserStatus.Active
      ) {
        throw new DomainException(
          MatchingErrors.USER_BANNED,
          'Một trong hai bên không còn khả dụng để ghép',
          HttpStatus.CONFLICT,
        );
      }

      // Step 8: Tạo 2 ticket matched, unique chặn user đang bận và ticket đôi
      const [ticketInviter, ticketInvitee] = await this.createMatchedTickets(
        manager,
        invite,
        inviter,
        invitee,
      );

      // [SERVICE] InviteService.createMatchedTickets
      try {
        // Insert cả 2 ticket trong CÙNG transaction: vi phạm uq_match_tickets_active_user (1
        // trong 2 bên đang bận queue hoặc session khác) rollback tất cả, lời mời giữ nguyên
        // pending.
        const [ticketInviter, ticketInvitee] = await manager.save([
          this.draftMatchedTicket(manager, invite, inviter, 'inviter'),
          this.draftMatchedTicket(manager, invite, invitee, 'invitee'),
        ]);

        // [SERVICE] InviteService.draftMatchedTicket
        const profile = inviter;
        // role = 'inviter'
        const bandSize = this.config.getOrThrow('MATCHING_AGE_BAND_SIZE', {
          infer: true,
        });
        const perPoint = this.config.getOrThrow(
          'MATCHING_TRUST_PENALTY_MS_PER_POINT',
          { infer: true },
        );
        const maxMs = this.config.getOrThrow('MATCHING_TRUST_PENALTY_MAX_MS', {
          infer: true,
        });
        return manager.create(MatchTicket, {
          userId: profile.id,
          matchType: invite.matchType,
          region: profile.region ?? 'GLOBAL',
          ageBand: ageBandOf(profile.birthDate, bandSize),
          // Tạo ở trạng thái matched để rơi vào phạm vi unique index (queued, matched): user đang
          // bận thì insert lỗi; chỉ sau khi có session mới chuyển sang confirmed.
          status: MatchTicketStatus.Matched,
          enqueuedAt: new Date(),
          priorityBoostMs: 0,
          trustPenaltyMs: trustPenaltyMsOf(profile.trustScore, perPoint, maxMs),
          sessionId: null,
          // Key theo (lời mời, vai trò): accept lặp lại ra đúng 2 key cũ, unique DB chặn tạo
          // ticket đôi.
          idempotencyKey: inviteAcceptIdempotencyKey(invite.id, role),
        });

        // [SERVICE] ageBandOf · matching/matching.constants.ts
        const birthDate = profile.birthDate;
        if (!birthDate) return UNKNOWN_AGE_BAND;
        const birth = new Date(birthDate);
        if (Number.isNaN(birth.getTime())) return UNKNOWN_AGE_BAND;
        const now = new Date();
        let age = now.getFullYear() - birth.getFullYear();
        const beforeBirthday =
          now.getMonth() < birth.getMonth() ||
          (now.getMonth() === birth.getMonth() && now.getDate() < birth.getDate());
        if (beforeBirthday) age -= 1;
        if (age < 0) return UNKNOWN_AGE_BAND;
        return Math.floor(age / bandSize);

        // [SERVICE] trustPenaltyMsOf
        const trustScore = profile.trustScore;
        return Math.min(maxMs, Math.max(0, 100 - trustScore) * perPoint);

        return [ticketInviter, ticketInvitee];
      } catch (err) {

        // [SERVICE] violatedConstraint · database/postgres-errors.ts
        const constraint = 'uq_match_tickets_active_user';
        const e = err as {
          driverError?: { constraint?: string };
          constraint?: string;
          message?: string;
        };
        const name = e.driverError?.constraint ?? e.constraint;
        return name === constraint || (e.message?.includes(constraint) ?? false);

        if (violatedConstraint(err, 'uq_match_tickets_active_user')) {
          throw new DomainException(
            MatchingErrors.INVITE_ACCEPT_USER_BUSY,
            'Một trong hai bên đang ở trong hàng đợi/phiên ghép khác',
            HttpStatus.CONFLICT,
          );
        }
        // Unique ở idempotency_key = replay accept cũ: đọc lại 2 ticket đã tạo. Nhánh này thực tế
        // không chạy tới (lời mời đã khóa nên accept thứ 2 trả sớm); nếu bỏ khóa thì phải tách
        // sang transaction riêng, vì Postgres hủy cả transaction khi 1 câu lỗi.

        // [SERVICE] isUniqueViolation · database/postgres-errors.ts
        const e = err as { code?: string; driverError?: { code?: string } };
        return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

        if (!isUniqueViolation(err)) throw err;

        const existing = await manager.find(MatchTicket, {
          where: {
            idempotencyKey: In([
              inviteAcceptIdempotencyKey(invite.id, 'inviter'),
              inviteAcceptIdempotencyKey(invite.id, 'invitee'),
            ]),
          },
        });
        const byKey = new Map(existing.map((t) => [t.idempotencyKey, t]));
        const ticketInviter = byKey.get(
          inviteAcceptIdempotencyKey(invite.id, 'inviter'),
        );
        const ticketInvitee = byKey.get(
          inviteAcceptIdempotencyKey(invite.id, 'invitee'),
        );
        if (!ticketInviter || !ticketInvitee) throw err;
        return [ticketInviter, ticketInvitee];
      }

      // Step 9: Tạo session confirmed, chuyển 2 ticket sang confirmed
      const confirmedAt = new Date();
      const session = await manager.save(
        manager.create(MatchSession, {
          matchType: invite.matchType,
          userAId: invite.inviterUserId,
          userBId: invite.inviteeUserId,
          ticketAId: ticketInviter.id,
          ticketBId: ticketInvitee.id,
          status: MatchSessionStatus.Confirmed,
          confirmedAAt: confirmedAt,
          confirmedBAt: confirmedAt,
        }),
      );
      ticketInviter.sessionId = session.id;
      ticketInvitee.sessionId = session.id;
      ticketInviter.status = MatchTicketStatus.Confirmed;
      ticketInvitee.status = MatchTicketStatus.Confirmed;
      await manager.save([ticketInviter, ticketInvitee]);

      // Step 10: Đổi lời mời sang accepted, lưu session_id
      invite.status = MatchInviteStatus.Accepted;
      invite.respondedAt = new Date();
      invite.sessionId = session.id;
      await manager.save(invite);

      matchedPair = [
        {
          userId: inviter.id,
          ticketId: ticketInviter.id,
          sessionId: session.id,
        },
        {
          userId: invitee.id,
          ticketId: ticketInvitee.id,
          sessionId: session.id,
        },
      ];
      return { invite, session, inviteeTicketId: ticketInvitee.id };
    });

    // Step 11: Sau commit, phát match.confirmed cho cả 2 bên qua Redis
    // [REDIS] Bắn SAU commit cùng event match.confirmed mà auto-match dùng; 2 bên refetch rồi vào
    // phòng ngay.
    if (matchedPair) {
      const pair = matchedPair as Array<{
        userId: string;
        ticketId: string;
        sessionId: string;
      }>;
      await Promise.all(
        pair.map(({ userId, ticketId, sessionId }) => {
          const envelope: RealtimeEnvelope<MatchConfirmedEventData> = {
            event: RealtimeEvents.MatchConfirmed,
            data: { ticketId, sessionId },
          };
          return publishRealtimeEvent(
            this.realtime,
            this.logger,
            userId,
            envelope,
          );

          // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
          const publisher = this.realtime;
          const logger = this.logger;
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
    }
    return result;

    // Step 12: Controller trả lời mời, session và inviteeTicketId
    return MatchInviteAcceptedDto.from(
      result,
      await this.userService.getByIdOrThrow(result.invite.inviterUserId),
    );

    // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
    // id = result.invite.inviterUserId
    const user = await this.userRepo.findOneBy({ id });
    if (!user) {
      throw new DomainException(
        UserErrors.PROFILE_NOT_FOUND,
        'Không tìm thấy user',
        HttpStatus.NOT_FOUND,
      );
    }
    return user;
  }
}

// [SERVICE] match-invite.entity.ts · matching/entities/match-invite.entity.ts
// Bảng chuyển trạng thái duy nhất: mọi thay đổi phải đi qua đây, không tin client gửi trạng thái
// đích. Chỉ pending mới đi tiếp được.
export const MATCH_INVITE_TRANSITIONS: Readonly<
  Record<MatchInviteStatus, readonly MatchInviteStatus[]>
> = {
  [MatchInviteStatus.Pending]: [
    MatchInviteStatus.Accepted,
    MatchInviteStatus.Declined,
    MatchInviteStatus.Expired,
    MatchInviteStatus.Cancelled,
  ],
  [MatchInviteStatus.Accepted]: [],
  [MatchInviteStatus.Declined]: [],
  [MatchInviteStatus.Expired]: [],
  [MatchInviteStatus.Cancelled]: [],
};

// [POSTGRES] CREATE UNIQUE INDEX uq_match_tickets_active_user · database/migrations/1752200000000-matching-core.ts
await queryRunner.query(`
  CREATE UNIQUE INDEX uq_match_tickets_active_user
    ON match_tickets(user_id)
    WHERE status IN ('queued','matched')
`);

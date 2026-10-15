//  POST /api/v1/auth/refresh
//  Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request: CSRF double-submit, refresh token đọc từ cookie
//  Step 2: Tìm token theo băm SHA-256, kiểm chưa thu hồi và còn hạn
//  Step 3: Sinh cặp token mới (chưa lưu vào DB)
//  Step 4: Giao adapter xoay token trong 1 transaction
//  Step 5: Đọc lại token cũ và khóa dòng (FOR UPDATE)
//  Step 6: Token đã xoay mà bị dùng lại thì thu hồi cả family
//  Step 7: Đánh dấu token cũ đã xoay (UPDATE có điều kiện)
//  Step 8: Ghi token mới cùng family, cùng transaction
//  Step 9: Kết quả invalid hoặc reused thì từ chối với 401
//  Step 10: Kiểm lại trạng thái user, bị khóa thì thu hồi token vừa cấp
//  Step 11: Đặt refresh token mới vào cookie httpOnly, không trả trong body
//  Step 12: Trả access token và csrfToken trong body

// [CONTROLLER] AuthController.refresh · auth/auth.controller.ts
@Public()
@Controller('auth')
export class AuthController {
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
    private readonly config: ConfigService<CoreApiEnv, true>,
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // TypeOrmRefreshSessionAdapter
    @InjectDataSource() private readonly dataSource: DataSource,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request: CSRF double-submit, refresh token đọc từ
  // cookie

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

  // [CONTROLLER] @Public · auth/auth.controller.ts
  @Public()

  // [SERVICE] Public · common/decorators/public.decorator.ts
  return SetMetadata(IS_PUBLIC_KEY, true);

  // [CONTROLLER] Public
  export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);

  @Post('refresh')
  @HttpCode(HttpStatus.OK)
  // CSRF double-submit: so cookie csrf_token với header X-CSRF-Token (giá trị client nhận qua
  // body lúc đăng nhập), lệch hoặc thiếu thì 401. Cần vì cookie refresh tự gắn theo request.
  @UseGuards(CsrfGuard)

  // [CONTROLLER] @UseGuards
  @UseGuards(CsrfGuard)

  // [CONTROLLER] CsrfGuard.canActivate · common/guards/csrf.guard.ts
  const req = context.switchToHttp().getRequest<Request>();
  const cookieValue = (req.cookies as Record<string, string> | undefined)?.[
    CSRF_COOKIE_NAME
  ];
  const headerValue = req.headers[CSRF_HEADER_NAME];

  // [SERVICE] isValidCsrfToken · common/csrf/csrf-token.ts
  const header = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  if (
    cookieValue === undefined ||
    cookieValue === '' ||
    header === undefined ||
    header === ''
  ) {
    return false;
  }
  const cookieBuf = Buffer.from(cookieValue);
  const headerBuf = Buffer.from(header);
  if (cookieBuf.length !== headerBuf.length) return false;
  return timingSafeEqual(cookieBuf, headerBuf);

  if (!isValidCsrfToken(cookieValue, headerValue)) {
    throw new DomainException(
      CommonErrors.CSRF_TOKEN_INVALID,
      'CSRF token không hợp lệ hoặc thiếu',
      HttpStatus.UNAUTHORIZED,
    );
  }
  return true;

  async refresh(
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ): Promise<AuthTokensDto> {
    const refreshToken = extractRefreshTokenCookie(req);

    // [SERVICE] extractRefreshTokenCookie · auth/auth.cookies.ts
    const value = (req.cookies as Record<string, string> | undefined)?.[
      REFRESH_TOKEN_COOKIE_NAME
    ];
    if (typeof value !== 'string' || value === '') {
      throw new DomainException(
        AuthErrors.REFRESH_TOKEN_INVALID,
        'Thiếu refresh token cookie',
        HttpStatus.UNAUTHORIZED,
      );
    }
    return value;

    return this.finalizeSession(
      await this.authService.refresh(refreshToken),
      res,
    );

    // [SERVICE] AuthService.refresh · auth/auth.service.ts
    const { userId, tokens } = await this.tokenService.rotate(refreshToken);

    // [SERVICE] TokenService.rotate · auth/services/token.service.ts
    const refreshPlain = refreshToken;

    // Step 2: Tìm token theo băm SHA-256, kiểm chưa thu hồi và còn hạn
    const session = await this.refreshSessions.findByTokenHash(
      this.hash(refreshPlain),
    );

    // [SERVICE] TokenService.hash
    // value = refreshPlain
    // SHA-256 là đủ vì token ngẫu nhiên 48 byte (không phải mật khẩu), không cần bcrypt.
    return createHash('sha256').update(value).digest('hex');

    // [POSTGRES] TypeOrmRefreshSessionAdapter.findByTokenHash · auth/repositories/typeorm-refresh-session.adapter.ts
    const tokenHash = this.hash(refreshPlain);
    const session = await this.dataSource
      .getRepository(RefreshToken)
      .findOneBy({ tokenHash });
    if (!session) return null;
    return this.toRecord(session);

    // [SERVICE] TypeOrmRefreshSessionAdapter.toRecord
    return {
      id: session.id,
      userId: session.userId,
      familyId: session.familyId,
      expiresAt: session.expiresAt,
      revokedAt: session.revokedAt,
      rotatedAt: session.rotatedAt,
    };

    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      throw this.invalidRefreshToken();

      // [SERVICE] TokenService.invalidRefreshToken
      return new DomainException(
        AuthErrors.REFRESH_TOKEN_INVALID,
        'Refresh token không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }

    const ttlDays = this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', {
      infer: true,
    });
    const user = await this.userService.getByIdOrThrow(session.userId);

    // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
    const id = session.userId;
    const user = await this.userRepo.findOneBy({ id });
    if (!user) {
      throw new DomainException(
        UserErrors.PROFILE_NOT_FOUND,
        'Không tìm thấy user',
        HttpStatus.NOT_FOUND,
      );
    }
    return user;

    // Step 3: Sinh cặp token mới (chưa lưu vào DB)
    // Chỉ sinh token (chưa lưu): việc lưu token mới nằm TRONG transaction xoay của adapter, cùng
    // với đánh dấu token cũ.
    const tokens = await this.issueTokens(user.id, user.isGuest, user.role);

    // [SERVICE] TokenService.issueTokens
    // userId = user.id
    const isGuest = user.isGuest;
    const role = user.role;
    const expiresIn = this.config.getOrThrow('JWT_ACCESS_TTL_SECONDS', {
      infer: true,
    });
    const accessToken = await this.jwtService.signAsync(
      {
        sub: userId,
        isGuest,
        role,
        jti: randomUUID(),
      } satisfies AccessTokenPayload,
      { expiresIn },
    );
    return {
      accessToken,
      refreshToken: randomBytes(REFRESH_TOKEN_BYTES).toString('base64url'),
      expiresIn,
    };

    // Step 4: Giao adapter xoay token trong 1 transaction
    const result = await this.refreshSessions.rotate({
      tokenId: session.id,
      replacement: {
        tokenHash: this.hash(tokens.refreshToken),
        expiresAt: this.refreshExpiresAt(ttlDays),
      },
    });

    // [SERVICE] TokenService.refreshExpiresAt
    return new Date(Date.now() + ttlDays * 24 * 3600 * 1000);

    // [POSTGRES] TypeOrmRefreshSessionAdapter.rotate · auth/repositories/typeorm-refresh-session.adapter.ts
    const input = {
      tokenId: session.id,
      replacement: {
        tokenHash: this.hash(tokens.refreshToken),
        expiresAt: this.refreshExpiresAt(ttlDays),
      },
    };
    return this.dataSource.transaction(async (manager) => {

      // Step 5: Đọc lại token cũ và khóa dòng (FOR UPDATE)
      const session = await manager.findOne(RefreshToken, {
        where: { id: input.tokenId },
        // Khóa dòng token (FOR UPDATE): 2 request song song cùng 1 token xếp hàng, chỉ 1 request
        // đánh dấu được rotated.
        lock: { mode: 'pessimistic_write' },
      });
      if (!session || session.revokedAt || session.expiresAt <= new Date()) {
        return 'invalid';
      }

      // Step 6: Token đã xoay mà bị dùng lại thì thu hồi cả family
      // Token đã xoay mà còn được dùng lại: thu hồi cả family (mọi token chưa revoke cùng
      // familyId).
      if (session.rotatedAt) {
        await manager.update(
          RefreshToken,
          { familyId: session.familyId, revokedAt: IsNull() },
          { revokedAt: new Date() },
        );
        return 'reused';
      }

      // Step 7: Đánh dấu token cũ đã xoay (UPDATE có điều kiện)
      // Đánh dấu rotated bằng UPDATE có điều kiện rotated_at IS NULL; 0 dòng bị ảnh hưởng nghĩa
      // là request khác đã xoay trước, coi như dùng lại.
      const marked = await manager.update(
        RefreshToken,
        { id: session.id, rotatedAt: IsNull(), revokedAt: IsNull() },
        { rotatedAt: new Date() },
      );
      if (!marked.affected) {
        await manager.update(
          RefreshToken,
          { familyId: session.familyId, revokedAt: IsNull() },
          { revokedAt: new Date() },
        );
        return 'reused';
      }

      // Step 8: Ghi token mới cùng family, cùng transaction
      // Token mới giữ nguyên familyId và ghi cùng transaction với việc đánh dấu token cũ: không
      // có trạng thái cũ đã xoay mà chưa có token mới.
      await manager.insert(RefreshToken, {
        userId: session.userId,
        tokenHash: input.replacement.tokenHash,
        familyId: session.familyId,
        expiresAt: input.replacement.expiresAt,
      });
      return 'rotated';
    });

    // Step 9: Kết quả invalid hoặc reused thì từ chối với 401
    if (result === 'invalid') {
      throw this.invalidRefreshToken();
    }
    // Token đã xoay mà bị dùng lại: adapter đã thu hồi cả family (mọi token cùng lần đăng nhập),
    // nên cả bên dùng lại lẫn người thật phải đăng nhập lại.
    if (result === 'reused') {
      throw new DomainException(
        AuthErrors.REFRESH_TOKEN_REUSED,
        'Refresh token đã bị dùng lại — phiên bị thu hồi',
        HttpStatus.UNAUTHORIZED,
      );
    }

    return { userId: session.userId, tokens };

    // Step 10: Kiểm lại trạng thái user, bị khóa thì thu hồi token vừa cấp
    // Xác minh lại trạng thái ĐÚNG THỜI ĐIỂM hành động: user bị khóa giữa 2 lần refresh thì không
    // được cấp phiên mới.
    const user = await this.userService.getByIdOrThrow(userId);
    try {
      this.assertActive(user);

      // [SERVICE] AuthService.assertActive
      if (user.status !== UserStatus.Active) {
        throw new DomainException(
          AuthErrors.USER_BANNED,
          'Tài khoản đã bị khoá',
          HttpStatus.FORBIDDEN,
        );
      }
      return user;
    } catch (err) {
      // Thu hồi luôn token mới vừa cấp, vì bước xoay đã lưu nó vào DB.
      await this.tokenService.revoke(tokens.refreshToken);

      // [SERVICE] TokenService.revoke · auth/services/token.service.ts
      // refreshPlain = tokens.refreshToken
      await this.refreshSessions.revoke(this.hash(refreshPlain));

      // [SERVICE] TypeOrmRefreshSessionAdapter.revoke · auth/repositories/typeorm-refresh-session.adapter.ts
      // tokenHash = this.hash(refreshPlain)
      await this.dataSource
        .getRepository(RefreshToken)
        .update({ tokenHash, revokedAt: IsNull() }, { revokedAt: new Date() });

      throw err;
    }
    return {
      ...tokens,
      csrfToken: generateCsrfToken(),
      userId: user.id,
      isGuest: user.isGuest,
    };

    // [SERVICE] generateCsrfToken · common/csrf/csrf-token.ts
    return randomBytes(CSRF_TOKEN_BYTES).toString('base64url');

    // [CONTROLLER] AuthController.finalizeSession · auth/auth.controller.ts
    // session = await this.authService.refresh(refreshToken)

    // Step 11: Đặt refresh token mới vào cookie httpOnly, không trả trong body
    // Refresh token chỉ đi qua cookie httpOnly rồi bị bóc khỏi body trả về: client (và XSS) không
    // bao giờ thấy giá trị này.
    setAuthCookies(res, {
      refreshToken: session.refreshToken,
      csrfToken: session.csrfToken,
      isProduction:
        this.config.get('NODE_ENV', { infer: true }) === 'production',
      crossOriginDev:
        this.config.get('AUTH_CROSS_ORIGIN_DEV', { infer: true }) === true,
      productionSameSite: this.config.getOrThrow('AUTH_COOKIE_SAME_SITE', {
        infer: true,
      }),
      ttlDays: this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', { infer: true }),
    });

    // [CONTROLLER] setAuthCookies · auth/auth.cookies.ts
    // input = đối số 2 của lời gọi ở trên
    const options = cookieOptions(
      input.isProduction,
      input.ttlDays,
      input.crossOriginDev ?? false,
      input.productionSameSite ?? 'strict',
    );

    // [CONTROLLER] cookieOptions
    const isProduction = input.isProduction;
    // ttlDays = input.ttlDays
    const crossOriginDev = input.crossOriginDev ?? false;
    const productionSameSite = input.productionSameSite ?? 'strict';
    const sameSite = isProduction
      ? productionSameSite
      : crossOriginDev
        ? 'none'
        : 'strict';
    return {
      httpOnly: true,
      secure: isProduction || sameSite === 'none',
      sameSite,
      path: REFRESH_TOKEN_COOKIE_PATH,
      maxAge: ttlDays * 24 * 3600 * 1000,
    };

    res.cookie(REFRESH_TOKEN_COOKIE_NAME, input.refreshToken, options);
    res.cookie(CSRF_COOKIE_NAME, input.csrfToken, options);

    // Step 12: Trả access token và csrfToken trong body
    return {
      accessToken: session.accessToken,
      csrfToken: session.csrfToken,
      expiresIn: session.expiresIn,
      userId: session.userId,
      isGuest: session.isGuest,
      guestDeviceToken: session.guestDeviceToken,
    };
  }
}

// [POSTGRES] CREATE TABLE refresh_tokens · database/migrations/1751900000000-init-auth-user.ts
await queryRunner.query(`
  CREATE TABLE refresh_tokens (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       uuid         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash    char(64)     NOT NULL,
    family_id     uuid         NOT NULL,
    expires_at    timestamptz  NOT NULL,
    revoked_at    timestamptz  NULL,
    rotated_at    timestamptz  NULL,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_refresh_tokens_token_hash UNIQUE (token_hash)
  )
`);

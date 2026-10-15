//  POST /api/v1/auth/otp/verify
//  Kiểm mã OTP có giới hạn số lần thử và chỉ dùng một lần, rồi tìm hoặc tạo tài khoản và cấp phiên đăng nhập.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận số điện thoại và mã OTP (giới hạn 10 lần/phút)
//  Step 2: Lấy mã OTP chưa dùng mới nhất của số này, kiểm còn hạn
//  Step 3: Cộng 1 lượt thử cho mã này, vượt giới hạn thì 429
//  Step 4: So băm HMAC của mã nhập với băm lưu trong DB
//  Step 5: Đánh dấu mã đã dùng (consumed_at), không dùng lại được
//  Step 6: Tìm tài khoản theo số điện thoại, chưa có thì tạo
//  Step 7: Chưa có tài khoản: tạo user và identity trong 1 transaction
//  Step 8: 2 request đầu tiên chạy song song: bên dính unique đọc lại identity
//  Step 9: Ký access token JWT
//  Step 10: Sinh refresh token ngẫu nhiên, DB chỉ lưu băm SHA-256
//  Step 11: Đặt refresh token vào cookie httpOnly, bóc khỏi body trả về
//  Step 12: Trả access token và csrfToken trong body

// [CONTROLLER] AuthController.verifyOtp · auth/auth.controller.ts
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
    // AuthService
    @InjectRepository(AuthIdentity)
    private readonly identityRepo: Repository<AuthIdentity>,
    @InjectDataSource() private readonly dataSource: DataSource,
    // OtpService
    @InjectRepository(PhoneOtp) private readonly otpRepo: Repository<PhoneOtp>,
    // UserService
    @InjectRepository(User) private readonly userRepo: Repository<User>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận số điện thoại và mã OTP (giới hạn 10 lần/phút)

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

  @Post('otp/verify')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 10, ttl: minutes(1) } })
  async verifyOtp(
    @Body() dto: VerifyOtpDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<AuthTokensDto> {
    return this.finalizeSession(
      await this.authService.verifyOtpAndLogin(dto.phone, dto.code),
      res,
    );

    // [SERVICE] AuthService.verifyOtpAndLogin · auth/auth.service.ts
    const phone = dto.phone;
    // code = dto.code
    this.assertPhoneOtpEnabled();

    // [SERVICE] AuthService.assertPhoneOtpEnabled
    if (this.config.getOrThrow('AUTH_PHONE_OTP_ENABLED', { infer: true }))
      return;
    throw new DomainException(
      AuthErrors.PHONE_OTP_DISABLED,
      'Đăng nhập bằng số điện thoại chưa khả dụng trên môi trường này',
      HttpStatus.FORBIDDEN,
    );

    await this.otpService.verifyOtp(phone, code);

    // [SERVICE] OtpService.verifyOtp · auth/services/otp.service.ts
    // Step 2: Lấy mã OTP chưa dùng mới nhất của số này, kiểm còn hạn
    // Chỉ xét mã chưa dùng mới nhất của số này: lúc yêu cầu mã mới, mã cũ đã bị vô hiệu nên chỉ
    // mã mới nhất có giá trị.
    const otp = await this.otpRepo.findOne({
      where: { phone, consumedAt: IsNull() },
      order: { createdAt: 'DESC' },
    });
    if (!otp || otp.expiresAt < new Date()) {
      throw new DomainException(
        AuthErrors.OTP_EXPIRED,
        'OTP đã hết hạn hoặc chưa được yêu cầu',
        HttpStatus.BAD_REQUEST,
      );
    }

    const maxAttempts = this.config.getOrThrow('AUTH_OTP_MAX_ATTEMPTS', {
      infer: true,
    });

    // Step 3: Cộng 1 lượt thử cho mã này, vượt giới hạn thì 429
    // [POSTGRES] Đếm lần thử bằng UPDATE có điều kiện attempt_count < max: nhiều request song
    // song cũng không vượt được giới hạn. Đếm TRƯỚC khi so mã nên đoán sai cũng tốn lượt.
    const counted = await this.otpRepo
      .createQueryBuilder()
      .update()
      .set({ attemptCount: () => 'attempt_count + 1' })
      .where('id = :id AND attempt_count < :max AND consumed_at IS NULL', {
        id: otp.id,
        max: maxAttempts,
      })
      .execute();
    if (!counted.affected) {
      throw new DomainException(
        AuthErrors.OTP_TOO_MANY_ATTEMPTS,
        'Nhập sai quá số lần cho phép, yêu cầu mã mới',
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }

    // Step 4: So băm HMAC của mã nhập với băm lưu trong DB
    // So băm HMAC bằng timingSafeEqual, thời gian so không lộ vị trí byte đầu tiên khác nhau; DB
    // chỉ chứa băm, không có mã gốc.
    const expected = Buffer.from(otp.codeHash, 'hex');
    const actual = Buffer.from(this.hash(phone, code), 'hex');

    // [SERVICE] OtpService.hash
    // HMAC-SHA256 với pepper của server: mã chỉ có 6 số nên băm thường sẽ bị dò ra ngay, có
    // pepper thì DB lộ cũng không suy ra được mã.
    const pepper = this.config.getOrThrow('AUTH_OTP_PEPPER', { infer: true });
    return createHmac('sha256', pepper)
      .update(`${phone}:${code}`)
      .digest('hex');

    if (
      expected.length !== actual.length ||
      !timingSafeEqual(expected, actual)
    ) {
      throw new DomainException(
        AuthErrors.OTP_INVALID,
        'Mã OTP không đúng',
        HttpStatus.BAD_REQUEST,
      );
    }

    // Step 5: Đánh dấu mã đã dùng (consumed_at), không dùng lại được
    // [POSTGRES] Tiêu thụ mã bằng UPDATE có điều kiện consumed_at IS NULL: 1 mã chỉ dùng được
    // đúng 1 lần, request song song thua sẽ bị báo mã sai.
    const consumed = await this.otpRepo.update(
      { id: otp.id, consumedAt: IsNull() },
      { consumedAt: new Date() },
    );
    if (!consumed.affected) {
      throw new DomainException(
        AuthErrors.OTP_INVALID,
        'Mã OTP không đúng',
        HttpStatus.BAD_REQUEST,
      );
    }

    // Step 6: Tìm tài khoản theo số điện thoại, chưa có thì tạo
    const user = await this.findOrCreateUser(AuthProvider.Phone, phone, {
      isGuest: false,
      nicknamePrefix: 'User',
    });

    // [SERVICE] AuthService.findOrCreateUser
    const provider = AuthProvider.Phone;
    const providerUid = phone;
    const opts = {
      isGuest: false,
      nicknamePrefix: 'User',
    };
    const existing = await this.identityRepo.findOneBy({
      provider,
      providerUid,
    });
    if (existing) {
      const user = this.assertActive(
        await this.userService.getByIdOrThrow(existing.userId),
      );

      // [SERVICE] UserService.getByIdOrThrow · user/user.service.ts
      const id = existing.userId;
      const user = await this.userRepo.findOneBy({ id });
      if (!user) {
        throw new DomainException(
          UserErrors.PROFILE_NOT_FOUND,
          'Không tìm thấy user',
          HttpStatus.NOT_FOUND,
        );
      }
      return user;

      // [SERVICE] AuthService.assertActive · auth/auth.service.ts
      // user = await this.userService.getByIdOrThrow(existing.userId)
      if (user.status !== UserStatus.Active) {
        throw new DomainException(
          AuthErrors.USER_BANNED,
          'Tài khoản đã bị khoá',
          HttpStatus.FORBIDDEN,
        );
      }
      return user;

      this.assertGuestIdentityStillActive(provider, user);

      // [SERVICE] AuthService.assertGuestIdentityStillActive
      if (provider !== AuthProvider.Guest || user.isGuest) return user;
      throw new DomainException(
        AuthErrors.GUEST_IDENTITY_RETIRED,
        'Guest credential đã bị thu hồi sau khi nâng cấp tài khoản',
        HttpStatus.UNAUTHORIZED,
      );

      return user;
    }

    try {

      // Step 7: Chưa có tài khoản: tạo user và identity trong 1 transaction
      // [POSTGRES] Tạo user và identity trong 1 transaction để không có user mồ côi thiếu
      // identity.
      return await this.dataSource.transaction(async (manager) => {
        const user = await this.userService.createWithManager(manager, {
          nickname: `${opts.nicknamePrefix}-${randomInt(100000, 999999)}`,
          isGuest: opts.isGuest,
        });

        // [SERVICE] UserService.createWithManager · user/user.service.ts
        const input = {
          nickname: `${opts.nicknamePrefix}-${randomInt(100000, 999999)}`,
          isGuest: opts.isGuest,
        };
        const user = manager.create(User, {
          nickname: input.nickname,
          gender: Gender.Unknown,
          avatarId: this.config.getOrThrow('USER_DEFAULT_AVATAR_ID', {
            infer: true,
          }),
          isGuest: input.isGuest,
          status: UserStatus.Active,
        });
        return manager.save(user);

        await manager.save(
          manager.create(AuthIdentity, {
            userId: user.id,
            provider,
            providerUid,
          }),
        );
        return user;
      });
    } catch (err) {

      // Step 8: 2 request đầu tiên chạy song song: bên dính unique đọc lại identity
      // Unique (provider, providerUid) ở DB là chốt chặn cuối: 2 request đầu tiên chạy song song
      // thì 1 bên dính unique violation rồi đọc lại identity của bên kia.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (isUniqueViolation(err)) {
        const identity = await this.identityRepo.findOneByOrFail({
          provider,
          providerUid,
        });
        return this.assertActive(
          this.assertGuestIdentityStillActive(
            provider,
            await this.userService.getByIdOrThrow(identity.userId),
          ),
        );
      }
      throw err;
    }

    return this.issue(user);

    // [SERVICE] AuthService.issue
    const tokens = await this.tokenService.issueForUser(
      user.id,
      user.isGuest,
      user.role,
    );

    // [SERVICE] TokenService.issueForUser · auth/services/token.service.ts
    const userId = user.id;
    const isGuest = user.isGuest;
    const role = user.role;
    const expiresIn = this.config.getOrThrow('JWT_ACCESS_TTL_SECONDS', {
      infer: true,
    });
    const payload: AccessTokenPayload = {
      sub: userId,
      isGuest,
      role,
      jti: randomUUID(),
    };

    // Step 9: Ký access token JWT
    const accessToken = await this.jwtService.signAsync(payload, { expiresIn });

    // Step 10: Sinh refresh token ngẫu nhiên, DB chỉ lưu băm SHA-256
    // Refresh token là chuỗi ngẫu nhiên 48 byte; DB chỉ lưu băm SHA-256 của nó, bản gốc chỉ trả
    // cho client.
    const refreshPlain = randomBytes(REFRESH_TOKEN_BYTES).toString('base64url');
    const ttlDays = this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', {
      infer: true,
    });
    await this.refreshSessions.issue({
      userId,
      tokenHash: this.hash(refreshPlain),
      // familyId nhóm mọi token sinh ra từ cùng 1 lần đăng nhập, để phát hiện dùng lại và thu hồi
      // cả họ (xem POST /auth/refresh).
      familyId: familyId ?? randomUUID(),
      expiresAt: this.refreshExpiresAt(ttlDays),
    });

    // [SERVICE] TokenService.hash
    // value = refreshPlain
    return createHash('sha256').update(value).digest('hex');

    // [SERVICE] TokenService.refreshExpiresAt
    return new Date(Date.now() + ttlDays * 24 * 3600 * 1000);

    // [SERVICE] TypeOrmRefreshSessionAdapter.issue · auth/repositories/typeorm-refresh-session.adapter.ts
    // input = { userId, tokenHash: this.hash(refreshPlain), familyId: familyId ?? randomUUID(),
    // expiresAt: this.refreshExpiresAt(ttlDays), }
    await this.dataSource.getRepository(RefreshToken).insert({
      userId: input.userId,
      tokenHash: input.tokenHash,
      familyId: input.familyId,
      expiresAt: input.expiresAt,
    });

    return { accessToken, refreshToken: refreshPlain, expiresIn };

    return {
      ...tokens,
      csrfToken: generateCsrfToken(),
      userId: user.id,
      isGuest: user.isGuest,
    };

    // [SERVICE] generateCsrfToken · common/csrf/csrf-token.ts
    return randomBytes(CSRF_TOKEN_BYTES).toString('base64url');

    // [CONTROLLER] AuthController.finalizeSession · auth/auth.controller.ts
    const session = await this.authService.verifyOtpAndLogin(dto.phone, dto.code);

    // Step 11: Đặt refresh token vào cookie httpOnly, bóc khỏi body trả về
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

    // [SERVICE] setAuthCookies · auth/auth.cookies.ts
    // input = đối số 2 của lời gọi ở trên
    const options = cookieOptions(
      input.isProduction,
      input.ttlDays,
      input.crossOriginDev ?? false,
      input.productionSameSite ?? 'strict',
    );

    // [SERVICE] cookieOptions
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

// [POSTGRES] CREATE TABLE auth_identities · database/migrations/1751900000000-init-auth-user.ts
await queryRunner.query(`
  CREATE TABLE auth_identities (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       uuid         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider      varchar(16)  NOT NULL,
    provider_uid  varchar(255) NOT NULL,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_auth_identities_provider_uid UNIQUE (provider, provider_uid)
  )
`);

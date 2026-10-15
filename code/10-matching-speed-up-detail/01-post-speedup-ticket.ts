//  POST /api/v1/matching/tickets/:id/speedup
//  Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id vé từ URL, Idempotency-Key từ header)
//  Step 2: Kiểm tra vé (đúng chủ vé, còn đang queued)
//  Step 3: Tính giá speed-up và tạo key idempotent
//  Step 4: Trừ Diamond qua Economy
//  Step 5: Cộng boost vào vé trong Postgres
//  Step 6: Đọc lại tổng boost, rồi đặt lại điểm của vé trong Redis
//  Step 7: Ghi điểm mới của vé vào sorted set của shard
//  Step 8: Service trả kết quả về controller
//  Step 9: Controller trả response kèm giá speed-up hiện hành

// [CONTROLLER] MatchingController.speedup · matching/matching.controller.ts
@Controller('matching')
export class MatchingController {
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
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // MatchingService
    @InjectRepository(MatchTicket)
    private readonly ticketRepo: Repository<MatchTicket>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // EconomyService
    @InjectRepository(Wallet) private readonly walletRepo: Repository<Wallet>,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // LedgerService
    super();
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id vé từ URL, Idempotency-Key
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

  @Post('tickets/:id/speedup')
  @HttpCode(HttpStatus.OK)
  async speedup(
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
  ): Promise<SpeedupResultDto> {
    const result = await this.matchingService.speedup(user, id, idempotencyKey);

    // [SERVICE] MatchingService.speedup · matching/matching.service.ts
    const ticketId = id;

    // Step 2: Kiểm tra vé (đúng chủ vé, còn đang queued)
    const ticket = await this.getTicket(user, ticketId);

    // [SERVICE] MatchingService.getTicket
    const ticket = await this.ticketRepo.findOneBy({ id: ticketId });
    if (!ticket) {
      throw new DomainException(
        MatchingErrors.TICKET_NOT_FOUND,
        'Không tìm thấy ticket',
        HttpStatus.NOT_FOUND,
      );
    }
    this.assertOwnership(ticket, user);

    // [SERVICE] MatchingService.assertOwnership
    if (ticket.userId !== user.userId) {
      throw new DomainException(
        MatchingErrors.TICKET_FORBIDDEN,
        'Ticket không thuộc về bạn',
        HttpStatus.FORBIDDEN,
      );
    }

    return ticket;

    if (ticket.status !== MatchTicketStatus.Queued) {
      throw new DomainException(
        MatchingErrors.TICKET_INVALID_TRANSITION,
        `Ticket đang '${ticket.status}', chỉ speed-up được khi đang 'queued'`,
        HttpStatus.CONFLICT,
      );
    }

    // Step 3: Tính giá speed-up và tạo key idempotent
    // Giá do server tính ngay trước khi trừ (theo hạng VIP hiện tại): client không tự gửi giá và
    // không được hard-code.
    const price = await this.getSpeedupPriceDiamondForUser(user.userId);

    // [SERVICE] MatchingService.getSpeedupPriceDiamondForUser
    const userId = user.userId;
    const tier = await this.economy.getActiveVipTier(userId);

    // [SERVICE] EconomyService.getActiveVipTier · economy/economy.service.ts
    const wallet = manager
      ? await manager.getRepository(Wallet).findOne({
          where: { userId },
          lock: { mode: 'pessimistic_write' },
        })
      : await this.walletRepo.findOneBy({ userId });
    return wallet?.activeVipTier ?? null;

    if (tier === VipTier.Svip)
      return this.config.getOrThrow('MATCHING_SVIP_SPEEDUP_PRICE_DIAMOND', {
        infer: true,
      });
    if (tier === VipTier.Vip)
      return this.config.getOrThrow('MATCHING_VIP_SPEEDUP_PRICE_DIAMOND', {
        infer: true,
      });
    return this.getSpeedupPriceDiamond();

    // [SERVICE] MatchingService.getSpeedupPriceDiamond
    return this.config.getOrThrow('MATCHING_SPEEDUP_PRICE_DIAMOND', {
      infer: true,
    });

    const boostMs = this.config.getOrThrow('MATCHING_PRIORITY_BOOST_MS', {
      infer: true,
    });
    // Key = (user, Idempotency-Key của client). Economy dùng nguyên chuỗi này làm key giao dịch,
    // nên bấm lại cùng key chỉ trừ tiền một lần.
    const prefixedKey = speedupIdempotencyKey(user.userId, idempotencyKey);

    // [SERVICE] speedupIdempotencyKey · matching/matching.constants.ts
    // userId = user.userId
    return `matching:speedup:${userId}:${idempotencyKey}`;

    // Step 4: Trừ Diamond qua Economy
    // [POSTGRES] Trừ Diamond TRƯỚC khi đụng vào hàng đợi: Economy tự khóa ví, kiểm số dư và ghi
    // sổ idempotent. Bước sau lỗi thì tiền đã trừ, nhưng retry cùng key không trừ lại.
    const spend = await this.economy.spendDiamond(
      user.userId,
      TransactionType.MatchingSpeedup,
      price,
      prefixedKey,
      { ticketId, priceDiamond: price },
    );

    // [SERVICE] EconomyService.spendDiamond · economy/economy.service.ts
    // userId = user.userId
    const type = TransactionType.MatchingSpeedup;
    const amountDiamond = price;
    // idempotencyKey = prefixedKey
    const metadata = { ticketId, priceDiamond: price };
    if (!Number.isSafeInteger(amountDiamond) || amountDiamond <= 0) {
      throw new Error(
        `spendDiamond: amountDiamond phải là số nguyên dương, nhận ${amountDiamond}`,
      );
    }
    const amount = BigInt(amountDiamond);
    const result = await this.ledger.record({
      type,
      idempotencyKey,
      actorUserId: userId,
      metadata,
      entries: [
        {
          accountKind: LedgerAccountKind.UserWallet,
          userId,
          direction: LedgerDirection.Debit,
          amount,
          currency: LedgerCurrency.Diamond,
        },
        {
          accountKind: LedgerAccountKind.SystemRevenue,
          direction: LedgerDirection.Credit,
          amount,
          currency: LedgerCurrency.Diamond,
        },
      ],
    });

    // [SERVICE] LedgerService.record · economy/services/ledger.service.ts
    // input = đối số 1 của lời gọi ở trên
    this.validateInput(input);

    // [SERVICE] LedgerService.validateInput
    this.validateEntries(input.entries);

    // [SERVICE] LedgerService.validateEntries
    const entries = input.entries;
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

    // Step 5: Cộng boost vào vé trong Postgres
    // [POSTGRES] Replay cùng key không cộng boost lần nữa (tránh double-boost); increment nguyên
    // tử, chỉ khi vé còn queued. Đánh đổi: trừ tiền và increment là 2 bước riêng, process chết
    // giữa chừng thì retry không cộng lại boost.
    if (!spend.replayed) {
      await this.ticketRepo.increment(
        { id: ticket.id, status: MatchTicketStatus.Queued },
        'priorityBoostMs',
        boostMs,
      );
    }

    // Step 6: Đọc lại tổng boost, rồi đặt lại điểm của vé trong Redis
    // Đọc lại tổng boost từ DB để tính score TUYỆT ĐỐI. Boost cộng dồn trong DB nên retry hay
    // replay cùng key chỉ sửa score về đúng tổng đã trả tiền, không double-boost.
    const fresh = await this.ticketRepo.findOneByOrFail({ id: ticket.id });
    await this.queue.enqueue(
      shardKeyOfTicket(fresh),
      String(ticketScore(fresh)),
      fresh.id,
      'XX',
    );

    // [REDIS] shardKeyOfTicket · matching/redis/matching-redis.provider.ts
    // ticket = fresh
    return matchingShardKey(ticket.matchType, ticket.region, ticket.ageBand);

    // [REDIS] matchingShardKey
    const matchType = ticket.matchType;
    const region = ticket.region;
    const ageBand = ticket.ageBand;
    return `matching:queue:${matchType}:${region}:${ageBand}`;

    // [REDIS] RedisMatchingQueue.enqueue · matching/redis/redis-matching-queue.adapter.ts
    // Step 7: Ghi điểm mới của vé vào sorted set của shard
    const shard = shardKeyOfTicket(fresh);
    const score = String(
      fresh.enqueuedAt.getTime() + fresh.trustPenaltyMs - fresh.priorityBoostMs,
    );
    // ticketId = fresh.id
    const mode = 'XX';
    // Mỗi shard (loại ghép + vùng + nhóm tuổi) là một sorted set riêng, điểm nhỏ ghép trước:
    // matcher lấy từng shard bằng ZPOPMIN 2 (atomic nên 2 matcher không lấy trùng vé), nên
    // speed-up chỉ cần hạ điểm của vé ngay trong set này. XX: vé đã rời hàng (ghép hoặc hủy) thì
    // không bị thêm lại.
    await this.redis.zadd(shard, mode, score, ticketId);

    // Step 8: Service trả kết quả về controller
    return {
      transactionId: spend.transactionId,
      replayed: spend.replayed,
      ticket: fresh,
    };

    // Step 9: Controller trả response kèm giá speed-up hiện hành
    return SpeedupResultDto.from(
      result.transactionId,
      result.replayed,
      result.ticket,
      await this.matchingService.getSpeedupPriceDiamondForUser(user.userId),
    );
  }
}

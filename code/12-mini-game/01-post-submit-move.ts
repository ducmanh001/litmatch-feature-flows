//  POST /api/v1/mini-game/sessions/:id/moves
//  Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id ván từ URL, nước đi trong body)
//  Step 2: Kiểm tra user thuộc ván (không thì 404)
//  Step 3: Xác định cột move của người nộp (low hay high)
//  Step 4: Nộp move bằng UPDATE có điều kiện trong transaction (0 dòng: 409)
//  Step 5: Đọc lại ván: mới 1 bên nộp thì trả về, chưa resolve
//  Step 6: Đủ 2 move: tính người thắng
//  Step 7: Ghi kết quả ván và nhả slot participant của cả 2 người
//  Step 8: Sau commit, báo minigame.session.resolved qua Redis cho cả 2
//  Step 9: Controller trả ván theo góc nhìn của người nộp

// [CONTROLLER] MiniGameController.submitMove · mini-game/mini-game.controller.ts
@Controller('mini-game')
export class MiniGameController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // MiniGameService
  private readonly logger = new Logger(MiniGameService.name);
  constructor(
    // RedisThrottlerStorage
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
    // JwtAuthGuard
    private readonly jwtService: JwtService,
    private readonly reflector: Reflector,
    config: ConfigService<CoreApiEnv, true>,
    // MiniGameService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(MiniGameSession)
    private readonly sessionRepo: Repository<MiniGameSession>,
    @Inject(MINI_GAME_REDIS) private readonly redis: Redis,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
  }

  // Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id ván từ URL, nước đi trong
  // body)

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

  @Post('sessions/:id/moves')
  @HttpCode(HttpStatus.OK)
  async submitMove(
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

    @Body() dto: SubmitMiniGameMoveDto,
  ): Promise<MiniGameSessionDto> {
    const session = await this.miniGameService.submitMove(
      user.userId,
      id,
      dto.move,
    );

    // [SERVICE] MiniGameService.submitMove · mini-game/mini-game.service.ts
    const userId = user.userId;
    const sessionId = id;
    const move = dto.move;

    // Step 2: Kiểm tra user thuộc ván (không thì 404)
    // Gộp 404 cho "không tồn tại" và "không phải thành viên ván": không lộ ván của người khác.
    const session = await this.getSessionForParticipant(userId, sessionId);

    // [SERVICE] MiniGameService.getSessionForParticipant
    const session = await this.sessionRepo.findOneBy({ id: sessionId });
    if (
      !session ||
      (session.userLowId !== userId && session.userHighId !== userId)
    ) {
      throw new DomainException(
        MiniGameErrors.NOT_FOUND,
        'Không tìm thấy ván chơi',
        HttpStatus.NOT_FOUND,
      );
    }
    return session;

    // Step 3: Xác định cột move của người nộp (low hay high)
    const moveColumn: MoveColumn =
      session.userLowId === userId ? 'lowMove' : 'highMove';
    const moveColumnSql = moveColumn === 'lowMove' ? 'low_move' : 'high_move';

    const { session: updatedSession, resolved } =
      await this.dataSource.transaction(async (manager) => {

        // Step 4: Nộp move bằng UPDATE có điều kiện trong transaction (0 dòng: 409)
        // [POSTGRES] Chốt chặn DUY NHẤT là UPDATE có điều kiện (move chưa nộp VÀ ván còn
        // waiting_moves), không phải check-rồi-ghi. UPDATE cũng khóa dòng ván: 2 bên nộp cùng lúc
        // bị xếp hàng, bên sau thấy đủ 2 move nên chỉ 1 bên resolve.
        const updateResult = await manager
          .createQueryBuilder()
          .update(MiniGameSession)
          .set({ [moveColumn]: move } as Partial<MiniGameSession>)
          .where(`id = :id AND ${moveColumnSql} IS NULL AND status = :status`, {
            id: sessionId,
            status: MiniGameSessionStatus.WaitingMoves,
          })
          .execute();

        // 0 dòng bị ảnh hưởng = đã nộp rồi hoặc ván không còn chờ: trả 409, move đã nộp không đổi
        // lại được.
        if (!updateResult.affected) {
          throw new DomainException(
            MiniGameErrors.MOVE_ALREADY_SUBMITTED,
            'Move đã được nộp hoặc ván không còn chờ move',
            HttpStatus.CONFLICT,
          );
        }

        // Step 5: Đọc lại ván: mới 1 bên nộp thì trả về, chưa resolve
        const reloaded = await manager
          .getRepository(MiniGameSession)
          .findOneByOrFail({ id: sessionId });

        // Mới 1 bên nộp: KHÔNG resolve, KHÔNG publish gì, để không lộ trạng thái đối phương cho
        // ai, kể cả người vừa nộp.
        if (!reloaded.lowMove || !reloaded.highMove) {
          return { session: reloaded, resolved: null };
        }

        // Step 6: Đủ 2 move: tính người thắng
        // Luật thắng thua là hàm thuần tách riêng khỏi service, để thêm game thứ 2 không phải
        // nhét if/else theo gameType vào service chung.
        const outcome = resolveWinner(reloaded.lowMove, reloaded.highMove);

        // [SERVICE] resolveWinner · mini-game/mini-game.constants.ts
        const lowMove = reloaded.lowMove;
        const highMove = reloaded.highMove;
        if (lowMove === highMove) return 'draw';

        const beats: Record<RockPaperScissorsMove, RockPaperScissorsMove> = {
          [RockPaperScissorsMove.Rock]: RockPaperScissorsMove.Scissors,
          [RockPaperScissorsMove.Scissors]: RockPaperScissorsMove.Paper,
          [RockPaperScissorsMove.Paper]: RockPaperScissorsMove.Rock,
        };
        return beats[lowMove] === highMove ? 'low' : 'high';

        const winnerUserId =
          outcome === 'draw'
            ? null
            : outcome === 'low'
              ? reloaded.userLowId
              : reloaded.userHighId;
        const resolvedAt = new Date();

        // Step 7: Ghi kết quả ván và nhả slot participant của cả 2 người
        // Update theo id, không cần điều kiện status: dòng đã bị khóa từ câu UPDATE đầu
        // transaction nên không ai chen vào được.
        await manager.update(
          MiniGameSession,
          { id: sessionId },
          {
            status: MiniGameSessionStatus.Resolved,
            winnerUserId,
            resolvedAt,
          },
        );
        // Nhả slot "mỗi user 1 ván đang chờ" của cả 2 người, cùng transaction với việc đổi
        // status.
        await manager.delete(MiniGameActiveParticipant, { sessionId });

        const resolvedSession: MiniGameSession = {
          ...reloaded,
          status: MiniGameSessionStatus.Resolved,
          winnerUserId,
          resolvedAt,
        };
        return {
          session: resolvedSession,
          resolved: {
            lowMove: reloaded.lowMove,
            highMove: reloaded.highMove,
            winnerUserId,
          },
        };
      });

    // Step 8: Sau commit, báo minigame.session.resolved qua Redis cho cả 2
    // [REDIS] Chỉ publish khi đã resolve, SAU commit, kèm cả 2 move cho cả 2 người. Không có
    // event "1 bên đã nộp" để bên kia không suy ra ai nộp trước.
    if (resolved) {
      const publishData: MiniGameSessionResolvedEventData = {
        sessionId,
        lowMove: resolved.lowMove,
        highMove: resolved.highMove,
        winnerUserId: resolved.winnerUserId,
      };
      const envelope: RealtimeEnvelope<MiniGameSessionResolvedEventData> = {
        event: RealtimeEvents.MiniGameSessionResolved,
        data: publishData,
      };
      await Promise.all(
        [session.userLowId, session.userHighId].map((uid) =>
          publishRealtimeEvent(this.redis, this.logger, uid, envelope),
        ),
      );

      // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
      const publisher = this.redis;
      const logger = this.logger;
      // userId = uid
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

    return updatedSession;

    // Step 9: Controller trả ván theo góc nhìn của người nộp
    return MiniGameSessionDto.from(session, user.userId);
  }
}

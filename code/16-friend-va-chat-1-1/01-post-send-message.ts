//  POST /api/v1/conversations/:id/messages
//  Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận.
//  Step 1: Request qua middleware, guard, interceptor và pipe áp dụng; đọc user/đầu vào theo decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id hội thoại từ URL, Idempotency-Key)
//  Step 2: Có ảnh đính kèm thì kiểm quyền sở hữu và đổi thành URL
//  Step 3: Kiểm user thuộc hội thoại
//  Step 4: Kiểm block 2 chiều, độ dài tin nhắn, tin rỗng
//  Step 5: Lưu tin nhắn idempotent (retry trả lại tin cũ, khác nội dung thì 409)
//  Step 6: Cập nhật streak sau khi tin đã lưu, trong transaction riêng
//  Step 7: Tạo dòng streak nếu chưa có, khóa dòng FOR UPDATE
//  Step 8: Cả 2 đã nhắn hôm nay thì tăng streak 1 lần/ngày, cứu lỡ 1 ngày
//  Step 9: Chạm mốc streak thì phát friend.streak.increased qua Redis
//  Step 10: Phát friend.message cho cả 2 người qua Redis (sau khi lưu)
//  Step 11: Tạo thông báo và push cho người nhận, trừ khi đã mute
//  Step 12: Controller trả tin nhắn vừa lưu

// [CONTROLLER] FriendController.sendMessage · friend/friend.controller.ts
@Controller()
export class FriendController {
  // RedisThrottlerStorage
  private readonly logger = new Logger(RedisThrottlerStorage.name);
  private readonly fallback = new ThrottlerStorageService();
  private redisRetryAt = 0;
  private lastWarnAt = 0;
  // JwtAuthGuard
  private readonly previousSecret: string | undefined;
  // GlobalExceptionFilter
  private readonly logger = new Logger(GlobalExceptionFilter.name);
  // S3ImageStorageProvider
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly publicBaseUrl: string;
  private readonly uploadUrlTtlSeconds: number;
  // FriendService
  private readonly logger = new Logger(FriendService.name);
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
    // MediaService
    @InjectRepository(ImageAsset)
    private readonly assetRepo: Repository<ImageAsset>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // FriendService
    @InjectRepository(ConversationMemberState)
    private readonly memberStateRepo: Repository<ConversationMemberState>,
    @Inject(FRIEND_REDIS) private readonly redis: Redis,
    // ConversationService
    @InjectRepository(Conversation)
    private readonly conversationRepo: Repository<Conversation>,
    @InjectRepository(Message)
    private readonly messageRepo: Repository<Message>,
    // SafetyService
    @InjectRepository(Block) private readonly blockRepo: Repository<Block>,
    // StreakService
    @InjectDataSource() private readonly dataSource: DataSource,
    // NotificationService
    @InjectRepository(Notification)
    private readonly notificationRepo: Repository<Notification>,
    // WebPushSubscriptionService
    @InjectRepository(WebPushSubscription)
    private readonly subscriptionRepo: Repository<WebPushSubscription>,
  ) {
    // JwtAuthGuard
    this.previousSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    // S3ImageStorageProvider
    super();
    if (
      !options.region ||
      !options.bucket ||
      !options.accessKeyId ||
      !options.secretAccessKey ||
      !options.publicBaseUrl
    ) {
      throw new Error(
        'S3-compatible media storage yêu cầu đầy đủ region, bucket, credential và MEDIA_PUBLIC_BASE_URL',
      );
    }

    this.bucket = options.bucket;
    this.publicBaseUrl = options.publicBaseUrl.replace(/\/+$/u, '');
    this.uploadUrlTtlSeconds = options.uploadUrlTtlSeconds;
    this.client = new S3Client({
      region: options.region,
      ...(options.endpoint ? { endpoint: options.endpoint } : {}),
      forcePathStyle: options.forcePathStyle,
      credentials: {
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
      },
    });
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
  // decorator, validate rồi gọi xử lý: Nhận request (user từ JWT, id hội thoại từ URL,
  // Idempotency-Key)

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

  @Post('conversations/:id/messages')
  @HttpCode(HttpStatus.CREATED)
  async sendMessage(
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

    @Body() dto: SendFriendMessageDto,
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
  ): Promise<MessageDto> {

    // Step 2: Có ảnh đính kèm thì kiểm quyền sở hữu và đổi thành URL
    const imageUrl = dto.imageAssetId
      ? await this.mediaService.resolveImageUrl(
          user.userId,
          dto.imageAssetId,
          ImageAssetPurpose.Message,
        )
      : undefined;

    // [SERVICE] MediaService.resolveImageUrl · media/media.service.ts
    const ownerUserId = user.userId;
    const assetId = dto.imageAssetId;
    const expectedPurpose = ImageAssetPurpose.Message;
    const asset = await this.assetRepo.findOne({
      where: { id: assetId, ownerUserId },
    });
    if (!asset) {
      throw new DomainException(
        MediaErrors.ASSET_NOT_FOUND,
        'Không tìm thấy ảnh đã upload',
        HttpStatus.NOT_FOUND,
      );
    }
    if (expectedPurpose !== undefined && asset.purpose !== expectedPurpose) {
      throw new DomainException(
        MediaErrors.ASSET_NOT_FOUND,
        'Không tìm thấy ảnh đã upload',
        HttpStatus.NOT_FOUND,
      );
    }
    if (asset.status !== ImageAssetStatus.Ready) {
      const provider = this.config.getOrThrow('MEDIA_STORAGE_PROVIDER', {
        infer: true,
      });
      if (provider !== 'dev') {
        const finalStorageKey = this.storage.generateFinalStorageKey(
          ownerUserId,
          asset.id,
        );

        // [EXTERNAL] DevImageStorageProvider.generateFinalStorageKey · media/ports/dev-image-storage.provider.ts
        // assetId = asset.id
        return `dev-image/${ownerUserId}/final/${assetId}`;

        // [EXTERNAL] S3ImageStorageProvider.generateFinalStorageKey · media/ports/s3-image-storage.provider.ts
        // assetId = asset.id
        return `images/${ownerUserId}/${assetId}`;

        const finalObject = await this.storage.head(finalStorageKey);

        // [EXTERNAL] DevImageStorageProvider.head · media/ports/dev-image-storage.provider.ts
        const storageKey = finalStorageKey;
        void storageKey;
        return null;

        // [EXTERNAL] S3ImageStorageProvider.head · media/ports/s3-image-storage.provider.ts
        // storageKey = finalStorageKey
        try {
          const result = await this.client.send(
            new HeadObjectCommand({ Bucket: this.bucket, Key: storageKey }),
          );
          return {
            sizeBytes: result.ContentLength ?? 0,
            contentType: result.ContentType ?? '',
          };
        } catch (error) {
          const name = error instanceof Error ? error.name : '';
          if (name === 'NotFound' || name === 'NoSuchKey') return null;
          throw error;
        }

        if (finalObject === null) {
          await this.assertUploadedImage(asset);

          // [SERVICE] MediaService.assertUploadedImage
          // storageKey = asset.storageKey
          const object = await this.storage.head(storageKey);
          if (
            object === null ||
            object.sizeBytes !== asset.sizeBytes ||
            object.contentType !== asset.contentType
          ) {
            if (object !== null) await this.storage.delete(storageKey);

            // [EXTERNAL] DevImageStorageProvider.delete · media/ports/dev-image-storage.provider.ts
            void storageKey;

            // [EXTERNAL] S3ImageStorageProvider.delete · media/ports/s3-image-storage.provider.ts
            await this.client.send(
              new DeleteObjectCommand({ Bucket: this.bucket, Key: storageKey }),
            );

            throw new DomainException(
              MediaErrors.ASSET_NOT_READY,
              'Ảnh chưa upload hoàn tất hoặc metadata không hợp lệ',
              HttpStatus.UNPROCESSABLE_ENTITY,
            );
          }

          const prefix = await this.storage.readPrefix(storageKey);

          // [EXTERNAL] DevImageStorageProvider.readPrefix · media/ports/dev-image-storage.provider.ts
          void storageKey;
          return null;

          // [EXTERNAL] S3ImageStorageProvider.readPrefix · media/ports/s3-image-storage.provider.ts
          try {
            const result = await this.client.send(
              new GetObjectCommand({
                Bucket: this.bucket,
                Key: storageKey,
                Range: 'bytes=0-4095',
              }),
            );
            return result.Body ? await result.Body.transformToByteArray() : null;
          } catch (error) {
            const name = error instanceof Error ? error.name : '';
            if (name === 'NotFound' || name === 'NoSuchKey') return null;
            throw error;
          }

          // [SERVICE] MediaService.detectImageMime · media/media.service.ts
          if (prefix === null) return undefined;
          if (
            prefix.length >= 8 &&
            prefix[0] === 0x89 &&
            prefix[1] === 0x50 &&
            prefix[2] === 0x4e &&
            prefix[3] === 0x47 &&
            prefix[4] === 0x0d &&
            prefix[5] === 0x0a &&
            prefix[6] === 0x1a &&
            prefix[7] === 0x0a
          ) {
            return 'image/png';
          }
          if (
            prefix.length >= 3 &&
            prefix[0] === 0xff &&
            prefix[1] === 0xd8 &&
            prefix[2] === 0xff
          ) {
            return 'image/jpeg';
          }
          if (
            prefix.length >= 6 &&
            prefix[0] === 0x47 &&
            prefix[1] === 0x49 &&
            prefix[2] === 0x46 &&
            prefix[3] === 0x38 &&
            (prefix[4] === 0x37 || prefix[4] === 0x39) &&
            prefix[5] === 0x61
          ) {
            return 'image/gif';
          }
          if (
            prefix.length >= 12 &&
            prefix[0] === 0x52 &&
            prefix[1] === 0x49 &&
            prefix[2] === 0x46 &&
            prefix[3] === 0x46 &&
            prefix[8] === 0x57 &&
            prefix[9] === 0x45 &&
            prefix[10] === 0x42 &&
            prefix[11] === 0x50
          ) {
            return 'image/webp';
          }
          return undefined;

          if (this.detectImageMime(prefix) !== asset.contentType) {
            await this.storage.delete(storageKey);
            throw new DomainException(
              MediaErrors.ASSET_NOT_READY,
              'Nội dung ảnh không khớp định dạng đã khai báo',
              HttpStatus.UNPROCESSABLE_ENTITY,
            );
          }

          await this.storage.promote(asset.storageKey, finalStorageKey);

          // [EXTERNAL] DevImageStorageProvider.promote · media/ports/dev-image-storage.provider.ts
          const sourceKey = asset.storageKey;
          const destinationKey = finalStorageKey;
          void sourceKey;
          void destinationKey;

          // [EXTERNAL] S3ImageStorageProvider.promote · media/ports/s3-image-storage.provider.ts
          // sourceKey = asset.storageKey
          // destinationKey = finalStorageKey
          await this.client.send(
            new CopyObjectCommand({
              Bucket: this.bucket,
              CopySource: `${this.bucket}/${encodeURIComponent(sourceKey)}`,
              Key: destinationKey,
              MetadataDirective: 'COPY',
            }),
          );
          await this.delete(sourceKey);
        } else {
          await this.assertUploadedImage(asset, finalStorageKey);
        }
        asset.storageKey = finalStorageKey;
      } else {
        asset.storageKey = this.storage.generateFinalStorageKey(
          ownerUserId,
          asset.id,
        );
      }
      asset.status = ImageAssetStatus.Ready;
      await this.assetRepo.save(asset);
    }
    return this.storage.getPublicUrl(asset.storageKey);

    // [EXTERNAL] DevImageStorageProvider.getPublicUrl · media/ports/dev-image-storage.provider.ts
    // storageKey = asset.storageKey
    return `https://dev-storage.invalid/images/${storageKey}`;

    // [EXTERNAL] S3ImageStorageProvider.getPublicUrl · media/ports/s3-image-storage.provider.ts
    // storageKey = asset.storageKey
    return `${this.publicBaseUrl}/${storageKey
      .split('/')
      .map((part) => encodeURIComponent(part))
      .join('/')}`;

    const message = await this.friendService.sendMessage(
      user.userId,
      id,
      dto.content ?? '',
      idempotencyKey,
      // HTTP chỉ tạo được attachment kiểu ảnh, từ asset đã được xác thực. Các kind khác (vd
      // story_reply) chỉ đi qua DI giữa module, client không tự gắn tùy ý.
      imageUrl !== undefined
        ? { kind: 'image', payload: { url: imageUrl } }
        : null,
    );

    // [SERVICE] FriendService.sendMessage · friend/friend.service.ts
    const userId = user.userId;
    const conversationId = id;
    const content = dto.content ?? '';
    const attachment = imageUrl !== undefined
      ? { kind: 'image', payload: { url: imageUrl } }
      : null;

    // Step 3: Kiểm user thuộc hội thoại
    const conversation = await this.getConversationForMember(
      userId,
      conversationId,
    );

    // [SERVICE] FriendService.getConversationForMember
    const conversation =
      await this.conversationService.findById(conversationId);

    // [SERVICE] ConversationService.findById · friend/services/conversation.service.ts
    // id = conversationId
    return this.conversationRepo.findOneBy({ id });

    if (
      !conversation ||
      (conversation.userLowId !== userId && conversation.userHighId !== userId)
    ) {
      throw new DomainException(
        FriendErrors.CONVERSATION_NOT_FOUND,
        'Không tìm thấy conversation',
        HttpStatus.NOT_FOUND,
      );
    }
    return conversation;

    // Step 4: Kiểm block 2 chiều, độ dài tin nhắn, tin rỗng
    // Block kiểm cả 2 chiều và trả CÙNG mã lỗi 404 với "không phải thành viên", để không ai biết
    // ai block ai qua mã lỗi.
    const partnerId =
      conversation.userLowId === userId
        ? conversation.userHighId
        : conversation.userLowId;

    // [SERVICE] SafetyService.isBlocked · safety/safety.service.ts
    const blockerUserId = userId;
    const targetUserId = partnerId;
    return (
      (await this.latestAction(blockerUserId, targetUserId)) ===
      BlockAction.Blocked
    );

    // [SERVICE] SafetyService.latestAction
    const blockedUserId = targetUserId;
    const latest = await this.blockRepo.findOne({
      where: { blockerUserId, blockedUserId },
      order: { createdAt: 'DESC' },
    });
    return latest?.action ?? null;

    if (
      (await this.safetyService.isBlocked(userId, partnerId)) ||
      (await this.safetyService.isBlocked(partnerId, userId))
    ) {
      throw new DomainException(
        FriendErrors.CONVERSATION_NOT_FOUND,
        'Không tìm thấy conversation',
        HttpStatus.NOT_FOUND,
      );
    }
    const maxLength = this.config.getOrThrow('FRIEND_MESSAGE_MAX_LENGTH', {
      infer: true,
    });
    if (content.length > maxLength) {
      throw new DomainException(
        FriendErrors.MESSAGE_TOO_LONG,
        `Message dài quá ${maxLength} ký tự`,
        HttpStatus.UNPROCESSABLE_ENTITY,
        { maxLength },
      );
    }
    if (content.trim().length === 0 && attachment === null) {
      throw new DomainException(
        FriendErrors.MESSAGE_EMPTY,
        'Nhập nội dung hoặc đính kèm ảnh',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    const message = await this.conversationService.sendMessage(
      conversation,
      userId,
      content,
      idempotencyKey,
      attachment,
    );

    // [SERVICE] ConversationService.sendMessage · friend/services/conversation.service.ts
    const senderUserId = userId;

    // Step 5: Lưu tin nhắn idempotent (retry trả lại tin cũ, khác nội dung thì 409)
    // [POSTGRES] Key của client được gắn prefix friend:msg:{userId}: unique ở DB (xem bên dưới),
    // retry không nhân đôi tin nhắn.
    const prefixedKey = messageIdempotencyKey(senderUserId, idempotencyKey);

    // [SERVICE] messageIdempotencyKey · friend/friend.constants.ts
    // userId = senderUserId
    // key = idempotencyKey
    return `friend:msg:${userId}:${key}`;

    let message: Message;
    try {
      message = await this.messageRepo.save(
        this.messageRepo.create({
          conversationId: conversation.id,
          senderUserId,
          content,
          idempotencyKey: prefixedKey,
          attachment,
        }),
      );
    } catch (err) {
      // Retry sau timeout mạng: dính unique thì đọc lại. Cùng hội thoại và cùng nội dung thì trả
      // tin cũ (replay); khác thì 409.

      // [SERVICE] isUniqueViolation · database/postgres-errors.ts
      const e = err as { code?: string; driverError?: { code?: string } };
      return (e.driverError?.code ?? e.code) === PG_UNIQUE_VIOLATION;

      if (!isUniqueViolation(err)) throw err;
      const existing = await this.messageRepo.findOneBy({
        idempotencyKey: prefixedKey,
      });
      if (
        existing &&
        existing.conversationId === conversation.id &&
        existing.content === content
      ) {
        return existing;
      }
      throw new DomainException(
        FriendErrors.MESSAGE_IDEMPOTENCY_CONFLICT,
        'Idempotency-Key đã dùng cho 1 message khác nội dung',
        HttpStatus.CONFLICT,
      );
    }
    // last_message_at chỉ để sắp xếp danh sách chat, không phải nguồn sự thật nên không cần chung
    // transaction.
    await this.conversationRepo.update(
      { id: conversation.id },
      { lastMessageAt: message.createdAt },
    );
    return message;

    try {

      // Step 6: Cập nhật streak sau khi tin đã lưu, trong transaction riêng
      // Streak chạy SAU khi tin đã lưu, trong transaction RIÊNG: nếu chung transaction với insert
      // tin thì lần dính unique của retry làm cả transaction bị abort. Replay gọi lại cũng không
      // tăng streak lần 2.
      const { milestoneHit } = await this.streakService.recordActivity(
        conversation,
        userId,
      );

      // [SERVICE] StreakService.recordActivity · friend/services/streak.service.ts
      // senderUserId = userId
      const isLow = conversation.userLowId === senderUserId;
      const today = todayUtc();

      // [SERVICE] todayUtc · common/date/utc-date.ts
      return new Date().toISOString().slice(0, 10);

      return this.dataSource.transaction(async (manager) => {

        // Step 7: Tạo dòng streak nếu chưa có, khóa dòng FOR UPDATE
        // [POSTGRES] Tạo dòng streak khi cần bằng ON CONFLICT DO NOTHING rồi khóa FOR UPDATE: 2
        // bên nhắn gần như cùng lúc vẫn tăng streak đúng 1 lần.
        await manager.query(
          `INSERT INTO conversation_streaks (conversation_id) VALUES ($1)
           ON CONFLICT (conversation_id) DO NOTHING`,
          [conversation.id],
        );
        const row = await manager
          .createQueryBuilder(ConversationStreak, 'cs')
          .where('cs.conversationId = :id', { id: conversation.id })
          .setLock('pessimistic_write')
          .getOneOrFail();

        if (isLow) row.userLowLastActiveDate = today;
        else row.userHighLastActiveDate = today;

        let milestoneHit: number | null = null;
        const bothActiveToday =
          row.userLowLastActiveDate === today &&
          row.userHighLastActiveDate === today;

        // Step 8: Cả 2 đã nhắn hôm nay thì tăng streak 1 lần/ngày, cứu lỡ 1 ngày
        // Chỉ tăng khi cả 2 đã nhắn hôm nay và hôm nay chưa xác nhận, nên gọi lại trong cùng ngày
        // không tăng lần 2. Ngày tính theo UTC của server, không theo múi giờ client.
        if (bothActiveToday && row.lastConfirmedDate !== today) {
          const gapDays = row.lastConfirmedDate
            ? daysBetweenUtc(row.lastConfirmedDate, today)
            : null;

          // [SERVICE] daysBetweenUtc · common/date/utc-date.ts
          const from = row.lastConfirmedDate;
          const to = today;
          const msPerDay = 24 * 60 * 60 * 1000;
          const fromMs = Date.parse(`${from}T00:00:00.000Z`);
          const toMs = Date.parse(`${to}T00:00:00.000Z`);
          return Math.round((toMs - fromMs) / msPerDay);

          if (gapDays === null || gapDays === 1) {
            row.currentStreak += 1;
          } else if (gapDays === 2) {
            // Lỡ đúng 1 ngày vẫn được cứu: vẫn +1 chứ không reset, ghi lại ngày được cứu. Lỡ từ 2
            // ngày trở lên thì reset về 1.
            row.currentStreak += 1;
            row.graceUsedForDate = addDaysUtc(row.lastConfirmedDate as string, 1);

            // [SERVICE] addDaysUtc · common/date/utc-date.ts
            const date = row.lastConfirmedDate as string;
            const days = 1;
            const ms = Date.parse(`${date}T00:00:00.000Z`) + days * 24 * 60 * 60 * 1000;
            return new Date(ms).toISOString().slice(0, 10);
          } else {
            row.currentStreak = 1;
          }
          row.longestStreak = Math.max(row.longestStreak, row.currentStreak);
          row.lastConfirmedDate = today;

          const milestones = this.parseMilestoneDays();

          // [SERVICE] StreakService.parseMilestoneDays
          return this.config
            .getOrThrow('STREAK_MILESTONE_DAYS', { infer: true })
            .split(',')
            .map((s) => Number(s.trim()))
            .filter((n) => Number.isFinite(n) && n > 0);

          if (milestones.includes(row.currentStreak)) {
            milestoneHit = row.currentStreak;
          }
        }

        await manager.save(row);
        return { streak: row, milestoneHit };
      });

      // Step 9: Chạm mốc streak thì phát friend.streak.increased qua Redis
      if (milestoneHit !== null) {
        const streakEnvelope: RealtimeEnvelope<FriendStreakIncreasedEventData> =
          {
            event: RealtimeEvents.FriendStreakIncreased,
            data: { conversationId, currentStreak: milestoneHit },
          };
        await Promise.all(
          [userId, partnerId].map((uid) =>
            publishRealtimeEvent(this.redis, this.logger, uid, streakEnvelope),
          ),
        );

        // [SERVICE] publishRealtimeEvent · common/realtime/publish-realtime.ts
        const publisher = this.redis;
        const logger = this.logger;
        // userId = uid
        // envelope = streakEnvelope
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
    } catch (err) {
      // Streak là phụ: lỗi tính streak chỉ ghi log, không rollback tin đã gửi.
      this.logger.error(
        `Tính streak cho conversation ${conversationId} lỗi (bỏ qua, message vẫn gửi thành công): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }

    // Step 10: Phát friend.message cho cả 2 người qua Redis (sau khi lưu)
    const envelope: RealtimeEnvelope<FriendMessageEventData> = {
      event: RealtimeEvents.FriendMessage,
      data: {
        conversationId,
        messageId: message.id,
        senderUserId: message.senderUserId,
        content: message.content,
        attachment: message.attachment,
        sentAt: message.createdAt.toISOString(),
      },
    };
    // [REDIS] Phát friend.message cho cả 2 thành viên qua kênh realtime:user:{id}, SAU khi lưu.
    // Best-effort, không dùng outbox: client vẫn poll được GET /conversations/:id/messages.
    await Promise.all(
      [userId, partnerId].map((uid) =>
        publishRealtimeEvent(this.redis, this.logger, uid, envelope),
      ),
    );

    try {

      // Step 11: Tạo thông báo và push cho người nhận, trừ khi đã mute
      const partnerState = await this.memberStateRepo.findOne({
        where: { conversationId, userId: partnerId },
      });
      // Người nhận đã mute thì bỏ qua cả in-app lẫn push; tin nhắn, realtime và unread vẫn chạy
      // bình thường.
      if (partnerState?.mutedAt != null) return message;
      // Thông báo chạy transaction riêng và best-effort: tin đã lưu xong nên lỗi ở đây không được
      // làm hỏng luồng gửi. Chat không ẩn danh nên lộ senderUserId là đúng.
      const notification = await this.notificationService.create({
        userId: partnerId,
        type: NotificationType.FriendMessage,
        payload: {
          conversationId,
          senderUserId: userId,
          preview: message.content,
        },
      });

      // [SERVICE] NotificationService.create · notification/notification.service.ts
      // input = đối số 1 của lời gọi ở trên
      return this.createWithManager(this.notificationRepo.manager, input);

      // [SERVICE] NotificationService.createWithManager
      const manager = this.notificationRepo.manager;
      return manager.save(
        manager.create(Notification, { ...input, readAt: null }),
      );

      await this.notificationService.sendPush(notification);

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
    } catch (err) {
      this.logger.warn(
        `Tạo notification friend_message lỗi (bỏ qua, message vẫn gửi thành công): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }

    return message;

    // Step 12: Controller trả tin nhắn vừa lưu
    return MessageDto.from(message);
  }
}

// [POSTGRES] CREATE TABLE messages · database/migrations/1752600000000-friend-chat.ts
// Unique idempotency_key là chốt chặn thật chống nhân đôi tin nhắn. seq bigint IDENTITY là thứ tự
// do DB cấp cho cursor, tránh trùng hoặc mất dòng khi phân trang theo created_at.
await queryRunner.query(`
  CREATE TABLE messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    seq             bigint GENERATED ALWAYS AS IDENTITY,
    conversation_id uuid         NOT NULL REFERENCES conversations(id),
    sender_user_id  uuid         NOT NULL REFERENCES users(id),
    content         text         NOT NULL,
    idempotency_key varchar(255) NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_messages_idempotency_key UNIQUE (idempotency_key)
  )
`);

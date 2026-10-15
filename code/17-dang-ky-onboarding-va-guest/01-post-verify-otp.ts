//  POST /api/v1/auth/otp/verify
//  Kiểm mã OTP có giới hạn số lần thử và chỉ dùng một lần, rồi tìm hoặc tạo tài khoản và cấp phiên đăng nhập.
//  Step 1: Nhận số điện thoại và mã OTP (giới hạn 10 lần/phút)
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

// [CONTROLLER] AuthController.verifyOtp  ·  auth.controller.ts L38–40, L75–88
@Public()
@Controller('auth')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // AuthService
    @InjectRepository(AuthIdentity)
    private readonly identityRepo: Repository<AuthIdentity>,
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly userService: UserService,
    private readonly tokenService: TokenService,
    private readonly otpService: OtpService,
    // OtpService
    @InjectRepository(PhoneOtp) private readonly otpRepo: Repository<PhoneOtp>,
    // TokenService
    @Inject(RefreshSessionPort)
    private readonly refreshSessions: RefreshSessionPort,
    private readonly jwtService: JwtService,
  ) {}

  // Step 1: Nhận số điện thoại và mã OTP (giới hạn 10 lần/phút)
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

    // [SERVICE] AuthService.verifyOtpAndLogin  ·  auth.service.ts L73–81
    const phone = dto.phone;
    const code = dto.code;
    this.assertPhoneOtpEnabled();
    await this.otpService.verifyOtp(phone, code);

    // [SERVICE] OtpService.verifyOtp  ·  otp.service.ts L73–131
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

    // [SERVICE] OtpService.hash  ·  otp.service.ts L133–138
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

    // [SERVICE] AuthService.findOrCreateUser  ·  auth.service.ts L151–198
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
      this.assertGuestIdentityStillActive(provider, user);
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

    // [SERVICE] AuthService.issue  ·  auth.service.ts L332–344
    const tokens = await this.tokenService.issueForUser(
      user.id,
      user.isGuest,
      user.role,
    );

    // [SERVICE] TokenService.issueForUser  ·  token.service.ts L32–61
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

    return { accessToken, refreshToken: refreshPlain, expiresIn };

    return {
      ...tokens,
      csrfToken: generateCsrfToken(),
      userId: user.id,
      isGuest: user.isGuest,
    };

    // [CONTROLLER] AuthController.finalizeSession  ·  auth.controller.ts L149–173
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

// [POSTGRES] CREATE TABLE auth_identities  ·  1751900000000-init-auth-user.ts L28–37
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

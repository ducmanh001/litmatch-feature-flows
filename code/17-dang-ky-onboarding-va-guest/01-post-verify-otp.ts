// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/auth/otp/verify
//  Kiểm mã OTP có giới hạn số lần thử và chỉ dùng một lần, rồi tìm hoặc tạo tài khoản và cấp phiên đăng nhập.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [CONTROLLER]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận số điện thoại và mã OTP (giới hạn 10 lần/phút), giao cho
//                 service, rồi đặt refresh token vào cookie httpOnly và bóc nó khỏi
//                 body trả về.
//    [SERVICE]    Xác minh OTP, tìm hoặc tạo tài khoản (không tạo trùng khi 2 request
//                 song song), cấp access token và refresh token.
//    [POSTGRES]   Mã OTP lưu dạng băm, đếm lần thử và tiêu thụ mã bằng UPDATE có điều
//                 kiện; unique (provider, provider_uid) chặn tạo trùng tài khoản;
//                 refresh token lưu dạng băm.
//
//  Bảng       phone_otps — mã OTP của từng số: code_hash (HMAC), expires_at,
//               attempt_count, consumed_at; chỉ mã chưa dùng mới nhất có giá trị.
//             auth_identities — cách đăng nhập của 1 user; unique (provider,
//               provider_uid) nên cùng số điện thoại không tạo được 2 tài khoản.
//             users — dòng người dùng mới (nickname ngẫu nhiên, isGuest = false) hoặc
//               dòng cũ để kiểm trạng thái.
//             refresh_tokens — ghi 1 dòng refresh token mới (chỉ lưu băm SHA-256) cùng
//               family_id của lần đăng nhập này.
//  Liên quan  POST /auth/otp/request — bước trước: tạo mã 6 số, giới hạn số lần yêu cầu
//               theo số điện thoại mỗi giờ, vô hiệu mã cũ, lưu băm (chưa chép).
//             User — createWithManager: tạo user cùng transaction với identity (không
//               chép).
//             POST /auth/refresh — bước sau: đổi refresh token lấy cặp token mới (xem
//               file 02).

// [CONTROLLER] auth.controller.ts · L75–88
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
}

// [SERVICE] auth.service.ts · L73–81
async verifyOtpAndLogin(phone: string, code: string): Promise<IssuedSession> {
  this.assertPhoneOtpEnabled();
  await this.otpService.verifyOtp(phone, code);
  const user = await this.findOrCreateUser(AuthProvider.Phone, phone, {
    isGuest: false,
    nicknamePrefix: 'User',
  });
  return this.issue(user);
}

// [SERVICE] otp.service.ts · L73–131, L133–138
async verifyOtp(phone: string, code: string): Promise<void> {
  // Chỉ xét mã chưa dùng mới nhất của số này: lúc yêu cầu mã mới, mã cũ đã bị vô hiệu
  // nên chỉ mã mới nhất có giá trị.
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
  // [POSTGRES] Đếm lần thử bằng UPDATE có điều kiện attempt_count < max: nhiều request
  // song song cũng không vượt được giới hạn. Đếm TRƯỚC khi so mã nên đoán sai cũng tốn
  // lượt.
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

  // So băm HMAC bằng timingSafeEqual, thời gian so không lộ vị trí byte đầu tiên khác
  // nhau; DB chỉ chứa băm, không có mã gốc.
  const expected = Buffer.from(otp.codeHash, 'hex');
  const actual = Buffer.from(this.hash(phone, code), 'hex');
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

  // [POSTGRES] Tiêu thụ mã bằng UPDATE có điều kiện consumed_at IS NULL: 1 mã chỉ dùng
  // được đúng 1 lần, request song song thua sẽ bị báo mã sai.
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
}

private hash(phone: string, code: string): string {
  // HMAC-SHA256 với pepper của server: mã chỉ có 6 số nên băm thường sẽ bị dò ra ngay,
  // có pepper thì DB lộ cũng không suy ra được mã.
  const pepper = this.config.getOrThrow('AUTH_OTP_PEPPER', { infer: true });
  return createHmac('sha256', pepper)
    .update(`${phone}:${code}`)
    .digest('hex');
}

// [SERVICE] auth.service.ts · L151–198, L299–308
private async findOrCreateUser(
  provider: AuthProvider,
  providerUid: string,
  opts: { isGuest: boolean; nicknamePrefix: string },
): Promise<User> {
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
    // Unique (provider, providerUid) ở DB là chốt chặn cuối: 2 request đầu tiên chạy
    // song song thì 1 bên dính unique violation rồi đọc lại identity của bên kia.
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
}

private assertActive(user: User): User {
  if (user.status !== UserStatus.Active) {
    throw new DomainException(
      AuthErrors.USER_BANNED,
      'Tài khoản đã bị khoá',
      HttpStatus.FORBIDDEN,
    );
  }
  return user;
}

// [POSTGRES] 1751900000000-init-auth-user.ts · L28–37
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

// [SERVICE] auth.service.ts · L332–344
private async issue(user: User): Promise<IssuedSession> {
  const tokens = await this.tokenService.issueForUser(
    user.id,
    user.isGuest,
    user.role,
  );
  return {
    ...tokens,
    csrfToken: generateCsrfToken(),
    userId: user.id,
    isGuest: user.isGuest,
  };
}

// [SERVICE] token.service.ts · L32–61
async issueForUser(
  userId: string,
  isGuest: boolean,
  role: Role,
  familyId?: string,
): Promise<IssuedTokens> {
  const expiresIn = this.config.getOrThrow('JWT_ACCESS_TTL_SECONDS', {
    infer: true,
  });
  const payload: AccessTokenPayload = {
    sub: userId,
    isGuest,
    role,
    jti: randomUUID(),
  };
  const accessToken = await this.jwtService.signAsync(payload, { expiresIn });

  // Refresh token là chuỗi ngẫu nhiên 48 byte; DB chỉ lưu băm SHA-256 của nó, bản gốc
  // chỉ trả cho client.
  const refreshPlain = randomBytes(REFRESH_TOKEN_BYTES).toString('base64url');
  const ttlDays = this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', {
    infer: true,
  });
  await this.refreshSessions.issue({
    userId,
    tokenHash: this.hash(refreshPlain),
    // familyId nhóm mọi token sinh ra từ cùng 1 lần đăng nhập, để phát hiện dùng lại và
    // thu hồi cả họ (xem POST /auth/refresh).
    familyId: familyId ?? randomUUID(),
    expiresAt: this.refreshExpiresAt(ttlDays),
  });

  return { accessToken, refreshToken: refreshPlain, expiresIn };
}

// [POSTGRES] typeorm-refresh-session.adapter.ts · L17–24
async issue(input: RefreshSessionIssue): Promise<void> {
  await this.dataSource.getRepository(RefreshToken).insert({
    userId: input.userId,
    tokenHash: input.tokenHash,
    familyId: input.familyId,
    expiresAt: input.expiresAt,
  });
}

// [CONTROLLER] auth.controller.ts · L149–173
private finalizeSession(
  session: IssuedSession,
  res: Response,
): AuthTokensDto {
  // Refresh token chỉ đi qua cookie httpOnly rồi bị bóc khỏi body trả về: client (và
  // XSS) không bao giờ thấy giá trị này.
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
  return {
    accessToken: session.accessToken,
    csrfToken: session.csrfToken,
    expiresIn: session.expiresIn,
    userId: session.userId,
    isGuest: session.isGuest,
    guestDeviceToken: session.guestDeviceToken,
  };
}

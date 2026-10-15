// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/auth/refresh
//  Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [CONTROLLER]
//
//  Vai trò từng tầng
//    [CONTROLLER] Chặn bằng CSRF double-submit, đọc refresh token từ cookie httpOnly,
//                 rồi đặt cặp cookie mới và trả access token (không trả refresh token
//                 trong body).
//    [SERVICE]    Sinh token mới và nhờ adapter xoay token cũ; kiểm lại trạng thái user
//                 tại thời điểm refresh, bị khóa thì thu hồi token vừa cấp.
//    [POSTGRES]   Khóa dòng token (FOR UPDATE), đánh dấu rotated bằng UPDATE có điều
//                 kiện, ghi token mới cùng transaction; dùng lại token cũ thì thu hồi
//                 cả family.
//
//  Bảng       refresh_tokens — mỗi lần xoay: dòng cũ được đánh dấu rotated_at, dòng mới
//               giữ cùng family_id; token_hash unique; dùng lại thì revoked_at được đặt
//               cho cả family.
//             users — đọc user để lấy role, isGuest và kiểm trạng thái còn Active.
//  Liên quan  CsrfGuard — chặn trước khi vào controller: so cookie csrf_token với
//               header X-CSRF-Token (không chép).
//             POST /auth/otp/verify, /auth/social, /auth/guest — nơi cấp refresh token
//               đầu tiên của 1 family (xem file 01).
//             POST /auth/logout — thu hồi token hiện tại và xóa cookie (chưa chép).

// [CONTROLLER] auth.controller.ts · L108–125
@Post('refresh')
@HttpCode(HttpStatus.OK)
// CSRF double-submit: so cookie csrf_token với header X-CSRF-Token (giá trị client nhận
// qua body lúc đăng nhập), lệch hoặc thiếu thì 401. Cần vì cookie refresh tự gắn theo
// request.
@UseGuards(CsrfGuard)
async refresh(
  @Req() req: Request,
  @Res({ passthrough: true }) res: Response,
): Promise<AuthTokensDto> {
  const refreshToken = extractRefreshTokenCookie(req);
  return this.finalizeSession(
    await this.authService.refresh(refreshToken),
    res,
  );
}

// [SERVICE] auth.service.ts · L124–141
async refresh(refreshToken: string): Promise<IssuedSession> {
  const { userId, tokens } = await this.tokenService.rotate(refreshToken);
  // Xác minh lại trạng thái ĐÚNG THỜI ĐIỂM hành động: user bị khóa giữa 2 lần refresh
  // thì không được cấp phiên mới.
  const user = await this.userService.getByIdOrThrow(userId);
  try {
    this.assertActive(user);
  } catch (err) {
    // Thu hồi luôn token mới vừa cấp, vì bước xoay đã lưu nó vào DB.
    await this.tokenService.revoke(tokens.refreshToken);
    throw err;
  }
  return {
    ...tokens,
    csrfToken: generateCsrfToken(),
    userId: user.id,
    isGuest: user.isGuest,
  };
}

// [SERVICE] token.service.ts · L68–103, L114–136, L151–153
async rotate(
  refreshPlain: string,
): Promise<{ userId: string; tokens: IssuedTokens }> {
  const session = await this.refreshSessions.findByTokenHash(
    this.hash(refreshPlain),
  );
  if (!session || session.revokedAt || session.expiresAt <= new Date()) {
    throw this.invalidRefreshToken();
  }

  const ttlDays = this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', {
    infer: true,
  });
  const user = await this.userService.getByIdOrThrow(session.userId);
  // Chỉ sinh token (chưa lưu): việc lưu token mới nằm TRONG transaction xoay của
  // adapter, cùng với đánh dấu token cũ.
  const tokens = await this.issueTokens(user.id, user.isGuest, user.role);
  const result = await this.refreshSessions.rotate({
    tokenId: session.id,
    replacement: {
      tokenHash: this.hash(tokens.refreshToken),
      expiresAt: this.refreshExpiresAt(ttlDays),
    },
  });

  if (result === 'invalid') {
    throw this.invalidRefreshToken();
  }
  // Token đã xoay mà bị dùng lại: adapter đã thu hồi cả family (mọi token cùng lần đăng
  // nhập), nên cả bên dùng lại lẫn người thật phải đăng nhập lại.
  if (result === 'reused') {
    throw new DomainException(
      AuthErrors.REFRESH_TOKEN_REUSED,
      'Refresh token đã bị dùng lại — phiên bị thu hồi',
      HttpStatus.UNAUTHORIZED,
    );
  }

  return { userId: session.userId, tokens };
}

private async issueTokens(
  userId: string,
  isGuest: boolean,
  role: Role,
): Promise<IssuedTokens> {
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
}

private hash(value: string): string {
  // SHA-256 là đủ vì token ngẫu nhiên 48 byte (không phải mật khẩu), không cần bcrypt.
  return createHash('sha256').update(value).digest('hex');
}

// [POSTGRES] typeorm-refresh-session.adapter.ts · L26–34, L36–78
async findByTokenHash(
  tokenHash: string,
): Promise<RefreshSessionRecord | null> {
  const session = await this.dataSource
    .getRepository(RefreshToken)
    .findOneBy({ tokenHash });
  if (!session) return null;
  return this.toRecord(session);
}

async rotate(
  input: RefreshSessionRotationInput,
): Promise<'rotated' | 'invalid' | 'reused'> {
  return this.dataSource.transaction(async (manager) => {
    const session = await manager.findOne(RefreshToken, {
      where: { id: input.tokenId },
      // Khóa dòng token (FOR UPDATE): 2 request song song cùng 1 token xếp hàng, chỉ 1
      // request đánh dấu được rotated.
      lock: { mode: 'pessimistic_write' },
    });
    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      return 'invalid';
    }
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

    // Đánh dấu rotated bằng UPDATE có điều kiện rotated_at IS NULL; 0 dòng bị ảnh hưởng
    // nghĩa là request khác đã xoay trước, coi như dùng lại.
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

    // Token mới giữ nguyên familyId và ghi cùng transaction với việc đánh dấu token cũ:
    // không có trạng thái cũ đã xoay mà chưa có token mới.
    await manager.insert(RefreshToken, {
      userId: session.userId,
      tokenHash: input.replacement.tokenHash,
      familyId: session.familyId,
      expiresAt: input.replacement.expiresAt,
    });
    return 'rotated';
  });
}

// [POSTGRES] 1751900000000-init-auth-user.ts · L42–54
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

// [CONTROLLER] auth.cookies.ts · L21–40, L42–61
function cookieOptions(
  isProduction: boolean,
  ttlDays: number,
  crossOriginDev: boolean,
  productionSameSite: 'strict' | 'none',
): CookieOptions {
  const sameSite = isProduction
    ? productionSameSite
    : crossOriginDev
      ? 'none'
      : 'strict';
  return {
    // httpOnly để JS (kể cả XSS) không đọc được cookie; csrf_token cũng httpOnly vì FE
    // nhận giá trị qua body, cookie chỉ để server so khớp.
    httpOnly: true,
    // sameSite none bắt buộc đi kèm secure (quy định của browser).
    secure: isProduction || sameSite === 'none',
    sameSite,
    // Path hẹp /api/v1/auth: cookie chỉ gửi kèm các route auth.
    path: REFRESH_TOKEN_COOKIE_PATH,
    maxAge: ttlDays * 24 * 3600 * 1000,
  };
}

export function setAuthCookies(
  res: Response,
  input: {
    refreshToken: string;
    csrfToken: string;
    isProduction: boolean;
    crossOriginDev?: boolean;
    productionSameSite?: 'strict' | 'none';
    ttlDays: number;
  },
): void {
  const options = cookieOptions(
    input.isProduction,
    input.ttlDays,
    input.crossOriginDev ?? false,
    input.productionSameSite ?? 'strict',
  );
  res.cookie(REFRESH_TOKEN_COOKIE_NAME, input.refreshToken, options);
  res.cookie(CSRF_COOKIE_NAME, input.csrfToken, options);
}

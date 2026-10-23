//  POST /api/v1/auth/refresh
//  Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token.
//  Step 1: Nhận request: CSRF double-submit, refresh token đọc từ cookie
//  Step 2: Tìm token theo băm SHA-256, kiểm chưa thu hồi và còn hạn
//  Step 3: Sinh cặp token mới (chưa lưu vào DB)
//  Step 4: Giao adapter xoay token trong 1 transaction
//  Step 5: Đọc lại token cũ và khóa dòng (FOR UPDATE)
//  Step 6: Token đã xoay mà bị dùng lại thì thu hồi cả family
//  Step 7: Đánh dấu token cũ đã xoay (UPDATE có điều kiện)
//  Step 8: Ghi token mới cùng family, cùng transaction
//  Step 9: Tuỳ chọn (mặc định TẮT): dọn token đã xoay cũ hơn N ngày của CÙNG họ, FOR UPDATE SKIP LOCKED
//  Step 10: Kết quả invalid hoặc reused thì từ chối với 401
//  Step 11: Kiểm lại trạng thái user, bị khóa thì thu hồi token vừa cấp
//  Step 12: Đặt refresh token mới vào cookie httpOnly, không trả trong body
//  Step 13: Trả access token và csrfToken trong body

// [CONTROLLER] AuthController.refresh · auth/auth.controller.ts
@Public()
@Controller('auth')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly config: ConfigService<CoreApiEnv, true>,
    // AuthService
    private readonly userService: UserService,
    private readonly tokenService: TokenService,
    // TokenService
    @Inject(RefreshSessionPort)
    private readonly refreshSessions: RefreshSessionPort,
    // TypeOrmRefreshSessionAdapter
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  // Step 1: Nhận request: CSRF double-submit, refresh token đọc từ cookie
  @Post('refresh')
  @HttpCode(HttpStatus.OK)
  // CSRF double-submit: so cookie csrf_token với header X-CSRF-Token (giá trị client nhận qua
  // body lúc đăng nhập), lệch hoặc thiếu thì 401. Cần vì cookie refresh tự gắn theo request.
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

    // [SERVICE] AuthService.refresh · auth/auth.service.ts
    const { userId, tokens } = await this.tokenService.rotate(refreshToken);

    // [SERVICE] TokenService.rotate · auth/services/token.service.ts
    const refreshPlain = refreshToken;

    // Step 2: Tìm token theo băm SHA-256, kiểm chưa thu hồi và còn hạn
    const session = await this.refreshSessions.findByTokenHash(
      this.hash(refreshPlain),
    );

    // [SERVICE] TokenService.hash
    const value = refreshPlain;
    // SHA-256 là đủ vì token ngẫu nhiên 48 byte (không phải mật khẩu), không cần bcrypt.
    return createHash('sha256').update(value).digest('hex');

    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      throw this.invalidRefreshToken();
    }

    const ttlDays = this.config.getOrThrow('AUTH_REFRESH_TTL_DAYS', {
      infer: true,
    });
    const user = await this.userService.getByIdOrThrow(session.userId);

    // Step 3: Sinh cặp token mới (chưa lưu vào DB)
    // Chỉ sinh token (chưa lưu): việc lưu token mới nằm TRONG transaction xoay của adapter, cùng
    // với đánh dấu token cũ.
    const tokens = await this.issueTokens(user.id, user.isGuest, user.role);
    const purgeDays =
      this.config.getOrThrow('RETENTION_MODE', { infer: true }) === 'delete'
        ? this.config.getOrThrow('AUTH_REFRESH_ROTATED_RETENTION_DAYS', {
            infer: true,
          })
        : 0;

    // Step 4: Giao adapter xoay token trong 1 transaction
    const result = await this.refreshSessions.rotate({
      tokenId: session.id,
      replacement: {
        tokenHash: this.hash(tokens.refreshToken),
        expiresAt: this.refreshExpiresAt(ttlDays),
      },
      purgeRotatedOlderThanDays: purgeDays,
    });

    // [POSTGRES] TypeOrmRefreshSessionAdapter.rotate · auth/repositories/typeorm-refresh-session.adapter.ts
    const input = {
      tokenId: session.id,
      replacement: {
        tokenHash: this.hash(tokens.refreshToken),
        expiresAt: this.refreshExpiresAt(ttlDays),
      },
      purgeRotatedOlderThanDays: purgeDays,
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
      if (
        input.purgeRotatedOlderThanDays !== undefined &&
        input.purgeRotatedOlderThanDays > 0
      ) {

        // Step 9: Tuỳ chọn (mặc định TẮT): dọn token đã xoay cũ hơn N ngày của CÙNG họ, FOR
        // UPDATE SKIP LOCKED
        // Dọn cơ hội lúc xoay (chỉ khi RETENTION_MODE=delete VÀ
        // AUTH_REFRESH_ROTATED_RETENTION_DAYS > 0; mặc định 0 = TẮT): xoá token ĐÃ XOAY cũ hơn N
        // ngày của cùng họ, dùng index theo family_id, chi phí tỉ lệ với hoạt động. Ở ≈ 290 triệu
        // hàng/ngày bảng này nếu không dọn thì không bao giờ ổn định.
        // FOR UPDATE SKIP LOCKED để việc dọn KHÔNG BAO GIỜ chờ khoá hàng:
        // revokeFamily/revokeForUser cập nhật các hàng của họ theo thứ tự index, nếu xoay cũng
        // chờ thì thành vòng chờ (deadlock). Hàng bị bỏ qua được dọn ở lần xoay sau.
        // Đánh đổi bảo mật (cần review trước khi bật): token đã dọn mà bị phát lại chỉ nhận 401
        // thường, không còn kích hoạt "thu hồi cả họ" — cửa sổ phát hiện token bị đánh cắp co từ
        // ~TTL xuống ~N ngày.
        await manager.query(
          `DELETE FROM refresh_tokens
            WHERE id IN (
              SELECT id FROM refresh_tokens
               WHERE family_id = $1
                 AND rotated_at IS NOT NULL
                 AND rotated_at < now() - make_interval(days => $2)
                 FOR UPDATE SKIP LOCKED
            )`,
          [session.familyId, input.purgeRotatedOlderThanDays],
        );
      }
      return 'rotated';
    });

    // Step 10: Kết quả invalid hoặc reused thì từ chối với 401
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

    // Step 11: Kiểm lại trạng thái user, bị khóa thì thu hồi token vừa cấp
    // Xác minh lại trạng thái ĐÚNG THỜI ĐIỂM hành động: user bị khóa giữa 2 lần refresh thì không
    // được cấp phiên mới.
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

    // [CONTROLLER] AuthController.finalizeSession · auth/auth.controller.ts
    // session = await this.authService.refresh(refreshToken)

    // Step 12: Đặt refresh token mới vào cookie httpOnly, không trả trong body
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

    // Step 13: Trả access token và csrfToken trong body
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

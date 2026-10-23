//  POST /api/v1/auth/otp/request
//  Đếm hạn mức theo SỐ ĐIỆN THOẠI ở server, sinh mã và chỉ lưu hash; chế độ sms-http lưu mã mới trước, giao qua cổng SMS rồi mới vô hiệu mã cũ, giao lỗi thì vô hiệu chính mã vừa tạo và trả 503 mà không lộ mã.
//  Step 1: Yêu cầu OTP: 5 lần mỗi phút theo IP
//  Step 2: Chặn sớm khi đăng nhập bằng số điện thoại đang tắt
//  Step 3: Hạn mức theo SỐ ĐIỆN THOẠI ở server (không chỉ theo IP)
//  Step 4: Sinh mã ngẫu nhiên an toàn mật mã; chỉ lưu hash
//  Step 5: Chế độ response (dev/e2e): vô hiệu mã cũ rồi trả mã trong body
//  Step 6: sms-http: lưu mã mới TRƯỚC, chưa vô hiệu mã cũ
//  Step 7: Cổng SMS không được chuyển hướng
//  Step 8: Giao lỗi: vô hiệu CHÍNH mã vừa tạo rồi trả 503
//  Step 9: Giao OK mới vô hiệu các mã CŨ HƠN

// [CONTROLLER] AuthController.requestOtp · auth/auth.controller.ts
@Public()
@Controller('auth')
export class AuthController {
  // OtpService
  private readonly logger = new Logger(OtpService.name);
  constructor(
    private readonly authService: AuthService,
    // AuthService
    private readonly otpService: OtpService,
    // OtpService
    @InjectRepository(PhoneOtp) private readonly otpRepo: Repository<PhoneOtp>,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly sender: OtpSenderPort,
    @Optional() private readonly metrics?: AuthMetrics,
  ) {}

  @Post('otp/request')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 5, ttl: minutes(1) } })

  // Step 1: Yêu cầu OTP: 5 lần mỗi phút theo IP
  requestOtp(
    @Body() dto: RequestOtpDto,
  ): Promise<{ code?: string; ttlSeconds: number }> {
    return this.authService.requestOtp(dto.phone);

    // [SERVICE] AuthService.requestOtp · auth/auth.service.ts
    const phone = dto.phone;

    // Step 2: Chặn sớm khi đăng nhập bằng số điện thoại đang tắt
    this.assertPhoneOtpEnabled();
    return this.otpService.requestOtp(phone);

    // [SERVICE] OtpService.requestOtp · auth/services/otp.service.ts
    const delivery = this.config.getOrThrow('AUTH_OTP_DELIVERY', {
      infer: true,
    });
    if (delivery === 'disabled') {
      throw new DomainException(
        AuthErrors.PHONE_OTP_DISABLED,
        'Đăng nhập bằng số điện thoại chưa khả dụng trên môi trường này',
        HttpStatus.FORBIDDEN,
      );
    }
    const perHour = this.config.getOrThrow('AUTH_OTP_REQUESTS_PER_HOUR', {
      infer: true,
    });
    const oneHourAgo = new Date(Date.now() - OTP_RATE_WINDOW_MS);

    // Step 3: Hạn mức theo SỐ ĐIỆN THOẠI ở server (không chỉ theo IP)
    // Hạn mức theo SỐ ĐIỆN THOẠI ở server vì limit chỉ ở frontend là vô nghĩa; throttler theo IP
    // ở controller là lớp thứ hai.
    const recentCount = await this.otpRepo.countBy({
      phone,
      createdAt: MoreThan(oneHourAgo),
    });
    if (recentCount >= perHour) {
      throw new DomainException(
        AuthErrors.OTP_REQUEST_RATE_LIMITED,
        'Yêu cầu OTP quá nhiều, thử lại sau',
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }

    const ttlSeconds = this.config.getOrThrow('AUTH_OTP_TTL_SECONDS', {
      infer: true,
    });

    // Step 4: Sinh mã ngẫu nhiên an toàn mật mã; chỉ lưu hash
    const code = randomInt(0, 10 ** OTP_CODE_DIGITS)
      .toString()
      .padStart(OTP_CODE_DIGITS, '0');

    const newOtp = () =>
      this.otpRepo.create({
        phone,
        codeHash: this.hash(phone, code),
        expiresAt: new Date(Date.now() + ttlSeconds * 1000),
      });

    // Step 5: Chế độ response (dev/e2e): vô hiệu mã cũ rồi trả mã trong body
    if (delivery === 'response') {
      await this.otpRepo.update(
        { phone, consumedAt: IsNull() },
        { consumedAt: new Date() },
      );
      await this.otpRepo.save(newOtp());
      return { code, ttlSeconds };
    }

    // Step 6: sms-http: lưu mã mới TRƯỚC, chưa vô hiệu mã cũ
    // Lưu mã mới TRƯỚC nhưng CHƯA vô hiệu mã cũ: giao lỗi thì người dùng vẫn còn mã cũ dùng được.
    // Giao OK mới vô hiệu các mã CŨ HƠN chứ không phải "mọi mã khác": hai request song song vẫn
    // để lại đúng mã mới nhất. Vô hiệu bắt buộc làm SAU khi giao vì verify chọn mã chưa dùng mới
    // nhất, nên mã cũ còn sống sẽ sống lại ngay khi mã mới được dùng.
    const otp = await this.otpRepo.save(newOtp());
    await this.deliverBySms(otp, phone, code, ttlSeconds);

    // [SERVICE] OtpService.deliverBySms
    const template = this.config.getOrThrow('AUTH_OTP_SMS_TEMPLATE', {
      infer: true,
    });
    const message = template
      .replaceAll('{code}', code)
      .replaceAll('{minutes}', String(Math.max(1, Math.ceil(ttlSeconds / 60))));
    try {
      await this.sender.send({ phone, message, reference: otp.id });

      // [EXTERNAL] HttpOtpSender.send · auth/services/http-otp-sender.ts
      const { phone, message, reference } = { phone, message, reference: otp.id };
      const url = this.config.getOrThrow('AUTH_OTP_SMS_HTTP_URL', {
        infer: true,
      });
      if (url === '') throw new Error('AUTH_OTP_SMS_HTTP_URL chưa được cấu hình');
      const token = this.config.getOrThrow('AUTH_OTP_SMS_HTTP_TOKEN', {
        infer: true,
      });
      const timeoutMs = this.config.getOrThrow('AUTH_OTP_SMS_HTTP_TIMEOUT_MS', {
        infer: true,
      });

      let status: number;
      try {
        const response = await fetch(url, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            ...(token === '' ? {} : { authorization: `Bearer ${token}` }),
          },
          body: JSON.stringify({ to: phone, message, reference }),
          signal: AbortSignal.timeout(timeoutMs),

          // Step 7: Cổng SMS không được chuyển hướng
          // Cổng SMS không được phép chuyển hướng: tránh gửi Bearer token sang một host khác.
          redirect: 'error',
        });
        status = response.status;
      } catch (error) {
        const name = errorName(error);
        // Thông điệp lỗi do adapter tự soạn, không chứa URL, số điện thoại, nội dung hay body
        // phản hồi (có thể lặp lại mã OTP), để caller log được mà không rò rỉ; `cause` giữ lỗi
        // gốc để điều tra nhưng không bao giờ ra tới response hay log.
        throw new Error(
          name === 'TimeoutError' || name === 'AbortError'
            ? `SMS gateway timeout sau ${timeoutMs}ms`
            : 'SMS gateway không kết nối được',
          { cause: error },
        );
      }
      if (status < 200 || status >= 300) {
        throw new Error(`SMS gateway trả HTTP ${status}`);
      }

      this.metrics?.observeOtpDelivery('sent');
    } catch (error) {

      // Step 8: Giao lỗi: vô hiệu CHÍNH mã vừa tạo rồi trả 503
      // Giao lỗi thì vô hiệu CHÍNH mã vừa tạo để không để mã hợp lệ "mồ côi"; timeout mơ hồ cũng
      // coi là lỗi nên SMS tới muộn không dùng được và người dùng yêu cầu lại.
      // Lượt lỗi vẫn tính vào hạn mức theo số điện thoại: cổng chập chờn có thể đã gửi SMS thật,
      // không đếm sẽ mở đường bơm SMS tốn tiền. Lỗi trả 503 (lỗi phụ thuộc ngoài) vì
      // DomainException chỉ dành cho 4xx.
      this.metrics?.observeOtpDelivery('failed');
      await this.otpRepo
        .update(
          { id: otp.id, consumedAt: IsNull() },
          { consumedAt: new Date() },
        )
        .catch(() => undefined);
      this.logger.error(
        `Gửi OTP qua SMS lỗi (reference=${otp.id}): ${error instanceof Error ? error.message : String(error)}`,
      );
      throw new ServiceUnavailableException(
        'Không gửi được mã OTP, thử lại sau',
      );
    }

    await this.otpRepo.update(
      {
        phone,
        consumedAt: IsNull(),

        // Step 9: Giao OK mới vô hiệu các mã CŨ HƠN
        createdAt: LessThan(otp.createdAt ?? new Date()),
      },
      { consumedAt: new Date() },
    );
    return { ttlSeconds };
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/economy/webhooks/payos
//  payOS báo thanh toán thành công: kiểm HMAC, đối chiếu snapshot của đơn, rồi cộng Diamond đúng một lần.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [EXTERNAL] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận webhook công khai của payOS, kiểm chữ ký HMAC trước mọi thứ rồi
//                 giao event đã xác thực cho PayosService.
//    [EXTERNAL]   payOS ký phần data bằng HMAC-SHA256 với checksum key; core-api dựng
//                 lại chuỗi chuẩn và so chữ ký.
//    [SERVICE]    Đối chiếu event với snapshot của đơn rồi ghi sổ Nợ system_iap / Có ví
//                 user, đánh dấu đơn paid trong cùng transaction.
//    [POSTGRES]   Khóa dòng đơn FOR UPDATE; các unique order_code, idempotency_key,
//                 payment_link_id, transaction_id chặn trùng.
//
//  Bảng       payos_payment_orders — đơn nạp: snapshot số tiền VND, số Diamond,
//               order_code, paymentLinkId, trạng thái; nối với đúng 1 transaction.
//             transactions — 1 giao dịch payos_topup, key idempotency do server sinh từ
//               order_code.
//             ledger_entries — bút toán sổ kép: Nợ system_iap, Có user_wallet.
//             wallets — số dư Diamond tăng thêm, bị khóa FOR UPDATE trong lúc ghi sổ.
//             outbox_events — event economy.diamond.credited ghi cùng transaction.
//  Liên quan  Economy (LedgerService) — record: idempotency theo key, khóa ví FOR
//               UPDATE, sổ kép, outbox; phần lõi này có trong luồng Mua VIP.
//             payOS (tạo đơn) — bước trước: POST /economy/payos/orders tạo đơn kèm
//               snapshot giá rồi gọi payOS tạo link thanh toán (chưa chép).
//             Web ví — trang ví gọi GET /economy/payos/orders/:id để biết đơn đã paid
//               chưa; redirect từ payOS không phải bằng chứng thanh toán.
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             chép ngay tại dòng gọi nó, ngang cấp (chỉ khối { } của chính code mới
//             thụt vào), không chép class hay chữ ký hàm (tên hàm nằm ở dòng tiêu đề //
//             [TẦNG]). Constructor gom hết ở đầu, dưới class của điểm vào, chỉ gồm các
//             dependency mà code bên dưới dùng (this.xxx); các dòng const là tham số
//             truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một biểu thức
//             được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên văn trong
//             repo.

// [CONTROLLER] EconomyWebhooksController.payos  ·  economy-webhooks.controller.ts L42–44, L121–134
@Public()
@Controller('economy/webhooks')
export class EconomyWebhooksController {
  constructor(
    private readonly payosClient: PayosClient,
    private readonly payosService: PayosService,
    // PayosService
    @InjectRepository(PayosPaymentOrder)
    private readonly orderRepo: Repository<PayosPaymentOrder>,
    private readonly ledger: LedgerPersistencePort,
  ) {}

  // Step 1: Nhận webhook công khai của payOS (tối đa 120 lần/phút)
  @Post('payos')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 120, ttl: minutes(1) } })
  async payos(@Body() dto: PayosWebhookDto): Promise<{ received: boolean }> {

    // Step 2: Kiểm chữ ký HMAC trước mọi truy vấn hoặc ghi
    // Endpoint công khai nhưng HMAC được kiểm TRƯỚC mọi truy vấn hoặc ghi: sai chữ ký thì 401 và
    // chưa chạm DB.
    const event = this.payosClient.verifyWebhook({
      code: dto.code,
      success: dto.success,
      data: dto.data,
      signature: dto.signature,
      ...(dto.desc ? { desc: dto.desc } : {}),
    });

    // [EXTERNAL] PayosHttpClientAdapter.verifyWebhook  ·  payos-client.ts L123–174
    const payload = {
      code: dto.code,
      success: dto.success,
      data: dto.data,
      signature: dto.signature,
      ...(dto.desc ? { desc: dto.desc } : {}),
    };
    const { checksumKey } = this.credentials();
    const signature = payload['signature'];
    const data = payload['data'];

    // [EXTERNAL] PayosHttpClientAdapter.matchesSignature  ·  payos-client.ts L278–287
    // data = data as Record<string, unknown>
    const expected = this.sign(data, checksumKey);

    // [EXTERNAL] PayosHttpClientAdapter.sign  ·  payos-client.ts L290–296
    // Step 3: Dựng chuỗi chuẩn từ data (sắp key tăng dần) rồi ký HMAC-SHA256
    // Định dạng chuẩn của payOS: sắp key tăng dần, không URI-encode giá trị. Sai định dạng thì
    // HMAC không khớp.
    const canonical = Object.keys(data)
      .sort()
      .map((key) => `${key}=${this.stringifyValue(data[key])}`)
      .join('&');
    return createHmac('sha256', checksumKey).update(canonical).digest('hex');

    // Step 4: So chữ ký tính được với chữ ký payOS gửi (timingSafeEqual)
    const left = Buffer.from(expected, 'hex');
    const right = Buffer.from(signature, 'hex');
    // So chữ ký bằng timingSafeEqual (thời gian so sánh không phụ thuộc nội dung) và kiểm độ dài
    // trước, vì hàm này ném lỗi khi 2 buffer khác độ dài.
    return left.length === right.length && timingSafeEqual(left, right);

    if (
      typeof signature !== 'string' ||
      !data ||
      typeof data !== 'object' ||
      Array.isArray(data) ||
      !this.matchesSignature(
        data as Record<string, unknown>,
        signature,
        checksumKey,
      )
    ) {
      throw new DomainException(
        EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
        'Webhook payOS có chữ ký không hợp lệ',
        HttpStatus.UNAUTHORIZED,
      );
    }
    const event = data as Record<string, unknown>;
    const orderCode = event['orderCode'];
    const amount = event['amount'];
    const currency = event['currency'];
    const paymentLinkId = event['paymentLinkId'];
    const paymentCode = event['code'];
    if (
      !this.isIntegerLike(orderCode) ||
      !this.isIntegerLike(amount) ||
      typeof currency !== 'string' ||
      typeof paymentLinkId !== 'string' ||
      paymentLinkId.length === 0 ||
      typeof paymentCode !== 'string'
    ) {
      throw new DomainException(
        EconomyErrors.PAYOS_WEBHOOK_INVALID,
        'Webhook payOS thiếu dữ liệu thanh toán bắt buộc',
        HttpStatus.BAD_REQUEST,
      );
    }
    return {
      // payOS chỉ ký object data; code và success ở ngoài sửa được mà chữ ký vẫn đúng. Vì vậy
      // quyết định credit chỉ dựa vào code nằm trong data đã verify.
      code: paymentCode,
      success: paymentCode === '00',
      orderCode: String(orderCode),
      amount: String(amount),
      currency,
      paymentLinkId,
    };

    // Step 5: Giao event đã xác thực cho PayosService để cộng Diamond
    await this.payosService.creditVerifiedWebhook(event);

    // [SERVICE] PayosService.creditVerifiedWebhook  ·  payos.service.ts L170–230
    // Step 6: Tìm đơn theo orderCode, đối chiếu snapshot (lệch thì bỏ qua)
    const order = await this.orderRepo.findOneBy({
      orderCode: event.orderCode,
    });
    // Không khớp snapshot của đơn (orderCode, số tiền, VND, mã thành công, paymentLinkId) thì
    // KHÔNG ghi sổ, trả false chứ không ném lỗi.

    // [SERVICE] PayosService.matchesPaidOrder  ·  payos.service.ts L321–334
    // Mọi trường tiền đều so với snapshot lúc tạo đơn, không tin số trong webhook. Đơn đã paid
    // thì không còn khớp nên không ghi sổ lần hai.
    return (
      order.status !== PayosPaymentOrderStatus.Paid &&
      event.success &&
      event.code === PAYOS_SUCCESS_CODE &&
      event.orderCode === order.orderCode &&
      event.amount === order.amountVnd &&
      event.currency === order.currency &&
      event.paymentLinkId === order.paymentLinkId
    );

    if (!order || !this.matchesPaidOrder(order, event)) return false;
    try {

      // Step 7: Ghi sổ Nợ system_iap / Có ví user, idempotent theo order_code
      const result = await this.ledger.record({
        type: TransactionType.PayosTopup,
        // Key do server sinh từ order_code, không lấy từ client: webhook gửi lại hay chạy song
        // song cũng chỉ cộng Diamond một lần.
        idempotencyKey: `payos-credit:${order.orderCode}`,
        actorUserId: order.userId,
        metadata: {
          provider: 'payos',
          orderId: order.id,
          orderCode: order.orderCode,
          packageId: order.packageId,
          amountVnd: order.amountVnd,
          diamonds: order.diamonds,
          paymentLinkId: order.paymentLinkId,
        },
        entries: [
          {
            accountKind: LedgerAccountKind.SystemIap,
            direction: LedgerDirection.Debit,
            amount: BigInt(order.diamonds),
            currency: LedgerCurrency.Diamond,
          },
          {
            accountKind: LedgerAccountKind.UserWallet,
            userId: order.userId,
            direction: LedgerDirection.Credit,
            amount: BigInt(order.diamonds),
            currency: LedgerCurrency.Diamond,
          },
        ],
        withinTransaction: async (manager, txn) => {

          // Step 8: Cùng transaction: khóa dòng đơn, đối chiếu lại snapshot
          // [POSTGRES] Khóa dòng đơn FOR UPDATE rồi đối chiếu lại snapshot ngay trong transaction
          // ghi sổ; lệch thì ném lỗi để rollback cả bút toán.
          const locked = await this.lockOrder(manager, order.id);

          // [SERVICE] PayosService.lockOrder  ·  payos.service.ts L309–319
          const orderId = order.id;
          return manager
            .getRepository(PayosPaymentOrder)
            .createQueryBuilder('payos_order')
            .setLock('pessimistic_write')
            .where('payos_order.id = :orderId', { orderId })
            .getOneOrFail();

          if (!this.matchesPaidOrder(locked, event))
            this.throwWebhookMismatch();

          // Step 9: Đánh dấu đơn paid và nối với giao dịch vừa ghi
          // Không lọc theo status: webhook hợp lệ đến trễ sau khi đơn bị coi là hết hạn vẫn phải
          // nối đơn với bút toán nguyên tử, không để ví đã cộng mà đơn còn expired.
          await manager.update(
            PayosPaymentOrder,
            { id: locked.id },
            {
              status: PayosPaymentOrderStatus.Paid,
              transactionId: txn.id,
            },
          );
        },
      });
      return !result.replayed;
    } catch (err) {
      if (
        err instanceof DomainException &&
        err.code === EconomyErrors.PAYOS_WEBHOOK_MISMATCH
      ) {
        return false;
      }
      throw err;
    }

    // Step 10: Trả { received: true } cho payOS
    return { received: true };
  }
}

// [POSTGRES] CREATE TABLE payos_payment_orders  ·  1756300000000-payos-diamond.ts L19–43
// Unique transaction_id: mỗi đơn chỉ nối được với đúng một transaction credit. order_code lấy từ
// sequence 16 chữ số để vẫn nằm trong số nguyên an toàn của JavaScript (payOS nhận number).
await queryRunner.query(`
  CREATE TABLE payos_payment_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id),
    package_id varchar(64) NOT NULL REFERENCES payos_packages(package_id),
    amount_vnd bigint NOT NULL CHECK (amount_vnd > 0),
    diamonds bigint NOT NULL CHECK (diamonds > 0),
    currency varchar(3) NOT NULL DEFAULT 'VND' CHECK (currency = 'VND'),
    order_code bigint NOT NULL DEFAULT nextval('payos_order_code_seq'),
    idempotency_key varchar(255) NOT NULL,
    request_hash char(64) NOT NULL,
    status varchar(32) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'cancelled', 'expired')),
    payment_link_id varchar(128) NULL,
    checkout_url text NULL,
    qr_code text NULL,
    expires_at timestamptz NOT NULL,
    transaction_id uuid NULL REFERENCES transactions(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_payos_payment_orders_order_code UNIQUE (order_code),
    CONSTRAINT uq_payos_payment_orders_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_payos_payment_orders_payment_link_id UNIQUE (payment_link_id),
    CONSTRAINT uq_payos_payment_orders_transaction_id UNIQUE (transaction_id)
  )
`);

// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/economy/iap/verify
//  Xác thực receipt với Apple hoặc Google ở server, rồi cộng Diamond đúng một lần theo id giao dịch của store.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [EXTERNAL] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận receipt hoặc purchase token từ app (giới hạn 10 lần/phút); user
//                 lấy từ JWT, provider, payload và productId từ body.
//    [SERVICE]    Tra catalog, nhờ verifier hỏi store để xác thực, rồi ghi sổ
//                 idempotent theo id giao dịch của store.
//    [EXTERNAL]   Apple verifyReceipt (production rồi sandbox) hoặc Google Play
//                 Developer API xác nhận giao dịch thật và trả providerTransactionId.
//    [POSTGRES]   Một transaction ghi giao dịch, bút toán, số dư ví và iap_receipts;
//                 unique (provider, provider_transaction_id) chặn credit đôi.
//
//  Bảng       iap_products — catalog: productId của store và provider ứng với bao nhiêu
//               Diamond (chỉ đọc).
//             iap_receipts — 1 dòng cho mỗi receipt đã credit, trỏ tới transaction;
//               unique (provider, provider_transaction_id).
//             transactions — 1 giao dịch iap_purchase, key idempotency do server sinh
//               từ id giao dịch của store.
//             ledger_entries — bút toán sổ kép: Nợ system_iap, Có user_wallet.
//             wallets — số dư Diamond tăng thêm; nếu đang âm (nợ do hoàn tiền trước đó)
//               thì khoản nạp tự bù nợ.
//             outbox_events — event economy.diamond.credited ghi cùng transaction.
//  Liên quan  Economy (LedgerService) — record: idempotency theo key, khóa ví FOR
//               UPDATE, sổ kép, outbox; phần lõi này có trong luồng Mua VIP.
//             Hoàn tiền từ Apple — luồng ngược: khi Apple báo REFUND, receipt này được
//               đảo bút toán và có thể làm số dư âm.
//             Job đối soát — coi receipt đã credit mà không có transaction là lệch, nên
//               transactionId của receipt là bắt buộc.
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             chép ngay tại dòng gọi nó, ngang cấp (chỉ khối { } của chính code mới
//             thụt vào), không chép class hay chữ ký hàm (tên hàm nằm ở dòng tiêu đề //
//             [TẦNG]). Constructor gom hết ở đầu, dưới class của điểm vào, chỉ gồm các
//             dependency mà code bên dưới dùng (this.xxx); các dòng const là tham số
//             truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một biểu thức
//             được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên văn trong
//             repo.

// [CONTROLLER] EconomyController.verifyIap  ·  economy.controller.ts L48–49, L118–132
@Controller('economy')
export class EconomyController {
  constructor(
    private readonly economyService: EconomyService,
    // EconomyService
    @InjectRepository(IapProduct)
    private readonly productRepo: Repository<IapProduct>,
    private readonly ledger: LedgerPersistencePort,
    private readonly iapVerifier: IapVerifier,
    // AppleReceiptApiAdapter
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Nhận receipt từ app (user từ JWT; tối đa 10 lần/phút)
  @Post('iap/verify')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 10, ttl: minutes(1) } })
  verifyIap(@CurrentUser() user: AuthenticatedUser, @Body() dto: VerifyIapDto) {
    return this.economyService.creditFromIap(
      user.userId,
      dto.provider,
      dto.payload,
      dto.productId,
    );

    // [SERVICE] EconomyService.creditFromIap  ·  economy.service.ts L294–364
    const userId = user.userId;
    const provider = dto.provider;
    const payload = dto.payload;
    const productId = dto.productId;

    // Step 2: Tra catalog để ra số Diamond (client không gửi số lượng)
    // Số Diamond lấy từ catalog server theo productId và provider; client không gửi số lượng.
    const product = await this.productRepo.findOneBy({
      productId,
      provider,
      active: true,
    });
    if (!product) {
      throw new DomainException(
        EconomyErrors.IAP_PRODUCT_UNKNOWN,
        `Product ${productId} không tồn tại`,
        HttpStatus.BAD_REQUEST,
      );
    }

    // Step 3: Hỏi Apple hoặc Google xác thực receipt ở server
    // [EXTERNAL] Hỏi store ở SERVER trước mọi ghi sổ. Bản verifier chọn theo env: production
    // thiếu credential dùng bản "disabled", từ chối ngay khi chưa đụng ledger.
    const verified = await this.iapVerifier.verify(
      provider,
      payload,
      productId,
    );

    // [EXTERNAL] AppleReceiptApiAdapter.verify  ·  apple-receipt-api.adapter.ts L39–91
    const input = provider;
    const sharedSecret = this.config.getOrThrow('ECONOMY_APPLE_SHARED_SECRET', {
      infer: true,
    });

    // Step 4: Apple: gọi production trước, receipt sandbox thì gọi lại sandbox
    let body = await this.postVerify(
      APPLE_VERIFY_RECEIPT_URL,
      input.receiptData,
      sharedSecret,
    );

    // [EXTERNAL] AppleReceiptApiAdapter.postVerify  ·  apple-receipt-api.adapter.ts L93–128
    const url = APPLE_VERIFY_RECEIPT_URL;
    const receiptData = input.receiptData;
    let response: Response;
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          'receipt-data': receiptData,
          password: sharedSecret,
        }),
        signal: storeApiAbortSignal(this.config),
      });
    } catch (error) {
      // Timeout hay lỗi mạng của store là lỗi dependency (503), không được đổi thành "receipt
      // sai" của client.
      if (isAbortError(error))
        throw storeProviderUnavailable('Apple Store', 'timeout');
      throw storeProviderUnavailable('Apple Store', 'không kết nối được');
    }
    if (!response.ok) {
      throw storeProviderUnavailable(
        'Apple Store',
        `trả HTTP ${response.status}`,
      );
    }
    try {
      return (await response.json()) as AppleReceiptResponse;
    } catch {
      throw storeProviderUnavailable(
        'Apple Store',
        'trả response không hợp lệ',
      );
    }

    // Quy tắc của Apple: gọi production trước; receipt sandbox (status 21007) thì gọi lại
    // endpoint sandbox.
    if (body.status === APPLE_STATUS_SANDBOX_RECEIPT) {
      body = await this.postVerify(
        APPLE_VERIFY_RECEIPT_SANDBOX_URL,
        input.receiptData,
        sharedSecret,
      );
    }
    if (body.status !== APPLE_STATUS_OK) {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        `Apple từ chối receipt (status ${body.status ?? 'unknown'})`,
        HttpStatus.BAD_REQUEST,
      );
    }

    const purchases = (body.receipt?.in_app ?? []) as ApplePurchase[];
    const matching = purchases.filter(
      (purchase) =>
        purchase.product_id === input.productId &&
        typeof purchase.transaction_id === 'string' &&
        purchase.transaction_id.length > 0,
    );

    // Step 5: Apple: khớp productId và transactionId trong receipt
    // Khớp cả productId lẫn transactionId. Receipt có nhiều giao dịch cùng product mà thiếu
    // transactionId thì từ chối, để không credit nhầm.
    const match = input.transactionId
      ? matching.find(
          (purchase) => purchase.transaction_id === input.transactionId,
        )
      : matching.length === 1
        ? matching[0]
        : undefined;
    if (!match || typeof match.transaction_id !== 'string') {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        input.transactionId
          ? 'Receipt không chứa transactionId/product này'
          : matching.length > 1
            ? 'Receipt consumable chứa nhiều giao dịch; cần transactionId cụ thể'
            : 'Receipt không chứa product này',
        HttpStatus.BAD_REQUEST,
      );
    }
    return { providerTransactionId: match.transaction_id };

    // [EXTERNAL] GooglePlayReceiptApiAdapter.verify  ·  google-play-receipt-api.adapter.ts L28–95
    // productId = provider
    const purchaseToken = payload;

    // Step 6: Google: hỏi Publisher API, chỉ nhận purchaseState = purchased
    let response: Response;
    try {
      const packageName = this.config.getOrThrow(
        'ECONOMY_GOOGLE_PACKAGE_NAME',
        {
          infer: true,
        },
      );
      const accessToken = await getGoogleServiceAccountAccessToken(
        this.config,
        ANDROID_PUBLISHER_SCOPE,
      );
      response = await fetch(
        `${ANDROID_PUBLISHER_API_BASE}/applications/${encodeURIComponent(packageName)}/purchases/products/${encodeURIComponent(productId)}/tokens/${encodeURIComponent(purchaseToken)}`,
        {
          headers: { Authorization: `Bearer ${accessToken}` },
          signal: storeApiAbortSignal(this.config),
        },
      );
    } catch (error) {
      if (error instanceof DomainException) throw error;
      if (isAbortError(error))
        throw storeProviderUnavailable('Google Play', 'timeout');
      throw storeProviderUnavailable('Google Play', 'không kết nối được');
    }

    if (!response.ok) {
      // Store trả 4xx nghĩa là purchase token bị từ chối (lỗi của client); mọi lỗi khác là lỗi
      // dependency (503).
      if (response.status >= 400 && response.status < 500) {
        throw new DomainException(
          EconomyErrors.IAP_RECEIPT_INVALID,
          `Google từ chối purchase token (${response.status})`,
          HttpStatus.BAD_REQUEST,
        );
      }
      throw storeProviderUnavailable(
        'Google Play',
        `trả HTTP ${response.status}`,
      );
    }

    // Chỉ chấp nhận purchaseState = purchased (0); giao dịch đang chờ hay đã hủy chưa được cộng
    // Diamond. orderId của Google làm providerTransactionId.
    let body: { purchaseState?: number; orderId?: string };
    try {
      body = (await response.json()) as {
        purchaseState?: number;
        orderId?: string;
      };
    } catch {
      throw storeProviderUnavailable(
        'Google Play',
        'trả response không hợp lệ',
      );
    }
    if (
      body.purchaseState !== GOOGLE_PURCHASE_STATE_PURCHASED ||
      !body.orderId
    ) {
      throw new DomainException(
        EconomyErrors.IAP_RECEIPT_INVALID,
        'Purchase chưa ở trạng thái purchased',
        HttpStatus.BAD_REQUEST,
      );
    }
    return { providerTransactionId: body.orderId };

    const diamonds = BigInt(product.diamonds);

    // Step 7: Ghi sổ Nợ system_iap / Có ví user, key theo id giao dịch store
    const result = await this.ledger.record({
      type: TransactionType.IapPurchase,
      // Key do server sinh từ id giao dịch của store, không lấy từ header client: gửi lại receipt
      // bao nhiêu lần cũng chỉ credit một lần. Key không chứa userId, nên user khác gửi lại
      // receipt này sẽ lệch request_hash và bị 409.
      idempotencyKey: `iap:${provider}:${verified.providerTransactionId}`,
      actorUserId: userId,
      metadata: {
        provider,
        productId,
        providerTransactionId: verified.providerTransactionId,
        diamonds: product.diamonds,
      },
      entries: [
        {
          accountKind: LedgerAccountKind.SystemIap,
          direction: LedgerDirection.Debit,
          amount: diamonds,
          currency: LedgerCurrency.Diamond,
        },
        {
          accountKind: LedgerAccountKind.UserWallet,
          userId,
          direction: LedgerDirection.Credit,
          amount: diamonds,
          currency: LedgerCurrency.Diamond,
        },
      ],

      // Step 8: Lưu iap_receipts cùng transaction (unique chặn credit đôi)
      // [POSTGRES] Lưu iap_receipts CÙNG transaction với bút toán; unique (provider,
      // provider_transaction_id) chặn credit đôi ở DB. transactionId bắt buộc vì job đối soát coi
      // receipt thiếu transaction là lệch.
      withinTransaction: async (manager, txn) => {
        await manager.save(
          manager.create(IapReceipt, {
            provider,
            providerTransactionId: verified.providerTransactionId,
            userId,
            productId,
            transactionId: txn.id,
            rawPayload: payload,
          }),
        );
      },
    });

    // Step 9: Trả transactionId, số Diamond và cờ replayed
    return {
      transactionId: result.transaction.id,
      diamonds: diamonds.toString(),
      replayed: result.replayed,
    };
  }
}

// [POSTGRES] CREATE TABLE iap_receipts  ·  1752000000000-economy-ledger.ts L116–129
await queryRunner.query(`
  CREATE TABLE iap_receipts (
    id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                 varchar(16)  NOT NULL,
    provider_transaction_id  varchar(255) NOT NULL,
    user_id                  uuid         NOT NULL REFERENCES users(id),
    product_id               varchar(128) NOT NULL,
    status                   varchar(16)  NOT NULL DEFAULT 'credited',
    transaction_id           uuid         NULL REFERENCES transactions(id),
    raw_payload              jsonb        NOT NULL DEFAULT '{}',
    created_at               timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_iap_receipts_provider_txn UNIQUE (provider, provider_transaction_id)
  )
`);

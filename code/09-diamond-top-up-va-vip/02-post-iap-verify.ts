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

// [CONTROLLER] economy.controller.ts · L118–132
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
}

// [SERVICE] economy.service.ts · L294–364
async creditFromIap(
  userId: string,
  provider: IapProvider,
  payload: Record<string, unknown>,
  productId: string,
): Promise<{ transactionId: string; diamonds: string; replayed: boolean }> {
  // Số Diamond lấy từ catalog server theo productId và provider; client không gửi số
  // lượng.
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

  // [EXTERNAL] Hỏi store ở SERVER trước mọi ghi sổ. Bản verifier chọn theo env:
  // production thiếu credential dùng bản "disabled", từ chối ngay khi chưa đụng ledger.
  const verified = await this.iapVerifier.verify(
    provider,
    payload,
    productId,
  );
  const diamonds = BigInt(product.diamonds);

  const result = await this.ledger.record({
    type: TransactionType.IapPurchase,
    // Key do server sinh từ id giao dịch của store, không lấy từ header client: gửi lại
    // receipt bao nhiêu lần cũng chỉ credit một lần. Key không chứa userId, nên user
    // khác gửi lại receipt này sẽ lệch request_hash và bị 409.
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
    // [POSTGRES] Lưu iap_receipts CÙNG transaction với bút toán; unique (provider,
    // provider_transaction_id) chặn credit đôi ở DB. transactionId bắt buộc vì job đối
    // soát coi receipt thiếu transaction là lệch.
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

  return {
    transactionId: result.transaction.id,
    diamonds: diamonds.toString(),
    replayed: result.replayed,
  };
}

// [EXTERNAL] apple-receipt-api.adapter.ts · L39–91, L93–128
async verify(
  input: AppleReceiptVerificationInput,
): Promise<VerifiedPurchase> {
  const sharedSecret = this.config.getOrThrow('ECONOMY_APPLE_SHARED_SECRET', {
    infer: true,
  });
  let body = await this.postVerify(
    APPLE_VERIFY_RECEIPT_URL,
    input.receiptData,
    sharedSecret,
  );
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
  // Khớp cả productId lẫn transactionId. Receipt có nhiều giao dịch cùng product mà
  // thiếu transactionId thì từ chối, để không credit nhầm.
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
}

private async postVerify(
  url: string,
  receiptData: string,
  sharedSecret: string,
): Promise<AppleReceiptResponse> {
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
    // Timeout hay lỗi mạng của store là lỗi dependency (503), không được đổi thành
    // "receipt sai" của client.
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
}

// [EXTERNAL] google-play-receipt-api.adapter.ts · L28–95
async verify(
  productId: string,
  purchaseToken: string,
): Promise<VerifiedPurchase> {
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
    // Store trả 4xx nghĩa là purchase token bị từ chối (lỗi của client); mọi lỗi khác
    // là lỗi dependency (503).
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

  // Chỉ chấp nhận purchaseState = purchased (0); giao dịch đang chờ hay đã hủy chưa
  // được cộng Diamond. orderId của Google làm providerTransactionId.
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
}

// [POSTGRES] 1752000000000-economy-ledger.ts · L116–129
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

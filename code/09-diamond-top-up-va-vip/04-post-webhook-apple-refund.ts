// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/economy/webhooks/apple
//  Apple báo REFUND hoặc REVOKE: kiểm chữ ký JWS, rồi đảo bút toán của lần nạp gốc; số dư có thể âm.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [EXTERNAL] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận App Store Server Notification V2, nhờ verifier kiểm chữ ký, đối
//                 chiếu bundleId, chỉ xử lý REFUND và REVOKE, luôn ACK 200 sau khi
//                 verify.
//    [EXTERNAL]   Apple ký thông báo bằng JWS (ES256) kèm chuỗi chứng chỉ x5c; core-api
//                 xác thực chuỗi lên Apple Root CA G3.
//    [SERVICE]    RefundService tìm receipt gốc rồi nhờ LedgerService.reverse ghi bút
//                 toán đảo và đánh dấu receipt refunded; gọi lại nhiều lần vẫn chỉ hoàn
//                 một lần.
//    [POSTGRES]   Một transaction ghi bút toán đảo, đổi trạng thái giao dịch gốc
//                 (UPDATE có điều kiện), đổi receipt, ghi outbox; số dư ví được phép
//                 âm.
//
//  Bảng       iap_receipts — receipt đã credit: chuyển sang refunded và trỏ
//               refund_transaction_id sang giao dịch đảo.
//             transactions — giao dịch gốc chuyển completed sang reversed; giao dịch
//               mới type reversal trỏ reversal_of về gốc, actor null.
//             ledger_entries — bút toán đảo chiều: Nợ ví user, Có system_iap; bảng chỉ
//               thêm, trigger chặn sửa và xóa.
//             wallets — số dư bị trừ lại, được phép âm (user nợ diamond).
//             outbox_events — event economy.diamond.refunded ghi cùng transaction.
//  Liên quan  Economy (LedgerService.record) — đường ghi sổ idempotent mà reverse gọi
//               vào; xem luồng Mua VIP.
//             Nạp IAP — luồng nạp ban đầu, tạo ra receipt credited mà webhook này đảo
//               lại.
//             Job quét hoàn tiền — IapRefundPollService quét Apple Refund History và
//               Google Voided Purchases làm đường dự phòng, gọi cùng
//               RefundService.refundIapPurchase (chưa chép).
//             Google RTDN — POST /economy/webhooks/google/rtdn chỉ log tín hiệu
//               CANCELED, không tự hoàn tiền (chưa chép).

// [CONTROLLER] economy-webhooks.controller.ts · L56–91
@Post('apple')
@HttpCode(HttpStatus.OK)
@Throttle({ default: { limit: 60, ttl: minutes(1) } })
async appleNotification(
  @Body() dto: AppleServerNotificationDto,
): Promise<{ received: boolean }> {
  // Chữ ký JWS được verify TRƯỚC khi tin bất kỳ trường nào; payload chưa verify không
  // được dùng.
  const payload = await this.appleVerifier.verify(dto.signedPayload);

  // Chain hợp lệ chỉ chứng minh "Apple ký", không chứng minh "ký cho app này" (mọi app
  // dùng chung chuỗi chứng chỉ), nên phải đối chiếu bundleId. Sai bundleId vẫn ACK 200
  // để store không retry dồn dập.
  const expectedBundleId = this.config.getOrThrow('ECONOMY_APPLE_BUNDLE_ID', {
    infer: true,
  });
  if (expectedBundleId && payload.data.bundleId !== expectedBundleId) {
    this.logger.warn(
      `Apple notification bundleId lạ: ${payload.data.bundleId}`,
    );
    return { received: true };
  }

  if (
    !APPLE_REFUND_NOTIFICATION_TYPES.has(payload.notificationType) ||
    !payload.data.signedTransactionInfo
  ) {
    return { received: true };
  }
  const txnInfo = this.appleVerifier.decodeTransactionInfo(
    payload.data.signedTransactionInfo,
  );
  await this.refundService.refundIapPurchase(
    IapProvider.Apple,
    txnInfo.transactionId,
    `apple:${payload.notificationType}${payload.subtype ? `:${payload.subtype}` : ''}`,
  );
  return { received: true };
}

// [EXTERNAL] notification-verifier.ts · L106–138, L146–167, L140–143
async verify(signedPayload: string): Promise<AppleNotificationPayload> {
  const header = decodeProtectedHeader(signedPayload) as { x5c?: string[] };
  if (!header.x5c || header.x5c.length === 0) {
    throw new DomainException(
      EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
      'Thiếu x5c trong JWS header',
      HttpStatus.UNAUTHORIZED,
    );
  }

  // Chuỗi chứng chỉ x5c trong header phải dẫn lên Apple Root CA G3 (nạp từ env, không
  // hardcode) và mọi chứng chỉ còn hạn.
  const certs = header.x5c.map(
    (b64) => new X509Certificate(Buffer.from(b64, 'base64')),
  );
  const trustedRootPem = this.config
    .getOrThrow('ECONOMY_APPLE_ROOT_CA_PEM', { infer: true })
    .replace(/\\n/g, '\n');
  const trustedRoot = new X509Certificate(trustedRootPem);
  if (!this.chainIsTrusted(certs, trustedRoot)) {
    throw new DomainException(
      EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
      'Chain chứng chỉ Apple không hợp lệ',
      HttpStatus.UNAUTHORIZED,
    );
  }

  // Khóa cứng ES256, không đọc alg từ header vì đó là dữ liệu CHƯA xác thực (chống
  // algorithm confusion).
  const leafPublicKey = await importX509(certs[0].toString(), 'ES256');
  const { payload } = await jwtVerify(signedPayload, leafPublicKey, {
    algorithms: ['ES256'],
  });
  return payload as unknown as AppleNotificationPayload;
}

private chainIsTrusted(
  certs: X509Certificate[],
  trustedRoot: X509Certificate,
): boolean {
  const now = new Date();
  if (
    certs.some(
      (c) => now < new Date(c.validFrom) || now > new Date(c.validTo),
    )
  )
    return false;
  for (let i = 0; i < certs.length - 1; i++) {
    if (
      !certs[i].checkIssued(certs[i + 1]) ||
      !certs[i].verify(certs[i + 1].publicKey)
    )
      return false;
  }
  const last = certs[certs.length - 1];
  if (last.fingerprint256 === trustedRoot.fingerprint256) return true;
  return last.checkIssued(trustedRoot) && last.verify(trustedRoot.publicKey);
}

decodeTransactionInfo(signedTransactionInfo: string): AppleTransactionInfo {
  // signedTransactionInfo lồng bên trong đã nằm trong phạm vi chữ ký của payload ngoài,
  // nên chỉ cần decode, không verify lần nữa.
  return decodeJwt(signedTransactionInfo) as unknown as AppleTransactionInfo;
}

// [SERVICE] refund.service.ts · L35–111
async refundIapPurchase(
  provider: IapProvider,
  providerTransactionId: string,
  reason: string,
): Promise<{ outcome: RefundOutcome; transactionId?: string }> {
  const receipt = await this.receiptRepo.findOneBy({
    provider,
    providerTransactionId,
  });
  // Receipt không rõ: có thể của app khác, hoặc refund đến gần như tức thời khi credit
  // chưa xử lý xong. Trả unknown_receipt, job quét định kỳ sẽ bắt lại; không phải lỗi
  // cần retry gấp.
  if (!receipt) {
    this.logger.warn(
      `Refund cho receipt không rõ: ${provider}:${providerTransactionId}`,
    );
    return { outcome: 'unknown_receipt' };
  }
  // Đã hoàn thì bỏ qua: webhook retry hay job quét trùng cũng chỉ hoàn một lần.
  if (receipt.status === IapReceiptStatus.Refunded) {
    return {
      outcome: 'already_refunded',
      transactionId: receipt.refundTransactionId ?? undefined,
    };
  }
  if (!receipt.transactionId) {
    this.logger.error(
      `Receipt ${receipt.id} đã credited nhưng không có transactionId — dữ liệu lệch, cần điều tra`,
    );
    return { outcome: 'receipt_not_credited' };
  }

  try {
    const result = await this.ledger.reverse(
      receipt.transactionId,
      `refund:${provider}:${providerTransactionId}`,
      reason,
      {
        // actor = null: hoàn tiền tự động từ store, không phải hành động của user.
        actorUserId: null,
        outboxEventTypeOverride: 'economy.diamond.refunded',
        // [POSTGRES] Đánh dấu receipt refunded CÙNG transaction với bút toán đảo, nên
        // không có trạng thái ví đã bị đảo mà receipt vẫn credited.
        withinTransaction: async (manager, reversalTxn) => {
          await manager.update(
            IapReceipt,
            { id: receipt.id },
            {
              status: IapReceiptStatus.Refunded,
              refundTransactionId: reversalTxn.id,
            },
          );
        },
      },
    );
    return { outcome: 'refunded', transactionId: result.transaction.id };
  } catch (err) {
    if (
      err instanceof DomainException &&
      err.code === EconomyErrors.TRANSACTION_ALREADY_REVERSED
    ) {
      // Giao dịch gốc đã bị đảo trước (vd admin) nhưng receipt chưa kịp cập nhật: coi
      // là idempotent, tìm đúng giao dịch đã đảo để giữ vết kiểm toán.
      const existingReversal = await this.transactionRepo.findOneBy({
        reversalOf: receipt.transactionId,
      });
      await this.receiptRepo.update(
        { id: receipt.id },
        {
          status: IapReceiptStatus.Refunded,
          refundTransactionId: existingReversal?.id ?? null,
        },
      );
      return {
        outcome: 'already_refunded',
        transactionId: existingReversal?.id,
      };
    }
    throw err;
  }
}

// [SERVICE] ledger.service.ts · L188–274, L427–499
async reverse(
  originalTransactionId: string,
  idempotencyKey: string,
  reason: string,
  opts: {
    actorUserId?: string | null;
    outboxEventTypeOverride?: string;
    withinTransaction?: (
      manager: EntityManager,
      reversalTxn: LedgerTransaction,
    ) => Promise<void>;
  } = {},
): Promise<RecordResult> {
  const original = await this.dataSource
    .getRepository(LedgerTransaction)
    .findOneBy({ id: originalTransactionId });
  if (!original) {
    throw new DomainException(
      EconomyErrors.TRANSACTION_NOT_FOUND,
      'Không tìm thấy giao dịch gốc',
      HttpStatus.NOT_FOUND,
    );
  }
  if (original.status === TransactionStatus.Reversed) {
    throw new DomainException(
      EconomyErrors.TRANSACTION_ALREADY_REVERSED,
      'Giao dịch đã được hoàn trước đó',
      HttpStatus.CONFLICT,
    );
  }

  const entries = await this.dataSource
    .getRepository(LedgerEntry)
    .findBy({ transactionId: original.id });
  const accounts = await this.dataSource
    .getRepository(LedgerAccount)
    .findBy(entries.map((e) => ({ id: e.accountId })));
  const accountById = new Map(accounts.map((a) => [a.id, a]));

  return this.record({
    type: TransactionType.Reversal,
    idempotencyKey,
    actorUserId:
      opts.actorUserId !== undefined
        ? (opts.actorUserId ?? undefined)
        : (original.actorUserId ?? undefined),
    reversalOf: original.id,
    metadata: { reason, originalType: original.type },
    outboxEventTypeOverride: opts.outboxEventTypeOverride,
    entries: entries.map((e) => {
      const account = accountById.get(e.accountId);
      if (!account)
        throw new Error(
          `Ledger account ${e.accountId} biến mất — dữ liệu hỏng`,
        );
      return {
        accountKind: account.kind,
        userId: account.userId ?? undefined,
        // Đảo chiều từng bút toán của giao dịch gốc, không sửa hay xóa giao dịch gốc:
        // trigger ở DB chặn UPDATE và DELETE trên ledger_entries.
        direction:
          e.direction === LedgerDirection.Debit
            ? LedgerDirection.Credit
            : LedgerDirection.Debit,
        amount: BigInt(e.amount),
        currency: e.currency,
      };
    }),
    withinTransaction: async (manager, reversalTxn) => {
      // [POSTGRES] UPDATE có điều kiện status = completed: 2 request cùng đảo thì chỉ 1
      // bên cập nhật được dòng; bên thua ném lỗi và rollback bút toán đảo của mình.
      const marked = await manager.update(
        LedgerTransaction,
        { id: original.id, status: TransactionStatus.Completed },
        { status: TransactionStatus.Reversed },
      );
      if (!marked.affected) {
        throw new DomainException(
          EconomyErrors.TRANSACTION_ALREADY_REVERSED,
          'Giao dịch đã được hoàn trước đó',
          HttpStatus.CONFLICT,
        );
      }
      await opts.withinTransaction?.(manager, reversalTxn);
    },
  });
}

private async applyWalletDeltas(
  manager: EntityManager,
  txn: LedgerTransaction,
  entries: LedgerEntryInput[],
  wallets: Map<string, Wallet>,
  outboxEventTypeOverride?: string,
): Promise<void> {
  for (const [userId, wallet] of wallets) {
    let balanceDelta = 0n;
    let earningsDelta = 0n;
    for (const e of entries) {
      if (e.userId !== userId) continue;
      const signed =
        e.direction === LedgerDirection.Credit ? e.amount : -e.amount;
      if (e.accountKind === LedgerAccountKind.UserWallet)
        balanceDelta += signed;
      if (e.accountKind === LedgerAccountKind.UserEarnings)
        earningsDelta += signed;
    }

    const newBalance = BigInt(wallet.balance) + balanceDelta;
    const newEarnings = BigInt(wallet.earnings) + earningsDelta;
    // Reversal được phép đẩy số dư âm = user nợ diamond (đã tiêu rồi mới bị hoàn).
    // Không kẹp về 0 vì sẽ phá sổ kép; nợ được trừ dần khi user nạp tiếp.
    const mayGoNegative =
      txn.type === TransactionType.Reversal ||
      txn.type === TransactionType.Adjustment;
    if (newBalance < 0n && !mayGoNegative) {
      throw new DomainException(
        EconomyErrors.WALLET_INSUFFICIENT_BALANCE,
        'Không đủ diamond',
        HttpStatus.UNPROCESSABLE_ENTITY,
        { required: (-newBalance).toString() },
      );
    }
    if (newEarnings < 0n) {
      throw new DomainException(
        EconomyErrors.WALLET_INSUFFICIENT_BALANCE,
        'Không đủ điểm quy đổi',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    wallet.balance = newBalance.toString();
    wallet.earnings = newEarnings.toString();
    await manager.save(wallet);

    if (balanceDelta !== 0n) {
      await manager.save(
        manager.create(OutboxEvent, {
          topic: ECONOMY_EVENTS_TOPIC,
          eventType:
            outboxEventTypeOverride ??
            (balanceDelta > 0n
              ? 'economy.diamond.credited'
              : 'economy.diamond.debited'),
          payload: {
            version: 1,
            transactionId: txn.id,
            transactionType: txn.type,
            userId,
            amount: (balanceDelta > 0n
              ? balanceDelta
              : -balanceDelta
            ).toString(),
          },
        }),
      );
    }
  }
}

// [POSTGRES] 1752000000000-economy-ledger.ts · L82–93
await queryRunner.query(`
  CREATE TABLE wallets (
    user_id         uuid PRIMARY KEY REFERENCES users(id),
    -- balance CÓ THỂ âm sau refund/chargeback (user nợ diamond) — không đặt CHECK >= 0
    balance         bigint      NOT NULL DEFAULT 0,
    -- earnings (PTS) chỉ được cộng ở giai đoạn này (chưa có luồng tiêu) → vẫn giữ CHECK >= 0
    earnings        bigint      NOT NULL DEFAULT 0 CHECK (earnings >= 0),
    vip_tier        varchar(16) NULL,
    vip_expires_at  timestamptz NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
  )
`);

// [POSTGRES] 1752000000000-economy-ledger.ts · L76–80
await queryRunner.query(`
  CREATE TRIGGER trg_ledger_entries_append_only
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_ledger_entry_mutation()
`);

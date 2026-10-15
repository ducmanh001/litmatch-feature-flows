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
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] EconomyWebhooksController.appleNotification  ·  economy-webhooks.controller.ts L42–44, L56–91
@Public()
@Controller('economy/webhooks')
export class EconomyWebhooksController {
  private readonly logger = new Logger(EconomyWebhooksController.name);
  constructor(
    private readonly refundService: RefundService,
    private readonly appleVerifier: AppleNotificationVerifier,
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Nhận webhook công khai của Apple (tối đa 60 lần/phút)
  @Post('apple')
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 60, ttl: minutes(1) } })
  async appleNotification(
    @Body() dto: AppleServerNotificationDto,
  ): Promise<{ received: boolean }> {

    // Step 2: Kiểm chữ ký JWS trước khi tin bất kỳ trường nào
    // Chữ ký JWS được verify TRƯỚC khi tin bất kỳ trường nào; payload chưa verify không được
    // dùng.
    const payload = await this.appleVerifier.verify(dto.signedPayload);

      // [EXTERNAL] StoreAppleNotificationVerifier.verify  ·  notification-verifier.ts L106–138
      // RefundService
      private readonly logger = new Logger(RefundService.name);
      constructor(
        // RefundService
        @InjectRepository(IapReceipt)
        private readonly receiptRepo: Repository<IapReceipt>,
        @InjectRepository(LedgerTransaction)
        private readonly transactionRepo: Repository<LedgerTransaction>,
        private readonly ledger: LedgerPersistencePort,
        // LedgerService
        @InjectDataSource() private readonly dataSource: DataSource,
      ) {}

      const signedPayload = dto.signedPayload;
      const header = decodeProtectedHeader(signedPayload) as { x5c?: string[] };
      if (!header.x5c || header.x5c.length === 0) {
        throw new DomainException(
          EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
          'Thiếu x5c trong JWS header',
          HttpStatus.UNAUTHORIZED,
        );
      }

      // Step 3: Xác thực chuỗi x5c lên Apple Root CA G3, rồi verify ES256
      // Chuỗi chứng chỉ x5c trong header phải dẫn lên Apple Root CA G3 (nạp từ env, không
      // hardcode) và mọi chứng chỉ còn hạn.
      const certs = header.x5c.map(
        (b64) => new X509Certificate(Buffer.from(b64, 'base64')),
      );
      const trustedRootPem = this.config
        .getOrThrow('ECONOMY_APPLE_ROOT_CA_PEM', { infer: true })
        .replace(/\\n/g, '\n');
      const trustedRoot = new X509Certificate(trustedRootPem);

        // [EXTERNAL] StoreAppleNotificationVerifier.chainIsTrusted  ·  notification-verifier.ts L146–167
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

      if (!this.chainIsTrusted(certs, trustedRoot)) {
        throw new DomainException(
          EconomyErrors.WEBHOOK_SIGNATURE_INVALID,
          'Chain chứng chỉ Apple không hợp lệ',
          HttpStatus.UNAUTHORIZED,
        );
      }

      // Khóa cứng ES256, không đọc alg từ header vì đó là dữ liệu CHƯA xác thực (chống algorithm
      // confusion).
      const leafPublicKey = await importX509(certs[0].toString(), 'ES256');
      const { payload } = await jwtVerify(signedPayload, leafPublicKey, {
        algorithms: ['ES256'],
      });
      return payload as unknown as AppleNotificationPayload;

    // Step 4: Đối chiếu bundleId, chỉ xử lý REFUND và REVOKE
    // Chain hợp lệ chỉ chứng minh "Apple ký", không chứng minh "ký cho app này" (mọi app dùng
    // chung chuỗi chứng chỉ), nên phải đối chiếu bundleId. Sai bundleId vẫn ACK 200 để store
    // không retry dồn dập.
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
    // signedTransactionInfo lồng bên trong đã nằm trong phạm vi chữ ký của payload ngoài, nên chỉ
    // cần decode, không verify lần nữa.
    const txnInfo = this.appleVerifier.decodeTransactionInfo(
      payload.data.signedTransactionInfo,
    );

    // Step 5: Giao RefundService tìm receipt gốc và hoàn tiền
    await this.refundService.refundIapPurchase(
      IapProvider.Apple,
      txnInfo.transactionId,
      `apple:${payload.notificationType}${payload.subtype ? `:${payload.subtype}` : ''}`,
    );

      // [SERVICE] RefundService.refundIapPurchase  ·  refund.service.ts L35–111
      const provider = IapProvider.Apple;
      const providerTransactionId = txnInfo.transactionId;
      const reason =
        `apple:${payload.notificationType}${payload.subtype ? `:${payload.subtype}` : ''}`;

      // Step 6: Tìm receipt gốc; không rõ hoặc đã hoàn thì dừng
      const receipt = await this.receiptRepo.findOneBy({
        provider,
        providerTransactionId,
      });
      // Receipt không rõ: có thể của app khác, hoặc refund đến gần như tức thời khi credit chưa
      // xử lý xong. Trả unknown_receipt, job quét định kỳ sẽ bắt lại; không phải lỗi cần retry
      // gấp.
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

        // Step 7: Nhờ LedgerService.reverse đảo bút toán của giao dịch gốc
        const result = await this.ledger.reverse(
          receipt.transactionId,
          `refund:${provider}:${providerTransactionId}`,
          reason,
          {
            // actor = null: hoàn tiền tự động từ store, không phải hành động của user.
            actorUserId: null,
            outboxEventTypeOverride: 'economy.diamond.refunded',
            // [POSTGRES] Đánh dấu receipt refunded CÙNG transaction với bút toán đảo, nên không
            // có trạng thái ví đã bị đảo mà receipt vẫn credited.
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

          // [SERVICE] LedgerService.reverse  ·  ledger.service.ts L188–274
          const originalTransactionId = receipt.transactionId;
          const idempotencyKey = `refund:${provider}:${providerTransactionId}`;
          // opts = đối số 4 của lời gọi ở trên
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

          // Step 8: Ghi giao dịch đảo vào sổ; số dư ví được phép âm
          return this.record({
            // Reversal được phép đẩy số dư âm = user nợ diamond (đã tiêu rồi mới bị hoàn). Không
            // kẹp về 0 vì sẽ phá sổ kép; nợ được trừ dần khi user nạp tiếp.
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

              // Step 9: Đánh dấu giao dịch gốc reversed (UPDATE có điều kiện)
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

        return { outcome: 'refunded', transactionId: result.transaction.id };
      } catch (err) {
        if (
          err instanceof DomainException &&
          err.code === EconomyErrors.TRANSACTION_ALREADY_REVERSED
        ) {

          // Step 10: Gốc đã bị đảo trước đó: coi là idempotent, nối receipt
          // Giao dịch gốc đã bị đảo trước (vd admin) nhưng receipt chưa kịp cập nhật: coi là
          // idempotent, tìm đúng giao dịch đã đảo để giữ vết kiểm toán.
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

    // Step 11: Luôn ACK 200 cho Apple sau khi đã verify
    return { received: true };
  }
}

// [POSTGRES] CREATE TABLE wallets  ·  1752000000000-economy-ledger.ts L82–93
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

// [POSTGRES] 1752000000000-economy-ledger.ts  ·  1752000000000-economy-ledger.ts L76–80
await queryRunner.query(`
  CREATE TRIGGER trg_ledger_entries_append_only
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_ledger_entry_mutation()
`);

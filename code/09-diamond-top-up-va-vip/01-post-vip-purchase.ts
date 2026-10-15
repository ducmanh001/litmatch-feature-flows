//  POST /api/v1/economy/vip/purchase
//  Trừ Diamond và gia hạn VIP trong một transaction của sổ kép: idempotent theo key, khóa ví FOR UPDATE, ghi outbox.
//  Step 1: Nhận request (user từ JWT, planId trong body, Idempotency-Key từ header)
//  Step 2: Đọc giá và số ngày của gói từ catalog server
//  Step 3: Dựng bút toán Nợ ví / Có system_revenue, gọi LedgerService.record
//  Step 4: Kiểm sổ kép: mỗi currency tổng Nợ phải bằng tổng Có
//  Step 5: Băm request; key đã có thì trả kết quả cũ, không mở transaction
//  Step 6: Trong transaction, chèn giao dịch (trùng key là replay)
//  Step 7: Tạo hoặc lấy tài khoản sổ, khóa ví FOR UPDATE theo thứ tự userId
//  Step 8: Ghi bút toán, trừ số dư (kiểm đủ tiền) và ghi outbox
//  Step 9: Phần việc riêng của VIP cùng transaction: gia hạn, ghi outbox
//  Step 10: Thua race cùng key: đọc lại dòng của bên thắng, trả như replay
//  Step 11: Service trả hạng và hạn VIP (replay thì không gia hạn lần 2)
//  Step 12: Worker nền: khóa lô outbox chưa gửi, gửi sang Kafka

// [CONTROLLER] EconomyController.purchaseVip  ·  economy.controller.ts L48–49, L134–151
@Controller('economy')
export class EconomyController {
  constructor(
    private readonly economyService: EconomyService,
    // EconomyService
    @InjectRepository(Wallet) private readonly walletRepo: Repository<Wallet>,
    @InjectRepository(VipPlan) private readonly planRepo: Repository<VipPlan>,
    private readonly ledger: LedgerPersistencePort,
    // LedgerService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly metrics: EconomyMetrics,
  ) {}

  // Step 1: Nhận request (user từ JWT, planId trong body, Idempotency-Key từ header)
  @Post('vip/purchase')
  @HttpCode(HttpStatus.OK)
  purchaseVip(
    @CurrentUser() user: AuthenticatedUser,
    @Body() dto: PurchaseVipDto,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<VipPurchaseResultDto> {
    return this.economyService.purchaseVip(
      user.userId,
      dto.planId,
      idempotencyKey,
    );

    // [SERVICE] EconomyService.purchaseVip  ·  economy.service.ts L367–460
    const userId = user.userId;
    const planId = dto.planId;

    // Step 2: Đọc giá và số ngày của gói từ catalog server
    // Giá và số ngày đọc từ catalog server; client chỉ gửi planId, không gửi giá.
    const plan = await this.planRepo.findOneBy({ id: planId, active: true });
    if (!plan) {
      throw new DomainException(
        EconomyErrors.VIP_PLAN_UNKNOWN,
        `Gói VIP ${planId} không tồn tại`,
        HttpStatus.NOT_FOUND,
      );
    }

    let newExpiry = new Date();

    // Step 3: Dựng bút toán Nợ ví / Có system_revenue, gọi LedgerService.record
    const result = await this.ledger.record({
      type: TransactionType.VipPurchase,
      idempotencyKey: `vip:${userId}:${idempotencyKey}`,
      actorUserId: userId,
      // Snapshot giá và thời hạn đã áp dụng; đổi catalog sau này không diễn giải lại giao dịch
      // cũ.
      metadata: {
        planId,
        tier: plan.tier,
        days: plan.days,
        priceDiamond: plan.priceDiamond,
      },
      entries: [
        {
          accountKind: LedgerAccountKind.UserWallet,
          userId,
          direction: LedgerDirection.Debit,
          amount: BigInt(plan.priceDiamond),
          currency: LedgerCurrency.Diamond,
        },
        {
          accountKind: LedgerAccountKind.SystemRevenue,
          direction: LedgerDirection.Credit,
          amount: BigInt(plan.priceDiamond),
          currency: LedgerCurrency.Diamond,
        },
      ],
      // Chạy ở cuối recordInManager, khi ví đã khóa FOR UPDATE: gia hạn VIP và outbox nằm CÙNG
      // transaction với bút toán, nên lỗi thì rollback cả tiền.
      withinTransaction: async (manager) => {
        const wallet = await manager
          .getRepository(Wallet)
          .findOneByOrFail({ userId });
        const base =
          wallet.vipExpiresAt && wallet.vipExpiresAt > new Date()
            ? wallet.vipExpiresAt
            : new Date();
        // Gia hạn cộng dồn: còn hạn thì cộng thêm vào hạn cũ, hết hạn thì tính từ bây giờ.
        newExpiry = new Date(base.getTime() + plan.days * 24 * 3600 * 1000);
        await manager.update(
          Wallet,
          { userId },
          { vipTier: plan.tier, vipExpiresAt: newExpiry },
        );
        await manager.save(
          manager.create(OutboxEvent, {
            topic: ECONOMY_EVENTS_TOPIC,
            eventType: 'economy.vip.purchased',
            payload: {
              version: 1,
              userId,
              planId,
              tier: plan.tier,
              vipExpiresAt: newExpiry.toISOString(),
            },
          }),
        );
      },
    });

    // [SERVICE] LedgerService.record  ·  ledger.service.ts L56–99
    // input = đối số 1 của lời gọi ở trên

    // Step 4: Kiểm sổ kép: mỗi currency tổng Nợ phải bằng tổng Có
    this.validateInput(input);

    // [SERVICE] LedgerService.validateInput  ·  ledger.service.ts L313–320
    this.validateEntries(input.entries);

    // [SERVICE] LedgerService.validateEntries  ·  ledger.service.ts L322–343
    const entries = input.entries;
    if (entries.length < 2)
      throw new Error('Giao dịch double-entry cần tối thiểu 2 bút toán');
    const sums = new Map<string, bigint>();
    for (const e of entries) {
      if (e.amount <= 0n)
        throw new Error('Bút toán phải có amount nguyên dương');
      if (USER_ACCOUNT_KINDS.has(e.accountKind) && !e.userId) {
        throw new Error(`Tài khoản ${e.accountKind} bắt buộc có userId`);
      }
      const delta =
        e.direction === LedgerDirection.Debit ? e.amount : -e.amount;
      sums.set(e.currency, (sums.get(e.currency) ?? 0n) + delta);
    }
    // Sổ kép: tổng Nợ phải bằng tổng Có, kiểm theo TỪNG currency và trước khi ghi bất cứ thứ gì.
    for (const [currency, sum] of sums) {
      if (sum !== 0n) {
        throw new Error(
          `Tổng Nợ != tổng Có cho currency ${currency} — vi phạm bất biến double-entry`,
        );
      }
    }

    if (input.type === TransactionType.Adjustment && !input.actorUserId) {
      throw new Error(
        'Transaction type=adjustment bắt buộc có actorUserId (ai thực hiện sửa sai) để audit',
      );
    }

    // Step 5: Băm request; key đã có thì trả kết quả cũ, không mở transaction
    const requestHash = this.hashRequest(input);

    // [SERVICE] LedgerService.hashRequest  ·  ledger.service.ts L345–361
    // request_hash gồm loại giao dịch, người thực hiện và toàn bộ bút toán: cùng key mà nội dung
    // khác thì bị 409, không được coi là replay.
    const normalized = {
      type: input.type,
      actorUserId: input.actorUserId ?? null,
      reversalOf: input.reversalOf ?? null,
      entries: input.entries.map((e) => ({
        k: e.accountKind,
        u: e.userId ?? null,
        d: e.direction,
        a: e.amount.toString(),
        c: e.currency,
      })),
    };
    return createHash('sha256')
      .update(JSON.stringify(normalized))
      .digest('hex');

    // Đường nhanh cho retry: key đã có thì kiểm tra cùng nội dung rồi trả kết quả cũ, không mở
    // transaction.
    const existing = await this.dataSource
      .getRepository(LedgerTransaction)
      .findOneBy({ idempotencyKey: input.idempotencyKey });
    if (existing) {
      try {
        const transaction = this.assertSameRequest(existing, requestHash);
        this.metrics.record(input.type, 'replayed');
        return { transaction, replayed: true };
      } catch (err) {
        this.metrics.record(input.type, 'failed');
        throw err;
      }
    }

    try {
      const result = await this.dataSource.transaction((manager) =>
        this.recordInManager(manager, input),
      );

      // [SERVICE] LedgerService.recordInManager  ·  ledger.service.ts L101–181
      this.validateInput(input);
      const requestHash = this.hashRequest(input);
      const existing = await manager.findOne(LedgerTransaction, {
        where: { idempotencyKey: input.idempotencyKey },
      });
      if (existing) {
        return {
          transaction: this.assertSameRequest(existing, requestHash),
          replayed: true,
        };
      }

      // Step 6: Trong transaction, chèn giao dịch (trùng key là replay)
      // [POSTGRES] ON CONFLICT DO NOTHING thay vì bắt lỗi unique, vì lỗi đó làm abort cả
      // transaction của caller. Postgres chờ bên thắng commit; không chèn được dòng mới thì đây
      // là replay, không ghi bút toán.
      const insertResult = await manager
        .createQueryBuilder()
        .insert()
        .into(LedgerTransaction)
        .values({
          type: input.type,
          status: TransactionStatus.Completed,
          idempotencyKey: input.idempotencyKey,
          requestHash,
          actorUserId: input.actorUserId ?? null,
          reversalOf: input.reversalOf ?? null,
          metadata: (input.metadata ?? {}) as never,
        })
        .orIgnore()
        .execute();

      const insertedId =
        insertResult.identifiers[0]?.id ?? insertResult.raw[0]?.id;

      const txn = await manager.findOneByOrFail(LedgerTransaction, {
        idempotencyKey: input.idempotencyKey,
      });
      if (!insertedId)
        return {
          transaction: this.assertSameRequest(txn, requestHash),
          replayed: true,
        };

      // Step 7: Tạo hoặc lấy tài khoản sổ, khóa ví FOR UPDATE theo thứ tự userId
      const accounts: LedgerAccount[] = [];
      for (const entry of input.entries)
        accounts.push(await this.resolveAccount(manager, entry));

        // [SERVICE] LedgerService.resolveAccount  ·  ledger.service.ts L377–406
        const isUserAccount = USER_ACCOUNT_KINDS.has(entry.accountKind);
        const where = {
          kind: entry.accountKind,
          userId: isUserAccount ? (entry.userId as string) : IsNull(),
          currency: entry.currency,
        };
        const found = await manager.getRepository(LedgerAccount).findOneBy(where);
        if (found) return found;
        // Tạo tài khoản kiểu ON CONFLICT DO NOTHING, KHÔNG insert-rồi-bắt-lỗi 23505: lỗi đó làm
        // abort cả transaction nên nhánh bắt-lỗi không bao giờ chạy được, bên thua sẽ chết 500.
        await manager.query(
          `INSERT INTO ledger_accounts (kind, user_id, currency) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
          [
            entry.accountKind,
            isUserAccount ? (entry.userId as string) : null,
            entry.currency,
          ],
        );
        return manager.getRepository(LedgerAccount).findOneByOrFail(where);

      // Khóa ví theo thứ tự userId đã sắp xếp để tránh deadlock khi một giao dịch chạm nhiều ví.
      const userIds = [
        ...new Set(
          input.entries.filter((e) => e.userId).map((e) => e.userId as string),
        ),
      ].sort();
      const wallets = new Map<string, Wallet>();
      for (const userId of userIds)
        wallets.set(userId, await this.lockWallet(manager, userId));

        // [SERVICE] LedgerService.lockWallet  ·  ledger.service.ts L408–425
        // [POSTGRES] Ví tạo khi cần rồi khóa FOR UPDATE: đây là điểm tuần tự hóa theo từng user,
        // 2 giao dịch cùng ví không chạy song song.
        await manager.query(
          `INSERT INTO wallets (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`,
          [userId],
        );
        const wallet = await manager
          .getRepository(Wallet)
          .createQueryBuilder('w')
          .setLock('pessimistic_write')
          .where('w.user_id = :userId', { userId })
          .getOne();
        if (!wallet) throw new Error(`Không lock được ví của user ${userId}`);
        return wallet;

      // Step 8: Ghi bút toán, trừ số dư (kiểm đủ tiền) và ghi outbox
      await manager.save(
        input.entries.map((entry, i) =>
          manager.create(LedgerEntry, {
            transactionId: txn.id,
            accountId: accounts[i].id,
            direction: entry.direction,
            amount: entry.amount.toString(),
            currency: entry.currency,
          }),
        ),
      );
      await this.applyWalletDeltas(
        manager,
        txn,
        input.entries,
        wallets,
        input.outboxEventTypeOverride,
      );

      // [SERVICE] LedgerService.applyWalletDeltas  ·  ledger.service.ts L427–499
      // entries = input.entries
      const outboxEventTypeOverride = input.outboxEventTypeOverride;
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
        // Chỉ reversal hoặc adjustment (refund, chargeback, sửa sai) mới được đẩy số dư âm = user
        // nợ diamond; giao dịch tiêu tiền thường thì không.
        const mayGoNegative =
          txn.type === TransactionType.Reversal ||
          txn.type === TransactionType.Adjustment;
        // Kiểm số dư SAU khi đã khóa ví, đúng lúc trừ tiền; không tin số dư đọc ở đầu luồng.
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

        // Outbox: event ghi CÙNG transaction với số dư, relay gửi Kafka sau. Nhờ vậy không có
        // chuyện đổi số dư mà mất event, hay ngược lại.
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

      // Step 9: Phần việc riêng của VIP cùng transaction: gia hạn, ghi outbox
      // Phần việc riêng của từng luồng (gia hạn VIP, lưu receipt...) chạy CÙNG transaction, sau
      // khi ví đã khóa và số dư đã cập nhật.
      await input.withinTransaction?.(manager, txn);
      return { transaction: txn, replayed: false };

      this.metrics.record(input.type, result.replayed ? 'replayed' : 'success');
      return result;
    } catch (err) {
      if (
        isUniqueViolation(err) &&
        violatedConstraint(err, UQ_TRANSACTIONS_IDEMPOTENCY_KEY)
      ) {

        // Step 10: Thua race cùng key: đọc lại dòng của bên thắng, trả như replay
        // [POSTGRES] Kiểm trước chỉ là đường nhanh; nguồn chân lý là unique idempotency_key ở DB.
        // 2 request cùng key chạy song song thì bên thua đọc lại dòng của bên thắng và trả như
        // replay.
        const winner = await this.dataSource
          .getRepository(LedgerTransaction)
          .findOneByOrFail({ idempotencyKey: input.idempotencyKey });
        try {
          const transaction = this.assertSameRequest(winner, requestHash);
          this.metrics.record(input.type, 'replayed');
          return { transaction, replayed: true };
        } catch (assertErr) {
          this.metrics.record(input.type, 'failed');
          throw assertErr;
        }
      }
      this.metrics.record(input.type, 'failed');
      throw err;
    }

    // Step 11: Service trả hạng và hạn VIP (replay thì không gia hạn lần 2)
    // Replay (cùng key) không gia hạn lần 2: chỉ trả trạng thái VIP hiện tại.
    if (result.replayed) {
      const wallet = await this.walletRepo.findOneByOrFail({ userId });
      return {
        transactionId: result.transaction.id,
        tier: plan.tier,
        vipExpiresAt: wallet.vipExpiresAt ?? newExpiry,
        replayed: true,
      };
    }
    return {
      transactionId: result.transaction.id,
      tier: plan.tier,
      vipExpiresAt: newExpiry,
      replayed: false,
    };
  }
}

// [WORKER] OutboxRelayService.flushOnce  ·  outbox-relay.service.ts L28–31, L65–107
@Injectable()
export class OutboxRelayService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(OutboxRelayService.name);
  private readonly job = new ManagedInterval();
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly eventBus: EventBusPort,
  ) {}

  async flushOnce(): Promise<number> {
    return this.job.runExclusive(async () => {
      return await this.dataSource.transaction(async (manager) => {

        // Step 12: Worker nền: khóa lô outbox chưa gửi, gửi sang Kafka
        // [POSTGRES] FOR UPDATE SKIP LOCKED: nhiều pod chạy relay song song không dẫm nhau, pod
        // nào đã khóa lô nào thì pod khác bỏ qua lô đó.
        const events: OutboxEvent[] = await manager
          .getRepository(OutboxEvent)
          .createQueryBuilder('e')
          .setLock('pessimistic_write')
          .setOnLocked('skip_locked')
          .where('e.published_at IS NULL')
          .andWhere('e.dead_lettered_at IS NULL')
          .orderBy('e.created_at', 'ASC')
          .limit(BATCH_SIZE)
          .getMany();
        if (events.length === 0) return 0;

        for (const event of events) {
          try {
            // [KAFKA] Gửi xong mới đánh dấu published_at (lưu ở cuối vòng). Process chết giữa
            // chừng thì event được gửi lại ở vòng sau: giao kiểu at-least-once, consumer phải
            // chịu được trùng.
            await this.eventBus.publish(toEventEnvelope(event));
            event.publishedAt = new Date();
            event.lastError = null;
          } catch (err) {
            event.attempts += 1;
            event.lastError = stringifyError(err);
            this.logger.error(
              {
                eventId: event.id,
                topic: event.topic,
                attempts: event.attempts,
                maxAttempts: this.maxAttempts(),
                err: event.lastError,
              },
              'Outbox event publish failed',
            );
            if (event.attempts >= this.maxAttempts()) {
              await this.deadLetter(event);

              // [WORKER] OutboxRelayService.deadLetter  ·  outbox-relay.service.ts L115–146
              try {
                await this.eventBus.publishDeadLetter(
                  toEventEnvelope(event),
                  event.lastError ?? 'unknown publish failure',
                  event.attempts,
                );
                this.logger.error(
                  {
                    eventId: event.id,
                    topic: event.topic,
                    attempts: event.attempts,
                    deadLetterTopic: `${event.topic}.DLQ`,
                  },
                  'Outbox event moved to dead-letter topic',
                );
              } catch (deadLetterError) {
                this.logger.error(
                  {
                    eventId: event.id,
                    topic: event.topic,
                    attempts: event.attempts,
                    err: stringifyError(deadLetterError),
                  },
                  'Outbox dead-letter publish failed; retaining terminal row for replay',
                );
              } finally {
                // Dù gửi dead-letter được hay không (Kafka vẫn hỏng) vẫn đánh dấu terminal để
                // khỏi lặp vô hạn; dòng còn giữ lại là nguồn để vận hành replay.
                event.deadLetteredAt = new Date();
              }
            }
          }
        }
        await manager.save(events);
        return events.filter((e) => e.publishedAt).length;
      });
    }, 0);
  }
}

// [POSTGRES] CREATE TABLE transactions  ·  1752000000000-economy-ledger.ts L32–45
await queryRunner.query(`
  CREATE TABLE transactions (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    type             varchar(32)  NOT NULL,
    status           varchar(16)  NOT NULL DEFAULT 'completed',
    idempotency_key  varchar(255) NOT NULL,
    request_hash     char(64)     NOT NULL,
    actor_user_id    uuid         NULL REFERENCES users(id),
    reversal_of      uuid         NULL REFERENCES transactions(id),
    metadata         jsonb        NOT NULL DEFAULT '{}',
    created_at       timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_transactions_idempotency_key UNIQUE (idempotency_key)
  )
`);

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

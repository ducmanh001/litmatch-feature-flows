// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/party/rooms/:roomId/gifts
//  Tặng quà trong Party Room: trừ DIA người tặng, cộng PTS người nhận, ghi GiftEvent trong cùng một transaction.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận yêu cầu tặng quà (giới hạn 30 lần/phút) kèm Idempotency-Key bắt
//                 buộc; user lấy từ JWT, giftId và người nhận từ body.
//    [SERVICE]    GiftService đọc giá quà từ DB, kiểm 2 người cùng ở trong phòng và
//                 tính điểm PTS; EconomyService dựng 2 chân sổ kép (DIA và PTS) rồi
//                 giao cho LedgerService ghi.
//    [POSTGRES]   Một transaction ghi giao dịch, bút toán DIA và PTS, số dư và điểm của
//                 ví, outbox, GiftEvent, notification; bước nào lỗi thì rollback tất
//                 cả.
//    [REDIS]      Sau khi commit, phát gift.sent tới từng thành viên đang ở phòng qua
//                 pub/sub để client chạy hiệu ứng.
//
//  Bảng       gifts — catalog quà: giá Diamond là dữ liệu trong DB, server đọc lại đúng
//               lúc tặng.
//             transactions — 1 giao dịch gift_send; key idempotency gift:send:{người
//               tặng}:{key client}.
//             ledger_entries — bút toán nhiều chân: DIA Nợ ví người tặng / Có
//               system_gift_pool; PTS Nợ system_points_mint / Có user_earnings của
//               người nhận.
//             wallets — ví người tặng bị trừ DIA (khóa FOR UPDATE); ví người nhận được
//               cộng điểm PTS vào earnings.
//             gift_events — 1 dòng cho mỗi lần tặng, unique transaction_id; chỉ là
//               snapshot hiển thị, nguồn sự thật là sổ cái.
//             outbox_events — event economy.gift.sent và economy.diamond.debited, ghi
//               cùng transaction.
//             notifications — thông báo gift_received cho người nhận, tạo trong cùng
//               transaction.
//             party_rooms — chỉ đọc: phòng phải đang mở (active) mới tặng được quà.
//             party_room_members — chỉ đọc: người tặng và người nhận phải là thành viên
//               đang ở trong phòng.
//  Redis      realtime:user:{userId} — kênh pub/sub theo người nhận; core-api phát
//               gift.sent cho từng thành viên trong phòng, signaling-gateway chuyển
//               xuống socket.
//  Liên quan  Economy (LedgerService) — record: idempotency theo key, khóa ví FOR
//               UPDATE, check số dư và earnings, outbox; phần lõi này có trong luồng
//               Mua VIP.
//             Party Room — getActiveRoomMembers: danh sách thành viên của phòng đang
//               mở.
//             User — getByIdOrThrow: đọc người nhận để biết có phải guest không.
//             Notification — createWithManager tạo thông báo trong transaction,
//               sendPush gửi push sau commit.
//             signaling-gateway — subscribe kênh realtime của user, đẩy gift.sent xuống
//               client.

// [CONTROLLER] gift.controller.ts · L50–73
@Post('party/rooms/:roomId/gifts')
@HttpCode(HttpStatus.OK)
@Throttle({ default: { limit: 30, ttl: minutes(1) } })
async sendGift(
  @CurrentUser() user: AuthenticatedUser,
  @Param('roomId', ParseUUIDPipe) roomId: string,
  @Body() body: SendGiftDto,
  @IdempotencyKey() idempotencyKey: string,
): Promise<GiftEventDto> {
  const { giftEvent, gift, replayed } = await this.giftService.sendGift(
    user,
    roomId,
    body.giftId,
    body.receiverUserId,
    idempotencyKey,
  );
  return GiftEventDto.from(giftEvent, gift.code, replayed);
}

// [SERVICE] gift.service.ts · L156–264
async sendGift(
  user: AuthenticatedUser,
  roomId: string,
  giftId: string,
  receiverUserId: string,
  idempotencyKey: string,
): Promise<SendGiftResult> {
  if (receiverUserId === user.userId) {
    throw new DomainException(
      GiftErrors.SELF_GIFT_FORBIDDEN,
      'Không tự tặng quà cho chính mình',
      HttpStatus.BAD_REQUEST,
    );
  }

  // Giá đọc từ DB đúng lúc tặng; client chỉ gửi giftId, không bao giờ gửi giá.
  const gift = await this.giftRepo.findOneBy({ id: giftId, active: true });
  if (!gift) {
    throw new DomainException(
      GiftErrors.GIFT_NOT_FOUND,
      'Quà không tồn tại hoặc đã ngừng bán',
      HttpStatus.NOT_FOUND,
    );
  }

  // Kiểm thành viên chỉ là luật nghiệp vụ, KHÔNG phải chốt an toàn tiền: tiền vẫn đúng
  // dù có race rời phòng nhờ transaction và idempotency của Economy.
  const { members } =
    await this.partyRoomService.getActiveRoomMembers(roomId);
  if (!members.some((m) => m.userId === user.userId)) {
    throw new DomainException(
      GiftErrors.SENDER_NOT_IN_ROOM,
      'Bạn không ở trong phòng này',
      HttpStatus.FORBIDDEN,
    );
  }
  if (!members.some((m) => m.userId === receiverUserId)) {
    throw new DomainException(
      GiftErrors.RECEIVER_NOT_IN_ROOM,
      'Người nhận không ở trong phòng này',
      HttpStatus.CONFLICT,
    );
  }

  const receiver = await this.userService.getByIdOrThrow(receiverUserId);
  const ratePercent = this.config.getOrThrow('GIFT_POINTS_RATE_PERCENT', {
    infer: true,
  });
  // Người nhận là guest thì chân PTS = 0 (quà vẫn tặng được); còn lại điểm = giá × tỉ
  // lệ%, làm tròn xuống.
  const pointsAwarded = receiver.isGuest
    ? 0
    : Math.floor((gift.priceDiamond * ratePercent) / 100);

  let notification: Notification | undefined;
  const result = await this.economy.sendGift({
    senderUserId: user.userId,
    receiverUserId,
    priceDiamond: gift.priceDiamond,
    pointsAwarded,
    // Key = gift:send:{người tặng}:{key client}, unique ở DB trên transactions: retry
    // không trừ tiền hai lần.
    idempotencyKey: giftSendIdempotencyKey(user.userId, idempotencyKey),
    // Snapshot tỉ lệ quy đổi vào metadata của giao dịch; đổi cấu hình sau này không
    // diễn giải lại giao dịch cũ.
    metadata: {
      giftId: gift.id,
      giftCode: gift.code,
      roomId,
      pointsRatePercent: ratePercent,
      receiverIsGuest: receiver.isGuest,
    },
    // [POSTGRES] GiftEvent và notification ghi CÙNG transaction với các bút toán: bước
    // nào lỗi thì rollback hết, không có chuyện trừ tiền mà không có quà.
    withinTransaction: async (manager, transactionId) => {
      await manager.save(
        manager.create(GiftEvent, {
          giftId: gift.id,
          roomId,
          senderUserId: user.userId,
          receiverUserId,
          priceDiamond: gift.priceDiamond,
          pointsAwarded,
          pointsRatePercent: ratePercent,
          transactionId,
        }),
      );
      notification = await this.notificationService.createWithManager(
        manager,
        {
          userId: receiverUserId,
          type: NotificationType.GiftReceived,
          payload: {
            roomId,
            senderUserId: user.userId,
            giftCode: gift.code,
            priceDiamond: gift.priceDiamond,
          },
        },
      );
    },
  });

  // Replay trả lại đúng GiftEvent cũ nhờ unique transaction_id, không tạo event mới.
  const giftEvent = await this.giftEventRepo.findOneByOrFail({
    transactionId: result.transactionId,
  });

  // Chỉ lần đầu mới phát realtime và push, và chỉ SAU khi giao dịch tiền đã commit:
  // retry không bắn hiệu ứng lần hai.
  if (!result.replayed) {
    await this.publishGiftSent(roomId, gift, giftEvent);
    if (notification) await this.notificationService.sendPush(notification);
  }
  return { giftEvent, gift, replayed: result.replayed };
}

// [SERVICE] economy.service.ts · L559–666
async sendGift(input: {
  senderUserId: string;
  receiverUserId: string;
  priceDiamond: number;
  pointsAwarded: number;
  idempotencyKey: string;
  metadata: Record<string, unknown>;
  withinTransaction?: (
    manager: EntityManager,
    transactionId: string,
  ) => Promise<void>;
}): Promise<{ transactionId: string; replayed: boolean }> {
  const {
    senderUserId,
    receiverUserId,
    priceDiamond,
    pointsAwarded,
    idempotencyKey,
    metadata,
  } = input;
  if (!Number.isSafeInteger(priceDiamond) || priceDiamond <= 0) {
    throw new Error(
      `sendGift: priceDiamond phải là số nguyên dương, nhận ${priceDiamond}`,
    );
  }
  if (!Number.isSafeInteger(pointsAwarded) || pointsAwarded < 0) {
    throw new Error(
      `sendGift: pointsAwarded phải là số nguyên >= 0, nhận ${pointsAwarded}`,
    );
  }
  // Bất biến chống nhân đôi giá trị: điểm quy đổi không bao giờ vượt giá quà (tỉ lệ
  // dưới 1:1).
  if (pointsAwarded > priceDiamond) {
    throw new Error(
      `sendGift: pointsAwarded (${pointsAwarded}) không được vượt priceDiamond (${priceDiamond})`,
    );
  }
  if (senderUserId === receiverUserId) {
    throw new Error('sendGift: không tự tặng quà cho chính mình');
  }

  const price = BigInt(priceDiamond);
  const points = BigInt(pointsAwarded);
  // Hai chân độc lập, mỗi chân tự cân theo currency: DIA từ người tặng vào quỹ quà, PTS
  // do hệ thống mint cho người nhận. Diamond không bao giờ chuyển thẳng user sang user;
  // phần chênh (giá trừ điểm) ở lại system_gift_pool.
  const entries = [
    {
      accountKind: LedgerAccountKind.UserWallet,
      userId: senderUserId,
      direction: LedgerDirection.Debit,
      amount: price,
      currency: LedgerCurrency.Diamond,
    },
    {
      accountKind: LedgerAccountKind.SystemGiftPool,
      direction: LedgerDirection.Credit,
      amount: price,
      currency: LedgerCurrency.Diamond,
    },
    // Chân PTS chỉ có khi điểm lớn hơn 0: bút toán amount 0 bị validateEntries chặn.
    ...(points > 0n
      ? [
          {
            accountKind: LedgerAccountKind.SystemPointsMint,
            direction: LedgerDirection.Debit,
            amount: points,
            currency: LedgerCurrency.Points,
          },
          {
            accountKind: LedgerAccountKind.UserEarnings,
            userId: receiverUserId,
            direction: LedgerDirection.Credit,
            amount: points,
            currency: LedgerCurrency.Points,
          },
        ]
      : []),
  ];

  const result = await this.ledger.record({
    type: TransactionType.GiftSend,
    idempotencyKey,
    actorUserId: senderUserId,
    metadata: { ...metadata, receiverUserId },
    entries,
    // Event economy.gift.sent riêng vì consumer cần người nhận (event mặc định chỉ nói
    // về ví người tặng); rồi chạy tiếp phần việc của module Gift (GiftEvent,
    // notification) trong cùng transaction.
    withinTransaction: async (manager, txn) => {
      await manager.save(
        manager.create(OutboxEvent, {
          topic: ECONOMY_EVENTS_TOPIC,
          eventType: 'economy.gift.sent',
          payload: {
            version: 1,
            transactionId: txn.id,
            senderUserId,
            receiverUserId,
            priceDiamond: price.toString(),
            pointsAwarded: points.toString(),
            ...metadata,
          },
        }),
      );
      await input.withinTransaction?.(manager, txn.id);
    },
  });
  return { transactionId: result.transaction.id, replayed: result.replayed };
}

// [SERVICE] ledger.service.ts · L322–343, L427–499
private validateEntries(entries: LedgerEntryInput[]): void {
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
  // Mỗi currency phải tự cân (tổng Nợ bằng tổng Có): giao dịch 2 currency được tách
  // thành các chân độc lập, DIA và PTS không bù trừ cho nhau.
  for (const [currency, sum] of sums) {
    if (sum !== 0n) {
      throw new Error(
        `Tổng Nợ != tổng Có cho currency ${currency} — vi phạm bất biến double-entry`,
      );
    }
  }
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
    // [POSTGRES] Ví người tặng đổi balance (DIA), ví người nhận đổi earnings (PTS),
    // cùng một vòng lặp trong cùng transaction: không thể trừ DIA mà không cộng PTS.
    const newEarnings = BigInt(wallet.earnings) + earningsDelta;
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
    // Điểm earnings không bao giờ âm (DB cũng có CHECK earnings >= 0), nên hoàn quà sau
    // khi người nhận đã tiêu điểm sẽ bị chặn ở đây; luồng hoàn quà chưa làm.
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

// [POSTGRES] 1752700000000-party-room-gift.ts · L77–93
// Các bất biến của Economy cũng được khóa ở DB: unique transaction_id (1 GiftEvent ứng
// 1 giao dịch), không tự tặng, điểm không vượt giá. Đây là bản gốc của bảng; sau đó
// thêm video_id và profile_user_id (CHECK đúng 1 ngữ cảnh).
await queryRunner.query(`
  CREATE TABLE gift_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    gift_id             uuid        NOT NULL REFERENCES gifts(id),
    room_id             uuid        NOT NULL REFERENCES party_rooms(id),
    sender_user_id      uuid        NOT NULL REFERENCES users(id),
    receiver_user_id    uuid        NOT NULL REFERENCES users(id),
    price_diamond       int         NOT NULL,
    points_awarded      int         NOT NULL,
    points_rate_percent int         NOT NULL,
    transaction_id      uuid        NOT NULL REFERENCES transactions(id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_gift_events_transaction UNIQUE (transaction_id),
    CONSTRAINT chk_gift_events_no_self_gift CHECK (sender_user_id <> receiver_user_id),
    CONSTRAINT chk_gift_events_points_lte_price CHECK (points_awarded <= price_diamond)
  )
`);

// [SERVICE] gift.service.ts · L475–507
private async publishGiftSent(
  roomId: string,
  gift: Gift,
  event: GiftEvent,
): Promise<void> {
  const envelope: RealtimeEnvelope<GiftSentEventData> = {
    event: RealtimeEvents.GiftSent,
    data: {
      roomId,
      giftEventId: event.id,
      giftCode: gift.code,
      senderUserId: event.senderUserId,
      receiverUserId: event.receiverUserId,
      priceDiamond: event.priceDiamond,
      pointsAwarded: event.pointsAwarded,
      sentAt: event.createdAt.toISOString(),
    },
  };
  try {
    const { members } =
      await this.partyRoomService.getActiveRoomMembers(roomId);
    // [REDIS] Phát gift.sent cho từng thành viên đang ở phòng qua kênh
    // realtime:user:{id}, SAU khi giao dịch commit: client chỉ thấy hiệu ứng khi tiền
    // đã trừ.
    await Promise.all(
      members.map((m) =>
        publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
      ),
    );
  } catch (err) {
    // Phòng vừa đóng giữa chừng thì chỉ mất hiệu ứng realtime, giao dịch quà vẫn hợp
    // lệ.
    this.logger.warn(
      `Fanout gift.sent phòng ${roomId} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

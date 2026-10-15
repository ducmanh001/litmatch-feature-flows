// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/matching/tickets/:id/speedup
//  Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận yêu cầu speed-up kèm header Idempotency-Key; user lấy từ JWT, id
//                 vé từ URL; trả kết quả kèm giá speed-up hiện hành.
//    [SERVICE]    Kiểm tra vé là của chính user và còn queued, tính giá theo hạng VIP,
//                 trừ Diamond qua Economy, cộng boost vào vé, rồi đặt lại điểm trong
//                 Redis.
//    [POSTGRES]   Nguồn sự thật của tổng boost trên vé (priority_boost_ms, cộng dồn
//                 nguyên tử) và của ví Diamond.
//    [REDIS]      Hàng đợi dẫn xuất: điểm của vé trong sorted set được đặt lại thấp hơn
//                 để matcher ghép vé này trước.
//
//  Bảng       match_tickets — vé đang chờ: trạng thái queued, nhóm ghép,
//               priority_boost_ms cộng dồn mỗi lần speed-up.
//             wallets — ví Diamond: bị khóa FOR UPDATE và trừ số dư trong lúc ghi sổ;
//               đọc hạng VIP để tính giá.
//             transactions — 1 giao dịch matching_speedup cho mỗi lần trả tiền, key
//               idempotency unique.
//             ledger_entries — bút toán sổ kép: Nợ ví user, Có system_revenue.
//             outbox_events — event economy.diamond.debited ghi cùng transaction với số
//               dư.
//  Redis      matching:queue:{matchType}:{region}:{ageBand} — sorted set; member =
//               ticketId, score = enqueuedAt + trustPenalty - priorityBoost (nhỏ ghép
//               trước); speed-up ghi lại score nhỏ hơn.
//  Liên quan  Economy — spendDiamond: ghi sổ kép, khóa ví FOR UPDATE, kiểm số dư,
//               idempotent theo key; getActiveVipTier: đọc hạng VIP để tính giá.
//             Matching worker — bước kế tiếp: matcher ZPOPMIN lấy vé điểm thấp nhất nên
//               vé đã boost được ghép trước (xem luồng vào hàng đợi Voice Match).

// ████████████████████████████████████████████████████████████████████████████
// ██  CONTROLLER  ·  Nhận request: user từ JWT, id vé từ URL, Idempotency-Key từ header
// ▸ matching.controller.ts · speedup L134–135, L143–145, L147
@Post('tickets/:id/speedup')
@HttpCode(HttpStatus.OK)
@CurrentUser() user: AuthenticatedUser,
@Param('id', ParseUUIDPipe) id: string,
@IdempotencyKey() idempotencyKey: string,

const result = await this.matchingService.speedup(user, id, idempotencyKey);

// ████████████████████████████████████████████████████████████████████████████
// ██  POSTGRES  ·  Đọc vé; không có thì 404; vé của người khác thì từ chối
// ▸ matching.service.ts · getTicket L229–237
const ticket = await this.ticketRepo.findOneBy({ id: ticketId });
if (!ticket) {
  throw new DomainException(
    MatchingErrors.TICKET_NOT_FOUND,
    'Không tìm thấy ticket',
    HttpStatus.NOT_FOUND,
  );
}
this.assertOwnership(ticket, user);

// ████████████████████████████████████████████████████████████████████████████
// ██  SERVICE  ·  Chỉ speed-up vé đang queued; tính giá; trừ Diamond qua Economy
// ▸ matching.service.ts · speedup L434–440, L442–446, L448–454
if (ticket.status !== MatchTicketStatus.Queued) {
  throw new DomainException(
    MatchingErrors.TICKET_INVALID_TRANSITION,
    `Ticket đang '${ticket.status}', chỉ speed-up được khi đang 'queued'`,
    HttpStatus.CONFLICT,
  );
}

// Giá do server tính ngay trước khi trừ (theo hạng VIP hiện tại): client không tự gửi
// giá và không được hard-code.
const price = await this.getSpeedupPriceDiamondForUser(user.userId);
const boostMs = this.config.getOrThrow('MATCHING_PRIORITY_BOOST_MS', {
  infer: true,
});
// Key = (user, Idempotency-Key của client). Economy dùng nguyên chuỗi này làm key giao
// dịch, nên bấm lại cùng key chỉ trừ tiền một lần.
const prefixedKey = speedupIdempotencyKey(user.userId, idempotencyKey);

// Trừ Diamond TRƯỚC khi đụng vào hàng đợi: Economy tự khóa ví, kiểm số dư và ghi sổ
// idempotent. Bước sau lỗi thì tiền đã trừ, nhưng retry cùng key không trừ lại.
const spend = await this.economy.spendDiamond(
  user.userId,
  TransactionType.MatchingSpeedup,
  price,
  prefixedKey,
  { ticketId, priceDiamond: price },
);

// ████████████████████████████████████████████████████████████████████████████
// ██  POSTGRES  ·  Cộng boost vào vé (nguyên tử), rồi đọc lại tổng boost
// ▸ matching.service.ts · speedup L455–463, L466
// Replay cùng key không cộng boost lần nữa (tránh double-boost); increment nguyên tử,
// chỉ khi vé còn queued. Đánh đổi: trừ tiền và increment là 2 bước riêng, process chết
// giữa chừng thì retry không cộng lại boost.
if (!spend.replayed) {
  await this.ticketRepo.increment(
    { id: ticket.id, status: MatchTicketStatus.Queued },
    'priorityBoostMs',
    boostMs,
  );
}

// Đọc lại tổng boost từ DB để tính score TUYỆT ĐỐI. Boost cộng dồn trong DB nên retry
// hay replay cùng key chỉ sửa score về đúng tổng đã trả tiền, không double-boost.
const fresh = await this.ticketRepo.findOneByOrFail({ id: ticket.id });

// ████████████████████████████████████████████████████████████████████████████
// ██  REDIS  ·  Đặt lại điểm của vé trong hàng đợi (sorted set)
// ▸ matching.service.ts · speedup L467–472
// ▸ matching-redis.provider.ts · matchingShardKey L29 · ticketScore L49–51
// ▸ redis-matching-queue.adapter.ts · enqueue L18
// ZADD XX chỉ cập nhật member đang có: vé đã rời hàng đợi (ghép hoặc hủy) thì bỏ qua,
// không bị thêm lại. Lúc vào hàng đợi thì ngược lại, dùng NX để không đè score đã
// boost.
await this.queue.enqueue(
  shardKeyOfTicket(fresh),
  String(ticketScore(fresh)),
  fresh.id,
  'XX',
);

// ▸ shardKeyOfTicket(fresh)
return `matching:queue:${matchType}:${region}:${ageBand}`;

// ▸ ticketScore(fresh)
// Điểm nhỏ ghép trước: speed-up trừ boost khỏi điểm nên vé nhảy lên đầu hàng.
// trustPenaltyMs là phần cộng thêm để hạ ưu tiên.
return (
  ticket.enqueuedAt.getTime() + ticket.trustPenaltyMs - ticket.priorityBoostMs
);

// ▸ RedisMatchingQueue.enqueue(shard, score, ticketId, mode)
await this.redis.zadd(shard, mode, score, ticketId);

// ████████████████████████████████████████████████████████████████████████████
// ██  CONTROLLER  ·  Trả kết quả kèm giá speed-up hiện hành
// ▸ matching.controller.ts · speedup L148–153
return SpeedupResultDto.from(
  result.transactionId,
  result.replayed,
  result.ticket,
  await this.matchingService.getSpeedupPriceDiamondForUser(user.userId),
);

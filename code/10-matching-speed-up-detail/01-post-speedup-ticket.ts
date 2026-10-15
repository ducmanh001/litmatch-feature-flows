//  POST /api/v1/matching/tickets/:id/speedup
//  Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis.
//  Step 1: Nhận request (user từ JWT, id vé từ URL, Idempotency-Key từ header)
//  Step 2: Kiểm tra vé (đúng chủ vé, còn đang queued)
//  Step 3: Tính giá speed-up và tạo key idempotent
//  Step 4: Trừ Diamond qua Economy
//  Step 5: Cộng boost vào vé trong Postgres
//  Step 6: Đọc lại tổng boost, rồi đặt lại điểm của vé trong Redis
//  Step 7: Ghi điểm mới của vé vào sorted set của shard
//  Step 8: Service trả kết quả về controller
//  Step 9: Controller trả response kèm giá speed-up hiện hành

// [CONTROLLER] MatchingController.speedup  ·  matching.controller.ts L42–43, L134–154
@Controller('matching')
export class MatchingController {
  constructor(
    private readonly matchingService: MatchingService,
    // MatchingService
    @InjectRepository(MatchTicket)
    private readonly ticketRepo: Repository<MatchTicket>,
    private readonly economy: EconomyService,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Inject(MATCHING_QUEUE) private readonly queue: MatchingQueuePort,
    // RedisMatchingQueue
    private readonly redis: Redis,
  ) {}

  // Step 1: Nhận request (user từ JWT, id vé từ URL, Idempotency-Key từ header)
  @Post('tickets/:id/speedup')
  @HttpCode(HttpStatus.OK)
  async speedup(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<SpeedupResultDto> {
    const result = await this.matchingService.speedup(user, id, idempotencyKey);

    // [SERVICE] MatchingService.speedup  ·  matching.service.ts L424–479
    const ticketId = id;

    // Step 2: Kiểm tra vé (đúng chủ vé, còn đang queued)
    const ticket = await this.getTicket(user, ticketId);
    if (ticket.status !== MatchTicketStatus.Queued) {
      throw new DomainException(
        MatchingErrors.TICKET_INVALID_TRANSITION,
        `Ticket đang '${ticket.status}', chỉ speed-up được khi đang 'queued'`,
        HttpStatus.CONFLICT,
      );
    }

    // Step 3: Tính giá speed-up và tạo key idempotent
    // Giá do server tính ngay trước khi trừ (theo hạng VIP hiện tại): client không tự gửi giá và
    // không được hard-code.
    const price = await this.getSpeedupPriceDiamondForUser(user.userId);
    const boostMs = this.config.getOrThrow('MATCHING_PRIORITY_BOOST_MS', {
      infer: true,
    });
    // Key = (user, Idempotency-Key của client). Economy dùng nguyên chuỗi này làm key giao dịch,
    // nên bấm lại cùng key chỉ trừ tiền một lần.
    const prefixedKey = speedupIdempotencyKey(user.userId, idempotencyKey);

    // Step 4: Trừ Diamond qua Economy
    // [POSTGRES] Trừ Diamond TRƯỚC khi đụng vào hàng đợi: Economy tự khóa ví, kiểm số dư và ghi
    // sổ idempotent. Bước sau lỗi thì tiền đã trừ, nhưng retry cùng key không trừ lại.
    const spend = await this.economy.spendDiamond(
      user.userId,
      TransactionType.MatchingSpeedup,
      price,
      prefixedKey,
      { ticketId, priceDiamond: price },
    );

    // Step 5: Cộng boost vào vé trong Postgres
    // [POSTGRES] Replay cùng key không cộng boost lần nữa (tránh double-boost); increment nguyên
    // tử, chỉ khi vé còn queued. Đánh đổi: trừ tiền và increment là 2 bước riêng, process chết
    // giữa chừng thì retry không cộng lại boost.
    if (!spend.replayed) {
      await this.ticketRepo.increment(
        { id: ticket.id, status: MatchTicketStatus.Queued },
        'priorityBoostMs',
        boostMs,
      );
    }

    // Step 6: Đọc lại tổng boost, rồi đặt lại điểm của vé trong Redis
    // Đọc lại tổng boost từ DB để tính score TUYỆT ĐỐI. Boost cộng dồn trong DB nên retry hay
    // replay cùng key chỉ sửa score về đúng tổng đã trả tiền, không double-boost.
    const fresh = await this.ticketRepo.findOneByOrFail({ id: ticket.id });
    await this.queue.enqueue(
      shardKeyOfTicket(fresh),
      String(ticketScore(fresh)),
      fresh.id,
      'XX',
    );

    // [REDIS] RedisMatchingQueue.enqueue  ·  redis-matching-queue.adapter.ts L12–19
    // Step 7: Ghi điểm mới của vé vào sorted set của shard
    const shard = `matching:queue:${fresh.matchType}:${fresh.region}:${fresh.ageBand}`;
    const score = String(
      fresh.enqueuedAt.getTime() + fresh.trustPenaltyMs - fresh.priorityBoostMs,
    );
    // ticketId = fresh.id
    const mode = 'XX';
    // Mỗi shard (loại ghép + vùng + nhóm tuổi) là một sorted set riêng, điểm nhỏ ghép trước:
    // matcher lấy từng shard bằng ZPOPMIN 2 (atomic nên 2 matcher không lấy trùng vé), nên
    // speed-up chỉ cần hạ điểm của vé ngay trong set này. XX: vé đã rời hàng (ghép hoặc hủy) thì
    // không bị thêm lại.
    await this.redis.zadd(shard, mode, score, ticketId);

    // Step 8: Service trả kết quả về controller
    return {
      transactionId: spend.transactionId,
      replayed: spend.replayed,
      ticket: fresh,
    };

    // Step 9: Controller trả response kèm giá speed-up hiện hành
    return SpeedupResultDto.from(
      result.transactionId,
      result.replayed,
      result.ticket,
      await this.matchingService.getSpeedupPriceDiamondForUser(user.userId),
    );
  }
}

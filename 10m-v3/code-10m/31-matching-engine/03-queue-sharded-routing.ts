//  QUEUE ShardedMatchingQueue (khi MATCHING_REDIS_URLS có từ hai Redis trở lên)
//  Mỗi hàng đợi logic được băm (FNV-1a) tới đúng MỘT Redis vật lý nên claim hai vé vẫn nguyên tử; liệt kê shard active gộp từ các Redis còn khoẻ và bỏ mục lỗi thời sau khi đổi topology.
//  Step 1: Mọi thao tác trên một hàng đợi đều đi qua cùng một hàm chọn Redis
//  Step 2: Hàng đợi logic → đúng một Redis bằng hash của tên hàng đợi
//  Step 3: Matcher hỏi mọi Redis xem hàng đợi nào đang có vé
//  Step 4: Chỉ nhận mục active nếu hash hiện tại trỏ đúng Redis đang giữ nó
//  Step 5: Chỉ khi KHÔNG Redis nào trả lời mới là lỗi

// [REDIS] ShardedMatchingQueue.claimPair · matching/redis/sharded-matching-queue.adapter.ts
export class ShardedMatchingQueue implements MatchingQueuePort {
  private readonly logger = new Logger(ShardedMatchingQueue.name);
  constructor(
    private readonly shards: readonly MatchingQueuePort[],
  ) {
    // ShardedMatchingQueue
    if (shards.length === 0)
      throw new Error('Matching requires at least one Redis shard');
  }

  // Step 1: Mọi thao tác trên một hàng đợi đều đi qua cùng một hàm chọn Redis
  claimPair(...args: Parameters<MatchingQueuePort['claimPair']>) {
    return this.shard(args[0]).claimPair(...args);

    // [REDIS] ShardedMatchingQueue.shard
    const key = args[0];

    // Step 2: Hàng đợi logic → đúng một Redis bằng hash của tên hàng đợi
    // Mỗi hàng đợi logic và lệnh claim nguyên tử hai vé của nó phải nằm trên MỘT Redis; hash theo
    // tên hàng đợi nên mọi pod chọn cùng Redis miễn là cùng danh sách có thứ tự. Đổi số hoặc thứ
    // tự Redis là đổi chỗ của hàng đợi, nên cần dừng và dựng lại có phối hợp.
    return this.shards[fnv1aHash(key) % this.shards.length];
  }
}

// [REDIS] ShardedMatchingQueue.listActiveShards
export class ShardedMatchingQueue implements MatchingQueuePort {

  // Step 3: Matcher hỏi mọi Redis xem hàng đợi nào đang có vé
  async listActiveShards(): Promise<string[]> {
    const results = await Promise.allSettled(
      this.shards.map((shard) => shard.listActiveShards()),
    );
    const active = new Set<string>();
    let healthy = 0;
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        // Một Redis lỗi không ngăn matcher đọc hàng đợi active của các Redis còn khoẻ; vé trên
        // Redis lỗi được thăm lại ở vòng quét repair kế tiếp.
        this.logger.warn({ shard: index }, 'Matching Redis shard unavailable');
        return;
      }
      healthy++;
      for (const key of result.value) {

        // Step 4: Chỉ nhận mục active nếu hash hiện tại trỏ đúng Redis đang giữ nó
        // Mục active cũ còn sót lại sau khi đổi topology không bao giờ được phép chuyển hướng một
        // hàng đợi: chỉ nhận mục khi hash hiện tại trỏ đúng Redis đang giữ nó.
        if (this.shard(key) === this.shards[index]) active.add(key);
      }
    });

    // Step 5: Chỉ khi KHÔNG Redis nào trả lời mới là lỗi
    // Chỉ khi không Redis nào trả lời thì mới ném lỗi (matcher bỏ tick); còn ít nhất một Redis
    // khoẻ thì phục vụ phần còn lại.
    if (healthy === 0)
      throw new Error('All Matching Redis shards are unavailable');
    return [...active];
  }
}

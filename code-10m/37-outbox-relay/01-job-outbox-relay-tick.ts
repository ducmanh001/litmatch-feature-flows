//  JOB OutboxRelayService.flushOnce (mỗi ECONOMY_OUTBOX_RELAY_INTERVAL_MS; TẮT mặc định)
//  Mỗi tick bắt kịp backlog qua tối đa MAX_BATCHES lô (100 event/lô) trong ngân sách thời gian: chọn event chưa publish bằng FOR UPDATE SKIP LOCKED, publish Kafka, thử lại có trần rồi dead-letter. Relay đang tắt mặc định và chưa có consumer.
//  Step 1: Khởi động: chỉ lên lịch relay khi ECONOMY_OUTBOX_RELAY_ENABLED bật
//  Step 2: Đăng ký interval; mỗi tick chạy flushOnce
//  Step 3: Bắt kịp backlog bằng nhiều lô trong một tick, có trần số lô và thời gian
//  Step 4: Một lô là một transaction giữ khoá hàng
//  Step 5: Lấy tối đa 100 event chưa publish bằng FOR UPDATE SKIP LOCKED
//  Step 6: Chỉ bắt đầu attempt mới khi còn ngân sách và chưa shutdown
//  Step 7: Publish lên Kafka rồi đánh dấu đã publish
//  Step 8: Publish lỗi: tăng attempts và lưu lỗi lên dòng đã khoá
//  Step 9: Đủ số lần thử thì chuyển dead-letter
//  Step 10: Lưu trạng thái cả lô rồi commit, nhả khoá
//  Step 11: Lô ngắn thì kết thúc tick

// [WORKER] OutboxRelayService.onApplicationBootstrap · economy/jobs/outbox-relay.service.ts
@Injectable()
export class OutboxRelayService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(OutboxRelayService.name);
  private readonly job = new ManagedInterval();
  private stopping = false;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly scheduler: SchedulerRegistry,
    private readonly eventBus: EventBusPort,
  ) {}

  // Step 1: Khởi động: chỉ lên lịch relay khi ECONOMY_OUTBOX_RELAY_ENABLED bật
  async onApplicationBootstrap(): Promise<void> {
    this.stopping = false;
    // TẮT mặc định ở mọi profile (env, K8s base, compose production, hosted) và repo chưa có
    // module nào subscribe topic này: tắt thì event outbox giữ published_at = NULL và bảng tích
    // luỹ, không tự bật Kafka/consumer và không xoá event chưa publish. Trước khi bật ở
    // production phải chốt consumer đầu tiên.
    if (
      !this.config.getOrThrow('ECONOMY_OUTBOX_RELAY_ENABLED', { infer: true })
    ) {
      this.logger.warn(
        'Outbox relay is disabled: unpublished events are retained and require a consumer/relay before scale',
      );
      return;
    }

    // Step 2: Đăng ký interval; mỗi tick chạy flushOnce
    // Không có khoá cluster-singleton như job đối soát: mọi pod đều chạy relay và FOR UPDATE SKIP
    // LOCKED chia event giữa các pod, nên relay không bị trần "một pod mỗi tick".
    this.job.start(this.scheduler, {
      jobName: RELAY_JOB,
      intervalMs: this.config.getOrThrow('ECONOMY_OUTBOX_RELAY_INTERVAL_MS', {
        infer: true,
      }),
      task: () => this.flushOnce(),
      logger: this.logger,
      errorMessage: 'Outbox relay lỗi',
    });

    // [WORKER] OutboxRelayService.flushOnce
    return this.job.runExclusive(async () => {
      const deadline =
        Date.now() +
        this.config.getOrThrow('ECONOMY_OUTBOX_RELAY_TICK_BUDGET_MS', {
          infer: true,
        });
      const maxBatches = this.config.getOrThrow(
        'ECONOMY_OUTBOX_RELAY_MAX_BATCHES_PER_TICK',
        { infer: true },
      );
      let published = 0;

      // Step 3: Bắt kịp backlog bằng nhiều lô trong một tick, có trần số lô và thời gian
      // Mặc định tối đa 10 lô × 100 event mỗi tick và bị chặn bởi TICK_BUDGET_MS (1000 ms):
      // backlog được rút dần qua nhiều tick mà mỗi tick vẫn có trần. stopping được đặt true ở
      // onApplicationShutdown nên shutdown cũng dừng vòng lặp.
      for (let batch = 0; batch < maxBatches && !this.stopping; batch++) {
        if (batch > 0 && Date.now() >= deadline) break;
        const count = await this.flushBatch(deadline);

        // [POSTGRES] OutboxRelayService.flushBatch
        // Step 4: Một lô là một transaction giữ khoá hàng
        // Event được publish TRONG lúc transaction giữ khoá hàng và chỉ được đánh dấu
        // published_at khi commit: crash giữa publish và commit làm event được publish lại ở tick
        // sau (at-least-once, không mất event), nên consumer phải khử trùng theo id event.
        return this.dataSource.transaction(async (manager) => {

          // Step 5: Lấy tối đa 100 event chưa publish bằng FOR UPDATE SKIP LOCKED
          // Các pod relay chạy song song lấy những hàng chưa bị pod khác khoá thay vì chờ hay dẫm
          // nhau, không deadlock. Điều kiện và thứ tự khớp index một phần
          // idx_outbox_events_relayable (created_at, chưa publish, chưa dead-letter).
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

          let attempted = 0;
          for (const event of events) {

            // Step 6: Chỉ bắt đầu attempt mới khi còn ngân sách và chưa shutdown
            // Attempt đầu của lô luôn được thử. Ngân sách chỉ chặn attempt MỚI: publish đang bay
            // không bị huỷ và vẫn chờ deadline của driver (EVENT_BUS_KAFKA_REQUEST_TIMEOUT_MS,
            // mặc định 5000 ms).
            if (this.stopping || (attempted > 0 && Date.now() >= deadline)) break;
            attempted++;
            try {

              // Step 7: Publish lên Kafka rồi đánh dấu đã publish
              await this.eventBus.publish(toEventEnvelope(event));
              event.publishedAt = new Date();
              event.lastError = null;
            } catch (err) {

              // Step 8: Publish lỗi: tăng attempts và lưu lỗi lên dòng đã khoá
              // Event Ở LẠI outbox với attempts + 1 và lastError ngay trên dòng đã khoá, tick sau
              // thử lại; chỉ khi attempts đạt ECONOMY_OUTBOX_MAX_ATTEMPTS (mặc định 5) mới bị
              // chuyển dead-letter.
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

                // Step 9: Đủ số lần thử thì chuyển dead-letter
                await this.deadLetter(event);

                // [WORKER] OutboxRelayService.deadLetter
                try {
                  // Publish bản sao sang topic .DLQ nhưng GIỮ dòng outbox gốc: đó là nguồn replay
                  // cho người vận hành, kể cả khi broker hay DLQ đang hỏng.
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
                  // Luôn đánh dấu dead-letter, kể cả khi publish DLQ cũng lỗi: tránh vòng thử vô
                  // hạn. Dòng giữ nguyên payload để replay tay và không bao giờ tự xoá.
                  event.deadLetteredAt = new Date();
                }
              }
              // Lỗi broker đầu tiên kết thúc lô: nếu thử tiếp, một lần broker chết sẽ tiêu hết
              // 100 timeout mạng trong khi transaction giữ khoá DB.
              break;
            }
          }

          // Step 10: Lưu trạng thái cả lô rồi commit, nhả khoá
          await manager.save(events);
          return events.filter((e) => e.publishedAt).length;
        });

        published += count;

        // Step 11: Lô ngắn thì kết thúc tick
        // Lô ngắn có thể do hết event, hết ngân sách, shutdown hoặc broker lỗi: dừng tick thay vì
        // đốt thêm lượt retry và dead-letter trong cùng một tick.
        if (count < BATCH_SIZE) break;
      }
      return published;
    }, 0);

    this.logger.log('Outbox relay đã bật');
  }
}

// [KAFKA] KafkaEventBusAdapter.publish (producer.send) và getProducer (cấu hình idempotent) · common/events/kafka-event-bus.adapter.ts
await producer.send({
  topic: event.topic,
  messages: [
    {
      // Id event ổn định nằm trong message (value là cả envelope) để consumer khử trùng: relay
      // chỉ bảo đảm at-least-once.
      key: event.key,
      value: JSON.stringify(event),
    },
  ],
});

const producer = this.kafka.producer({
  // Producer idempotent với 1 request in-flight giữ thứ tự theo key và làm retry của producer an
  // toàn; outbox vẫn chỉ at-least-once khi DB crash.
  idempotent: true,
  maxInFlightRequests: 1,
  retry: {
    retries: this.config.getOrThrow('EVENT_BUS_KAFKA_RETRIES', {
      infer: true,
    }),
  },
});

// [POSTGRES] outbox_events: trạng thái dead-letter và index của relay · database/migrations/1757000000000-outbox-reliability.ts
await queryRunner.query(
  // dead_lettered_at là trạng thái cuối của relay: event vượt số lần thử được giữ nguyên payload
  // để người vận hành xem hay replay, và relay không lấy lại nó.
  `ALTER TABLE outbox_events ADD COLUMN dead_lettered_at timestamptz NULL`,
);
await queryRunner.query(
  `ALTER TABLE outbox_events ADD COLUMN last_error text NULL`,
);
await queryRunner.query(
  // Index một phần đúng theo điều kiện của relay (chưa publish và chưa dead-letter, theo
  // created_at): lô tiếp theo là các khoá đầu của index.
  `CREATE INDEX idx_outbox_events_relayable ON outbox_events(created_at) WHERE published_at IS NULL AND dead_lettered_at IS NULL`,
);

//  JOB BroadcastDispatchJob.runOnce (mỗi ADMIN_BROADCAST_JOB_INTERVAL_MS)
//  Nhặt broadcast queued/running cũ nhất, đọc một trang user theo keyset, tạo thông báo và đẩy cursor trong CÙNG transaction, rồi push sau commit.
//  Step 1: Một tick: xử lý tối đa ADMIN_BROADCAST_MAX_BATCHES_PER_TICK lô
//  Step 2: Lấy broadcast queued/running cũ nhất
//  Step 3: Xử lý một lô (mặc định 500 người)
//  Step 4: Đọc một trang user theo keyset (id lớn hơn cursor), không OFFSET
//  Step 5: Lọc theo audience (tất cả / VIP / không VIP)
//  Step 6: Khoá dòng broadcast và kiểm cursor còn đúng (runner khác đẩy đi trước thì bỏ lô này)
//  Step 7: Tạo thông báo hàng loạt cho lô này
//  Step 8: Đẩy cursor và cộng recipient_count TRONG CÙNG transaction với thông báo
//  Step 9: Push SAU commit với số lời gọi đồng thời có trần
//  Step 10: Lỗi một lô: tăng failure_count, quá ngưỡng thì failed; cursor không mất nên tick sau thử lại

// [WORKER] BroadcastDispatchJob.runOnce · admin/jobs/broadcast-dispatch.job.ts
@Injectable()
export class BroadcastDispatchJob
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(BroadcastDispatchJob.name);
  private readonly job = new ManagedInterval();
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly userService: UserService,
    private readonly economyService: EconomyService,
    private readonly notificationService: NotificationService,
    private readonly auditLogService: AuditLogService,
  ) {}

  // Step 1: Một tick: xử lý tối đa ADMIN_BROADCAST_MAX_BATCHES_PER_TICK lô
  async runOnce(): Promise<number> {
    return this.job.runExclusive(async () => {
      const batchSize = this.config.getOrThrow('ADMIN_BROADCAST_BATCH_SIZE', {
        infer: true,
      });
      const maxBatches = this.config.getOrThrow(
        'ADMIN_BROADCAST_MAX_BATCHES_PER_TICK',
        { infer: true },
      );
      let processed = 0;
      while (processed < maxBatches) {

        // Step 2: Lấy broadcast queued/running cũ nhất
        const broadcast = await this.nextOpenBroadcast();

        // [WORKER] BroadcastDispatchJob.nextOpenBroadcast
        return this.dataSource.getRepository(NotificationBroadcast).findOne({
          where: { status: In([BroadcastStatus.Queued, BroadcastStatus.Running]) },
          order: { createdAt: 'ASC', id: 'ASC' },
        });

        if (broadcast === null) break;
        try {

          // Step 3: Xử lý một lô (mặc định 500 người)
          const advanced = await this.processBatch(broadcast, batchSize);

          // [WORKER] BroadcastDispatchJob.processBatch
          // Step 4: Đọc một trang user theo keyset (id lớn hơn cursor), không OFFSET
          const userIds = await this.userService.listActiveUserIdsPage({
            afterId: broadcast.cursorUserId,
            createdAtOrBefore: broadcast.createdAt,
            limit: batchSize,
          });

          // Step 5: Lọc theo audience (tất cả / VIP / không VIP)
          const recipientIds = await this.filterAudience(broadcast.audience, userIds);

          // [WORKER] BroadcastDispatchJob.filterAudience
          const audience = broadcast.audience;
          if (audience === BroadcastAudience.All) return [...userIds];
          const vip = await this.economyService.filterActiveVipUserIds(userIds);
          return userIds.filter((userId) =>
            audience === BroadcastAudience.Vip ? vip.has(userId) : !vip.has(userId),
          );

          // Trang ngắn hơn lô nghĩa là đã hết user: hoàn tất ngay trong cùng transaction, khỏi
          // tốn thêm một tick rỗng.
          const done = userIds.length < batchSize;
          const nextCursor =
            userIds.length > 0 ? userIds[userIds.length - 1] : broadcast.cursorUserId;

          const created = await this.dataSource.transaction(async (manager) => {

            // Step 6: Khoá dòng broadcast và kiểm cursor còn đúng (runner khác đẩy đi trước thì
            // bỏ lô này)
            // [POSTGRES] Khoá dòng broadcast và kiểm cursor ngay trong transaction (READ
            // COMMITTED): hai pod cùng nhặt một broadcast thì chỉ một bên ghi được lô này, bên
            // kia thấy cursor đã đổi và bỏ qua; huỷ broadcast cũng thấy ngay ở đây.
            const [locked] = await manager.query<Array<{ id: string }>>(
              `SELECT id FROM notification_broadcasts
                WHERE id = $1
                  AND status IN ('queued', 'running')
                  AND cursor_user_id IS NOT DISTINCT FROM $2::uuid
                  FOR UPDATE`,
              [broadcast.id, broadcast.cursorUserId],
            );
            if (locked === undefined) return null;

            // Step 7: Tạo thông báo hàng loạt cho lô này
            const notifications =
              await this.notificationService.createBroadcastWithManager(
                manager,
                recipientIds,
                { title: broadcast.title, body: broadcast.body },
              );

            // Step 8: Đẩy cursor và cộng recipient_count TRONG CÙNG transaction với thông báo
            // Thông báo và cursor cùng một transaction: không có trạng thái "đã tạo thông báo mà
            // cursor chưa đi" hay ngược lại, nên crash giữa chừng rồi resume không gửi đôi và
            // không bỏ sót (in-app đúng đúng một lần).
            const [rows] = await manager.query<
              [Array<{ recipientCount: number }>, number]
            >(
              `UPDATE notification_broadcasts
                  SET cursor_user_id = $2::uuid,
                      recipient_count = recipient_count + $3::int,
                      status = $4::varchar,
                      started_at = COALESCE(started_at, now()),
                      completed_at = CASE WHEN $5::boolean THEN now() ELSE NULL END,
                      failure_count = 0,
                      last_error = NULL,
                      updated_at = now()
                WHERE id = $1
                RETURNING recipient_count AS "recipientCount"`,
              [
                broadcast.id,
                nextCursor,
                notifications.length,
                done ? BroadcastStatus.Completed : BroadcastStatus.Running,
                done,
              ],
            );
            if (done) {
              await this.auditLogService.record(
                {
                  actorUserId: broadcast.actorUserId,
                  action: 'notification.broadcast.completed',
                  targetType: 'notification_broadcast',
                  targetId: broadcast.id,
                  metadata: {
                    audience: broadcast.audience,
                    recipientCount: rows[0]?.recipientCount ?? notifications.length,
                    title: broadcast.title,
                  },
                },
                manager,
              );
            }
            return notifications;
          });
          if (created === null) return false;

          // Step 9: Push SAU commit với số lời gọi đồng thời có trần
          await this.pushAll(created);

          // [WORKER] BroadcastDispatchJob.pushAll
          // notifications = created
          let next = 0;
          // sendPush không bao giờ throw (best-effort) nên chỉ cần giới hạn số lời gọi đồng thời
          // tới nhà cung cấp. Push vẫn đi từng người nhận: ở 10 triệu người cần topic/batch của
          // nhà cung cấp (chưa làm).
          const worker = async (): Promise<void> => {
            while (next < notifications.length) {
              const notification = notifications[next];
              next += 1;
              await this.notificationService.sendPush(notification);
            }
          };
          await Promise.all(
            Array.from(
              { length: Math.min(PUSH_CONCURRENCY, notifications.length) },
              worker,
            ),
          );

          return true;

          // Runner khác đã đẩy cursor đi trước: nhường tick sau đọc lại trạng thái mới, không ghi
          // gì.
          if (!advanced) break;
          processed += 1;
        } catch (error) {

          // Step 10: Lỗi một lô: tăng failure_count, quá ngưỡng thì failed; cursor không mất nên
          // tick sau thử lại
          await this.recordFailure(broadcast, error);

          // [WORKER] BroadcastDispatchJob.recordFailure
          const message = (
            error instanceof Error ? error.message : String(error)
          ).slice(0, 500);
          this.logger.error(
            { broadcastId: broadcast.id, err: message },
            'Lô broadcast lỗi — thử lại ở tick sau',
          );
          try {
            await this.dataSource.transaction(async (manager) => {
              const [rows] = await manager.query<
                [Array<{ status: BroadcastStatus }>, number]
              >(
                `UPDATE notification_broadcasts
                    SET failure_count = failure_count + 1,
                        last_error = $2,
                        status = CASE WHEN failure_count + 1 >= $3::int THEN 'failed' ELSE status END,
                        updated_at = now()
                  WHERE id = $1 AND status IN ('queued', 'running')
                  RETURNING status`,
                [broadcast.id, message, MAX_CONSECUTIVE_FAILURES],
              );
              if (rows[0]?.status !== BroadcastStatus.Failed) return;
              await this.auditLogService.record(
                {
                  actorUserId: broadcast.actorUserId,
                  action: 'notification.broadcast.failed',
                  targetType: 'notification_broadcast',
                  targetId: broadcast.id,
                  metadata: {
                    audience: broadcast.audience,
                    title: broadcast.title,
                    error: message,
                  },
                },
                manager,
              );
            });
          } catch (recordError) {
            this.logger.error(
              { broadcastId: broadcast.id, err: `${recordError}` },
              'Không ghi được trạng thái lỗi của broadcast',
            );
          }

          break;
        }
      }
      return processed;
    }, 0);
  }
}

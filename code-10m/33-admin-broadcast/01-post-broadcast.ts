//  POST /api/v1/admin/notifications/broadcast
//  Ghi một dòng broadcast ở trạng thái queued kèm audit (202); Idempotency-Key làm bấm hai lần chỉ tạo một broadcast.
//  Step 1: Nhận yêu cầu broadcast (chỉ admin, Idempotency-Key bắt buộc); trả 202 vì chỉ xếp hàng
//  Step 2: Cùng (người gửi, key) đã có broadcast thì replay thay vì tạo mới
//  Step 3: Cùng key nhưng khác nội dung thì 409 (key đã dùng cho broadcast khác)
//  Step 4: Ghi MỘT dòng queued: request không gửi gì cả
//  Step 5: Ghi audit cùng transaction
//  Step 6: Hai request đồng thời cùng key: unique chặn bên thua, bên thua đọc lại bản của bên thắng

// [CONTROLLER] AdminController.broadcastNotification · admin/admin.controller.ts
@RequireRoles(Roles.Admin, Roles.Moderator)
@UseGuards(AdminPermissionGuard)
@Controller('admin')
export class AdminController {
  constructor(
    private readonly adminService: AdminService,
    // AdminService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly auditLogService: AuditLogService,
  ) {}

  // Step 1: Nhận yêu cầu broadcast (chỉ admin, Idempotency-Key bắt buộc); trả 202 vì chỉ xếp hàng
  @Post('notifications/broadcast')
  @RequireAdminPermission(AdminPermission.ManageConfig)
  @HttpCode(HttpStatus.ACCEPTED)
  broadcastNotification(
    @CurrentUser() actor: AuthenticatedUser,
    @Body() body: BroadcastNotificationDto,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<BroadcastNotificationResultDto> {
    return this.adminService.broadcastNotification(
      actor.userId,
      body,
      idempotencyKey,
    );

    // [SERVICE] AdminService.broadcastNotification · admin/admin.service.ts
    const actorUserId = actor.userId;
    const input = body;

    // Step 2: Cùng (người gửi, key) đã có broadcast thì replay thay vì tạo mới
    const existing = await this.findBroadcastByKey(actorUserId, idempotencyKey);

    // [SERVICE] AdminService.findBroadcastByKey
    return this.dataSource
      .getRepository(NotificationBroadcast)
      .findOneBy({ actorUserId, idempotencyKey });

    if (existing !== null) return this.replayBroadcast(existing, input);

    // [SERVICE] AdminService.replayBroadcast
    if (
      existing.title !== input.title ||
      existing.body !== input.body ||
      existing.audience !== input.audience
    ) {

      // Step 3: Cùng key nhưng khác nội dung thì 409 (key đã dùng cho broadcast khác)
      throw new DomainException(
        AdminErrors.BROADCAST_IDEMPOTENCY_CONFLICT,
        'Idempotency-Key đã dùng cho một broadcast khác',
        HttpStatus.CONFLICT,
      );
    }
    return { broadcastId: existing.id, status: existing.status };

    try {
      return await this.dataSource.transaction(async (manager) => {
        const repo = manager.getRepository(NotificationBroadcast);

        // Step 4: Ghi MỘT dòng queued: request không gửi gì cả
        // Trước đây POST gửi cho mọi người ngay trong request. Với hàng triệu người nhận đó là
        // một request giữ kết nối và transaction hàng giờ; nay request chỉ ghi một dòng và job
        // nền gửi dần (xem trang kế tiếp).
        const broadcast = await repo.save(
          repo.create({
            actorUserId,
            title: input.title,
            body: input.body,
            audience: input.audience,
            status: BroadcastStatus.Queued,
            idempotencyKey,
          }),
        );

        // Step 5: Ghi audit cùng transaction
        await this.auditLogService.record(
          {
            actorUserId,
            action: 'notification.broadcast.queued',
            targetType: 'notification_broadcast',
            targetId: broadcast.id,
            metadata: { audience: input.audience, title: input.title },
          },
          manager,
        );
        return { broadcastId: broadcast.id, status: broadcast.status };
      });
    } catch (error) {

      // Step 6: Hai request đồng thời cùng key: unique chặn bên thua, bên thua đọc lại bản của
      // bên thắng
      // [POSTGRES] Unique một phần (actor_user_id, idempotency_key) là chốt thật: kiểm tra "đã có
      // chưa" ở trên chỉ là đường nhanh, hai request cùng key đồng thời vẫn chỉ tạo một
      // broadcast.
      if (!isUniqueViolation(error)) throw error;
      const raced = await this.findBroadcastByKey(actorUserId, idempotencyKey);
      if (raced === null) throw error;
      return this.replayBroadcast(raced, input);
    }
  }
}

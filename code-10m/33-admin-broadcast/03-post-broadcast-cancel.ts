//  POST /api/v1/admin/notifications/broadcast/:id/cancel
//  Một UPDATE có điều kiện đổi queued/running thành cancelled và là chốt chặn nguyên tử; audit chỉ ghi khi thật sự chuyển trạng thái nên huỷ lặp là idempotent; lô đã commit không thu hồi được.
//  Step 1: Endpoint huỷ broadcast (chỉ admin)
//  Step 2: Một transaction: chuyển trạng thái, ghi audit, đọc lại
//  Step 3: UPDATE có điều kiện: chỉ queued hoặc running mới huỷ được
//  Step 4: Chỉ ghi audit khi thật sự chuyển trạng thái
//  Step 5: Không chuyển được: đã cancelled thì trả nguyên, đã completed/failed thì 409

// [CONTROLLER] AdminController.cancelBroadcast · admin/admin.controller.ts
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

  @Post('notifications/broadcast/:id/cancel')
  @RequireAdminPermission(AdminPermission.ManageConfig)
  @HttpCode(HttpStatus.OK)

  // Step 1: Endpoint huỷ broadcast (chỉ admin)
  async cancelBroadcast(
    @CurrentUser() actor: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
  ): Promise<BroadcastStatusDto> {
    return BroadcastStatusDto.from(
      await this.adminService.cancelBroadcast(actor.userId, id),
    );

    // [SERVICE] AdminService.cancelBroadcast · admin/admin.service.ts
    const actorUserId = actor.userId;
    const broadcastId = id;

    // Step 2: Một transaction: chuyển trạng thái, ghi audit, đọc lại
    return this.dataSource.transaction(async (manager) => {

      // Step 3: UPDATE có điều kiện: chỉ queued hoặc running mới huỷ được
      // Các lô ĐÃ commit không thu hồi được (thông báo đã tới người dùng); phần còn lại ngừng
      // ngay vì job chỉ nhặt broadcast queued/running và khoá dòng trước khi ghi mỗi lô, nên một
      // lô đang chạy dở hoàn tất rồi dừng. UPDATE … RETURNING qua manager.query trả cặp [hàng, số
      // hàng].
      const [cancelled] = await manager.query<
        [Array<{ recipientCount: number }>, number]
      >(
        `UPDATE notification_broadcasts
            SET status = 'cancelled', completed_at = now(), updated_at = now()
          WHERE id = $1 AND status IN ('queued', 'running')
          RETURNING recipient_count AS "recipientCount"`,
        [broadcastId],
      );

      // Step 4: Chỉ ghi audit khi thật sự chuyển trạng thái
      // Huỷ lại broadcast đã huỷ trả nguyên trạng thái và không ghi audit lần hai: audit chỉ ghi
      // khi chính câu UPDATE này làm chuyển trạng thái.
      if (cancelled.length > 0) {
        await this.auditLogService.record(
          {
            actorUserId,
            action: 'notification.broadcast.cancelled',
            targetType: 'notification_broadcast',
            targetId: broadcastId,
            metadata: { recipientCountAtCancel: cancelled[0]?.recipientCount },
          },
          manager,
        );
      }
      const broadcast = await manager
        .getRepository(NotificationBroadcast)
        .findOneBy({ id: broadcastId });
      if (broadcast === null) {
        throw new DomainException(
          AdminErrors.BROADCAST_NOT_FOUND,
          'Không tìm thấy lần gửi thông báo',
          HttpStatus.NOT_FOUND,
        );
      }
      if (

        // Step 5: Không chuyển được: đã cancelled thì trả nguyên, đã completed/failed thì 409
        // Broadcast đã completed hoặc failed thì không còn gì để huỷ: 409 thay vì giả vờ thành
        // công.
        cancelled.length === 0 &&
        broadcast.status !== BroadcastStatus.Cancelled
      ) {
        throw new DomainException(
          AdminErrors.BROADCAST_NOT_CANCELLABLE,
          `Broadcast đã ${broadcast.status}, không huỷ được`,
          HttpStatus.CONFLICT,
        );
      }
      return broadcast;
    });
  }
}

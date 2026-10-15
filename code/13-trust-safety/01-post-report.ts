// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/safety/reports
//  Ghi report và trừ trust score của người bị báo cáo trong 1 transaction, có chống lạm dụng theo cặp và theo ngày.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận report (giới hạn 10 lần/giờ), giao cho service và trả report vừa
//                 ghi.
//    [SERVICE]    Chặn tự report, khóa người bị báo cáo, tính điểm phạt theo cooldown
//                 của cặp và trần penalty trong ngày, ghi report rồi trừ điểm.
//    [POSTGRES]   Khóa dòng user (FOR UPDATE) tuần tự hóa mọi penalty của 1 người;
//                 report ghi append-only; trừ điểm bằng UPDATE nguyên tử có sàn.
//
//  Bảng       users — dòng người bị báo cáo bị khóa FOR UPDATE; cột trust_score bị trừ
//               (không xuống dưới sàn).
//             reports — log append-only: người báo, người bị báo, lý do,
//               trust_penalty_applied (số điểm đã trừ thật, có thể là 0); đồng thời là
//               nguồn đếm cooldown theo cặp và tổng penalty trong ngày.
//  Liên quan  User — getByIdOrThrow kiểm người bị báo cáo tồn tại; adjustTrustScore trừ
//               điểm bằng UPDATE GREATEST(sàn, điểm + delta) cùng transaction (không
//               chép).
//             Matching — đọc trust_score lúc user vào hàng đợi: điểm thấp thì bị cộng
//               thêm thời gian chờ ảo nên xếp sau; canPair còn không ghép lại cặp vừa
//               report trong N ngày (chưa chép).

// [CONTROLLER] safety.controller.ts · L33–53
@Post('reports')
@HttpCode(HttpStatus.CREATED)
@Throttle({ default: { limit: 10, ttl: minutes(60) } })
async createReport(
  @CurrentUser() user: AuthenticatedUser,
  @Body() dto: CreateReportDto,
): Promise<ReportDto> {
  const report = await this.safetyService.report(
    user.userId,
    dto.targetUserId,
    dto.reason,
    dto.description,
  );
  return ReportDto.from(report);
}

// [SERVICE] safety.service.ts · L53–137
async report(
  reporterUserId: string,
  targetUserId: string,
  reason: ReportReason,
  description?: string,
): Promise<Report> {
  if (reporterUserId === targetUserId) {
    throw new DomainException(
      SafetyErrors.SELF_REPORT_FORBIDDEN,
      'Không thể tự report chính mình',
      HttpStatus.BAD_REQUEST,
    );
  }
  await this.userService.getByIdOrThrow(targetUserId);

  const pairCooldownDays = this.config.getOrThrow(
    'SAFETY_REPORT_COOLDOWN_DAYS',
    { infer: true },
  );
  const penaltyPerReport = this.config.getOrThrow(
    'SAFETY_TRUST_PENALTY_PER_REPORT',
    { infer: true },
  );
  const dailyCap = this.config.getOrThrow('SAFETY_TRUST_PENALTY_DAILY_CAP', {
    infer: true,
  });
  // Sàn trust score clamp ở tầng ứng dụng, không đặt CHECK ở DB (cùng tinh thần
  // Wallet).
  const floor = this.config.getOrThrow('SAFETY_TRUST_SCORE_FLOOR', {
    infer: true,
  });

  return this.dataSource.transaction(async (manager) => {
    // [POSTGRES] Khóa dòng user bị báo cáo (FOR UPDATE) để tuần tự hóa mọi penalty của
    // 1 người: không khóa thì 2 report đồng thời cùng đọc tổng penalty và cooldown rồi
    // cùng vượt trần.
    await manager.getRepository(User).findOne({
      where: { id: targetUserId },
      lock: { mode: 'pessimistic_write' },
    });

    const pairCutoff = new Date(Date.now() - pairCooldownDays * DAY_MS);
    // Mỗi cặp người báo → người bị báo chỉ bị trừ điểm 1 lần trong mỗi cooldown; report
    // thêm vẫn được ghi để giữ bằng chứng nhưng penalty = 0, chống "vote-kick".
    const alreadyPenalizedThisPair =
      (await manager.count(Report, {
        where: {
          reporterUserId,
          targetUserId,
          createdAt: MoreThanOrEqual(pairCutoff),
          trustPenaltyApplied: MoreThan(0),
        },
      })) > 0;

    // Trần penalty trong 24 giờ cho 1 người: chặn nhiều người báo khác nhau dồn dập
    // đánh sập trust score của ai đó trong vài phút.
    const dailyCutoff = new Date(Date.now() - DAY_MS);
    const raw = await manager
      .createQueryBuilder(Report, 'r')
      .select('COALESCE(SUM(r.trustPenaltyApplied), 0)', 'sum')
      .where('r.targetUserId = :targetUserId', { targetUserId })
      .andWhere('r.createdAt >= :dailyCutoff', { dailyCutoff })
      .getRawOne<{ sum: string }>();
    const appliedToday = Number(raw?.sum ?? 0);
    const remainingCap = Math.max(0, dailyCap - appliedToday);

    const penalty = alreadyPenalizedThisPair
      ? 0
      : Math.min(penaltyPerReport, remainingCap);

    // Luôn ghi report (append-only), kể cả khi penalty = 0. trustPenaltyApplied lưu
    // ĐÚNG số điểm đã trừ, để audit mà không phải suy ngược từ config hiện tại.
    const report = await manager.save(
      manager.create(Report, {
        reporterUserId,
        targetUserId,
        reason,
        description: description ?? null,
        trustPenaltyApplied: penalty,
      }),
    );

    // Trừ điểm dùng chung manager nên cùng transaction với insert report: không có
    // trạng thái "đã ghi report mà chưa trừ điểm".
    if (penalty > 0) {
      await this.userService.adjustTrustScore(
        manager,
        targetUserId,
        -penalty,
        floor,
      );
    }
    return report;
  });
}

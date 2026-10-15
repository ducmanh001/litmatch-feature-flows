//  POST /api/v1/safety/reports
//  Ghi report và trừ trust score của người bị báo cáo trong 1 transaction, có chống lạm dụng theo cặp và theo ngày.
//  Step 1: Nhận report (giới hạn 10 lần/giờ; user từ JWT, người bị báo từ body)
//  Step 2: Chặn tự report, kiểm người bị báo cáo có tồn tại
//  Step 3: Mở transaction, khóa dòng user bị báo cáo (FOR UPDATE)
//  Step 4: Đếm report đã trừ điểm của cặp này trong cooldown
//  Step 5: Tính phần trần penalty 24 giờ còn lại của người bị báo
//  Step 6: Tính điểm phạt: 0 nếu cặp đã bị phạt, không vượt trần ngày
//  Step 7: Ghi report (append-only) kèm số điểm đã trừ thật
//  Step 8: Trừ trust score (có sàn) trong cùng transaction
//  Step 9: Controller trả report vừa ghi

// [CONTROLLER] SafetyController.createReport  ·  safety.controller.ts L29–30, L33–53
@Controller('safety')
export class SafetyController {
  constructor(
    private readonly safetyService: SafetyService,
    // SafetyService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly userService: UserService,
    private readonly config: ConfigService<CoreApiEnv, true>,
  ) {}

  // Step 1: Nhận report (giới hạn 10 lần/giờ; user từ JWT, người bị báo từ body)
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

    // [SERVICE] SafetyService.report  ·  safety.service.ts L53–137
    const reporterUserId = user.userId;
    const targetUserId = dto.targetUserId;
    const reason = dto.reason;
    const description = dto.description;

    // Step 2: Chặn tự report, kiểm người bị báo cáo có tồn tại
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
    // Sàn trust score clamp ở tầng ứng dụng, không đặt CHECK ở DB (cùng tinh thần Wallet).
    const floor = this.config.getOrThrow('SAFETY_TRUST_SCORE_FLOOR', {
      infer: true,
    });

    // Step 3: Mở transaction, khóa dòng user bị báo cáo (FOR UPDATE)
    return this.dataSource.transaction(async (manager) => {
      // [POSTGRES] Khóa dòng user bị báo cáo (FOR UPDATE) để tuần tự hóa mọi penalty của 1 người:
      // không khóa thì 2 report đồng thời cùng đọc tổng penalty và cooldown rồi cùng vượt trần.
      await manager.getRepository(User).findOne({
        where: { id: targetUserId },
        lock: { mode: 'pessimistic_write' },
      });

      const pairCutoff = new Date(Date.now() - pairCooldownDays * DAY_MS);

      // Step 4: Đếm report đã trừ điểm của cặp này trong cooldown
      // Mỗi cặp người báo → người bị báo chỉ bị trừ điểm 1 lần trong mỗi cooldown; report thêm
      // vẫn được ghi để giữ bằng chứng nhưng penalty = 0, chống "vote-kick".
      const alreadyPenalizedThisPair =
        (await manager.count(Report, {
          where: {
            reporterUserId,
            targetUserId,
            createdAt: MoreThanOrEqual(pairCutoff),
            trustPenaltyApplied: MoreThan(0),
          },
        })) > 0;

      // Step 5: Tính phần trần penalty 24 giờ còn lại của người bị báo
      // Trần penalty trong 24 giờ cho 1 người: chặn nhiều người báo khác nhau dồn dập đánh sập
      // trust score của ai đó trong vài phút.
      const dailyCutoff = new Date(Date.now() - DAY_MS);
      const raw = await manager
        .createQueryBuilder(Report, 'r')
        .select('COALESCE(SUM(r.trustPenaltyApplied), 0)', 'sum')
        .where('r.targetUserId = :targetUserId', { targetUserId })
        .andWhere('r.createdAt >= :dailyCutoff', { dailyCutoff })
        .getRawOne<{ sum: string }>();
      const appliedToday = Number(raw?.sum ?? 0);
      const remainingCap = Math.max(0, dailyCap - appliedToday);

      // Step 6: Tính điểm phạt: 0 nếu cặp đã bị phạt, không vượt trần ngày
      const penalty = alreadyPenalizedThisPair
        ? 0
        : Math.min(penaltyPerReport, remainingCap);

      // Step 7: Ghi report (append-only) kèm số điểm đã trừ thật
      // Luôn ghi report (append-only), kể cả khi penalty = 0. trustPenaltyApplied lưu ĐÚNG số
      // điểm đã trừ, để audit mà không phải suy ngược từ config hiện tại.
      const report = await manager.save(
        manager.create(Report, {
          reporterUserId,
          targetUserId,
          reason,
          description: description ?? null,
          trustPenaltyApplied: penalty,
        }),
      );

      // Step 8: Trừ trust score (có sàn) trong cùng transaction
      // Trừ điểm dùng chung manager nên cùng transaction với insert report: không có trạng thái
      // "đã ghi report mà chưa trừ điểm".
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

    // Step 9: Controller trả report vừa ghi
    return ReportDto.from(report);
  }
}

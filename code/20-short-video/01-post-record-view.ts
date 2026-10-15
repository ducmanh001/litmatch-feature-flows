// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/videos/:id/views
//  Ghi watch-time của người xem và cộng viewCount đúng 1 lần khi xem đủ ngưỡng, kể cả khi nhiều request đến cùng lúc.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận watchTimeMs mà client báo, giao cho service và trả 204.
//    [SERVICE]    Kiểm video xem được, bỏ qua tác giả tự xem, so watch-time với ngưỡng
//                 "xem đủ", cộng viewCount đúng 1 lần.
//    [POSTGRES]   Dòng video_views (unique video, người xem) tạo bằng ON CONFLICT DO
//                 NOTHING rồi khóa FOR UPDATE; viewCount cộng nguyên tử trong cùng
//                 transaction.
//
//  Bảng       video_views — mỗi cặp (video, người xem) 1 dòng: watch_time_ms lớn nhất
//               đã báo và cờ qualified (chỉ chuyển false → true 1 lần); unique
//               (video_id, viewer_id).
//             videos — đọc để kiểm quyền xem (tác giả hoặc đã published); view_count
//               được cộng khi người xem đạt ngưỡng lần đầu.
//  Liên quan  VideoRankingService — job nền tính rankScore từ viewCount, likeCount,
//               commentCount, nên đếm đúng 1 lần ảnh hưởng thứ hạng (chưa chép).
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] ShortVideoController.recordView  ·  short-video.controller.ts L48–49, L132–141
@Controller('videos')
export class ShortVideoController {
  constructor(
    private readonly videoService: ShortVideoService,
  ) {}

  // Step 1: Nhận watchTimeMs mà client báo (user từ JWT, id video từ URL), trả 204
  @Post(':id/views')
  @HttpCode(HttpStatus.NO_CONTENT)
  async recordView(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: RecordViewDto,
  ): Promise<void> {
    await this.videoService.recordView(user, id, dto.watchTimeMs);

      // [SERVICE] ShortVideoService.recordView  ·  short-video.service.ts L254–301
      constructor(
        @InjectDataSource() private readonly dataSource: DataSource,
        private readonly config: ConfigService<CoreApiEnv, true>,
      ) {}

      const videoId = id;
      const watchTimeMs = dto.watchTimeMs;

      // Step 2: Kiểm video xem được (tác giả hoặc đã published)
      // Không tồn tại, removed, rejected, hoặc chưa published (với người không phải tác giả) đều
      // trả CÙNG mã 404.
      const video = await this.getVideoOrThrow(user, videoId);

      // Step 3: Tác giả tự xem thì không tính lượt xem
      if (video.authorUserId === user.userId) return;

      const qualifiedMinMs = this.config.getOrThrow(
        'VIDEO_QUALIFIED_VIEW_MIN_MS',
        { infer: true },
      );

      // Step 4: So watch-time với ngưỡng "xem đủ"
      const nowQualifies = watchTimeMs >= qualifiedMinMs;

      // Step 5: Mở transaction DB để đếm đúng 1 lần dưới race
      await this.dataSource.transaction(async (manager) => {

        // Step 6: Tạo dòng video_views nếu chưa có (INSERT ... ON CONFLICT DO NOTHING)
        // [POSTGRES] Tạo dòng bằng INSERT ... ON CONFLICT DO NOTHING: không ném unique violation
        // nên Postgres không hủy transaction (insert lỗi rồi đọc lại trong cùng transaction sẽ
        // dính "current transaction is aborted").
        await manager
          .createQueryBuilder()
          .insert()
          .into(VideoView)
          .values({
            videoId,
            viewerId: user.userId,
            watchTimeMs: 0,
            qualified: false,
          })
          .orIgnore()
          .execute();

        // Step 7: Khóa dòng (FOR UPDATE) để request song song xếp hàng
        // Rồi KHÓA dòng (FOR UPDATE): 2 request đồng thời của cùng người xem chạy tuần tự,
        // request sau thấy qualified = true của request trước nên không cộng viewCount lần nữa.
        const view = await manager.findOneOrFail(VideoView, {
          where: { videoId, viewerId: user.userId },
          lock: { mode: 'pessimistic_write' },
        });
        // qualified chỉ chuyển false → true đúng 1 lần; các lần cập nhật watch-time sau không
        // cộng lại (idempotent theo cờ).
        const shouldQualifyNow = nowQualifies && !view.qualified;

        // Step 8: Ghi watch-time lớn nhất và cờ qualified
        await manager.update(
          VideoView,
          { id: view.id },
          {
            watchTimeMs: Math.max(view.watchTimeMs, watchTimeMs),
            qualified: view.qualified || nowQualifies,
          },
        );

        // Step 9: Lần đầu đạt ngưỡng thì cộng viewCount nguyên tử
        if (shouldQualifyNow) {
          await manager.increment(Video, { id: videoId }, 'viewCount', 1);
        }
      });
  }
}

// [POSTGRES] CREATE TABLE video_views  ·  1754800000000-short-video.ts L41–51
await queryRunner.query(`
  CREATE TABLE video_views (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id      uuid NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    viewer_id     uuid NOT NULL REFERENCES users(id),
    watch_time_ms int NOT NULL DEFAULT 0,
    qualified     boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_video_views_video_viewer UNIQUE (video_id, viewer_id)
  )
`);

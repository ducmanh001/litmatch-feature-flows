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

// [CONTROLLER] short-video.controller.ts · L132–141
@Post(':id/views')
@HttpCode(HttpStatus.NO_CONTENT)
async recordView(
  @CurrentUser() user: AuthenticatedUser,
  @Param('id', ParseUUIDPipe) id: string,
  @Body() dto: RecordViewDto,
): Promise<void> {
  await this.videoService.recordView(user, id, dto.watchTimeMs);
}

// [SERVICE] short-video.service.ts · L254–301
async recordView(
  user: AuthenticatedUser,
  videoId: string,
  watchTimeMs: number,
): Promise<void> {
  const video = await this.getVideoOrThrow(user, videoId);
  if (video.authorUserId === user.userId) return;

  const qualifiedMinMs = this.config.getOrThrow(
    'VIDEO_QUALIFIED_VIEW_MIN_MS',
    { infer: true },
  );
  const nowQualifies = watchTimeMs >= qualifiedMinMs;

  await this.dataSource.transaction(async (manager) => {
    // [POSTGRES] Tạo dòng bằng INSERT ... ON CONFLICT DO NOTHING: không ném unique
    // violation nên Postgres không hủy transaction (insert lỗi rồi đọc lại trong cùng
    // transaction sẽ dính "current transaction is aborted").
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
    // Rồi KHÓA dòng (FOR UPDATE): 2 request đồng thời của cùng người xem chạy tuần tự,
    // request sau thấy qualified = true của request trước nên không cộng viewCount lần
    // nữa.
    const view = await manager.findOneOrFail(VideoView, {
      where: { videoId, viewerId: user.userId },
      lock: { mode: 'pessimistic_write' },
    });
    // qualified chỉ chuyển false → true đúng 1 lần; các lần cập nhật watch-time sau
    // không cộng lại (idempotent theo cờ).
    const shouldQualifyNow = nowQualifies && !view.qualified;
    await manager.update(
      VideoView,
      { id: view.id },
      {
        watchTimeMs: Math.max(view.watchTimeMs, watchTimeMs),
        qualified: view.qualified || nowQualifies,
      },
    );
    if (shouldQualifyNow) {
      await manager.increment(Video, { id: videoId }, 'viewCount', 1);
    }
  });
}

// [SERVICE] short-video.service.ts · L170–187
async getVideoOrThrow(
  user: AuthenticatedUser,
  videoId: string,
): Promise<Video> {
  const video = await this.videoRepo.findOneBy({ id: videoId });
  const visible =
    video &&
    (video.authorUserId === user.userId ||
      video.status === VideoStatus.Published);
  if (!visible) {
    // Không tồn tại, removed, rejected, hoặc chưa published (với người không phải tác
    // giả) đều trả CÙNG mã 404.
    throw new DomainException(
      ShortVideoErrors.VIDEO_NOT_FOUND,
      'Không tìm thấy video',
      HttpStatus.NOT_FOUND,
    );
  }
  return video;
}

// [POSTGRES] 1754800000000-short-video.ts · L41–51
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

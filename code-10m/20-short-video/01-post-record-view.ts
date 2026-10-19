//  POST /api/v1/videos/:id/views
//  Ghi watch-time của người xem và cộng viewCount đúng 1 lần khi xem đủ ngưỡng, kể cả khi nhiều request đến cùng lúc.
//  Step 1: Nhận watchTimeMs mà client báo (user từ JWT, id video từ URL), trả 204
//  Step 2: Kiểm video xem được (tác giả hoặc đã published)
//  Step 3: Tác giả tự xem thì không tính lượt xem
//  Step 4: So watch-time với ngưỡng "xem đủ"
//  Step 5: Mở transaction DB để đếm đúng 1 lần dưới race
//  Step 6: Tạo dòng video_views nếu chưa có (INSERT ... ON CONFLICT DO NOTHING)
//  Step 7: Khóa dòng (FOR UPDATE) để request song song xếp hàng
//  Step 8: Ghi watch-time lớn nhất và cờ qualified
//  Step 9: Bộ đệm TẮT (mặc định): lần đầu đạt ngưỡng thì cộng viewCount ngay trong transaction
//  Step 10: UPDATE một hàng videos: cộng đếm và kéo hạn tính lại ranking về sớm
//  Step 11: Bộ đệm BẬT: sau khi commit chỉ ghi delta vào Redis, một job nền gộp lại thành một UPDATE
//  Step 12: Ghi delta vào Redis; lỗi Redis thì cộng thẳng vào DB
//  Step 13: MULTI: HINCRBY và SADD cùng một giao dịch Redis

// [CONTROLLER] ShortVideoController.recordView · short-video/short-video.controller.ts
@Controller('videos')
export class ShortVideoController {
  // ShortVideoService
  private readonly logger = new Logger(ShortVideoService.name);
  constructor(
    private readonly videoService: ShortVideoService,
    // ShortVideoService
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    @Optional() private readonly counterBuffer?: VideoCounterBufferPort,
    // RedisVideoCounterBufferAdapter
    private readonly redis: Redis,
    private readonly prefix = 'video:counter:',
  ) {
    // RedisVideoCounterBufferAdapter
    super();
  }

  // Step 1: Nhận watchTimeMs mà client báo (user từ JWT, id video từ URL), trả 204
  @Post(':id/views')
  @HttpCode(HttpStatus.NO_CONTENT)
  async recordView(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: RecordViewDto,
  ): Promise<void> {
    await this.videoService.recordView(user, id, dto.watchTimeMs);

    // [SERVICE] ShortVideoService.recordView · short-video/short-video.service.ts
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
    const qualifiedNow = await this.dataSource.transaction(async (manager) => {

      // Step 6: Tạo dòng video_views nếu chưa có (INSERT ... ON CONFLICT DO NOTHING)
      // [POSTGRES] Tạo dòng bằng INSERT ... ON CONFLICT DO NOTHING: không ném unique violation
      // nên Postgres không hủy transaction (insert lỗi rồi đọc lại trong cùng transaction sẽ dính
      // "current transaction is aborted").
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
      // Rồi KHÓA dòng (FOR UPDATE): 2 request đồng thời của cùng người xem chạy tuần tự, request
      // sau thấy qualified = true của request trước nên không cộng viewCount lần nữa.
      const view = await manager.findOneOrFail(VideoView, {
        where: { videoId, viewerId: user.userId },
        lock: { mode: 'pessimistic_write' },
      });
      // qualified chỉ chuyển false → true đúng 1 lần; các lần cập nhật watch-time sau không cộng
      // lại (idempotent theo cờ).
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

      // Step 9: Bộ đệm TẮT (mặc định): lần đầu đạt ngưỡng thì cộng viewCount ngay trong
      // transaction
      // Mỗi video chỉ có MỘT hàng videos giữ view/like/comment_count: ở quy mô lớn mọi lượt xem
      // đủ ngưỡng của video nóng cùng UPDATE một hàng và xếp hàng sau khoá hàng đó. Vì vậy có cờ
      // VIDEO_COUNTER_BUFFER_ENABLED (mặc định TẮT): bật thì bỏ UPDATE này khỏi transaction.
      if (shouldQualifyNow && !this.buffered) {
        await this.bumpCounter(manager, videoId, 'viewCount', 1);

        // [SERVICE] ShortVideoService.bumpCounter
        const column = 'viewCount';
        const delta = 1;
        const sqlColumn =
          column === 'viewCount'
            ? 'view_count'
            : column === 'likeCount'
              ? 'like_count'
              : 'comment_count';
        const activeRefreshSeconds = this.config.getOrThrow(
          'VIDEO_RANKING_ACTIVE_REFRESH_SECONDS',
          { infer: true },
        );

        // Step 10: UPDATE một hàng videos: cộng đếm và kéo hạn tính lại ranking về sớm
        // [POSTGRES] Cùng câu UPDATE kéo rank_due_at về sớm: video đang có tương tác được tính
        // lại ranking sớm (trong VIDEO_RANKING_ACTIVE_REFRESH_SECONDS) thay vì chờ chu kỳ
        // tuổi-thích-nghi.
        await manager.query(
          `UPDATE videos
              SET ${sqlColumn} = GREATEST(${sqlColumn} + $2::int, 0),
                  rank_due_at = LEAST(COALESCE(rank_due_at, now()), now() + make_interval(secs => $3::int))
            WHERE id = $1`,
          [videoId, delta, activeRefreshSeconds],
        );
      }
      return shouldQualifyNow;
    });

    // Step 11: Bộ đệm BẬT: sau khi commit chỉ ghi delta vào Redis, một job nền gộp lại thành một
    // UPDATE
    // Ghi vào Redis SAU commit: nếu Redis timeout mơ hồ sau khi lệnh thực ra đã chạy, lần thử lại
    // cộng thẳng vào DB sẽ đếm dư một lần. Đánh đổi chấp nhận được cho bộ đếm chỉ để hiển thị.
    if (qualifiedNow && this.buffered) {
      await this.bufferCounter(videoId, 'viewCount', 1);

      // [SERVICE] ShortVideoService.bufferCounter
      // column = 'viewCount'
      // delta = 1
      const field =
        column === 'viewCount'
          ? 'view'
          : column === 'likeCount'
            ? 'like'
            : 'comment';
      try {

        // Step 12: Ghi delta vào Redis; lỗi Redis thì cộng thẳng vào DB
        await (this.counterBuffer as VideoCounterBufferPort).add(
          videoId,
          field,
          delta,
        );

        // [REDIS] RedisVideoCounterBufferAdapter.add · short-video/redis/redis-video-counter-buffer.adapter.ts
        if (!Number.isInteger(delta) || delta === 0) {
          throw new Error('Video counter delta must be a non-zero integer');
        }

        // Step 13: MULTI: HINCRBY và SADD cùng một giao dịch Redis
        // [REDIS] MULTI (không phải pipeline): HINCRBY và SADD vào tập "dirty" cùng thành công
        // hoặc cùng không chạy, nên không để lại hash mồ côi mà job flush không bao giờ thấy.
        const results = await this.redis
          .multi()
          .hincrby(`${this.prefix}${videoId}`, FIELD_CODE[field], delta)
          .sadd(`${this.prefix}dirty`, videoId)
          .exec();
        for (const [error] of results ?? []) {
          if (error) throw error;
        }

        return;
      } catch (error) {
        this.logger.warn(
          `Counter buffer lỗi, cộng thẳng vào DB: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
      await this.bumpCounter(this.dataSource.manager, videoId, column, delta);
    }
  }
}

// [POSTGRES] CREATE TABLE video_views · database/migrations/1754800000000-short-video.ts
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

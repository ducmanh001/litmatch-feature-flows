// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/movie-match/sessions
//  Tạo phiên xem video chung giữa 2 người bạn, idempotent theo cặp, mỗi user chỉ có 1 phiên đang xem.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận id bạn và URL video, giao cho service, trả phiên theo góc nhìn
//                 của người tạo.
//    [SERVICE]    Kiểm URL video theo whitelist, kiểm đã là bạn, chuẩn hóa cặp, trả lại
//                 phiên cũ nếu cặp đang xem; không thì ghi phiên + 2 participant trong
//                 1 transaction.
//    [POSTGRES]   Bảng participant với PK user_id là chốt chặn "1 user 1 phiên đang
//                 xem", bất kể user là low hay high; ghi cùng transaction với phiên.
//    [REDIS]      Pub/sub báo movie.session.started cho cả 2 người khi vừa tạo phiên
//                 mới.
//
//  Bảng       movie_sessions — phiên mới: cặp (low, high) canonical, video_url, vị trí
//               phát 0, status active, mode friend (mặc định).
//             movie_session_active_participants — mỗi người trong phiên một dòng, PK
//               user_id; insert trùng nghĩa là user đang có phiên active khác.
//  Redis      realtime:user:{userId} — publish movie.session.started cho từng người
//               trong cặp.
//  Liên quan  Friend — FriendService.areFriends: chỉ 2 người đã là bạn mới tạo được
//               phiên (không chép).
//             signaling-gateway — subscribe pub/sub và đẩy movie.session.started xuống
//               socket.
//             updateState / endSession — bước sau: cập nhật playback (last-write-wins,
//               không khóa) và kết thúc phiên, endSession xóa 2 dòng participant (chưa
//               chép).

// [CONTROLLER] movie-match.controller.ts · L50–67
@Post('sessions')
@HttpCode(HttpStatus.OK)
async createSession(
  @CurrentUser() user: AuthenticatedUser,
  @Body() dto: CreateMovieSessionDto,
): Promise<MovieSessionDto> {
  const session = await this.movieMatchService.createSession(
    user.userId,
    dto.friendUserId,
    dto.videoUrl,
  );
  return MovieSessionDto.from(session, user.userId);
}

// [SERVICE] movie-match.service.ts · L109–186
async createSession(
  userId: string,
  friendUserId: string,
  videoUrl: string,
): Promise<MovieSession> {
  this.assertValidVideoUrl(videoUrl);

  if (userId === friendUserId) {
    throw new DomainException(
      MovieMatchErrors.NOT_FRIEND,
      'Không thể xem chung với chính mình',
      HttpStatus.NOT_FOUND,
    );
  }
  // Tự xem với mình, không phải bạn, không tồn tại đều trả cùng 404 NOT_FRIEND: không
  // để lộ quan hệ (oracle).
  if (!(await this.friendService.areFriends(userId, friendUserId))) {
    throw new DomainException(
      MovieMatchErrors.NOT_FRIEND,
      'Không phải bạn của bạn',
      HttpStatus.NOT_FOUND,
    );
  }

  const [userLowId, userHighId] = canonicalPair(userId, friendUserId);

  // Idempotent theo cặp: đã active đúng cặp này thì trả lại phiên cũ. Active với CẶP
  // KHÁC thì 409, không tự ý kết thúc phiên cũ thay user.
  const existing = await this.findActiveByPair(userLowId, userHighId);
  if (existing) return existing;

  const now = new Date();
  let created: MovieSession;
  try {
    // [POSTGRES] Phiên và 2 dòng participant ghi trong 1 transaction: insert
    // participant lỗi thì rollback cả phiên, không mồ côi dòng.
    created = await this.dataSource.transaction(async (manager) => {
      const session = await manager.save(
        manager.create(MovieSession, {
          userLowId,
          userHighId,
          videoUrl,
          positionSeconds: 0,
          isPlaying: false,
          positionUpdatedAt: now,
          status: MovieSessionStatus.Active,
        }),
      );
      // PK user_id chặn user đang active ở phiên KHÁC, dù là low hay high ở đó. 2
      // unique index đơn cột trên user_low_id/user_high_id không đủ: mỗi index chỉ soi
      // 1 cột nên không thấy xung đột chéo (low ở A, high ở B).
      await manager.insert(MovieSessionActiveParticipant, [
        { userId: userLowId, sessionId: session.id },
        { userId: userHighId, sessionId: session.id },
      ]);
      return session;
    });
  } catch (err) {
    // Thua race: đọc lại để phân biệt "đúng cặp" (replay, trả lại phiên) với "user đã
    // active ở cặp khác" (409, không đoán ý user).
    if (!isUniqueViolation(err)) throw err;
    const raced = await this.findActiveByPair(userLowId, userHighId);
    if (raced) return raced;
    throw new DomainException(
      MovieMatchErrors.ALREADY_ACTIVE,
      'Đang có phiên xem chung khác đang active',
      HttpStatus.CONFLICT,
    );
  }

  const envelope: RealtimeEnvelope<MovieSessionStartedEventData> = {
    event: RealtimeEvents.MovieSessionStarted,
    data: {
      sessionId: created.id,
      videoUrl: created.videoUrl,
      initiatorUserId: userId,
    },
  };
  // [REDIS] Chỉ bắn khi vừa tạo phiên mới (các nhánh replay đã return). Best-effort:
  // client còn poll GET /sessions/:id.
  await Promise.all(
    [userLowId, userHighId].map((uid) =>
      publishRealtimeEvent(this.redis, this.logger, uid, envelope),
    ),
  );
  return created;
}

// [SERVICE] movie-match.service.ts · L901–949
private assertValidVideoUrl(videoUrl: string): void {
  const maxLength = this.config.getOrThrow('MOVIE_MATCH_URL_MAX_LENGTH', {
    infer: true,
  });
  if (videoUrl.length > maxLength) {
    throw new DomainException(
      MovieMatchErrors.INVALID_VIDEO_URL,
      `videoUrl dài quá ${maxLength} ký tự`,
      HttpStatus.UNPROCESSABLE_ENTITY,
      { maxLength },
    );
  }

  let url: URL;
  try {
    url = new URL(videoUrl);
  } catch {
    throw new DomainException(
      MovieMatchErrors.INVALID_VIDEO_URL,
      'videoUrl không hợp lệ',
      HttpStatus.UNPROCESSABLE_ENTITY,
    );
  }
  if (url.protocol !== 'https:' && url.protocol !== 'http:') {
    throw new DomainException(
      MovieMatchErrors.INVALID_VIDEO_URL,
      'videoUrl phải dùng http/https',
      HttpStatus.UNPROCESSABLE_ENTITY,
    );
  }

  const hostname = url.hostname.toLowerCase();
  const allowedHosts = this.config
    .getOrThrow('MOVIE_MATCH_ALLOWED_VIDEO_HOSTS', { infer: true })
    .split(',')
    .map((h) => h.trim().toLowerCase())
    .filter((h) => h.length > 0);
  // Khớp hostname CHÍNH XÁC hoặc subdomain thật (host hoặc .host), không
  // substring/regex: chặn bypass kiểu youtube.com.evil.com.
  const allowed = allowedHosts.some(
    (host) => hostname === host || hostname.endsWith(`.${host}`),
  );
  if (!allowed) {
    throw new DomainException(
      MovieMatchErrors.INVALID_VIDEO_URL,
      'Domain video không nằm trong danh sách được phép',
      HttpStatus.UNPROCESSABLE_ENTITY,
      { allowedHosts },
    );
  }
}

// [SERVICE] movie-match.service.ts · L644–656
private async findActiveByPair(
  userLowId: string,
  userHighId: string,
): Promise<MovieSession | null> {
  return this.sessionRepo.findOneBy({
    userLowId,
    userHighId,
    status: MovieSessionStatus.Active,
    // Ghi rõ mode friend: 2 người bạn đang tình cờ ghép ẩn danh với nhau KHÔNG được coi
    // là replay của phiên xem chung bạn bè (flow và quyền riêng tư khác nhau).
    mode: MovieSessionMode.Friend,
  });
}

// [POSTGRES] 1753200000000-movie-match.ts · L45–50
await queryRunner.query(`
  CREATE TABLE movie_session_active_participants (
    user_id    uuid PRIMARY KEY REFERENCES users(id),
    session_id uuid NOT NULL REFERENCES movie_sessions(id)
  )
`);

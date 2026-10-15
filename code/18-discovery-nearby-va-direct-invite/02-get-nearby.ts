// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/discovery/nearby
//  Trả danh sách người gần vị trí của mình dưới dạng nhóm khoảng cách, có 3 lớp chống dò vị trí.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [REDIS] → [POSTGRES] → [SERVICE]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận bộ lọc (giới tính, tuổi) và cursor, trả trang thẻ người dùng kèm
//                 nhóm khoảng cách.
//    [SERVICE]    Kiểm hạn mức, opt-in và độ tươi của vị trí; lấy ứng viên, tính khoảng
//                 cách haversine cộng jitter, sắp xếp và phân trang bằng keyset cursor
//                 ngay ở app.
//    [REDIS]      Bộ đếm hạn mức truy vấn nearby theo user (Lua nguyên tử), chống dò
//                 quét bằng nhiều truy vấn.
//    [POSTGRES]   Lọc hộp giới hạn bằng btree (lat, lon) cùng các điều kiện hiển thị
//                 trong 1 câu SELECT; vị trí chỉ lưu dạng đã làm tròn.
//
//  Bảng       user_locations — vị trí đã làm tròn của từng user (PK user_id) cùng
//               updated_at để biết còn tươi; index btree (lat, lon) phục vụ lọc hộp
//               giới hạn.
//             discovery_settings — cờ nearby_visible: opt-in của chính mình (điều kiện
//               để được xem người khác) và của ứng viên (điều kiện để được thấy).
//             user_privacy_settings — cột hide_profile: người ẩn hồ sơ bị loại khỏi ứng
//               viên.
//             users — đọc hồ sơ ứng viên còn active, lọc theo giới tính, tuổi, guest.
//  Redis      discovery:nearby-query:count:{userId} — bộ đếm số truy vấn nearby của
//               user trong cửa sổ 1 giờ; vượt hạn mức thì trả 429.
//  Liên quan  Safety — getHiddenUserIds: tập user bị ẩn vì block (2 chiều) hoặc từng
//               report (vĩnh viễn) (không chép).
//             User — findActiveByIds lọc ứng viên còn active theo giới tính, tuổi,
//               guest; PrivacySettings cho cờ showDistance (không chép).
//             common/redis rate-limit — script Lua INCR + EXPIRE nguyên tử, dùng chung
//               với Matching speed-up và Invite (không chép).
//             PUT /discovery/nearby/location — lớp 1 chống trilateration: làm tròn tọa
//               độ ~500m ngay khi ghi, nên user_locations không chứa tọa độ thô (chưa
//               chép).
//             PUT /discovery/nearby/visible — bật/tắt opt-in; tắt thì xóa dòng vị trí
//               cùng transaction (chưa chép).

// [CONTROLLER] discovery.controller.ts · L75–89
@Get('nearby')
async nearby(
  @CurrentUser() user: AuthenticatedUser,
  @Query() query: NearbyQueryDto,
): Promise<NearbyCardsPageDto> {
  return NearbyCardsPageDto.from(
    await this.nearbyService.listNearby(user, query),
  );
}

// [SERVICE] nearby.service.ts · L124–255
async listNearby(
  user: AuthenticatedUser,
  query: NearbyQueryDto,
): Promise<CursorPage<NearbyCardRow>> {
  // [REDIS] Hạn mức truy vấn nearby theo user (lớp 3 chống trilateration): chặn dò quét
  // vị trí bằng nhiều truy vấn.
  const allowed = await checkRateLimit(
    this.redis,
    nearbyQueryCountKey(user.userId),
    this.config.getOrThrow('DISCOVERY_NEARBY_QUERY_RATE_LIMIT_PER_HOUR', {
      infer: true,
    }),
    RATE_LIMIT_WINDOW_SECONDS,
  );
  if (!allowed) {
    throw new DomainException(
      DiscoveryErrors.NEARBY_RATE_LIMITED,
      'Truy vấn nearby quá nhanh, thử lại sau',
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }

  const mySetting = await this.settingRepo.findOne({
    where: { userId: user.userId },
  });
  // Có đi có lại (reciprocity): chưa bật hiển thị Nearby của chính mình thì không được
  // xem người khác.
  if (!mySetting?.nearbyVisible) {
    throw new DomainException(
      DiscoveryErrors.NEARBY_NOT_OPTED_IN,
      'Bạn cần bật hiển thị Nearby trước khi xem người khác',
      HttpStatus.FORBIDDEN,
    );
  }

  const myLocation = await this.locationRepo.findOne({
    where: { userId: user.userId },
  });
  const freshnessHours = this.config.getOrThrow(
    'DISCOVERY_LOCATION_FRESHNESS_HOURS',
    { infer: true },
  );
  // Vị trí quá hạn coi như không có: vị trí cũ tự biến mất khỏi kết quả, không cần cron
  // dọn bảng.
  if (!myLocation || !this.isFresh(myLocation.updatedAt, freshnessHours)) {
    throw new DomainException(
      DiscoveryErrors.NEARBY_LOCATION_MISSING,
      'Bạn chưa cập nhật vị trí (hoặc vị trí đã quá hạn)',
      HttpStatus.CONFLICT,
    );
  }

  const cursor = this.decodeNearbyCursor(query.cursor);
  const radiusKm = this.config.getOrThrow('DISCOVERY_NEARBY_RADIUS_KM', {
    infer: true,
  });
  // Loại người đã block hoặc report với mình (2 chiều); report ẩn vĩnh viễn vì bảng
  // reports là append-only.
  const hiddenUserIds = await this.safetyService.getHiddenUserIds(
    user.userId,
  );

  // Spatial MVP: lọc hộp giới hạn bằng btree rồi haversine ở app (không PostGIS), tối
  // đa DISCOVERY_NEARBY_CANDIDATE_CAP ứng viên; sort và cursor cũng làm ở app.
  const candidates = await this.queryCandidates(
    myLocation,
    radiusKm,
    [user.userId, ...hiddenUserIds],
    freshnessHours,
  );
  if (candidates.length === 0) {
    return { items: [], meta: { nextCursor: null } };
  }

  const users = await this.userService.findActiveByIds(
    candidates.map((c) => c.userId),
    {
      gender: query.gender,
      ageMin: query.ageMin,
      ageMax: query.ageMax,
      excludeGuests: !this.config.getOrThrow('DISCOVERY_GUEST_VISIBLE', {
        infer: true,
      }),
    },
  );
  const userById = new Map(users.map((u) => [u.id, u]));
  const privacyByUserId = await this.privacySettings.findForUsers(
    users.map((u) => u.id),
  );

  const forDate = todayUtc();
  const distanceBucketsCsv = this.config.getOrThrow(
    'DISCOVERY_DISTANCE_BUCKETS_KM',
    { infer: true },
  );
  const jitterMaxKm = radiusKm * 0.1;

  const rows = candidates
    .filter((c) => userById.has(c.userId))
    .map((c) => {
      const rawDistanceKm = haversineDistanceKm(
        myLocation.latQuantized,
        myLocation.lonQuantized,
        c.latQuantized,
        c.lonQuantized,
      );
      // Lớp 2 chống trilateration: cộng jitter tất định theo cặp và ngày vào khoảng
      // cách trước khi tính nhóm.
      const jitter = nearbyJitterKm(
        user.userId,
        c.userId,
        forDate,
        jitterMaxKm,
      );
      const jitteredDistanceKm = Math.max(0, rawDistanceKm + jitter);
      const targetPrivacy = privacyByUserId.get(c.userId);
      return {
        userId: c.userId,
        jitteredDistanceKm,
        user: userById.get(c.userId) as User,
        showDistance: targetPrivacy?.showDistance ?? true,
      };
    })
    .sort(
      (a, b) =>
        a.jitteredDistanceKm - b.jitteredDistanceKm ||
        (a.userId < b.userId ? -1 : a.userId > b.userId ? 1 : 0),
    )
    // Cursor giữ khoảng cách đã jitter; nhờ jitter cố định theo cặp-theo-ngày nên thứ
    // tự giữa các trang không bị xáo.
    .filter((r) => !cursor || this.isAfterCursor(r, cursor));

  const page = buildCursorPage(rows, query.limit, (last) => ({
    distanceKm: last.jitteredDistanceKm,
    userId: last.userId,
  }));
  return {
    items: page.items.map((r) => ({
      user: r.user,
      // Chỉ trả nhóm khoảng cách, không bao giờ trả km hay tọa độ chính xác; người tắt
      // showDistance thì null.
      distanceBucket: r.showDistance
        ? computeDistanceBucket(r.jitteredDistanceKm, distanceBucketsCsv)
        : null,
    })),
    meta: page.meta,
  };
}

// [REDIS] discovery-redis.provider.ts · L16–18
export function nearbyQueryCountKey(userId: string): string {
  return `discovery:nearby-query:count:${userId}`;
}

// [SERVICE] nearby.service.ts · L294–355
private async queryCandidates(
  myLocation: UserLocation,
  radiusKm: number,
  excludeUserIds: string[],
  freshnessHours: number,
): Promise<UserLocation[]> {
  // [POSTGRES] Hộp giới hạn xấp xỉ từ bán kính km (1 độ vĩ ≈ 111.32 km, độ kinh co lại
  // theo cos vĩ độ) chỉ là prefilter; khoảng cách thật tính lại bằng haversine ở bước
  // lọc cuối.
  const latDeltaDeg = radiusKm / 111.32;
  const lonDeltaDeg =
    radiusKm /
    (111.32 *
      Math.max(0.1, Math.cos((myLocation.latQuantized * Math.PI) / 180)));
  const cap = this.config.getOrThrow('DISCOVERY_NEARBY_CANDIDATE_CAP', {
    infer: true,
  });

  const rows: Array<{
    user_id: string;
    lat_quantized: string;
    lon_quantized: string;
    updated_at: Date;
  }> = await this.locationRepo.query(
    `SELECT ul.user_id, ul.lat_quantized, ul.lon_quantized, ul.updated_at
     FROM user_locations ul
     JOIN discovery_settings ds ON ds.user_id = ul.user_id
     LEFT JOIN user_privacy_settings ups ON ups.user_id = ul.user_id
     WHERE ds.nearby_visible = true
       AND COALESCE(ups.hide_profile, false) = false
       AND ul.updated_at > now() - make_interval(hours => $1::int)
       AND ul.lat_quantized BETWEEN $2 AND $3
       AND ul.lon_quantized BETWEEN $4 AND $5
       AND ul.user_id != ALL($6::uuid[])
     LIMIT $7`,
    [
      freshnessHours,
      myLocation.latQuantized - latDeltaDeg,
      myLocation.latQuantized + latDeltaDeg,
      myLocation.lonQuantized - lonDeltaDeg,
      myLocation.lonQuantized + lonDeltaDeg,
      excludeUserIds,
      cap,
    ],
  );

  return rows
    .map((r) => {
      const loc = new UserLocation();
      loc.userId = r.user_id;
      loc.latQuantized = Number(r.lat_quantized);
      loc.lonQuantized = Number(r.lon_quantized);
      loc.updatedAt = r.updated_at;
      return loc;
    })
    .filter(
      (loc) =>
        haversineDistanceKm(
          myLocation.latQuantized,
          myLocation.lonQuantized,
          loc.latQuantized,
          loc.lonQuantized,
        ) <= radiusKm,
    );
}

// [POSTGRES] 1754600000000-discovery-nearby.ts · L21–33
await queryRunner.query(`
  CREATE TABLE user_locations (
    user_id       uuid PRIMARY KEY REFERENCES users(id),
    lat_quantized double precision NOT NULL,
    lon_quantized double precision NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_user_locations_lat CHECK (lat_quantized BETWEEN -90 AND 90),
    CONSTRAINT chk_user_locations_lon CHECK (lon_quantized BETWEEN -180 AND 180)
  )
`);
await queryRunner.query(`
  CREATE INDEX idx_user_locations_lat_lon ON user_locations (lat_quantized, lon_quantized)
`);

// [SERVICE] nearby.constants.ts · L10–25
export function haversineDistanceKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const earthRadiusKm = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// [SERVICE] nearby.constants.ts · L35–45
export function nearbyJitterKm(
  userIdA: string,
  userIdB: string,
  forDate: string,
  maxJitterKm: number,
): number {
  const [low, high] = canonicalPair(userIdA, userIdB);
  // Seed theo cặp (canonical) và ngày UTC: cùng cặp cùng ngày luôn cùng jitter, nên
  // truy vấn lặp trong ngày không lộ thêm; sang ngày khác đổi, nên không cộng dồn nhiều
  // ngày để suy ra vị trí thật.
  const seed = fnv1aHash(`${low}:${high}:${forDate}`);
  return (seed % 1000) * (maxJitterKm / 1000) - maxJitterKm / 2;
}

// [SERVICE] nearby.constants.ts · L51–73
export function computeDistanceBucket(
  distanceKm: number,
  boundariesCsv: string,
): string {
  const boundaries = boundariesCsv
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n))
    .sort((a, b) => a - b);

  if (boundaries.length === 0 || distanceKm < boundaries[0]) {
    return `<${boundaries[0] ?? 0}km`;
  }
  for (let i = boundaries.length - 1; i >= 0; i--) {
    if (distanceKm >= boundaries[i]) {
      const upper = i + 1 < boundaries.length ? boundaries[i + 1] : null;
      return upper !== null
        ? `${boundaries[i]}-${upper}km`
        : `${boundaries[i]}km+`;
    }
  }
  return `<${boundaries[0]}km`;
}

// [SERVICE] nearby.service.ts · L262–270
private isAfterCursor(
  row: { jitteredDistanceKm: number; userId: string },
  cursor: { distanceKm: number; userId: string },
): boolean {
  if (row.jitteredDistanceKm !== cursor.distanceKm) {
    return row.jitteredDistanceKm > cursor.distanceKm;
  }
  return row.userId > cursor.userId;
}

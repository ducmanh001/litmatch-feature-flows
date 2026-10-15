// ════════════════════════════════════════════════════════════════════════════
//  AVATAR TÙY CHỈNH — code của tính năng, đọc từ trên xuống theo luồng request
// ════════════════════════════════════════════════════════════════════════════
//  Snapshot   litmatch @ d3cb28c · 2026-10-15
//  Nguồn      apps/core-api/src/modules/avatar · apps/web/src/features/avatar
//  Cách đọc   Code là bản thật, trích nguyên văn (số dòng gốc ghi ở mỗi nhãn). Chỉ bỏ
//             import, decorator Swagger (@Api…) và comment cũ; mọi comment trong file
//             này là chú thích mới. Dòng [DB] có dấu ≈ là SQL tương đương để dễ hình
//             dung; câu lệnh thật do TypeORM sinh ra.
//             Hàm trong class được trích riêng từng cái theo luồng, nên đoạn thụt 2 ô
//             là thuộc AvatarController hoặc AvatarService. [CLIENT] chỉ trích dòng gọi API.
//
//  Nhãn trên từng đoạn
//    [CLIENT]      web gọi API
//    [PIPE]        kiểm tra tham số / body trước khi vào controller
//    [CONTROLLER]  nhận request, gọi service, không chứa nghiệp vụ
//    [SERVICE]     nghiệp vụ
//    [REPOSITORY]  đọc / ghi bảng qua TypeORM
//    [DB]          bảng và ràng buộc trong PostgreSQL
//    [ECONOMY]     module khác, chỉ ghi chú, không chép code
//
//  Không chép vì dùng chung cho mọi tính năng (vẫn chạy, chỉ là không thuộc Avatar)
//    Trước controller  Middleware (helmet, cookie, request-id) → ThrottlerGuard (Redis)
//                      → JwtAuthGuard → RolesGuard → Interceptor → ValidationPipe
//    Sau controller    Interceptor bọc kết quả thành { data } → ExceptionFilter nếu lỗi
//
//  Tính năng này không dùng: Signaling Gateway, Media Server, BullMQ, cache Redis.
//  Riêng mua item có đi qua module Economy: ghi outbox → job nền đẩy sang Kafka (mục 3).
//
//  3 bảng dữ liệu (SQL ở mục 6)
//    avatar_assets        catalog: slot, giá price_diamond, z_index, active
//    user_avatar_items    user SỞ HỮU item nào    (unique: user_id + avatar_asset_id)
//    user_avatar_configs  user ĐANG MẶC item nào  (1 dòng / user, 1 cột / slot)
//  5 slot: base, hair, face, outfit, accessory. Cột snake_case trong DB tương ứng
//  camelCase trong code (vd user_id ↔ userId).

// ────────────────────────────────────────────────────────────────────────────
// 1) XEM CATALOG VÀ ĐỒ CỦA MÌNH
// GET /api/v1/avatar/catalog   ·   GET /api/v1/avatar/me/items
// ────────────────────────────────────────────────────────────────────────────

// [CLIENT] api.ts · L32–33, L44–45
// Hook useAvatarCatalog và useMyAvatarItems. api-client tự gắn Authorization: Bearer
// <JWT> và cookie vào MỌI request.
const res = await apiClient.GET('/api/v1/avatar/catalog');
return res.data?.data ?? [];

const res = await apiClient.GET('/api/v1/avatar/me/items');
return res.data?.data ?? [];

// [PIPE] avatar.controller.ts · L37–41
// ValidationPipe toàn cục dựng object này từ query string rồi kiểm tra: slot được bỏ
// trống hoặc phải là 1 trong 5 slot. Sai hoặc thừa tham số → 400, request không vào
// controller.
class CatalogQueryDto {
  @IsOptional()
  @IsEnum(AvatarSlot)
  slot?: AvatarSlot;
}

// [CONTROLLER] avatar.controller.ts · L45–47, L49–60, L62–71
// Controller chỉ nhận request rồi gọi service. Tiền tố /api/v1 do main.ts nối vào
// (setGlobalPrefix).
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  @Get('catalog')
  async listCatalog(
    @Query() query: CatalogQueryDto,
  ): Promise<AvatarAssetDto[]> {
    return (await this.avatarService.listCatalog(query.slot)).map(
      AvatarAssetDto.from,
    );
  }

  @Get('me/items')
  async listMyItems(
    // userId lấy từ JWT (JwtAuthGuard gắn vào request), không nhận từ client → không
    // thể xem đồ của người khác.
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<AvatarAssetDto[]> {
    return (await this.avatarService.listMyItems(user.userId)).map(
      AvatarAssetDto.from,
    );
  }
  // … class AvatarController còn tiếp: các handler khác nằm ở các mục sau.

// [CONTROLLER] avatar.dtos.ts · L6–26 — kiểu dữ liệu trả về
// Chỉ trả 7 trường client cần; không lộ active và sortOrder của entity.
export class AvatarAssetDto {
  id!: string;
  slot!: AvatarSlot;
  code!: string;
  name!: string;
  imageUrl!: string;
  zIndex!: number;
  priceDiamond!: number;

  static from(asset: AvatarAsset): AvatarAssetDto {
    const dto = new AvatarAssetDto();
    dto.id = asset.id;
    dto.slot = asset.slot;
    dto.code = asset.code;
    dto.name = asset.name;
    dto.imageUrl = asset.imageUrl;
    dto.zIndex = asset.zIndex;
    dto.priceDiamond = asset.priceDiamond;
    return dto;
  }
}

// [SERVICE] avatar.service.ts · L25–36, L38–43, L45–52
@Injectable()
export class AvatarService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    // [REPOSITORY] Mỗi entity có một Repository do TypeORM cấp qua @InjectRepository
    // (assetRepo, itemRepo, configRepo).
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    @InjectRepository(UserAvatarItem)
    private readonly itemRepo: Repository<UserAvatarItem>,
    @InjectRepository(UserAvatarConfig)
    private readonly configRepo: Repository<UserAvatarConfig>,
    // [ECONOMY] module khác, dùng ở mục 3 (mua item).
    private readonly economy: EconomyService,
  ) {}

  async listCatalog(slot?: AvatarSlot): Promise<AvatarAsset[]> {
    // [DB] ≈ SELECT … FROM avatar_assets WHERE active = true [AND slot = $1] ORDER BY
    // sort_order, code
    // Giá nằm trong DB nên server là nguồn sự thật; client không gửi giá.
    return this.assetRepo.find({
      where: { active: true, ...(slot ? { slot } : {}) },
      order: { sortOrder: 'ASC', code: 'ASC' },
    });
  }

  async listMyItems(userId: string): Promise<AvatarAsset[]> {
    // [DB] ≈ SELECT … FROM user_avatar_items WHERE user_id = $1
    const items = await this.itemRepo.findBy({ userId });
    if (items.length === 0) return [];
    // Truy vấn thứ 2: nạp chi tiết các item đó bằng WHERE id IN (...). In(...) của
    // TypeORM sinh ra điều kiện này.
    return this.assetRepo.find({
      where: { id: In(items.map((i) => i.avatarAssetId)) },
      order: { sortOrder: 'ASC' },
    });
  }
  // … class AvatarService còn tiếp: các method khác nằm ở các mục sau.

// ────────────────────────────────────────────────────────────────────────────
// 2) NHẬN ITEM MIỄN PHÍ
// POST /api/v1/avatar/items/:assetId/claim
// ────────────────────────────────────────────────────────────────────────────

// [CLIENT] api.ts · L55–57
// Hook useClaimAvatarItem. assetId đi trên URL, không có body.
await apiClient.POST('/api/v1/avatar/items/{assetId}/claim', {
  params: { path: { assetId } },
});

// [CONTROLLER] avatar.controller.ts · L73–81
  @Post('items/:assetId/claim')
  // Thành công trả 204, không có nội dung.
  @HttpCode(HttpStatus.NO_CONTENT)
  async claim(
    @CurrentUser() user: AuthenticatedUser,
    // [PIPE] ParseUUIDPipe: assetId không phải UUID → 400 ngay tại đây, không chạm DB.
    @Param('assetId', ParseUUIDPipe) assetId: string,
  ): Promise<void> {
    await this.avatarService.claim(user.userId, assetId);
  }

// [SERVICE] avatar.service.ts · L55–65
  async claim(userId: string, assetId: string): Promise<void> {
    const asset = await this.getAssetOrThrow(assetId);
    // Item có giá thì không nhận thẳng được: phải đi đường mua (mục 3).
    if (asset.priceDiamond !== 0) {
      // Lỗi nghiệp vụ: DomainException(code, message, HTTP status).
      // GlobalExceptionFilter (dùng chung) đổi nó thành { error: { code, message,
      // traceId } }.
      throw new DomainException(
        AvatarErrors.ASSET_REQUIRES_PURCHASE,
        'Item này phải mua — dùng buy(), không claim() được',
        HttpStatus.BAD_REQUEST,
      );
    }
    // Item free: không cần qua Economy, ghi quyền sở hữu luôn.
    await this.grantOwnership(this.itemRepo.manager, userId, assetId);
  }

// [REPOSITORY] avatar.service.ts · L159–169 — getAssetOrThrow (dùng ở claim, buy, equip)
  private async getAssetOrThrow(assetId: string): Promise<AvatarAsset> {
    // [DB] ≈ SELECT … FROM avatar_assets WHERE id = $1 AND active = true LIMIT 1
    // Không có hoặc đã bị tắt (active = false) → 404 AVATAR_ASSET_NOT_FOUND.
    const asset = await this.assetRepo.findOneBy({ id: assetId, active: true });
    if (!asset) {
      throw new DomainException(
        AvatarErrors.ASSET_NOT_FOUND,
        'Không tìm thấy item avatar',
        HttpStatus.NOT_FOUND,
      );
    }
    return asset;
  }

// [REPOSITORY] avatar.service.ts · L171–183 — grantOwnership (dùng ở buy, initDefaultConfig)
// Ghi "user này sở hữu item này". manager là EntityManager truyền từ ngoài vào để
// initDefaultConfig gọi được trong cùng một transaction.
  private async grantOwnership(
    manager: DataSource['manager'],
    userId: string,
    assetId: string,
  ): Promise<void> {
    await manager
      .createQueryBuilder()
      .insert()
      .into(UserAvatarItem)
      .values({ userId, avatarAssetId: assetId })
      // [DB] orIgnore() = INSERT … ON CONFLICT DO NOTHING.
      // Gọi lần 2 (bấm đúp, retry) không lỗi và không tạo dòng thứ hai nhờ ràng buộc
      // unique (user_id, avatar_asset_id). Đây là chỗ làm cho claim và buy idempotent.
      .orIgnore()
      .execute();
  }

// ────────────────────────────────────────────────────────────────────────────
// 3) MUA ITEM TRẢ PHÍ
// POST /api/v1/avatar/items/:assetId/buy
// ────────────────────────────────────────────────────────────────────────────

// [CLIENT] api.ts · L70–76
// Hook useBuyAvatarItem. Idempotency-Key là một UUID sinh 1 lần cho mỗi lần user bấm
// Mua và giữ nguyên khi retry mạng. Nhờ vậy server nhận ra "cùng một lần mua" và không
// trừ tiền hai lần.
const res = await apiClient.POST('/api/v1/avatar/items/{assetId}/buy', {
  params: {
    path: { assetId: input.assetId },
    header: { 'Idempotency-Key': input.idempotencyKey },
  },
});
return res.data?.data;

// [CONTROLLER] avatar.controller.ts · L83–94
  @Post('items/:assetId/buy')
  // POST nhưng trả 200: retry trùng key vẫn là thành công, không có tài nguyên mới.
  @HttpCode(HttpStatus.OK)
  async buy(
    @CurrentUser() user: AuthenticatedUser,
    // [PIPE] ParseUUIDPipe: assetId không phải UUID → 400.
    @Param('assetId', ParseUUIDPipe) assetId: string,
    // [PIPE] Param decorator dùng chung: đọc header Idempotency-Key; thiếu hoặc dài quá
    // 128 ký tự → 400.
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<AvatarBuyResultDto> {
    return this.avatarService.buy(user.userId, assetId, idempotencyKey);
  }

// [SERVICE] avatar.constants.ts · L7–12 — khóa idempotency gửi sang Economy
// Key gửi Economy = avatar:buy:{userId}:{key của client}. Prefix theo tính năng và
// userId để key của user khác, hay của tính năng khác, không đụng nhau.
export function avatarBuyIdempotencyKey(
  userId: string,
  clientKey: string,
): string {
  return `avatar:buy:${userId}:${clientKey}`;
}

// [SERVICE] avatar.service.ts · L72–94
  async buy(
    userId: string,
    assetId: string,
    idempotencyKey: string,
  ): Promise<{ replayed: boolean }> {
    // Giá đọc từ DB (asset.priceDiamond); client không gửi giá nên không thể sửa giá.
    const asset = await this.getAssetOrThrow(assetId);
    // Item free thì phải dùng claim (mục 2), không mua.
    if (asset.priceDiamond === 0) {
      throw new DomainException(
        AvatarErrors.ASSET_IS_FREE,
        'Item này miễn phí — dùng claim(), không buy() được',
        HttpStatus.BAD_REQUEST,
      );
    }
    // [ECONOMY] EconomyService.spendDiamond, module dùng chung với Gift, VIP, Speed-up
    // (không chép code). Bên trong, phần ghi nằm trong 1 transaction Postgres:
    // 1. Key này đã xử lý rồi → trả replayed: true, KHÔNG trừ lần hai.
    // 2. Khóa ví bằng SELECT … FOR UPDATE: các giao dịch của cùng một user xếp hàng,
    //    không trừ chồng.
    // 3. Kiểm số dư ngay lúc trừ; thiếu → 422 ECONOMY_WALLET_INSUFFICIENT_BALANCE.
    // 4. Ghi sổ kép (Nợ ví người dùng / Có doanh thu hệ thống) và 1 dòng outbox_events
    //    cùng transaction. Job nền sẽ đẩy event đó sang Kafka sau.
    const { replayed } = await this.economy.spendDiamond(
      userId,
      TransactionType.AvatarPurchase,
      asset.priceDiamond,
      avatarBuyIdempotencyKey(userId, idempotencyKey),
      { avatarAssetId: assetId, assetCode: asset.code },
    );
    // Trừ tiền xong MỚI ghi sở hữu. Đây là hai transaction riêng. Nếu server chết giữa
    // hai bước, user bấm lại với cùng key: spendDiamond trả replayed: true (không trừ
    // lại) rồi dòng này chạy tiếp, nên không có chuyện mất tiền mà không có item.
    await this.grantOwnership(this.itemRepo.manager, userId, assetId);
    // replayed = true nghĩa là đây là lần retry của một lần mua đã trừ tiền trước đó.
    return { replayed };
  }

// [DB] Lần mua đầu tiên: transactions +1, ledger_entries +2, wallets.balance giảm,
// outbox_events +1, user_avatar_items +1.

// ────────────────────────────────────────────────────────────────────────────
// 4) TRANG BỊ ITEM VÀO SLOT
// PUT /api/v1/avatar/me/equip   body { slot, avatarAssetId }
// ────────────────────────────────────────────────────────────────────────────

// [CLIENT] api.ts · L90–93
// Hook useEquipAvatarItem.
const res = await apiClient.PUT('/api/v1/avatar/me/equip', {
  body: input,
});
return res.data?.data;

// [PIPE] avatar.dtos.ts · L28–36
// ValidationPipe kiểm body: slot thuộc 5 slot, avatarAssetId là UUID. Thiếu hoặc thừa
// trường → 400.
export class EquipAvatarItemDto {
  @IsEnum(AvatarSlot)
  slot!: AvatarSlot;

  @IsUUID()
  avatarAssetId!: string;
}

// [CONTROLLER] avatar.controller.ts · L96–111
  @Put('me/equip')
  async equip(
    @CurrentUser() user: AuthenticatedUser,
    @Body() dto: EquipAvatarItemDto,
  ): Promise<AvatarConfigDto> {
    const { config, layers } = await this.avatarService.equip(
      user.userId,
      dto.slot,
      dto.avatarAssetId,
    );
    // Gói kết quả thành { userId, layers }: layers là các item đang mặc, đã sắp theo
    // zIndex.
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }

// [SERVICE] avatar.service.ts · L14–17 — kiểu kết quả nội bộ
// Service trả về 2 thứ: config (id các item đang mặc) và layers (chi tiết từng item).
export interface MyAvatar {
  config: UserAvatarConfig;
  layers: AvatarAsset[];
}

// [SERVICE] avatar.service.ts · L97–129
// Chống IDOR: chỉ được mặc item MÌNH ĐÃ SỞ HỮU và đúng slot. Thiếu bước kiểm này thì
// client gửi id của item chưa mua vẫn mặc được.
  async equip(
    userId: string,
    slot: AvatarSlot,
    assetId: string,
  ): Promise<MyAvatar> {
    const asset = await this.getAssetOrThrow(assetId);
    // Slot client gửi phải khớp slot thật của item (không nhét tóc vào ô áo) → 422.
    if (asset.slot !== slot) {
      throw new DomainException(
        AvatarErrors.ASSET_SLOT_MISMATCH,
        `Item thuộc slot '${asset.slot}', không phải '${slot}'`,
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }
    // [DB] ≈ SELECT 1 FROM user_avatar_items WHERE user_id = $1 AND avatar_asset_id =
    // $2
    // userId lấy từ JWT, không phải từ client. Chưa sở hữu → 403.
    const owned = await this.itemRepo.exists({
      where: { userId, avatarAssetId: assetId },
    });
    if (!owned) {
      throw new DomainException(
        AvatarErrors.ITEM_NOT_OWNED,
        'Chưa sở hữu item này — mua/claim trước khi trang bị',
        HttpStatus.FORBIDDEN,
      );
    }

    // Lần đầu chưa có dòng config thì tạo avatar mặc định trước (mục 5), rồi mới đổi 1
    // slot.
    const existing = await this.configRepo.findOneBy({ userId });
    if (!existing) await this.initDefaultConfig(userId);
    // [DB] ≈ UPDATE user_avatar_configs SET <cột của slot> = $1 WHERE user_id = $2. Chỉ
    // đổi đúng 1 cột.
    await this.configRepo.update(
      { userId },
      this.slotColumnPatch(slot, assetId),
    );

    // Đọc lại để trả avatar đầy đủ sau khi đổi.
    return this.getMyAvatar(userId);
  }

// [SERVICE] avatar.service.ts · L185–201 — slot → tên cột
// Đổi slot thành tên cột cần cập nhật, ví dụ hair → hairAssetId, outfit →
// outfitAssetId.
  private slotColumnPatch(
    slot: AvatarSlot,
    assetId: string,
  ): Partial<UserAvatarConfig> {
    switch (slot) {
      case AvatarSlot.Base:
        return { baseAssetId: assetId };
      case AvatarSlot.Hair:
        return { hairAssetId: assetId };
      case AvatarSlot.Face:
        return { faceAssetId: assetId };
      case AvatarSlot.Outfit:
        return { outfitAssetId: assetId };
      case AvatarSlot.Accessory:
        return { accessoryAssetId: assetId };
    }
  }

// ────────────────────────────────────────────────────────────────────────────
// 5) XEM AVATAR
// GET /api/v1/avatar/me   ·   GET /api/v1/avatar/users/:userId
// ────────────────────────────────────────────────────────────────────────────

// [CLIENT] api.ts · L22–23
// Hook useMyAvatar.
// GET /avatar/users/:userId: chưa thấy chỗ nào trong apps/web gọi; endpoint đã có sẵn ở
// server.
const res = await apiClient.GET('/api/v1/avatar/me');
return res.data?.data;

// [CONTROLLER] avatar.controller.ts · L113–125, L127–135
  @Get('me')
  async getMyAvatar(
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<AvatarConfigDto> {
    const { config, layers } = await this.avatarService.getMyAvatar(
      user.userId,
    );
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }

  @Get('users/:userId')
  async getAvatarOf(
    // Xem avatar người khác: không cần là bạn bè, nhưng vẫn phải đăng nhập (route không
    // có @Public()).
    @Param('userId', ParseUUIDPipe) userId: string,
  ): Promise<AvatarConfigDto> {
    const { config, layers } = await this.avatarService.getAvatarOf(userId);
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }

// [SERVICE] avatar.service.ts · L131–136, L139–155
  async getMyAvatar(userId: string): Promise<MyAvatar> {
    // Lazy-init: user chưa từng mở avatar thì chưa có dòng config → tạo avatar mặc định
    // ngay lúc này (không gắn vào bước đăng ký).
    const config =
      (await this.configRepo.findOneBy({ userId })) ??
      (await this.initDefaultConfig(userId));
    return { config, layers: await this.resolveLayers(config) };
  }

  async getAvatarOf(userId: string): Promise<MyAvatar> {
    const config = await this.configRepo.findOneBy({ userId });
    // Người này chưa có avatar: trả cấu hình rỗng, layers = []. Chỉ ĐỌC, không tạo dữ
    // liệu cho người khác.
    if (!config) {
      return {
        config: Object.assign(new UserAvatarConfig(), {
          userId,
          baseAssetId: null,
          hairAssetId: null,
          faceAssetId: null,
          outfitAssetId: null,
          accessoryAssetId: null,
        }),
        layers: [],
      };
    }
    return { config, layers: await this.resolveLayers(config) };
  }

// [SERVICE] avatar.service.ts · L203–216 — ghép layer
// Từ 5 cột id của config (có thể null) ra danh sách item đầy đủ, sắp theo zIndex tăng
// dần. Web vẽ chồng các ảnh theo zIndex nên layer có zIndex lớn nằm trên.
  private async resolveLayers(
    config: UserAvatarConfig,
  ): Promise<AvatarAsset[]> {
    // Gom 5 cột id; filter ở cuối bỏ các slot đang trống (null).
    const ids = [
      config.baseAssetId,
      config.hairAssetId,
      config.faceAssetId,
      config.outfitAssetId,
      config.accessoryAssetId,
    ].filter((id): id is string => id !== null);
    if (ids.length === 0) return [];
    // [DB] ≈ SELECT … FROM avatar_assets WHERE id IN (...). Một truy vấn lấy cả nhóm.
    const assets = await this.assetRepo.find({ where: { id: In(ids) } });
    // Sắp ở đây vì SQL không bảo đảm thứ tự kết quả của IN (...).
    return assets.sort((a, b) => a.zIndex - b.zIndex);
  }

// [SERVICE] avatar.service.ts · L223–253 — tạo avatar mặc định lần đầu
// Avatar mặc định = mỗi slot lấy item FREE đầu tiên (sortOrder nhỏ nhất).
  private async initDefaultConfig(userId: string): Promise<UserAvatarConfig> {
    // Mọi item free đang bật, theo sortOrder.
    const freeAssets = await this.assetRepo.find({
      where: { active: true, priceDiamond: 0 },
      order: { sortOrder: 'ASC' },
    });
    // Với mỗi slot chỉ giữ item đầu tiên.
    const bySlot = new Map<AvatarSlot, AvatarAsset>();
    for (const asset of freeAssets) {
      if (!bySlot.has(asset.slot)) bySlot.set(asset.slot, asset);
    }

    try {
      // Ghi quyền sở hữu + tạo config trong CÙNG một transaction: hoặc có đủ cả hai,
      // hoặc không có gì.
      return await this.dataSource.transaction(async (manager) => {
        for (const asset of bySlot.values()) {
          await this.grantOwnership(manager, userId, asset.id);
        }
        return manager.save(
          manager.create(UserAvatarConfig, {
            userId,
            baseAssetId: bySlot.get(AvatarSlot.Base)?.id ?? null,
            hairAssetId: bySlot.get(AvatarSlot.Hair)?.id ?? null,
            faceAssetId: bySlot.get(AvatarSlot.Face)?.id ?? null,
            outfitAssetId: bySlot.get(AvatarSlot.Outfit)?.id ?? null,
            accessoryAssetId: bySlot.get(AvatarSlot.Accessory)?.id ?? null,
          }),
        );
      });
    } catch (err) {
      // Race: 2 request đầu tiên đến cùng lúc cùng tạo config. Khóa chính user_id làm
      // bên thua dính unique violation (23505): bắt lại và đọc dòng của bên thắng,
      // không trả 500. Lỗi khác thì ném tiếp.
      if (!isUniqueViolation(err)) throw err;
      return this.configRepo.findOneByOrFail({ userId });
    }
  }

// [DB] Kết quả: tối đa 5 dòng user_avatar_items và 1 dòng user_avatar_configs, cùng
// transaction.

// ────────────────────────────────────────────────────────────────────────────
// 6) BẢNG TRONG POSTGRESQL
// Cuối luồng: dữ liệu nằm ở đây
// ────────────────────────────────────────────────────────────────────────────

// [DB] 1753100000000-avatar.ts · L12–26, L31–39, L41–51
// Điểm cần nhớ:
// - uq_user_avatar_items_pair: mỗi user chỉ sở hữu 1 lần mỗi item. Đây là nền của ON
//   CONFLICT DO NOTHING ở grantOwnership.
// - chk_avatar_assets_price_nonneg: giá không âm. Giá là dữ liệu trong DB (đổi bằng
//   UPDATE hoặc admin), không phải biến môi trường.
// - user_avatar_configs.user_id là khóa chính: mỗi user đúng 1 dòng, nên race ở
//   initDefaultConfig bị chặn.
// - Mỗi slot một cột, null = slot đang trống.
await queryRunner.query(`
  CREATE TABLE avatar_assets (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slot          varchar(16)   NOT NULL,
    code          varchar(64)   NOT NULL,
    name          varchar(128)  NOT NULL,
    image_url     varchar(2048) NOT NULL,
    z_index       int           NOT NULL,
    price_diamond int           NOT NULL DEFAULT 0,
    active        boolean       NOT NULL DEFAULT true,
    sort_order    int           NOT NULL DEFAULT 0,
    CONSTRAINT uq_avatar_assets_code UNIQUE (code),
    CONSTRAINT chk_avatar_assets_price_nonneg CHECK (price_diamond >= 0)
  )
`);

await queryRunner.query(`
  CREATE TABLE user_avatar_items (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id),
    avatar_asset_id uuid        NOT NULL REFERENCES avatar_assets(id),
    acquired_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_user_avatar_items_pair UNIQUE (user_id, avatar_asset_id)
  )
`);

await queryRunner.query(`
  CREATE TABLE user_avatar_configs (
    user_id            uuid PRIMARY KEY REFERENCES users(id),
    base_asset_id      uuid NULL REFERENCES avatar_assets(id),
    hair_asset_id      uuid NULL REFERENCES avatar_assets(id),
    face_asset_id      uuid NULL REFERENCES avatar_assets(id),
    outfit_asset_id    uuid NULL REFERENCES avatar_assets(id),
    accessory_asset_id uuid NULL REFERENCES avatar_assets(id),
    updated_at         timestamptz NOT NULL DEFAULT now()
  )
`);

// Hết. Sơ đồ luồng của tính năng:
// https://ducmanh001.github.io/litmatch-feature-flows/#08-avatar-tuy-chinh

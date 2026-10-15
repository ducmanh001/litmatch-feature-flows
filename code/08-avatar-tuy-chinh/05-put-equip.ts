// ════════════════════════════════════════════════════════════════════════════
//  PUT /api/v1/avatar/me/equip
//  Trang bị một item vào slot (chỉ item đã sở hữu, đúng slot).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.dtos.ts → avatar.controller.ts → avatar.service.ts
//  Bảng       avatar_assets, user_avatar_items, user_avatar_configs
//  Liên quan  Cuối equip gọi getMyAvatar (tạo avatar mặc định lần đầu, ghép layer): xem
//             06-get-me.ts.
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useEquipAvatarItem · L90–93
const res = await apiClient.PUT('/api/v1/avatar/me/equip', {
  body: input,
});
return res.data?.data;

// avatar.dtos.ts · L28–36
/**
 * Body: slot (1 trong 5 slot) và avatarAssetId (UUID). Thiếu hoặc thừa trường → 400.
 */
export class EquipAvatarItemDto {
  @IsEnum(AvatarSlot)
  slot!: AvatarSlot;

  @IsUUID()
  avatarAssetId!: string;
}

// avatar.controller.ts · L45–47, L96–111
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Trang bị một item vào slot.
   * In:    user từ JWT, body { slot, avatarAssetId }
   * Out:   { userId, layers } — layers là các item đang mặc, sắp theo zIndex
   */
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
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }
}

// avatar.service.ts · L14–17, L25–27, L29–34, L36, L97–129, L159–169, L185–201
/** Kết quả của service: config (id từng slot) và layers (item đầy đủ). */
export interface MyAvatar {
  config: UserAvatarConfig;
  layers: AvatarAsset[];
}

@Injectable()
export class AvatarService {
  constructor(
    // …
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    @InjectRepository(UserAvatarItem)
    private readonly itemRepo: Repository<UserAvatarItem>,
    @InjectRepository(UserAvatarConfig)
    private readonly configRepo: Repository<UserAvatarConfig>,
    // …
  ) {}

  /**
   * Trang bị item: chỉ được mặc item đã sở hữu và đúng slot.
   * In:    userId, slot, assetId
   * Out:   MyAvatar sau khi đổi
   * Lỗi:   404 item không có · 422 sai slot · 403 chưa sở hữu
   */
  async equip(
    userId: string,
    slot: AvatarSlot,
    assetId: string,
  ): Promise<MyAvatar> {
    const asset = await this.getAssetOrThrow(assetId);
    if (asset.slot !== slot) {
      throw new DomainException(
        AvatarErrors.ASSET_SLOT_MISMATCH,
        `Item thuộc slot '${asset.slot}', không phải '${slot}'`,
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }
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

    const existing = await this.configRepo.findOneBy({ userId });
    if (!existing) await this.initDefaultConfig(userId);
    await this.configRepo.update(
      { userId },
      this.slotColumnPatch(slot, assetId),
    );

    return this.getMyAvatar(userId);
  }

  /**
   * Lấy item đang bật theo id.
   * In:    assetId
   * Out:   AvatarAsset
   * Lỗi:   404 không có hoặc đã tắt
   */
  private async getAssetOrThrow(assetId: string): Promise<AvatarAsset> {
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

  /**
   * Slot → cột cần cập nhật của config.
   * In:    slot, assetId
   * Out:   { hairAssetId } / { outfitAssetId } / … tùy slot
   */
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
}

// 1753100000000-avatar.ts · L12–26, L31–39, L41–51
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

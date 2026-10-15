// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/me
//  Avatar của chính mình; lần đầu tự tạo avatar mặc định.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.controller.ts → avatar.service.ts
//  Bảng       user_avatar_configs, avatar_assets, user_avatar_items
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useMyAvatar · L22–23
const res = await apiClient.GET('/api/v1/avatar/me');
return res.data?.data;

// avatar.controller.ts · L45–47, L113–125
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Avatar của chính mình.
   * In:    user từ JWT
   * Out:   { userId, layers } — layers sắp theo zIndex
   */
  @Get('me')
  async getMyAvatar(
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<AvatarConfigDto> {
    const { config, layers } = await this.avatarService.getMyAvatar(
      user.userId,
    );
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }
}

// avatar.service.ts · L14–17, L25–34, L36, L131–136, L223–253, L171–183, L203–216
/** Kết quả của service: config (id từng slot) và layers (item đầy đủ). */
export interface MyAvatar {
  config: UserAvatarConfig;
  layers: AvatarAsset[];
}

@Injectable()
export class AvatarService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    @InjectRepository(UserAvatarItem)
    private readonly itemRepo: Repository<UserAvatarItem>,
    @InjectRepository(UserAvatarConfig)
    private readonly configRepo: Repository<UserAvatarConfig>,
    // …
  ) {}

  /**
   * Avatar của mình; chưa có thì tạo mặc định.
   * In:    userId
   * Out:   MyAvatar { config, layers }
   */
  async getMyAvatar(userId: string): Promise<MyAvatar> {
    const config =
      (await this.configRepo.findOneBy({ userId })) ??
      (await this.initDefaultConfig(userId));
    return { config, layers: await this.resolveLayers(config) };
  }

  /**
   * Tạo avatar mặc định: mỗi slot lấy item FREE đầu tiên (sortOrder nhỏ nhất); ghi sở
   * hữu và config trong 1 transaction.
   * In:    userId
   * Out:   UserAvatarConfig vừa tạo
   * Lưu ý: 2 request đầu tiên cùng lúc thì bên thua dính unique violation (khóa chính
   *        userId); bắt lại, đọc dòng của bên thắng, không trả 500.
   */
  private async initDefaultConfig(userId: string): Promise<UserAvatarConfig> {
    const freeAssets = await this.assetRepo.find({
      where: { active: true, priceDiamond: 0 },
      order: { sortOrder: 'ASC' },
    });
    const bySlot = new Map<AvatarSlot, AvatarAsset>();
    for (const asset of freeAssets) {
      if (!bySlot.has(asset.slot)) bySlot.set(asset.slot, asset);
    }

    try {
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
      if (!isUniqueViolation(err)) throw err;
      return this.configRepo.findOneByOrFail({ userId });
    }
  }

  /**
   * Ghi "user sở hữu item"; đã có thì bỏ qua (ON CONFLICT DO NOTHING).
   * In:    manager (EntityManager), userId, assetId
   * Out:   void
   */
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
      .orIgnore()
      .execute();
  }

  /**
   * 5 cột id của config thành danh sách item đầy đủ, sắp theo zIndex tăng dần (web vẽ
   * chồng theo thứ tự này).
   * In:    config
   * Out:   AvatarAsset[] ([] nếu mọi slot trống)
   */
  private async resolveLayers(
    config: UserAvatarConfig,
  ): Promise<AvatarAsset[]> {
    const ids = [
      config.baseAssetId,
      config.hairAssetId,
      config.faceAssetId,
      config.outfitAssetId,
      config.accessoryAssetId,
    ].filter((id): id is string => id !== null);
    if (ids.length === 0) return [];
    const assets = await this.assetRepo.find({ where: { id: In(ids) } });
    return assets.sort((a, b) => a.zIndex - b.zIndex);
  }
}

// 1753100000000-avatar.ts · L41–51, L12–26, L31–39
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

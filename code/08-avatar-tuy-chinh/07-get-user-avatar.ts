// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/users/:userId
//  Xem avatar của người khác (không cần là bạn bè).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      avatar.controller.ts → avatar.service.ts
//  Bảng       user_avatar_configs, avatar_assets
//  Liên quan  Web chưa gọi endpoint này (không có hook trong features/avatar).
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// avatar.controller.ts · L45–47, L127–135
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Avatar của người khác (không cần là bạn bè).
   * In:    userId (UUID) của người cần xem
   * Out:   { userId, layers } — layers rỗng nếu họ chưa có avatar
   */
  @Get('users/:userId')
  async getAvatarOf(
    @Param('userId', ParseUUIDPipe) userId: string,
  ): Promise<AvatarConfigDto> {
    const { config, layers } = await this.avatarService.getAvatarOf(userId);
    return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
  }
}

// avatar.service.ts · L14–17, L25–27, L29–30, L33–34, L36, L139–155, L203–216
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
    // …
    @InjectRepository(UserAvatarConfig)
    private readonly configRepo: Repository<UserAvatarConfig>,
    // …
  ) {}

  /**
   * Avatar của người khác; chỉ đọc, không tạo dữ liệu.
   * In:    userId của người cần xem
   * Out:   MyAvatar — chưa có config thì config rỗng và layers = []
   */
  async getAvatarOf(userId: string): Promise<MyAvatar> {
    const config = await this.configRepo.findOneBy({ userId });
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

// 1753100000000-avatar.ts · L41–51, L12–26
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

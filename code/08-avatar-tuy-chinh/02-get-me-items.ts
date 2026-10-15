// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/me/items
//  Danh sách item avatar mình đang sở hữu.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.controller.ts → avatar.dtos.ts → avatar.service.ts
//  Bảng       user_avatar_items, avatar_assets
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useMyAvatarItems · L44–45
const res = await apiClient.GET('/api/v1/avatar/me/items');
return res.data?.data ?? [];

// avatar.controller.ts · L45–47, L62–71
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Item avatar mình đang sở hữu.
   * In:    user từ JWT
   * Out:   AvatarAssetDto[]
   */
  @Get('me/items')
  async listMyItems(
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<AvatarAssetDto[]> {
    return (await this.avatarService.listMyItems(user.userId)).map(
      AvatarAssetDto.from,
    );
  }
}

// avatar.dtos.ts · L6–26
/** Dạng 1 item trả về client: 7 trường, không lộ active và sortOrder. */
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

// avatar.service.ts · L25–27, L29–32, L36, L45–52
@Injectable()
export class AvatarService {
  constructor(
    // …
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    @InjectRepository(UserAvatarItem)
    private readonly itemRepo: Repository<UserAvatarItem>,
    // …
  ) {}

  /**
   * Item user đang sở hữu.
   * In:    userId
   * Out:   AvatarAsset[] sắp theo sortOrder ([] nếu chưa có gì)
   */
  async listMyItems(userId: string): Promise<AvatarAsset[]> {
    const items = await this.itemRepo.findBy({ userId });
    if (items.length === 0) return [];
    return this.assetRepo.find({
      where: { id: In(items.map((i) => i.avatarAssetId)) },
      order: { sortOrder: 'ASC' },
    });
  }
}

// 1753100000000-avatar.ts · L31–39, L12–26
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

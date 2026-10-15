// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/avatar/items/:assetId/claim
//  Nhận item miễn phí (giá 0).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.controller.ts → avatar.service.ts
//  Bảng       avatar_assets, user_avatar_items
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useClaimAvatarItem · L55–57
await apiClient.POST('/api/v1/avatar/items/{assetId}/claim', {
  params: { path: { assetId } },
});

// avatar.controller.ts · L45–47, L73–81
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Nhận item miễn phí.
   * In:    user từ JWT, assetId (UUID)
   * Out:   204, không có nội dung
   */
  @Post('items/:assetId/claim')
  @HttpCode(HttpStatus.NO_CONTENT)
  async claim(
    @CurrentUser() user: AuthenticatedUser,
    @Param('assetId', ParseUUIDPipe) assetId: string,
  ): Promise<void> {
    await this.avatarService.claim(user.userId, assetId);
  }
}

// avatar.service.ts · L25–27, L29–32, L36, L55–65, L159–169, L171–183
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
   * Nhận item miễn phí (priceDiamond = 0). Gọi lại nhiều lần vẫn an toàn.
   * In:    userId, assetId
   * Out:   void
   * Lỗi:   404 item không có hoặc đã tắt · 400 item có giá (phải mua)
   */
  async claim(userId: string, assetId: string): Promise<void> {
    const asset = await this.getAssetOrThrow(assetId);
    if (asset.priceDiamond !== 0) {
      throw new DomainException(
        AvatarErrors.ASSET_REQUIRES_PURCHASE,
        'Item này phải mua — dùng buy(), không claim() được',
        HttpStatus.BAD_REQUEST,
      );
    }
    await this.grantOwnership(this.itemRepo.manager, userId, assetId);
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
}

// 1753100000000-avatar.ts · L12–26, L31–39
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

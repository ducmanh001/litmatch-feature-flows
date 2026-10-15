// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/avatar/items/:assetId/buy
//  Mua item avatar trả phí: trừ diamond rồi ghi quyền sở hữu.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.controller.ts → avatar.constants.ts → avatar.service.ts
//  Bảng       avatar_assets, user_avatar_items
//  Liên quan  Economy (module khác): trừ diamond, ghi sổ kép, outbox → Kafka. Không
//             chép code.
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useBuyAvatarItem · L70–76
const res = await apiClient.POST('/api/v1/avatar/items/{assetId}/buy', {
  params: {
    path: { assetId: input.assetId },
    header: { 'Idempotency-Key': input.idempotencyKey },
  },
});
return res.data?.data;

// avatar.controller.ts · L45–47, L83–94
@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Mua item trả phí.
   * In:    user từ JWT, assetId (UUID), header Idempotency-Key
   * Out:   { replayed } (HTTP 200)
   */
  @Post('items/:assetId/buy')
  @HttpCode(HttpStatus.OK)
  async buy(
    @CurrentUser() user: AuthenticatedUser,
    @Param('assetId', ParseUUIDPipe) assetId: string,
    @IdempotencyKey() idempotencyKey: string,
  ): Promise<AvatarBuyResultDto> {
    return this.avatarService.buy(user.userId, assetId, idempotencyKey);
  }
}

// avatar.constants.ts · L7–12
/**
 * Khóa chống trừ tiền 2 lần.
 * In:    userId, clientKey (Idempotency-Key của client)
 * Out:   "avatar:buy:{userId}:{clientKey}" (có userId để key của user khác không đụng
 *        nhau)
 */
export function avatarBuyIdempotencyKey(
  userId: string,
  clientKey: string,
): string {
  return `avatar:buy:${userId}:${clientKey}`;
}

// avatar.service.ts · L25–27, L29–32, L35–36, L72–94, L159–169, L171–183
@Injectable()
export class AvatarService {
  constructor(
    // …
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    @InjectRepository(UserAvatarItem)
    private readonly itemRepo: Repository<UserAvatarItem>,
    // …
    private readonly economy: EconomyService,
  ) {}

  /**
   * Mua item trả phí: trừ diamond rồi ghi quyền sở hữu.
   * In:    userId, assetId, idempotencyKey
   * Out:   { replayed } — true nếu là retry của lần mua đã trừ tiền
   * Lỗi:   404 item không có · 400 item miễn phí · 422 không đủ diamond
   * Lưu ý: giá lấy từ DB, client không gửi giá. Trừ tiền và ghi sở hữu là 2 bước riêng,
   *        mỗi bước đều idempotent nên retry cùng key an toàn.
   */
  async buy(
    userId: string,
    assetId: string,
    idempotencyKey: string,
  ): Promise<{ replayed: boolean }> {
    const asset = await this.getAssetOrThrow(assetId);
    if (asset.priceDiamond === 0) {
      throw new DomainException(
        AvatarErrors.ASSET_IS_FREE,
        'Item này miễn phí — dùng claim(), không buy() được',
        HttpStatus.BAD_REQUEST,
      );
    }
    const { replayed } = await this.economy.spendDiamond(
      userId,
      TransactionType.AvatarPurchase,
      asset.priceDiamond,
      avatarBuyIdempotencyKey(userId, idempotencyKey),
      { avatarAssetId: assetId, assetCode: asset.code },
    );
    await this.grantOwnership(this.itemRepo.manager, userId, assetId);
    return { replayed };
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

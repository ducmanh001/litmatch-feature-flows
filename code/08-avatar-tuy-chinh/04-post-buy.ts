// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/avatar/items/:assetId/buy
//  Mua item avatar trả phí: trừ diamond rồi ghi quyền sở hữu.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       avatar_assets, user_avatar_items
//  Liên quan  Economy (không chép): trừ diamond, ghi sổ kép, outbox → Kafka.
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L83–94
/** In: user từ JWT, assetId, header Idempotency-Key → Out: { replayed } */
@Post('items/:assetId/buy')
@HttpCode(HttpStatus.OK)
async buy(
  @CurrentUser() user: AuthenticatedUser,
  @Param('assetId', ParseUUIDPipe) assetId: string,
  @IdempotencyKey() idempotencyKey: string,
): Promise<AvatarBuyResultDto> {
  return this.avatarService.buy(user.userId, assetId, idempotencyKey);
}

// avatar.service.ts · L72–94, L159–169, L171–183
/**
 * In:    userId, assetId, idempotencyKey
 * Out:   { replayed } — true nếu là retry của lần mua đã trừ tiền
 * Lưu ý: trừ diamond trước, ghi sở hữu sau (2 bước riêng); mỗi bước đều idempotent
 *        nên retry cùng key không trừ lại, không ghi trùng.
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

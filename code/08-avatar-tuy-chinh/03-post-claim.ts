// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/avatar/items/:assetId/claim
//  Nhận item miễn phí (giá 0).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       avatar_assets, user_avatar_items
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L73–81
/** In: user từ JWT, assetId → Out: 204, không có nội dung */
@Post('items/:assetId/claim')
@HttpCode(HttpStatus.NO_CONTENT)
async claim(
  @CurrentUser() user: AuthenticatedUser,
  @Param('assetId', ParseUUIDPipe) assetId: string,
): Promise<void> {
  await this.avatarService.claim(user.userId, assetId);
}

// avatar.service.ts · L55–65, L159–169, L171–183
/** In: userId, assetId → Out: void (gọi lại nhiều lần vẫn an toàn) */
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

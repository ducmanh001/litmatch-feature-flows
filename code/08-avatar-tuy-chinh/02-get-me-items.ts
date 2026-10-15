// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/me/items
//  Danh sách item avatar mình đang sở hữu.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       user_avatar_items, avatar_assets
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L62–71
/** In: user từ JWT → Out: AvatarAssetDto[] (item mình sở hữu) */
@Get('me/items')
async listMyItems(
  @CurrentUser() user: AuthenticatedUser,
): Promise<AvatarAssetDto[]> {
  return (await this.avatarService.listMyItems(user.userId)).map(
    AvatarAssetDto.from,
  );
}

// avatar.service.ts · L45–52
/** In: userId → Out: item đang sở hữu ([] nếu chưa có) */
async listMyItems(userId: string): Promise<AvatarAsset[]> {
  const items = await this.itemRepo.findBy({ userId });
  if (items.length === 0) return [];
  return this.assetRepo.find({
    where: { id: In(items.map((i) => i.avatarAssetId)) },
    order: { sortOrder: 'ASC' },
  });
}

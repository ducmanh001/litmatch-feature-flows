// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/me
//  Avatar của chính mình; lần đầu tự tạo avatar mặc định.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       user_avatar_configs, avatar_assets, user_avatar_items
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L113–125
/** In: user từ JWT → Out: { userId, layers } */
@Get('me')
async getMyAvatar(
  @CurrentUser() user: AuthenticatedUser,
): Promise<AvatarConfigDto> {
  const { config, layers } = await this.avatarService.getMyAvatar(
    user.userId,
  );
  return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
}

// avatar.service.ts · L131–136, L223–253, L171–183, L203–216
/** In: userId → Out: { config, layers }; chưa có config thì tạo avatar mặc định */
async getMyAvatar(userId: string): Promise<MyAvatar> {
  const config =
    (await this.configRepo.findOneBy({ userId })) ??
    (await this.initDefaultConfig(userId));
  return { config, layers: await this.resolveLayers(config) };
}

/**
 * In:    userId
 * Out:   UserAvatarConfig mới (mỗi slot lấy item free đầu tiên)
 * Lưu ý: 2 request đầu tiên cùng lúc thì bên thua dính unique violation; bắt lại và
 *        đọc dòng của bên thắng thay vì trả lỗi 500.
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

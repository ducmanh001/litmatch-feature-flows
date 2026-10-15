// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/users/:userId
//  Xem avatar của người khác (không cần là bạn bè).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       user_avatar_configs, avatar_assets
//  Liên quan  Web chưa gọi endpoint này.
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L127–135
/** In: userId của người cần xem → Out: { userId, layers } */
@Get('users/:userId')
async getAvatarOf(
  @Param('userId', ParseUUIDPipe) userId: string,
): Promise<AvatarConfigDto> {
  const { config, layers } = await this.avatarService.getAvatarOf(userId);
  return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
}

// avatar.service.ts · L139–155, L203–216
/**
 * In:    userId → Out: { config, layers }; chưa có config thì config rỗng, layers =
 *        [] (chỉ đọc)
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

// ════════════════════════════════════════════════════════════════════════════
//  PUT /api/v1/avatar/me/equip
//  Trang bị một item vào slot (chỉ item đã sở hữu, đúng slot).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       avatar_assets, user_avatar_items, user_avatar_configs
//  Liên quan  getMyAvatar ở cuối equip: xem 06-get-me.ts.
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L96–111
/** In: user từ JWT, body { slot, avatarAssetId } → Out: { userId, layers } */
@Put('me/equip')
async equip(
  @CurrentUser() user: AuthenticatedUser,
  @Body() dto: EquipAvatarItemDto,
): Promise<AvatarConfigDto> {
  const { config, layers } = await this.avatarService.equip(
    user.userId,
    dto.slot,
    dto.avatarAssetId,
  );
  return { userId: config.userId, layers: layers.map(AvatarAssetDto.from) };
}

// avatar.service.ts · L97–129, L159–169
/** In: userId, slot, assetId → Out: avatar sau khi đổi { config, layers } */
async equip(
  userId: string,
  slot: AvatarSlot,
  assetId: string,
): Promise<MyAvatar> {
  const asset = await this.getAssetOrThrow(assetId);
  if (asset.slot !== slot) {
    throw new DomainException(
      AvatarErrors.ASSET_SLOT_MISMATCH,
      `Item thuộc slot '${asset.slot}', không phải '${slot}'`,
      HttpStatus.UNPROCESSABLE_ENTITY,
    );
  }
  const owned = await this.itemRepo.exists({
    where: { userId, avatarAssetId: assetId },
  });
  if (!owned) {
    throw new DomainException(
      AvatarErrors.ITEM_NOT_OWNED,
      'Chưa sở hữu item này — mua/claim trước khi trang bị',
      HttpStatus.FORBIDDEN,
    );
  }

  const existing = await this.configRepo.findOneBy({ userId });
  if (!existing) await this.initDefaultConfig(userId);
  await this.configRepo.update(
    { userId },
    this.slotColumnPatch(slot, assetId),
  );

  return this.getMyAvatar(userId);
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

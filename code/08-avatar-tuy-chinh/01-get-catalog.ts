// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/catalog
//  Danh sách item avatar đang bán, lọc được theo slot.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      client → avatar.controller.ts → avatar.service.ts → PostgreSQL
//  Bảng       avatar_assets
//  Nguồn      litmatch @ d3cb28c · 2026-10-15

// avatar.controller.ts · L49–60
/** In: query slot (tùy chọn) → Out: AvatarAssetDto[] */
@Get('catalog')
async listCatalog(
  @Query() query: CatalogQueryDto,
): Promise<AvatarAssetDto[]> {
  return (await this.avatarService.listCatalog(query.slot)).map(
    AvatarAssetDto.from,
  );
}

// avatar.service.ts · L38–43
/** In: slot (tùy chọn) → Out: item đang bật, sắp theo sortOrder */
async listCatalog(slot?: AvatarSlot): Promise<AvatarAsset[]> {
  return this.assetRepo.find({
    where: { active: true, ...(slot ? { slot } : {}) },
    order: { sortOrder: 'ASC', code: 'ASC' },
  });
}

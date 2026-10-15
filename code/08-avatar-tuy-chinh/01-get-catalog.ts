// ════════════════════════════════════════════════════════════════════════════
//  GET /api/v1/avatar/catalog
//  Danh sách item avatar đang bán, lọc được theo slot.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      api.ts → avatar.controller.ts → avatar.dtos.ts → avatar.service.ts
//  Bảng       avatar_assets
//  Nguồn      litmatch @ d3cb28c · 2026-10-15. Code trích nguyên văn, đã lược import,
//             comment cũ và decorator Swagger.

// api.ts · useAvatarCatalog · L32–33
const res = await apiClient.GET('/api/v1/avatar/catalog');
return res.data?.data ?? [];

// avatar.controller.ts · L37–41, L45–47, L49–60
/**
 * Query ?slot (không bắt buộc): base | hair | face | outfit | accessory. Sai giá trị
 * hoặc thừa tham số → 400.
 */
class CatalogQueryDto {
  @IsOptional()
  @IsEnum(AvatarSlot)
  slot?: AvatarSlot;
}

@Controller('avatar')
export class AvatarController {
  constructor(private readonly avatarService: AvatarService) {}

  /**
   * Danh sách item đang bán.
   * In:    query slot (tùy chọn)
   * Out:   AvatarAssetDto[]
   */
  @Get('catalog')
  async listCatalog(
    @Query() query: CatalogQueryDto,
  ): Promise<AvatarAssetDto[]> {
    return (await this.avatarService.listCatalog(query.slot)).map(
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

// avatar.service.ts · L25–27, L29–30, L36, L38–43
@Injectable()
export class AvatarService {
  constructor(
    // …
    @InjectRepository(AvatarAsset)
    private readonly assetRepo: Repository<AvatarAsset>,
    // …
  ) {}

  /**
   * Item đang bật, lọc theo slot nếu có.
   * In:    slot (tùy chọn)
   * Out:   AvatarAsset[] sắp theo sortOrder, code
   */
  async listCatalog(slot?: AvatarSlot): Promise<AvatarAsset[]> {
    return this.assetRepo.find({
      where: { active: true, ...(slot ? { slot } : {}) },
      order: { sortOrder: 'ASC', code: 'ASC' },
    });
  }
}

// 1753100000000-avatar.ts · L12–26
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

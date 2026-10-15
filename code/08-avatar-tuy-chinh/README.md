# Avatar tùy chỉnh: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo luồng: client, controller, service, bảng.
Code trích nguyên văn từ repo (số dòng gốc ghi ở đầu mỗi đoạn); comment chỉ là khối ghi đầu hàm (mục đích, input, output).
Phần dùng chung không chép: middleware, guard, interceptor, pipe toàn cục, exception filter.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `GET /api/v1/avatar/catalog` | Danh sách item avatar đang bán, lọc được theo slot. | avatar_assets | [01-get-catalog.ts](01-get-catalog.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/01-get-catalog.html) |
| 02 | `GET /api/v1/avatar/me/items` | Danh sách item avatar mình đang sở hữu. | user_avatar_items, avatar_assets | [02-get-me-items.ts](02-get-me-items.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/02-get-me-items.html) |
| 03 | `POST /api/v1/avatar/items/:assetId/claim` | Nhận item miễn phí (giá 0). | avatar_assets, user_avatar_items | [03-post-claim.ts](03-post-claim.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/03-post-claim.html) |
| 04 | `POST /api/v1/avatar/items/:assetId/buy` | Mua item avatar trả phí: trừ diamond rồi ghi quyền sở hữu. | avatar_assets, user_avatar_items | [04-post-buy.ts](04-post-buy.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/04-post-buy.html) |
| 05 | `PUT /api/v1/avatar/me/equip` | Trang bị một item vào slot (chỉ item đã sở hữu, đúng slot). | avatar_assets, user_avatar_items, user_avatar_configs | [05-put-equip.ts](05-put-equip.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/05-put-equip.html) |
| 06 | `GET /api/v1/avatar/me` | Avatar của chính mình; lần đầu tự tạo avatar mặc định. | user_avatar_configs, avatar_assets, user_avatar_items | [06-get-me.ts](06-get-me.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/06-get-me.html) |
| 07 | `GET /api/v1/avatar/users/:userId` | Xem avatar của người khác (không cần là bạn bè). | user_avatar_configs, avatar_assets | [07-get-user-avatar.ts](07-get-user-avatar.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/08-avatar-tuy-chinh/07-get-user-avatar.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#08-avatar-tuy-chinh

# Discovery, Nearby và Direct Invite: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/matching/invites/:id/accept` | Người được mời chấp nhận: tạo thẳng 2 ticket và 1 session đã confirmed, bỏ qua hàng đợi, idempotent theo (lời mời, vai trò). | match_invites, match_tickets, match_sessions, users | [01-post-accept-invite.ts](01-post-accept-invite.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/01-post-accept-invite.html) |
| 02 | `GET /api/v1/discovery/nearby` | Trả danh sách người gần vị trí của mình dưới dạng nhóm khoảng cách, có 3 lớp chống dò vị trí. | user_locations, discovery_settings, user_privacy_settings, users | [02-get-nearby.ts](02-get-nearby.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/02-get-nearby.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#18-discovery-nearby-va-direct-invite

# Discovery, Nearby và Direct Invite: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi thụt vào ngay dưới dòng gọi (service, Postgres, Redis, worker...), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Constructor chỉ gồm các dependency được dùng.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/matching/invites/:id/accept` | Người được mời chấp nhận: tạo thẳng 2 ticket và 1 session đã confirmed, bỏ qua hàng đợi, idempotent theo (lời mời, vai trò). | match_invites, match_tickets, match_sessions, users | [01-post-accept-invite.ts](01-post-accept-invite.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/01-post-accept-invite.html) |
| 02 | `GET /api/v1/discovery/nearby` | Trả danh sách người gần vị trí của mình dưới dạng nhóm khoảng cách, có 3 lớp chống dò vị trí. | user_locations, discovery_settings, user_privacy_settings, users | [02-get-nearby.ts](02-get-nearby.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/02-get-nearby.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#18-discovery-nearby-va-direct-invite

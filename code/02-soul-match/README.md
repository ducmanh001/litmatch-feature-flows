# Soul Match: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/soul-match/sessions/:id/messages` | Gửi tin nhắn trong phòng ẩn danh: giờ chat tính ở server, idempotent theo key, realtime riêng cho từng người nhận. | match_sessions, soul_chat_messages, users, friendships | [01-post-send-message.ts](01-post-send-message.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/01-post-send-message.html) |
| 02 | `POST /api/v1/soul-match/sessions/:id/rating` | Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction. | match_sessions, soul_match_ratings, soul_chat_messages, friendships, conversations, messages | [02-post-rating.ts](02-post-rating.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/02-post-rating.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#02-soul-match

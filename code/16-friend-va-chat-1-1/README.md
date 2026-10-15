# Friend và Chat 1-1: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/conversations/:id/messages` | Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận. | conversations, messages, conversation_streaks, conversation_member_states, blocks, notifications | [01-post-send-message.ts](01-post-send-message.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/16-friend-va-chat-1-1/01-post-send-message.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#16-friend-va-chat-1-1

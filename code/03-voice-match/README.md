# Voice Match: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/matching/tickets` | Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession. | match_tickets, match_sessions, match_daily_quotas, users | [01-post-join-queue.ts](01-post-join-queue.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/01-post-join-queue.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#03-voice-match

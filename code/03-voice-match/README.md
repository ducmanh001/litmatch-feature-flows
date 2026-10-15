# Voice Match: code theo từng endpoint

Mỗi file là **một endpoint**: từ controller xuống service, Redis và phần xử lý bên dưới (worker, DB), chỉ giữ luồng xử lý chính.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment chỉ là khối ghi đầu hàm: input, output và chỗ logic khó hiểu.
Không chép: DTO, interface, schema, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/matching/tickets` | Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession. | match_tickets, match_sessions, match_daily_quotas, users | [01-post-join-queue.ts](01-post-join-queue.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/01-post-join-queue.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#03-voice-match

# Voice Match: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi thụt vào ngay dưới dòng gọi (service, Postgres, Redis, worker...), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Constructor chỉ gồm các dependency được dùng.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/matching/tickets` | Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession. | match_tickets, match_sessions, match_daily_quotas, users | [01-post-join-queue.ts](01-post-join-queue.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/01-post-join-queue.html) |
| 02 | `POST /api/v1/calling/match-sessions/:matchSessionId/join` | Mở phòng gọi cho cặp đã ghép: tạo CallSession duy nhất rồi cấp token LiveKit riêng cho từng người. | call_sessions, match_sessions, users | [02-post-join-call.ts](02-post-join-call.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/02-post-join-call.html) |
| 03 | `POST /api/v1/calling/webhooks/livekit` | LiveKit báo ai vào hoặc rời phòng, ticker nền canh giờ (reconnect, tối đa 420 giây) rồi kết thúc call. | call_sessions, match_sessions | [03-post-webhook-livekit.ts](03-post-webhook-livekit.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/03-post-webhook-livekit.html) |
| 04 | `POST /api/v1/calling/calls/:id/like` | Ghi lượt thích bất biến; khi cả 2 cùng thích thì tạo bạn bè và phòng chat trong cùng transaction. | call_sessions, voice_match_reactions, friendships, conversations | [04-post-like-call.ts](04-post-like-call.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/04-post-like-call.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#03-voice-match

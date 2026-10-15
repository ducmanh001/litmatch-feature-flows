# Mini Game: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/mini-game/sessions/:id/moves` | Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai. | mini_game_sessions, mini_game_active_participants | [01-post-submit-move.ts](01-post-submit-move.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/12-mini-game/01-post-submit-move.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#12-mini-game

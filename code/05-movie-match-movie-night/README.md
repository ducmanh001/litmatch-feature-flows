# Movie Match / Movie Night: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/movie-match/sessions` | Tạo phiên xem video chung giữa 2 người bạn, idempotent theo cặp, mỗi user chỉ có 1 phiên đang xem. | movie_sessions, movie_session_active_participants | [01-post-create-session.ts](01-post-create-session.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/05-movie-match-movie-night/01-post-create-session.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#05-movie-match-movie-night

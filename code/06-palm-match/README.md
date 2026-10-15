# Palm Match: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/palm-match/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match. | palm_match_queue_entries, palm_match_active_participants, palm_match_sessions, palm_reading_templates | [01-post-join-queue.ts](01-post-join-queue.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/06-palm-match/01-post-join-queue.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#06-palm-match

# Matching engine: matcher, định tuyến shard và repair hàng đợi: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File | Nguồn |
|---|----------|--------|------|------|-------|
| 01 | `JOB MatcherWorkerService.runOnce (đánh thức khi có ticket + backstop định kỳ)` | Duyệt các shard đang có vé, chọn cặp tương thích trong cửa sổ đầu shard, claim cả hai vé nguyên tử, xác minh lại dưới khoá rồi tạo session. | match_tickets, match_sessions, users, notifications | [01-job-matcher-tick.ts](01-job-matcher-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/01-job-matcher-tick.html) | local, chưa commit |
| 02 | `JOB QueueRepairService.runOnce (mỗi MATCHING_QUEUE_REPAIR_INTERVAL_MS, một pod duy nhất)` | Quét một trang vé queued còn trong hạn theo cursor bền vững (enqueued_at, id), khôi phục vé thiếu trong Redis bằng NX rồi refresh score theo DB bằng XX LT, đánh dấu shard active và đánh thức matcher. | match_tickets, runtime_job_checkpoints | [02-job-matching-queue-repair.ts](02-job-matching-queue-repair.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/02-job-matching-queue-repair.html) | local, chưa commit |
| 03 | `QUEUE ShardedMatchingQueue (khi MATCHING_REDIS_URLS có từ hai Redis trở lên)` | Mỗi hàng đợi logic được băm (FNV-1a) tới đúng MỘT Redis vật lý nên claim hai vé vẫn nguyên tử; liệt kê shard active gộp từ các Redis còn khoẻ và bỏ mục lỗi thời sau khi đổi topology. |  | [03-queue-sharded-routing.ts](03-queue-sharded-routing.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/03-queue-sharded-routing.html) | local, chưa commit |

Kiến trúc 10M của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/matching.html

Sơ đồ sản phẩm của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#01-matching-engine

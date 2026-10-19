# Vòng đời dữ liệu: xoá theo lô mà không tranh kết nối: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `JOB RetentionJob.runOnce (mỗi RETENTION_INTERVAL_MS, một pod duy nhất)` | Chạy dưới khoá cluster-singleton và ngân sách đồng thời, lấy quy tắc của module, xoá theo lô bằng câu lệnh ngắn tự commit, ghi metric. | refresh_tokens, phone_otps | [01-job-retention-tick.ts](01-job-retention-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/01-job-retention-tick.html) |
| 02 | `JOB StorySweeperService.runOnce (mỗi STORY_SWEEPER_INTERVAL_MS)` | Xoá story hết hạn theo lô 500; story_views tự xoá theo FK ON DELETE CASCADE; tick lặp lô tới hết backlog hoặc hết ngân sách 30 giây. | stories, story_views | [02-job-story-sweeper-tick.ts](02-job-story-sweeper-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/02-job-story-sweeper-tick.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#00-project-architecture

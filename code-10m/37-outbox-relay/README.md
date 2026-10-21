# Outbox relay: bắt kịp backlog có ngân sách: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File | Nguồn |
|---|----------|--------|------|------|-------|
| 01 | `JOB OutboxRelayService.flushOnce (mỗi ECONOMY_OUTBOX_RELAY_INTERVAL_MS; TẮT mặc định)` | Mỗi tick bắt kịp backlog qua tối đa MAX_BATCHES lô (100 event/lô) trong ngân sách thời gian: chọn event chưa publish bằng FOR UPDATE SKIP LOCKED, publish Kafka, thử lại có trần rồi dead-letter. Relay đang tắt mặc định và chưa có consumer. | outbox_events | [01-job-outbox-relay-tick.ts](01-job-outbox-relay-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/37-outbox-relay/01-job-outbox-relay-tick.html) | local, chưa commit |

Kiến trúc 10M của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/ledger-integrity.html

Sơ đồ sản phẩm của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#09-diamond-top-up-va-vip

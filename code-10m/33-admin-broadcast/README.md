# Admin broadcast: xếp hàng rồi gửi nền theo lô: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/admin/notifications/broadcast` | Ghi một dòng broadcast ở trạng thái queued kèm audit (202); Idempotency-Key làm bấm hai lần chỉ tạo một broadcast. | notification_broadcasts, admin_audit_logs | [01-post-broadcast.ts](01-post-broadcast.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/01-post-broadcast.html) |
| 02 | `JOB BroadcastDispatchJob.runOnce (mỗi ADMIN_BROADCAST_JOB_INTERVAL_MS)` | Nhặt broadcast queued/running cũ nhất, đọc một trang user theo keyset, tạo thông báo và đẩy cursor trong CÙNG transaction, rồi push sau commit. | notification_broadcasts, notifications, users | [02-job-broadcast-dispatch-tick.ts](02-job-broadcast-dispatch-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/02-job-broadcast-dispatch-tick.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#00-project-architecture

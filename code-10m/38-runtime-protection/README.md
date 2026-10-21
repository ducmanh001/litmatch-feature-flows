# Bảo vệ tài nguyên: admission HTTP và readiness: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File | Nguồn |
|---|----------|--------|------|------|-------|
| 01 | `HTTP RequestConcurrencyInterceptor.intercept (APP_INTERCEPTOR: mọi request HTTP trừ health)` | Mỗi request giữ một slot trong ngân sách của pod; hết chỗ thì trả 503 kèm Retry-After: 1 ngay, không xếp hàng; slot được nhả khi request xong, lỗi hoặc bị huỷ. |  | [01-http-request-admission.ts](01-http-request-admission.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/38-runtime-protection/01-http-request-admission.html) | local, chưa commit |
| 02 | `GET /health/ready` | Gộp các probe song song, cache kết quả 1 giây và chỉ giữ tối đa một SQL probe chưa xong, để kubelet thăm dò dày hoặc DB chậm không làm đầy pool. |  | [02-get-health-ready.ts](02-get-health-ready.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/38-runtime-protection/02-get-health-ready.html) | local, chưa commit |

Kiến trúc 10M của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/protection.html


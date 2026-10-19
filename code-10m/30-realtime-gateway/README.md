# Realtime gateway: kết nối và giữ kết nối: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `WS socket.io: handshake middleware → connect` | Handshake xác thực JWT, xin lease slot (tối đa N socket/user) trên Redis và SUBSCRIBE kênh của chính user; chỉ khi xong hết mới đăng ký gia hạn lease. |  | [01-ws-handshake-admit.ts](01-ws-handshake-admit.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/01-ws-handshake-admit.html) |
| 02 | `JOB LeaseRefresher.tick (mỗi giây, một timer cho cả pod)` | Mỗi giây gom các lease đến hạn của cả pod thành vài pipeline, thử cấp lại lease Redis báo mất, nhả lease cho socket đã ngắt, ngắt socket chỉ khi không còn chứng minh được lease. |  | [02-job-lease-refresh-tick.ts](02-job-lease-refresh-tick.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/02-job-lease-refresh-tick.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#00-project-architecture

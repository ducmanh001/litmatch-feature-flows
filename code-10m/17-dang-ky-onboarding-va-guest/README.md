# Đăng nhập: refresh token (bảng tăng nhanh nhất) và giao OTP: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File | Nguồn |
|---|----------|--------|------|------|-------|
| 01 | `POST /api/v1/auth/refresh` | Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token. | refresh_tokens, users | [01-post-refresh.ts](01-post-refresh.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/17-dang-ky-onboarding-va-guest/01-post-refresh.html) | đã đẩy (PR #84) |
| 02 | `POST /api/v1/auth/otp/request` | Đếm hạn mức theo SỐ ĐIỆN THOẠI ở server, sinh mã và chỉ lưu hash; chế độ sms-http lưu mã mới trước, giao qua cổng SMS rồi mới vô hiệu mã cũ, giao lỗi thì vô hiệu chính mã vừa tạo và trả 503 mà không lộ mã. | phone_otps | [02-post-otp-request.ts](02-post-otp-request.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/17-dang-ky-onboarding-va-guest/02-post-otp-request.html) | đã đẩy (PR #84) |

Kiến trúc 10M của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/data-lifecycle.html

Sơ đồ sản phẩm của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#17-dang-ky-onboarding-va-guest

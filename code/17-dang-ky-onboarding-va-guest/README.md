# Đăng ký, Onboarding và Guest: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hết dependency được dùng ở đầu trang.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/auth/otp/verify` | Kiểm mã OTP có giới hạn số lần thử và chỉ dùng một lần, rồi tìm hoặc tạo tài khoản và cấp phiên đăng nhập. | phone_otps, auth_identities, users, refresh_tokens | [01-post-verify-otp.ts](01-post-verify-otp.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/17-dang-ky-onboarding-va-guest/01-post-verify-otp.html) |
| 02 | `POST /api/v1/auth/refresh` | Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token. | refresh_tokens, users | [02-post-refresh.ts](02-post-refresh.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/17-dang-ky-onboarding-va-guest/02-post-refresh.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#17-dang-ky-onboarding-va-guest

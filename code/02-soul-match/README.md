# Soul Match: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hết dependency được dùng ở đầu trang.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/soul-match/sessions/:id/messages` | Gửi tin nhắn trong phòng ẩn danh: giờ chat tính ở server, idempotent theo key, realtime riêng cho từng người nhận. | match_sessions, soul_chat_messages, users, friendships | [01-post-send-message.ts](01-post-send-message.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/01-post-send-message.html) |
| 02 | `POST /api/v1/soul-match/sessions/:id/rating` | Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction. | match_sessions, soul_match_ratings, soul_chat_messages, friendships, conversations, messages | [02-post-rating.ts](02-post-rating.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/02-post-rating.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#02-soul-match

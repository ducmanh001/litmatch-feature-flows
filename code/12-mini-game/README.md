# Mini Game: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hết dependency được dùng ở đầu trang.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/mini-game/sessions/:id/moves` | Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai. | mini_game_sessions, mini_game_active_participants | [01-post-submit-move.ts](01-post-submit-move.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/12-mini-game/01-post-submit-move.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#12-mini-game

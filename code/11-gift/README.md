# Gift: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hết dependency được dùng ở đầu trang.
Code trích nguyên văn từ repo (số dòng gốc ghi ở dòng tiêu đề mỗi khối), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/party/rooms/:roomId/gifts` | Tặng quà trong Party Room: trừ DIA người tặng, cộng PTS người nhận, ghi GiftEvent trong cùng một transaction. | gifts, transactions, ledger_entries, wallets, gift_events, outbox_events, notifications, party_rooms, party_room_members | [01-post-party-room-gift.ts](01-post-party-room-gift.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/11-gift/01-post-party-room-gift.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#11-gift

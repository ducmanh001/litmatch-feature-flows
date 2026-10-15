# Gift: code theo từng endpoint

Mỗi file là **một endpoint**, đi từ trên xuống theo thứ tự chạy: controller, service, Redis, Postgres, worker bên dưới.
Mỗi đoạn có nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ); bản Pages tô màu các nhãn này.
Code trích nguyên văn từ repo (số dòng gốc ở đầu mỗi đoạn), đã lược import, comment cũ, decorator Swagger và thụt lề của class. Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Không chép: DTO, interface, phần dùng chung (middleware, guard, interceptor, pipe toàn cục, exception filter).

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/party/rooms/:roomId/gifts` | Tặng quà trong Party Room: trừ DIA người tặng, cộng PTS người nhận, ghi GiftEvent trong cùng một transaction. | gifts, transactions, ledger_entries, wallets, gift_events, outbox_events, notifications, party_rooms, party_room_members | [01-post-party-room-gift.ts](01-post-party-room-gift.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/11-gift/01-post-party-room-gift.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#11-gift

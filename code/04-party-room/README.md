# Party Room: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/party/rooms/:id/join` | Vào phòng Party làm audience, hoặc vào lại khi rớt mạng, rồi nhận token LiveKit theo role hiện tại (audience không được publish). | party_rooms, party_room_members | [01-post-join-room.ts](01-post-join-room.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/01-post-join-room.html) |
| 02 | `POST /api/v1/party/rooms` | Tạo phòng và membership host trong 1 transaction, rồi tạo room trên LiveKit; SFU lỗi thì đóng phòng bù trừ. | party_rooms, party_room_members, users | [02-post-create-room.ts](02-post-create-room.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/02-post-create-room.html) |
| 03 | `POST /api/v1/party/rooms/:id/speaker-invite/accept` | Audience đồng ý lời mời của host để lên speaker: đếm giới hạn dưới khóa phòng, commit role, rồi mở quyền publish trên LiveKit. | party_rooms, party_room_members | [03-post-accept-speaker-invite.ts](03-post-accept-speaker-invite.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/03-post-accept-speaker-invite.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#04-party-room

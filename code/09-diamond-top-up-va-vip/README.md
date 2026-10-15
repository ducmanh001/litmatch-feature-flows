# Diamond, Top-up và VIP: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File |
|---|----------|--------|------|------|
| 01 | `POST /api/v1/economy/vip/purchase` | Trừ Diamond và gia hạn VIP trong một transaction của sổ kép: idempotent theo key, khóa ví FOR UPDATE, ghi outbox. | transactions, ledger_entries, ledger_accounts, wallets, vip_plans, outbox_events | [01-post-vip-purchase.ts](01-post-vip-purchase.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/01-post-vip-purchase.html) |
| 02 | `POST /api/v1/economy/iap/verify` | Xác thực receipt với Apple hoặc Google ở server, rồi cộng Diamond đúng một lần theo id giao dịch của store. | iap_products, iap_receipts, transactions, ledger_entries, wallets, outbox_events | [02-post-iap-verify.ts](02-post-iap-verify.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/02-post-iap-verify.html) |
| 03 | `POST /api/v1/economy/webhooks/payos` | payOS báo thanh toán thành công: kiểm HMAC, đối chiếu snapshot của đơn, rồi cộng Diamond đúng một lần. | payos_payment_orders, transactions, ledger_entries, wallets, outbox_events | [03-post-webhook-payos.ts](03-post-webhook-payos.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/03-post-webhook-payos.html) |
| 04 | `POST /api/v1/economy/webhooks/apple` | Apple báo REFUND hoặc REVOKE: kiểm chữ ký JWS, rồi đảo bút toán của lần nạp gốc; số dư có thể âm. | iap_receipts, transactions, ledger_entries, wallets, outbox_events | [04-post-webhook-apple-refund.ts](04-post-webhook-apple-refund.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/04-post-webhook-apple-refund.html) |

Sơ đồ luồng của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#09-diamond-top-up-va-vip

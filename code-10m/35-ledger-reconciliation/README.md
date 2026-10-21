# Đối soát sổ cái: trang bền vững có checkpoint, quét dở không báo sạch: code theo từng endpoint

Mỗi file là **một endpoint**, đọc từ trên xuống theo thứ tự chạy: code controller, rồi hàm nó gọi được chép ngay tại dòng gọi, ngang cấp (service, Postgres, Redis, worker...; chỉ khối { } của code mới thụt vào), đánh số Step N.
Mỗi khối có dòng tiêu đề "// [TẦNG] Class.method" (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ, EXTERNAL, GATEWAY); bản Pages tô màu các nhãn này. Một constructor gom hạ tầng trực tiếp và phần khởi tạo liên quan ở đầu trang.
Code trích nguyên văn từ repo (đường dẫn nguồn hiện khi chuyển file, không ghi số dòng), đã lược import, comment cũ, decorator Swagger, class và chữ ký của hàm được gọi; ngoại lệ là các dòng "const ... = ..." ở đầu mỗi khối (tham số truyền vào, do công cụ gán từ chỗ gọi). Comment viết thêm chỉ trả lời "vì sao" ở chỗ logic khó, đặt ngay cạnh dòng code liên quan.
Pipeline request lấy từ main.ts, AppModule và decorator thực tế của endpoint. Lời gọi nội bộ bung đệ quy tới hạ tầng; mỗi hàm chỉ bung một lần. Cho phép lược DTO và Metrics; thư viện ngoài giữ lời gọi.

| # | Endpoint | Làm gì | Bảng | File | Nguồn |
|---|----------|--------|------|------|-------|
| 01 | `JOB ReconciliationService.runFast (mỗi ECONOMY_RECONCILIATION_FAST_INTERVAL_MS)` | Tiến checkpoint bền theo từng trang transaction (keyset created_at, id) trong ngân sách mỗi tick, đếm receipt đã credit mà thiếu transaction, rồi báo lệch qua log và metric; quét dở không bao giờ báo sạch. | transactions, ledger_entries, runtime_job_checkpoints, iap_receipts | [01-job-ledger-reconciliation-fast.ts](01-job-ledger-reconciliation-fast.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/35-ledger-reconciliation/01-job-ledger-reconciliation-fast.html) | local, chưa commit |
| 02 | `JOB ReconciliationService.runDeep (mỗi ECONOMY_RECONCILIATION_INTERVAL_MS)` | Lấy một mẫu ví kế tiếp theo cursor user_id bền, so snapshot với số dư dẫn xuất từ ledger trong cùng một statement, rồi lưu cursor cùng transaction; ok chỉ phản ánh mẫu vừa kiểm. | wallets, ledger_accounts, ledger_entries, runtime_job_checkpoints | [02-job-wallet-reconciliation-deep.ts](02-job-wallet-reconciliation-deep.ts) · [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/35-ledger-reconciliation/02-job-wallet-reconciliation-deep.html) | local, chưa commit |

Kiến trúc 10M của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/ledger-integrity.html

Sơ đồ sản phẩm của tính năng: https://ducmanh001.github.io/litmatch-feature-flows/#09-diamond-top-up-va-vip

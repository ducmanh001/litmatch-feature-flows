# Litmatch ở 10 triệu đồng thời: code thật của các luồng khó đã đổi

Bản riêng cho bài toán 10 triệu đồng thời (fork của [bản gốc /code](../code/README.md); bản gốc không bị sửa). Mỗi trang là code thật đọc từ nhánh `feat/realtime-scale-10m` (PR [#84](https://github.com/ducmanh001/litmatch/pull/84)): hàm được gọi chép ngay tại dòng gọi, đánh số Step, nhãn tầng, chỉ giải thích "vì sao" ở chỗ khó.

**Trạng thái trung thực:** hệ thống CHƯA được chứng nhận 10 triệu đồng thời — chưa có load test ở quy mô đó; mọi số đo là lab (Docker, một kết nối, dữ liệu trong cache) hoặc mô hình với giả định ghi rõ.

Bài toán theo mô hình của repo (`pnpm capacity:model`): 10 triệu kết nối đồng thời, ~10.000 người đang chat, 10 triệu lượt ghép đôi mỗi ngày.

Chưa làm hoặc chưa chứng minh: load test hỗn hợp chạy lâu và diễn tập lỗi; HA Redis/Postgres/media; cursor/sequence phía client; archive và phân vùng chat; outbox relay vẫn tắt ở K8s.


## 02 · Soul Match: đánh giá trong một kết nối

Cả 2 cùng thích thì thành bạn và giữ lại lịch sử chat. Ở quy mô lớn điều quan trọng là toàn bộ bước đánh giá chạy trên MỘT kết nối pool: các hàm phụ (areFriends, canPair) dùng chung transaction thay vì mở thêm kết nối thứ hai trong lúc đang giữ khoá.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Đánh giá đối phương và thành bạn](02-soul-match/01-post-rating.ts) | `POST /api/v1/soul-match/sessions/:id/rating` | Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/02-soul-match/01-post-rating.html) |

## 05 · Movie Match ẩn danh: hàng đợi không khoá toàn cục

Ghép ẩn danh hai người xem chung một video do server chọn. Ở quy mô lớn, hàng đợi này không được xếp hàng sau một khoá toàn cục: mỗi user chỉ khoá chính mình, người mới giành người cũ bằng SKIP LOCKED.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Vào hàng đợi Movie Match ẩn danh](05-movie-match-anon/01-post-join-anon-queue.ts) | `POST /api/v1/movie-match/anon/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành một phiên xem chung với video do server chọn. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/05-movie-match-anon/01-post-join-anon-queue.html) |

## 06 · Palm Match: hàng đợi không khoá toàn cục

Ghép ẩn danh hai người qua trò bói bài. Ở quy mô lớn, hàng đợi này không được xếp hàng sau một khoá toàn cục: mỗi user chỉ khoá chính mình, người mới giành người cũ bằng SKIP LOCKED.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Vào hàng đợi Palm Match](06-palm-match/01-post-join-queue.ts) | `POST /api/v1/palm-match/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/06-palm-match/01-post-join-queue.html) |

## 10 · Matching Speed-up

Trả Diamond để vé đang chờ được đẩy lên đầu hàng đợi Redis và ghép sớm hơn.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Speed-up vé đang chờ ghép](10-matching-speed-up-detail/01-post-speedup-ticket.ts) | `POST /api/v1/matching/tickets/:id/speedup` | Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/10-matching-speed-up-detail/01-post-speedup-ticket.html) |

## 16 · Friend và Chat 1-1

Chat 1-1 bền vững giữa hai người đã là bạn hoặc đã mở chat từ hồ sơ: gửi tin nhắn, streak, realtime và thông báo.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Gửi tin nhắn chat 1-1](16-friend-va-chat-1-1/01-post-send-message.ts) | `POST /api/v1/conversations/:id/messages` | Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/16-friend-va-chat-1-1/01-post-send-message.html) |

## 17 · Refresh token: bảng tăng nhanh nhất

Mỗi lần xoay refresh token (≈ 15 phút một lần khi đang mở app) là một INSERT + một UPDATE: ở quy mô đề bài đó là ≈ 290 triệu hàng/ngày, bảng tăng nhanh nhất của hệ thống. Trang này cho thấy đường xoay, phát hiện dùng lại và đường dọn token cũ tuỳ chọn.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Đổi refresh token lấy phiên mới](17-dang-ky-onboarding-va-guest/01-post-refresh.ts) | `POST /api/v1/auth/refresh` | Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/17-dang-ky-onboarding-va-guest/01-post-refresh.html) |

## 20 · Short Video

Video ngắn: tác giả tải lên, người xem lướt, thả tim, bình luận; lượt xem và thứ hạng được đếm chống trùng.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Ghi nhận lượt xem video](20-short-video/01-post-record-view.ts) | `POST /api/v1/videos/:id/views` | Ghi watch-time của người xem và cộng viewCount đúng 1 lần khi xem đủ ngưỡng, kể cả khi nhiều request đến cùng lúc. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/20-short-video/01-post-record-view.html) |

## 30 · Realtime gateway: kết nối và giữ kết nối

Mô hình của repo giả định 10 triệu socket đồng thời (≈ 200 pod ở 50.000 socket/pod, giả định chưa benchmark). Ở quy mô đó mỗi socket không được tốn một timer hay một lệnh Redis riêng, và mỗi sự kiện không được đánh thức mọi pod.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Kết nối WebSocket: JWT, lease slot, subscribe theo user](30-realtime-gateway/01-ws-handshake-admit.ts) | `WS socket.io: handshake middleware → connect` | Handshake xác thực JWT, xin lease slot (tối đa N socket/user) trên Redis và SUBSCRIBE kênh của chính user; chỉ khi xong hết mới đăng ký gia hạn lease. | GATEWAY → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/01-ws-handshake-admit.html) |
| [Gia hạn lease theo lô](30-realtime-gateway/02-job-lease-refresh-tick.ts) | `JOB LeaseRefresher.tick (mỗi giây, một timer cho cả pod)` | Mỗi giây gom các lease đến hạn của cả pod thành vài pipeline, thử cấp lại lease Redis báo mất, nhả lease cho socket đã ngắt, ngắt socket chỉ khi không còn chứng minh được lease. | GATEWAY → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/02-job-lease-refresh-tick.html) |

## 31 · Matching engine: một tick của matcher

Hàng đợi ghép ở Redis, chọn cặp theo cửa sổ ứng viên, claim nguyên tử rồi xác minh lại dưới khoá Postgres. Đây là đường nóng của 10 triệu lượt ghép mỗi ngày (≈ 115 lượt/giây trung bình, đỉnh gấp ~10 lần).

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Một tick của matcher](31-matching-engine/01-job-matcher-tick.ts) | `JOB MatcherWorkerService.runOnce (đánh thức khi có ticket + backstop định kỳ)` | Duyệt các shard đang có vé, chọn cặp tương thích trong cửa sổ đầu shard, claim cả hai vé nguyên tử, xác minh lại dưới khoá rồi tạo session. | WORKER → REDIS → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/01-job-matcher-tick.html) |

## 32 · Vòng đời dữ liệu: xoá theo lô mà không tranh kết nối

Ở 10 triệu đồng thời, bảng refresh_tokens nhận ≈ 290 triệu hàng/ngày (mô hình ADR 0014). Nếu không ai xoá, bảng không bao giờ ổn định; nếu xoá bằng một lệnh lớn, nó chiếm khoá và kết nối của request. Các trang này là cách xoá đều đặn, có trần, có công tắc an toàn.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Một tick retention](32-data-lifecycle/01-job-retention-tick.ts) | `JOB RetentionJob.runOnce (mỗi RETENTION_INTERVAL_MS, một pod duy nhất)` | Chạy dưới khoá cluster-singleton và ngân sách đồng thời, lấy quy tắc của module, xoá theo lô bằng câu lệnh ngắn tự commit, ghi metric. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/01-job-retention-tick.html) |
| [Story sweeper: xoá story hết hạn cùng lượt xem](32-data-lifecycle/02-job-story-sweeper-tick.ts) | `JOB StorySweeperService.runOnce (mỗi STORY_SWEEPER_INTERVAL_MS)` | Xoá story hết hạn theo lô 500; story_views tự xoá theo FK ON DELETE CASCADE; tick lặp lô tới hết backlog hoặc hết ngân sách 30 giây. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/02-job-story-sweeper-tick.html) |

## 33 · Admin broadcast: xếp hàng rồi gửi nền theo lô

Gửi thông báo cho 10 triệu người = 10 triệu insert notification + 10 triệu lần push. Request chỉ ghi một dòng "queued" (idempotent); một job nền xử lý từng lô 500 người theo keyset, có thể huỷ và resume.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Xếp hàng một broadcast](33-admin-broadcast/01-post-broadcast.ts) | `POST /api/v1/admin/notifications/broadcast` | Ghi một dòng broadcast ở trạng thái queued kèm audit (202); Idempotency-Key làm bấm hai lần chỉ tạo một broadcast. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/01-post-broadcast.html) |
| [Job gửi broadcast theo lô](33-admin-broadcast/02-job-broadcast-dispatch-tick.ts) | `JOB BroadcastDispatchJob.runOnce (mỗi ADMIN_BROADCAST_JOB_INTERVAL_MS)` | Nhặt broadcast queued/running cũ nhất, đọc một trang user theo keyset, tạo thông báo và đẩy cursor trong CÙNG transaction, rồi push sau commit. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/02-job-broadcast-dispatch-tick.html) |

## 34 · Xếp hạng video: tính lại theo hạn, không xoay vòng cả bảng

Điểm hạng = tương tác / (1 + tuổi/decay) đổi chậm theo thời gian nên mỗi video chỉ cần tính lại sau một khoảng tỉ lệ với (decay + tuổi): video mới ~30 phút, video nhiều tháng tuổi vài ngày. Việc mỗi giờ tỉ lệ với video trẻ và đang hoạt động, không tăng tuyến tính theo số video.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Tính lại điểm hạng các video đến hạn](34-video-ranking/01-job-video-ranking-tick.ts) | `JOB VideoRankingService.runOnce (mỗi VIDEO_RANKING_JOB_INTERVAL_MS)` | Mỗi tick chạy vài lô: mỗi lô là một câu UPDATE chọn các video published đến hạn (SKIP LOCKED), tính điểm mới và đặt hạn tính lại kế tiếp. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/34-video-ranking/01-job-video-ranking-tick.html) |

## 35 · Đối soát sổ cái: kiểm Nợ = Có theo cửa sổ, không quét cả bảng

Mỗi giao dịch ledger cân Nợ = Có theo currency, nên tổng cộng dồn mọi giao dịch cũ hơn một mốc watermark phải bằng 0. Job giữ tổng cộng dồn trong bộ nhớ và mỗi tick chỉ cộng thêm cửa sổ giao dịch mới; chi phí không còn tăng theo cả bảng ledger_entries.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Đối soát nhanh bất biến Nợ = Có](35-ledger-reconciliation/01-job-ledger-reconciliation-fast.ts) | `JOB ReconciliationService.runFast (mỗi ECONOMY_RECONCILIATION_FAST_INTERVAL_MS)` | Cộng thêm các cửa sổ giao dịch mới vào tổng Nợ − Có cộng dồn, đếm receipt đã credit mà thiếu giao dịch, rồi báo lệch qua log và metric. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/35-ledger-reconciliation/01-job-ledger-reconciliation-fast.html) |

Sơ đồ luồng của các tính năng: https://ducmanh001.github.io/litmatch-feature-flows/

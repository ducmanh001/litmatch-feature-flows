# Litmatch ở 10 triệu đồng thời: code thật của các luồng khó đã đổi

Bản riêng cho bài toán 10 triệu đồng thời (fork của [bản gốc /code](../code/README.md); bản gốc không bị sửa). Mỗi trang là code thật đọc từ nhánh `feat/realtime-scale-10m` (PR [#84](https://github.com/ducmanh001/litmatch/pull/84)) và từ thay đổi local chưa commit: hàm được gọi chép ngay tại dòng gọi, đánh số Step, nhãn tầng, chỉ giải thích "vì sao" ở chỗ khó. Mỗi tính năng có liên kết tới sơ đồ **Kiến trúc 10M** của nó: [architecture-10m](../architecture-10m/README.md).

**Trạng thái trung thực:** hệ thống CHƯA được chứng nhận 10 triệu đồng thời: chưa có load test ở quy mô đó, Postgres vẫn là một primary, chưa diễn tập failover. Mọi con số là lab (Docker, một kết nối, dữ liệu trong cache), mô hình với giả định ghi rõ, hoặc số học theo cấu hình mặc định.

**Nguồn của từng trang:** "đã đẩy (PR #84)" = mọi file nguồn khớp commit `0c052aa` (đầu của PR #84, có trên GitHub). "local, chưa commit" = trang dùng ít nhất một file có thay đổi chưa commit trong cây làm việc local (chụp 2026-10-21 trên nền `0c052aa`): code đó chưa có trên GitHub và có thể đổi trước khi commit; theo ADR 0015 và plan 2026-10-20/21, phần local đã viết xong về mặt nguồn nhưng kiểm chứng chạy thật mới có một phần.

Bài toán theo mô hình của repo (`pnpm capacity:model`): 10 triệu kết nối đồng thời, ~10.000 người đang chat, 10 triệu lượt ghép đôi mỗi ngày.

Chưa làm hoặc chưa chứng minh: load test hỗn hợp chạy lâu và diễn tập lỗi; HA Redis/Postgres/media (media production khai hai replica, một room vẫn nằm trên một node); cursor/sequence phía client; chính sách thử lại của frontend chưa có trang code; archive và phân vùng chat; outbox relay vẫn tắt ở K8s và chưa có consumer.


## 02 · Soul Match: đánh giá trong một kết nối

Cả 2 cùng thích thì thành bạn và giữ lại lịch sử chat. Ở quy mô lớn điều quan trọng là toàn bộ bước đánh giá chạy trên MỘT kết nối pool: các hàm phụ (areFriends, canPair) dùng chung transaction thay vì mở thêm kết nối thứ hai trong lúc đang giữ khoá.

Kiến trúc: [Đường nóng trong Postgres: khoá, một kết nối, SKIP LOCKED](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/hot-paths.html) · Sơ đồ sản phẩm: [02-soul-match](https://ducmanh001.github.io/litmatch-feature-flows/#02-soul-match)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Đánh giá đối phương và thành bạn](02-soul-match/01-post-rating.ts) | `POST /api/v1/soul-match/sessions/:id/rating` | Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/02-soul-match/01-post-rating.html) | đã đẩy (PR #84) |

## 04 · Party Room: bình luận không xếp hàng sau khoá phòng

Phòng đông người có rất nhiều bình luận. Nếu mỗi bình luận khoá FOR UPDATE hàng của phòng thì mọi bình luận xếp hàng nối đuôi nhau. Bình luận chỉ cần giữ FOR SHARE: chúng chạy song song với nhau, còn join, leave, đổi role và đóng phòng (FOR UPDATE) vẫn chờ các bình luận đang chạy xong.

Kiến trúc: [Đường nóng trong Postgres: khoá, một kết nối, SKIP LOCKED](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/hot-paths.html) · Sơ đồ sản phẩm: [04-party-room](https://ducmanh001.github.io/litmatch-feature-flows/#04-party-room)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Gửi bình luận trong phòng](04-party-room/01-post-comment.ts) | `POST /api/v1/party/rooms/:id/comments` | Giữ FOR SHARE trên hàng phòng, kiểm membership dưới khoá, chèn bình luận append-only có idempotency, rồi phát realtime cho các thành viên SAU commit. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/04-party-room/01-post-comment.html) | đã đẩy (PR #84) |

## 05 · Movie Match ẩn danh: hàng đợi không khoá toàn cục

Ghép ẩn danh hai người xem chung một video do server chọn. Ở quy mô lớn, hàng đợi này không được xếp hàng sau một khoá toàn cục: mỗi user chỉ khoá chính mình, người mới giành người cũ bằng SKIP LOCKED.

Kiến trúc: [Đường nóng trong Postgres: khoá, một kết nối, SKIP LOCKED](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/hot-paths.html) · Sơ đồ sản phẩm: [05-movie-match-movie-night](https://ducmanh001.github.io/litmatch-feature-flows/#05-movie-match-movie-night)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Vào hàng đợi Movie Match ẩn danh](05-movie-match-anon/01-post-join-anon-queue.ts) | `POST /api/v1/movie-match/anon/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành một phiên xem chung với video do server chọn. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/05-movie-match-anon/01-post-join-anon-queue.html) | đã đẩy (PR #84) |

## 06 · Palm Match: hàng đợi không khoá toàn cục

Ghép ẩn danh hai người qua trò bói bài. Ở quy mô lớn, hàng đợi này không được xếp hàng sau một khoá toàn cục: mỗi user chỉ khoá chính mình, người mới giành người cũ bằng SKIP LOCKED.

Kiến trúc: [Đường nóng trong Postgres: khoá, một kết nối, SKIP LOCKED](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/hot-paths.html) · Sơ đồ sản phẩm: [06-palm-match](https://ducmanh001.github.io/litmatch-feature-flows/#06-palm-match)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Vào hàng đợi Palm Match](06-palm-match/01-post-join-queue.ts) | `POST /api/v1/palm-match/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/06-palm-match/01-post-join-queue.html) | đã đẩy (PR #84) |

## 10 · Matching Speed-up

Trả Diamond để vé đang chờ được đẩy lên đầu hàng đợi Redis và ghép sớm hơn.

Kiến trúc: [Matching: hàng đợi Redis chia shard, matcher, repair, speed-up](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/matching.html) · Sơ đồ sản phẩm: [10-matching-speed-up-detail](https://ducmanh001.github.io/litmatch-feature-flows/#10-matching-speed-up-detail)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Speed-up vé đang chờ ghép](10-matching-speed-up-detail/01-post-speedup-ticket.ts) | `POST /api/v1/matching/tickets/:id/speedup` | Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/10-matching-speed-up-detail/01-post-speedup-ticket.html) | đã đẩy (PR #84) |

## 16 · Friend và Chat 1-1

Chat 1-1 bền vững giữa hai người đã là bạn hoặc đã mở chat từ hồ sơ: gửi tin nhắn, streak, realtime và thông báo.

Kiến trúc: [Đường nóng trong Postgres: khoá, một kết nối, SKIP LOCKED](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/hot-paths.html) · Sơ đồ sản phẩm: [16-friend-va-chat-1-1](https://ducmanh001.github.io/litmatch-feature-flows/#16-friend-va-chat-1-1)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Gửi tin nhắn chat 1-1](16-friend-va-chat-1-1/01-post-send-message.ts) | `POST /api/v1/conversations/:id/messages` | Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/16-friend-va-chat-1-1/01-post-send-message.html) | đã đẩy (PR #84) |

## 17 · Đăng nhập: refresh token (bảng tăng nhanh nhất) và giao OTP

Mỗi lần xoay refresh token (≈ 15 phút một lần khi đang mở app) là một INSERT + một UPDATE: ở quy mô đề bài đó là ≈ 290 triệu hàng/ngày, bảng tăng nhanh nhất của hệ thống. Trang 01 cho thấy đường xoay, phát hiện dùng lại và đường dọn token cũ tuỳ chọn. Trang 02 là cách giao OTP qua cổng SMS mà không để mã hợp lệ "mồ côi" và không bơm SMS tốn tiền.

Kiến trúc: [Vòng đời dữ liệu: retention, sweeper, admission cho job nền](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/data-lifecycle.html) · Sơ đồ sản phẩm: [17-dang-ky-onboarding-va-guest](https://ducmanh001.github.io/litmatch-feature-flows/#17-dang-ky-onboarding-va-guest)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Đổi refresh token lấy phiên mới](17-dang-ky-onboarding-va-guest/01-post-refresh.ts) | `POST /api/v1/auth/refresh` | Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/17-dang-ky-onboarding-va-guest/01-post-refresh.html) | đã đẩy (PR #84) |
| [Yêu cầu OTP đăng nhập bằng số điện thoại](17-dang-ky-onboarding-va-guest/02-post-otp-request.ts) | `POST /api/v1/auth/otp/request` | Đếm hạn mức theo SỐ ĐIỆN THOẠI ở server, sinh mã và chỉ lưu hash; chế độ sms-http lưu mã mới trước, giao qua cổng SMS rồi mới vô hiệu mã cũ, giao lỗi thì vô hiệu chính mã vừa tạo và trả 503 mà không lộ mã. | CONTROLLER → SERVICE → POSTGRES → EXTERNAL | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/17-dang-ky-onboarding-va-guest/02-post-otp-request.html) | đã đẩy (PR #84) |

## 20 · Short Video

Video ngắn: tác giả tải lên, người xem lướt, thả tim, bình luận; lượt xem và thứ hạng được đếm chống trùng.

Kiến trúc: [Việc nền theo lô: broadcast, xếp hạng video, bộ đệm đếm](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/batch-jobs.html) · Sơ đồ sản phẩm: [20-short-video](https://ducmanh001.github.io/litmatch-feature-flows/#20-short-video)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Ghi nhận lượt xem video](20-short-video/01-post-record-view.ts) | `POST /api/v1/videos/:id/views` | Ghi watch-time của người xem và cộng viewCount đúng 1 lần khi xem đủ ngưỡng, kể cả khi nhiều request đến cùng lúc. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/20-short-video/01-post-record-view.html) | đã đẩy (PR #84) |

## 30 · Realtime gateway: kết nối và giữ kết nối

Mô hình của repo giả định 10 triệu socket đồng thời (≈ 200 pod ở 50.000 socket/pod, giả định chưa benchmark; cấu hình mặc định hiện giới hạn mỗi process ở 2.000 socket và 64 handshake bất đồng bộ). Ở quy mô đó mỗi socket không được tốn một timer hay một lệnh Redis riêng, mỗi sự kiện không được đánh thức mọi pod, và một socket chậm không được làm đầy bộ nhớ của pod.

Kiến trúc: [Realtime gateway: admission, quota, fanout, ngân sách gửi](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/realtime-gateway.html) · Sơ đồ sản phẩm: [25-realtime-fanout](https://ducmanh001.github.io/litmatch-feature-flows/#25-realtime-fanout)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Kết nối WebSocket: admission, JWT, lease slot, subscribe theo user](30-realtime-gateway/01-ws-handshake-admit.ts) | `WS socket.io: handshake middleware → connect` | Giữ chỗ admission (trần socket và trần handshake, không hàng chờ), xác thực JWT, xin lease slot (tối đa N socket/user) trên Redis và SUBSCRIBE kênh của chính user; chỉ khi xong hết mới đăng ký gia hạn lease. | GATEWAY → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/01-ws-handshake-admit.html) | local, chưa commit |
| [Gia hạn lease theo lô](30-realtime-gateway/02-job-lease-refresh-tick.ts) | `JOB LeaseRefresher.tick (mỗi giây, một timer cho cả pod)` | Mỗi giây gom các lease đến hạn của cả pod thành vài pipeline, thử cấp lại lease Redis báo mất, nhả lease cho socket đã ngắt, ngắt socket chỉ khi không còn chứng minh được lease. | GATEWAY → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/02-job-lease-refresh-tick.html) | local, chưa commit |
| [Relay sự kiện tới socket của user với ngân sách gửi](30-realtime-gateway/03-ws-relay-outbound-budget.ts) | `WS Redis pub/sub message → SignalingGateway.relay (mỗi sự kiện realtime)` | Nhận envelope từ Redis, chỉ emit cho socket CỤC BỘ của user, đặt chỗ ngân sách gửi (theo socket và theo cả pod) TRƯỚC khi enqueue, và đóng transport của socket chậm để nó reconnect rồi đọc bù qua REST. | REDIS → GATEWAY | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/03-ws-relay-outbound-budget.html) | local, chưa commit |
| [Khởi động: khoá placement quota bằng fingerprint topology](30-realtime-gateway/04-boot-quota-topology-guard.ts) | `BOOT ConnectionQuotaService.onModuleInit (lúc gateway khởi động)` | Mỗi gateway tính fingerprint của danh sách shard Redis có thứ tự và đăng ký nó một lần trên Redis điều phối; fingerprint khác thì từ chối khởi động để rolling update không tạo hai quota độc lập cho cùng một user. | GATEWAY → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/30-realtime-gateway/04-boot-quota-topology-guard.html) | local, chưa commit |

## 31 · Matching engine: matcher, định tuyến shard và repair hàng đợi

Hàng đợi ghép ở Redis (chỉ là bản chiếu dẫn xuất; Postgres là nguồn sự thật), chọn cặp theo cửa sổ ứng viên, claim nguyên tử rồi xác minh lại dưới khoá Postgres. Đây là đường nóng của 10 triệu lượt ghép mỗi ngày (≈ 115 lượt/giây trung bình, đỉnh gấp ~10 lần). Hàng đợi có thể chia sang nhiều Redis vật lý theo hash của shard, và một job repair dựng lại hàng đợi từ DB khi Redis mất dữ liệu.

Kiến trúc: [Matching: hàng đợi Redis chia shard, matcher, repair, speed-up](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/matching.html) · Sơ đồ sản phẩm: [01-matching-engine](https://ducmanh001.github.io/litmatch-feature-flows/#01-matching-engine)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Một tick của matcher](31-matching-engine/01-job-matcher-tick.ts) | `JOB MatcherWorkerService.runOnce (đánh thức khi có ticket + backstop định kỳ)` | Duyệt các shard đang có vé, chọn cặp tương thích trong cửa sổ đầu shard, claim cả hai vé nguyên tử, xác minh lại dưới khoá rồi tạo session. | WORKER → REDIS → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/01-job-matcher-tick.html) | local, chưa commit |
| [Repair hàng đợi ghép từ DB](31-matching-engine/02-job-matching-queue-repair.ts) | `JOB QueueRepairService.runOnce (mỗi MATCHING_QUEUE_REPAIR_INTERVAL_MS, một pod duy nhất)` | Quét một trang vé queued còn trong hạn theo cursor bền vững (enqueued_at, id), khôi phục vé thiếu trong Redis bằng NX rồi refresh score theo DB bằng XX LT, đánh dấu shard active và đánh thức matcher. | WORKER → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/02-job-matching-queue-repair.html) | local, chưa commit |
| [Hàng đợi chia shard: định tuyến theo hash](31-matching-engine/03-queue-sharded-routing.ts) | `QUEUE ShardedMatchingQueue (khi MATCHING_REDIS_URLS có từ hai Redis trở lên)` | Mỗi hàng đợi logic được băm (FNV-1a) tới đúng MỘT Redis vật lý nên claim hai vé vẫn nguyên tử; liệt kê shard active gộp từ các Redis còn khoẻ và bỏ mục lỗi thời sau khi đổi topology. | REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/31-matching-engine/03-queue-sharded-routing.html) | local, chưa commit |

## 32 · Vòng đời dữ liệu: xoá theo lô mà không tranh kết nối

Ở 10 triệu đồng thời, bảng refresh_tokens nhận ≈ 290 triệu hàng/ngày (mô hình ADR 0014). Nếu không ai xoá, bảng không bao giờ ổn định; nếu xoá bằng một lệnh lớn, nó chiếm khoá và kết nối của request. Các trang này là cách xoá đều đặn, có trần, có công tắc an toàn.

Kiến trúc: [Vòng đời dữ liệu: retention, sweeper, admission cho job nền](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/data-lifecycle.html)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Một tick retention](32-data-lifecycle/01-job-retention-tick.ts) | `JOB RetentionJob.runOnce (mỗi RETENTION_INTERVAL_MS, một pod duy nhất)` | Xếp hàng admission theo DataSource (chờ mà không giữ kết nối), chạy dưới ngân sách đồng thời và khoá cluster-singleton, lấy quy tắc của module, xoá theo lô bằng câu lệnh ngắn tự commit, ghi metric. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/01-job-retention-tick.html) | local, chưa commit |
| [Story sweeper: xoá story hết hạn cùng lượt xem](32-data-lifecycle/02-job-story-sweeper-tick.ts) | `JOB StorySweeperService.runOnce (mỗi STORY_SWEEPER_INTERVAL_MS)` | Xoá story hết hạn theo lô 500; story_views tự xoá theo FK ON DELETE CASCADE; tick lặp lô tới hết backlog hoặc hết ngân sách 30 giây. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/32-data-lifecycle/02-job-story-sweeper-tick.html) | đã đẩy (PR #84) |

## 33 · Admin broadcast: xếp hàng rồi gửi nền theo lô

Gửi thông báo cho 10 triệu người = 10 triệu insert notification + 10 triệu lần push. Request chỉ ghi một dòng "queued" (idempotent); một job nền xử lý từng lô 500 người theo keyset, có thể huỷ và resume.

Kiến trúc: [Việc nền theo lô: broadcast, xếp hạng video, bộ đệm đếm](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/batch-jobs.html) · Sơ đồ sản phẩm: [22-support-va-admin-operations](https://ducmanh001.github.io/litmatch-feature-flows/#22-support-va-admin-operations)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Xếp hàng một broadcast](33-admin-broadcast/01-post-broadcast.ts) | `POST /api/v1/admin/notifications/broadcast` | Ghi một dòng broadcast ở trạng thái queued kèm audit (202); Idempotency-Key làm bấm hai lần chỉ tạo một broadcast. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/01-post-broadcast.html) | đã đẩy (PR #84) |
| [Job gửi broadcast theo lô](33-admin-broadcast/02-job-broadcast-dispatch-tick.ts) | `JOB BroadcastDispatchJob.runOnce (mỗi ADMIN_BROADCAST_JOB_INTERVAL_MS)` | Nhặt broadcast queued/running cũ nhất, đọc một trang user theo keyset, tạo thông báo và đẩy cursor trong CÙNG transaction, rồi push sau commit. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/02-job-broadcast-dispatch-tick.html) | đã đẩy (PR #84) |
| [Huỷ một broadcast đang xếp hàng hoặc đang gửi](33-admin-broadcast/03-post-broadcast-cancel.ts) | `POST /api/v1/admin/notifications/broadcast/:id/cancel` | Một UPDATE có điều kiện đổi queued/running thành cancelled và là chốt chặn nguyên tử; audit chỉ ghi khi thật sự chuyển trạng thái nên huỷ lặp là idempotent; lô đã commit không thu hồi được. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/33-admin-broadcast/03-post-broadcast-cancel.html) | đã đẩy (PR #84) |

## 34 · Xếp hạng video: tính lại theo hạn, không xoay vòng cả bảng

Điểm hạng = tương tác / (1 + tuổi/decay) đổi chậm theo thời gian nên mỗi video chỉ cần tính lại sau một khoảng tỉ lệ với (decay + tuổi): video mới ~30 phút, video nhiều tháng tuổi vài ngày. Việc mỗi giờ tỉ lệ với video trẻ và đang hoạt động, không tăng tuyến tính theo số video.

Kiến trúc: [Việc nền theo lô: broadcast, xếp hạng video, bộ đệm đếm](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/batch-jobs.html) · Sơ đồ sản phẩm: [20-short-video](https://ducmanh001.github.io/litmatch-feature-flows/#20-short-video)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Tính lại điểm hạng các video đến hạn](34-video-ranking/01-job-video-ranking-tick.ts) | `JOB VideoRankingService.runOnce (mỗi VIDEO_RANKING_JOB_INTERVAL_MS)` | Mỗi tick chạy vài lô: mỗi lô là một câu UPDATE chọn các video published đến hạn (SKIP LOCKED), tính điểm mới và đặt hạn tính lại kế tiếp. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/34-video-ranking/01-job-video-ranking-tick.html) | đã đẩy (PR #84) |

## 35 · Đối soát sổ cái: trang bền vững có checkpoint, quét dở không báo sạch

Mỗi transaction ledger cân Nợ = Có theo currency khi ghi, nên tổng Nợ − Có của một tập transaction trọn vẹn phải bằng 0. Tier fast cộng dồn tổng đó theo từng trang transaction (keyset created_at, id; chỉ dữ liệu cũ hơn now() − LAG) và commit tổng cùng cursor trong một dòng runtime_job_checkpoints: mỗi tick có trần số lô và thời gian, pod thay thế tiếp tục đúng chỗ thay vì quét lại từ đầu, và một lượt quét dở không bao giờ báo sạch (complete = false thì ok = false). Tier deep lấy mẫu ví luân phiên theo khoá chính rồi so với số dư dẫn xuất từ ledger. Job chỉ đọc bảng tài chính, chỉ ghi metadata chẩn đoán dựng lại được. Giới hạn: thời gian bắt kịp và rebase phụ thuộc backlog và cấu hình (mặc định tối đa 10 lô × 2000 transaction mỗi tick); mã này thuộc đợt thay đổi chưa chạy kiểm chứng runtime (ADR 0015), nên đây là mô tả cơ chế, không phải chứng nhận công suất.

Kiến trúc: [Sổ cái: đối soát phân trang, checkpoint, outbox relay](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/ledger-integrity.html) · Sơ đồ sản phẩm: [09-diamond-top-up-va-vip](https://ducmanh001.github.io/litmatch-feature-flows/#09-diamond-top-up-va-vip)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Đối soát nhanh bất biến Nợ = Có theo trang bền vững](35-ledger-reconciliation/01-job-ledger-reconciliation-fast.ts) | `JOB ReconciliationService.runFast (mỗi ECONOMY_RECONCILIATION_FAST_INTERVAL_MS)` | Tiến checkpoint bền theo từng trang transaction (keyset created_at, id) trong ngân sách mỗi tick, đếm receipt đã credit mà thiếu transaction, rồi báo lệch qua log và metric; quét dở không bao giờ báo sạch. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/35-ledger-reconciliation/01-job-ledger-reconciliation-fast.html) | local, chưa commit |
| [Đối soát sâu: mẫu ví luân phiên so với số dư dẫn xuất từ ledger](35-ledger-reconciliation/02-job-wallet-reconciliation-deep.ts) | `JOB ReconciliationService.runDeep (mỗi ECONOMY_RECONCILIATION_INTERVAL_MS)` | Lấy một mẫu ví kế tiếp theo cursor user_id bền, so snapshot với số dư dẫn xuất từ ledger trong cùng một statement, rồi lưu cursor cùng transaction; ok chỉ phản ánh mẫu vừa kiểm. | WORKER → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/35-ledger-reconciliation/02-job-wallet-reconciliation-deep.html) | local, chưa commit |

## 36 · Calling: worker có lease, reconnect grace, cleanup bền vững

Timer của Voice Match chạy trên mọi pod core-api: mỗi lượt claim nguyên tử tối đa 100 call bằng một câu UPDATE ... SKIP LOCKED với lease theo đồng hồ DB, terminal hoá call quá hạn dưới row lock và chỉ để lại marker cleanup_pending. Một vòng cleanup riêng dọn Matching và room LiveKit có thử lại, còn GET trạng thái call đối soát với SFU mà không gia hạn reconnect grace đã lưu và không để snapshot cũ ghi đè webhook mới hơn. Lease chỉ cho at-least-once (trạng thái đúng vẫn do kiểm lại dưới row lock); chưa có số đo tải nên đây không phải chứng nhận 10 triệu đồng thời.

Kiến trúc: [Calling: worker có lease, reconnect grace, cleanup bền vững](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/calling-recovery.html) · Sơ đồ sản phẩm: [03-voice-match](https://ducmanh001.github.io/litmatch-feature-flows/#03-voice-match)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Timer cuộc gọi: claim lô có lease, terminal hoá call quá hạn](36-calling-recovery/01-job-call-timer-tick.ts) | `JOB CallTickerService.runOnce (mỗi CALLING_TICKER_INTERVAL_MS, trên mọi pod core-api)` | Mỗi pod claim nguyên tử tối đa 100 call bằng một câu UPDATE ... FOR UPDATE SKIP LOCKED (lease theo đồng hồ DB), chốt call pending quá hạn, quá reconnect grace hoặc vượt giới hạn free trong ngân sách thời gian của tick, chỉ để marker cleanup rồi nhả lease. | WORKER → POSTGRES → SERVICE | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/36-calling-recovery/01-job-call-timer-tick.html) | local, chưa commit |
| [Cleanup call đã ended: Matching rồi room LiveKit, giữ marker khi lỗi](36-calling-recovery/02-job-call-cleanup-tick.ts) | `JOB CallTickerService.runCleanupOnce (mỗi CALLING_TICKER_INTERVAL_MS, trên mọi pod core-api)` | Claim các call ended còn cleanup_pending bằng lease cleanup_claim_until, dọn Matching rồi xoá room LiveKit với độ đồng thời có trần; lỗi thì giữ marker để thử lại và cả lượt lỗi thì nghỉ ngẫu nhiên 2,5–5 giây. | WORKER → POSTGRES → SERVICE → EXTERNAL | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/36-calling-recovery/02-job-call-cleanup-tick.html) | local, chưa commit |
| [Trạng thái call: đối soát SFU mà không gia hạn reconnect grace](36-calling-recovery/03-get-call-state.ts) | `GET /api/v1/calling/calls/:id` | Chỉ member đọc được; mỗi lần poll hỏi SFU ai đang có mặt (ngoài row lock), nếu cần ghi thì khoá hàng và kiểm lại status + snapshot trước khi mở hoặc giữ nguyên reconnect grace và phục hồi timer; SFU lỗi thì giữ nguyên call. | CONTROLLER → SERVICE → EXTERNAL → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/36-calling-recovery/03-get-call-state.html) | local, chưa commit |
| [Webhook LiveKit: verify chữ ký, ghi joined / left idempotent](36-calling-recovery/04-post-livekit-webhook.ts) | `POST /api/v1/calling/webhooks/livekit` | Endpoint công khai chỉ tin event sau khi verify chữ ký trên nguyên văn body; mỗi event chạy trong một transaction khoá hàng với set-if-null và kiểm status nên webhook gửi lại, đến trễ hoặc sai thứ tự không phá trạng thái; rời phòng chỉ mở reconnect grace, không end call. | CONTROLLER → EXTERNAL → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/36-calling-recovery/04-post-livekit-webhook.html) | local, chưa commit |

## 37 · Outbox relay: bắt kịp backlog có ngân sách

Outbox Pattern: event kinh tế được ghi vào outbox_events cùng transaction với sổ cái, relay đọc rồi publish Kafka sau. Mỗi tick relay bắt kịp backlog qua nhiều lô (mặc định tối đa 10 lô × 100 event) nhưng bị chặn bởi ngân sách thời gian (mặc định 1000 ms), không bắt đầu publish mới khi hết ngân sách hoặc shutdown, và lỗi broker đầu tiên kết thúc lô để không giữ khoá DB qua nhiều lần timeout. Nhiều pod chạy song song nhờ FOR UPDATE SKIP LOCKED; event lỗi được thử lại có trần rồi chuyển dead-letter và giữ nguyên để replay. Lưu ý trung thực: relay đang TẮT mặc định ở mọi profile (env, K8s base, compose production, hosted) và repo chưa có consumer nào subscribe topic litmatch.economy.events, nên đây là cơ chế sẵn có chứ chưa phải đường dữ liệu đang chạy; mã này thuộc đợt thay đổi chưa chạy kiểm chứng runtime (ADR 0015), không phải chứng nhận công suất.

Kiến trúc: [Sổ cái: đối soát phân trang, checkpoint, outbox relay](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/ledger-integrity.html) · Sơ đồ sản phẩm: [09-diamond-top-up-va-vip](https://ducmanh001.github.io/litmatch-feature-flows/#09-diamond-top-up-va-vip)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Outbox relay: bắt kịp backlog có ngân sách mỗi tick](37-outbox-relay/01-job-outbox-relay-tick.ts) | `JOB OutboxRelayService.flushOnce (mỗi ECONOMY_OUTBOX_RELAY_INTERVAL_MS; TẮT mặc định)` | Mỗi tick bắt kịp backlog qua tối đa MAX_BATCHES lô (100 event/lô) trong ngân sách thời gian: chọn event chưa publish bằng FOR UPDATE SKIP LOCKED, publish Kafka, thử lại có trần rồi dead-letter. Relay đang tắt mặc định và chưa có consumer. | WORKER → POSTGRES → KAFKA | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/37-outbox-relay/01-job-outbox-relay-tick.html) | local, chưa commit |

## 38 · Bảo vệ tài nguyên: admission HTTP và readiness

Quá tải phải trả lỗi sớm thay vì kéo dài hàng đợi: mỗi pod chỉ nhận một số request đang xử lý hữu hạn (hết chỗ thì 503 kèm Retry-After), và readiness không được tự làm đầy pool khi DB chậm. Số mặc định (4 × pool = 40 request đồng thời) là điểm khởi đầu thận trọng, chưa phải năng lực đã đo.

Kiến trúc: [Bảo vệ tài nguyên: admission HTTP, timeout, readiness, HPA/PDB](https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/protection.html)

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang | Nguồn |
|---|---|---|---|---|---|
| [Admission HTTP: tối đa N request đang xử lý mỗi pod](38-runtime-protection/01-http-request-admission.ts) | `HTTP RequestConcurrencyInterceptor.intercept (APP_INTERCEPTOR: mọi request HTTP trừ health)` | Mỗi request giữ một slot trong ngân sách của pod; hết chỗ thì trả 503 kèm Retry-After: 1 ngay, không xếp hàng; slot được nhả khi request xong, lỗi hoặc bị huỷ. | CONTROLLER → SERVICE | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/38-runtime-protection/01-http-request-admission.html) | local, chưa commit |
| [Readiness: Postgres và Redis sẵn sàng nhận traffic](38-runtime-protection/02-get-health-ready.ts) | `GET /health/ready` | Gộp các probe song song, cache kết quả 1 giây và chỉ giữ tối đa một SQL probe chưa xong, để kubelet thăm dò dày hoặc DB chậm không làm đầy pool. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code-10m/38-runtime-protection/02-get-health-ready.html) | local, chưa commit |

Kiến trúc 10M: https://ducmanh001.github.io/litmatch-feature-flows/architecture-10m/index.html · Sơ đồ sản phẩm: https://ducmanh001.github.io/litmatch-feature-flows/

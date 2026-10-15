# Các luồng khó của Litmatch: code kèm lời giải thích

Chỉ chọn luồng có nhiều nghiệp vụ hoặc cơ chế kỹ thuật (Redis, transaction DB, idempotency, webhook, worker nền); bỏ CRUD cơ bản.
Mỗi chức năng là một file, chép code thật đi từ trên xuống theo thứ tự chạy; nhãn tầng (CONTROLLER, SERVICE, WORKER, POSTGRES, REDIS, KAFKA, BULLMQ) tô màu ở bản Pages; comment chỉ trả lời "vì sao" ở chỗ logic khó.

## 02 · Soul Match

Chat ẩn danh vài phút giữa 2 người đã ghép, đánh giá nhau; cả 2 cùng thích thì thành bạn và giữ lại lịch sử chat.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Gửi tin nhắn ẩn danh](02-soul-match/01-post-send-message.ts) | `POST /api/v1/soul-match/sessions/:id/messages` | Gửi tin nhắn trong phòng ẩn danh: giờ chat tính ở server, idempotent theo key, realtime riêng cho từng người nhận. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/01-post-send-message.html) |
| [Đánh giá đối phương và thành bạn](02-soul-match/02-post-rating.ts) | `POST /api/v1/soul-match/sessions/:id/rating` | Ghi rating bất biến; khi cả 2 cùng like thì tạo Friendship, Conversation và chuyển lịch sử chat trong một transaction. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/02-soul-match/02-post-rating.html) |

## 04 · Party Room

Phòng audio nhiều người trên LiveKit: host mở phòng, audience vào nghe, host mời speaker lên nói.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Vào phòng Party (join hoặc join lại)](04-party-room/01-post-join-room.ts) | `POST /api/v1/party/rooms/:id/join` | Vào phòng Party làm audience, hoặc vào lại khi rớt mạng, rồi nhận token LiveKit theo role hiện tại (audience không được publish). | CONTROLLER → SERVICE → POSTGRES → REDIS → EXTERNAL | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/01-post-join-room.html) |
| [Tạo phòng Party](04-party-room/02-post-create-room.ts) | `POST /api/v1/party/rooms` | Tạo phòng và membership host trong 1 transaction, rồi tạo room trên LiveKit; SFU lỗi thì đóng phòng bù trừ. | CONTROLLER → SERVICE → POSTGRES → EXTERNAL | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/02-post-create-room.html) |
| [Đồng ý lời mời lên speaker](04-party-room/03-post-accept-speaker-invite.ts) | `POST /api/v1/party/rooms/:id/speaker-invite/accept` | Audience đồng ý lời mời của host để lên speaker: đếm giới hạn dưới khóa phòng, commit role, rồi mở quyền publish trên LiveKit. | CONTROLLER → SERVICE → POSTGRES → EXTERNAL → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/04-party-room/03-post-accept-speaker-invite.html) |

## 05 · Movie Match / Movie Night

Hai người cùng xem một video đồng bộ: với bạn bè (Movie Night) hoặc ghép người lạ ẩn danh (Movie Match).

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Tạo phiên xem chung với một người bạn](05-movie-match-movie-night/01-post-create-session.ts) | `POST /api/v1/movie-match/sessions` | Tạo phiên xem video chung giữa 2 người bạn, idempotent theo cặp, mỗi user chỉ có 1 phiên đang xem. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/05-movie-match-movie-night/01-post-create-session.html) |

## 06 · Palm Match

Ghép ẩn danh hai người qua trò bói bài: lật lá, xem độ hợp, rồi like hoặc bỏ qua.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Vào hàng đợi Palm Match](06-palm-match/01-post-join-queue.ts) | `POST /api/v1/palm-match/queue` | Vào hàng đợi ghép ẩn danh; nếu có người chờ phù hợp thì ghép ngay thành 1 phiên Palm Match. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/06-palm-match/01-post-join-queue.html) |

## 09 · Diamond, Top-up và VIP

Nạp Diamond qua store hoặc payOS, mua VIP bằng Diamond, hoàn tiền từ store; mọi thay đổi số dư đi qua một sổ kép duy nhất.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Mua VIP bằng Diamond](09-diamond-top-up-va-vip/01-post-vip-purchase.ts) | `POST /api/v1/economy/vip/purchase` | Trừ Diamond và gia hạn VIP trong một transaction của sổ kép: idempotent theo key, khóa ví FOR UPDATE, ghi outbox. | CONTROLLER → SERVICE → POSTGRES → WORKER → KAFKA | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/01-post-vip-purchase.html) |
| [Nạp Diamond bằng receipt của store](09-diamond-top-up-va-vip/02-post-iap-verify.ts) | `POST /api/v1/economy/iap/verify` | Xác thực receipt với Apple hoặc Google ở server, rồi cộng Diamond đúng một lần theo id giao dịch của store. | CONTROLLER → SERVICE → EXTERNAL → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/02-post-iap-verify.html) |
| [Webhook payOS cộng Diamond](09-diamond-top-up-va-vip/03-post-webhook-payos.ts) | `POST /api/v1/economy/webhooks/payos` | payOS báo thanh toán thành công: kiểm HMAC, đối chiếu snapshot của đơn, rồi cộng Diamond đúng một lần. | CONTROLLER → EXTERNAL → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/03-post-webhook-payos.html) |
| [Webhook Apple hoàn tiền](09-diamond-top-up-va-vip/04-post-webhook-apple-refund.ts) | `POST /api/v1/economy/webhooks/apple` | Apple báo REFUND hoặc REVOKE: kiểm chữ ký JWS, rồi đảo bút toán của lần nạp gốc; số dư có thể âm. | CONTROLLER → EXTERNAL → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/09-diamond-top-up-va-vip/04-post-webhook-apple-refund.html) |

## 10 · Matching Speed-up

Trả Diamond để vé đang chờ được đẩy lên đầu hàng đợi Redis và ghép sớm hơn.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Speed-up vé đang chờ ghép](10-matching-speed-up-detail/01-post-speedup-ticket.ts) | `POST /api/v1/matching/tickets/:id/speedup` | Trả Diamond để vé được ưu tiên: trừ tiền trước, cộng boost vào DB, rồi đặt lại điểm trong Redis. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/10-matching-speed-up-detail/01-post-speedup-ticket.html) |

## 11 · Gift

Tặng quà trong Party Room: trừ Diamond của người tặng và cộng điểm PTS cho người nhận trong cùng một transaction.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Tặng quà trong phòng](11-gift/01-post-party-room-gift.ts) | `POST /api/v1/party/rooms/:roomId/gifts` | Tặng quà trong Party Room: trừ DIA người tặng, cộng PTS người nhận, ghi GiftEvent trong cùng một transaction. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/11-gift/01-post-party-room-gift.html) |

## 12 · Mini Game

Hai người bạn chơi oẳn tù tì với nhau: tạo ván, mỗi bên nộp một nước đi, đủ hai nước thì có kết quả.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Nộp nước đi oẳn tù tì](12-mini-game/01-post-submit-move.ts) | `POST /api/v1/mini-game/sessions/:id/moves` | Mỗi bên nộp đúng 1 nước đi; khi đủ 2 nước thì tính thắng/thua và báo kết quả cho cả hai. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/12-mini-game/01-post-submit-move.html) |

## 13 · Trust & Safety

Báo cáo người dùng và chặn người dùng: report trừ trust score có chống lạm dụng, trust score thấp thì bị xếp sau khi ghép.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Báo cáo một người dùng](13-trust-safety/01-post-report.ts) | `POST /api/v1/safety/reports` | Ghi report và trừ trust score của người bị báo cáo trong 1 transaction, có chống lạm dụng theo cặp và theo ngày. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/13-trust-safety/01-post-report.html) |

## 16 · Friend và Chat 1-1

Chat 1-1 bền vững giữa hai người đã là bạn hoặc đã mở chat từ hồ sơ: gửi tin nhắn, streak, realtime và thông báo.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Gửi tin nhắn chat 1-1](16-friend-va-chat-1-1/01-post-send-message.ts) | `POST /api/v1/conversations/:id/messages` | Gửi tin nhắn idempotent, cập nhật streak ngày, phát realtime cho 2 người và thông báo cho người nhận. | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/16-friend-va-chat-1-1/01-post-send-message.html) |

## 17 · Đăng ký, Onboarding và Guest

Đăng nhập bằng OTP, social hoặc guest, tự tạo tài khoản ở lần đầu và giữ phiên bằng refresh token xoay vòng.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Xác minh OTP và đăng nhập](17-dang-ky-onboarding-va-guest/01-post-verify-otp.ts) | `POST /api/v1/auth/otp/verify` | Kiểm mã OTP có giới hạn số lần thử và chỉ dùng một lần, rồi tìm hoặc tạo tài khoản và cấp phiên đăng nhập. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/17-dang-ky-onboarding-va-guest/01-post-verify-otp.html) |
| [Đổi refresh token lấy phiên mới](17-dang-ky-onboarding-va-guest/02-post-refresh.ts) | `POST /api/v1/auth/refresh` | Xoay refresh token mỗi lần dùng và phát hiện dùng lại: token đã xoay mà bị dùng lại thì thu hồi cả họ token. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/17-dang-ky-onboarding-va-guest/02-post-refresh.html) |

## 18 · Discovery, Nearby và Direct Invite

Duyệt người dùng quanh mình (Nearby) rồi mời ghép Voice/Soul Match trực tiếp, bỏ qua hàng đợi khi người kia chấp nhận.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Chấp nhận lời mời ghép trực tiếp](18-discovery-nearby-va-direct-invite/01-post-accept-invite.ts) | `POST /api/v1/matching/invites/:id/accept` | Người được mời chấp nhận: tạo thẳng 2 ticket và 1 session đã confirmed, bỏ qua hàng đợi, idempotent theo (lời mời, vai trò). | CONTROLLER → SERVICE → POSTGRES → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/01-post-accept-invite.html) |
| [Duyệt người dùng gần mình (Nearby)](18-discovery-nearby-va-direct-invite/02-get-nearby.ts) | `GET /api/v1/discovery/nearby` | Trả danh sách người gần vị trí của mình dưới dạng nhóm khoảng cách, có 3 lớp chống dò vị trí. | CONTROLLER → SERVICE → REDIS → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/18-discovery-nearby-va-direct-invite/02-get-nearby.html) |

## 20 · Short Video

Video ngắn: tác giả tải lên, người xem lướt, thả tim, bình luận; lượt xem và thứ hạng được đếm chống trùng.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Ghi nhận lượt xem video](20-short-video/01-post-record-view.ts) | `POST /api/v1/videos/:id/views` | Ghi watch-time của người xem và cộng viewCount đúng 1 lần khi xem đủ ngưỡng, kể cả khi nhiều request đến cùng lúc. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/20-short-video/01-post-record-view.html) |

## 03 · Voice Match

Ghép hai người để nói chuyện bằng giọng nói: vào hàng đợi, ghép cặp, rồi vào cuộc gọi LiveKit.

| Chức năng | Endpoint | Làm gì | Tầng đi qua | Trang |
|---|---|---|---|---|
| [Vào hàng đợi ghép Voice Match](03-voice-match/01-post-join-queue.ts) | `POST /api/v1/matching/tickets` | Vào hàng đợi Voice Match tới khi 2 người được ghép thành MatchSession. | CONTROLLER → SERVICE → POSTGRES → REDIS → WORKER | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/01-post-join-queue.html) |
| [Vào cuộc gọi Voice Match](03-voice-match/02-post-join-call.ts) | `POST /api/v1/calling/match-sessions/:matchSessionId/join` | Mở phòng gọi cho cặp đã ghép: tạo CallSession duy nhất rồi cấp token LiveKit riêng cho từng người. | CONTROLLER → SERVICE → POSTGRES → EXTERNAL | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/02-post-join-call.html) |
| [Webhook LiveKit và đồng hồ cuộc gọi](03-voice-match/03-post-webhook-livekit.ts) | `POST /api/v1/calling/webhooks/livekit` | LiveKit báo ai vào hoặc rời phòng, ticker nền canh giờ (reconnect, tối đa 420 giây) rồi kết thúc call. | CONTROLLER → EXTERNAL → SERVICE → POSTGRES → WORKER → REDIS | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/03-post-webhook-livekit.html) |
| [Thích đối phương sau Voice Match](03-voice-match/04-post-like-call.ts) | `POST /api/v1/calling/calls/:id/like` | Ghi lượt thích bất biến; khi cả 2 cùng thích thì tạo bạn bè và phòng chat trong cùng transaction. | CONTROLLER → SERVICE → POSTGRES | [Pages](https://ducmanh001.github.io/litmatch-feature-flows/code/03-voice-match/04-post-like-call.html) |

Sơ đồ luồng của các tính năng: https://ducmanh001.github.io/litmatch-feature-flows/

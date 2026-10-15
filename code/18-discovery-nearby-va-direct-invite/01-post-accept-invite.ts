// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/matching/invites/:id/accept
//  Người được mời chấp nhận: tạo thẳng 2 ticket và 1 session đã confirmed, bỏ qua hàng đợi, idempotent theo (lời mời, vai trò).
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [SERVICE] → [POSTGRES] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận accept của người được mời, lấy thêm hồ sơ người mời để dựng
//                 response gồm lời mời, session và inviteeTicketId.
//    [SERVICE]    Precheck trong transaction riêng (khóa lời mời, kiểm hạn và chuyển
//                 trạng thái, kiểm canPair), rồi transaction chính tạo 2 ticket +
//                 session confirmed và đổi lời mời sang accepted; bắn realtime sau
//                 commit.
//    [POSTGRES]   Khóa dòng lời mời (FOR UPDATE); unique index chặn user đang bận;
//                 unique idempotency_key chặn ticket đôi; mọi ghi nằm trong 1
//                 transaction.
//    [REDIS]      Pub/sub báo match.confirmed cho cả 2 bên để vào phòng ngay.
//
//  Bảng       match_invites — 1 dòng cho mỗi lời mời: trạng thái (pending → accepted,
//               declined, expired, cancelled), hạn expires_at, session_id khi đã
//               accept; bị khóa FOR UPDATE.
//             match_tickets — 2 dòng mới (người mời, người được mời) với
//               idempotency_key theo (lời mời, vai trò); unique index "1 user 1 ticket
//               đang hoạt động" chặn user đang bận.
//             match_sessions — 1 dòng cho cặp vừa ghép, trạng thái confirmed, trỏ tới 2
//               ticket.
//             users — đọc 2 user để kiểm còn Active và lấy vùng, nhóm tuổi, trust score
//               cho ticket.
//  Redis      realtime:user:{userId} — publish match.confirmed ({ ticketId, sessionId
//               }) cho cả 2 bên; signaling-gateway đẩy xuống socket.
//  Liên quan  Safety — canPair (qua MATCH_INTERACTION_POLICY): từ chối nếu hai người
//               block hoặc report nhau (không chép).
//             createInvite — bước trước: tạo lời mời pending, giới hạn số lời mời mỗi
//               giờ bằng Redis, unique 1 lời mời pending cho mỗi cặp (chưa chép).
//             signaling-gateway — subscribe pub/sub và đẩy match.confirmed xuống socket
//               của 2 bên.
//             Soul Match / Calling — bước kế tiếp: từ đây luồng y hệt auto-match,
//               client vào phòng ngay (chưa chép).
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             thụt vào ngay dưới dòng gọi nó, không chép class hay chữ ký hàm (tên hàm
//             nằm ở dòng tiêu đề // [TẦNG]). Constructor gom ở đầu khối service, chỉ
//             gồm các dependency mà code bên dưới dùng (this.xxx); các dòng const là
//             tham số truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một
//             biểu thức được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên
//             văn trong repo.

// [CONTROLLER] InviteController.accept  ·  invite.controller.ts L45–46, L93–109
@Controller('matching/invites')
export class InviteController {
  constructor(
    private readonly inviteService: InviteService,
    private readonly userService: UserService,
  ) {}

  // Step 1: Nhận accept của người được mời (user từ JWT, id lời mời từ URL)
  @Post(':id/accept')
  @HttpCode(HttpStatus.OK)
  async accept(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
  ): Promise<MatchInviteAcceptedDto> {
    const result = await this.inviteService.acceptInvite(user, id);

      // [SERVICE] InviteService.acceptInvite  ·  invite.service.ts L279–424
      private readonly logger = new Logger(InviteService.name);
      constructor(
        @InjectDataSource() private readonly dataSource: DataSource,
        @Inject(MATCH_INTERACTION_POLICY)
        private readonly interactionPolicy: MatchInteractionPolicy,
        @Inject(MATCHING_REALTIME)
        private readonly realtime: RealtimePublisherPort,
        private readonly config: ConfigService<CoreApiEnv, true>,
      ) {}

      const inviteId = id;

      // Step 2: Precheck trong transaction riêng, commit xong mới throw
      // Precheck chạy ở transaction RIÊNG và COMMIT xong mới throw: canPair fail thì lời mời phải
      // ghi Declined, nhưng throw trong CÙNG transaction sẽ rollback luôn phần Declined vừa ghi
      // (bắt được qua test thật).
      const precheck = await this.precheckAccept(user, inviteId);

        // [SERVICE] InviteService.precheckAccept  ·  invite.service.ts L434–487
        return this.dataSource.transaction(async (manager) => {

          // Step 3: Khóa dòng lời mời (FOR UPDATE), kiểm đúng người được mời
          const invite = await this.lockInvite(manager, inviteId);

            // [SERVICE] InviteService.lockInvite  ·  invite.service.ts L601–617
            const invite = await manager.findOne(MatchInvite, {
              where: { id: inviteId },
              // [POSTGRES] FOR UPDATE: mọi thao tác trên 1 lời mời (accept, decline, cancel) xếp
              // hàng qua đúng dòng này.
              lock: { mode: 'pessimistic_write' },
            });
            if (!invite) {
              throw new DomainException(
                MatchingErrors.INVITE_NOT_FOUND,
                'Không tìm thấy lời mời',
                HttpStatus.NOT_FOUND,
              );
            }
            return invite;

          if (invite.inviteeUserId !== user.userId) {
            throw new DomainException(
              MatchingErrors.INVITE_FORBIDDEN,
              'Lời mời không thuộc về bạn',
              HttpStatus.FORBIDDEN,
            );
          }

          if (invite.status === MatchInviteStatus.Accepted && invite.sessionId) {
            const session = await manager.findOneByOrFail(MatchSession, {
              id: invite.sessionId,
            });
            const inviteeTicket = await manager.findOneByOrFail(MatchTicket, {
              idempotencyKey: inviteAcceptIdempotencyKey(invite.id, 'invitee'),
            });
            return {
              kind: 'already_accepted' as const,
              invite,
              session,
              inviteeTicketId: inviteeTicket.id,
            };
          }

          // Lazy-expire: sweeper có thể chưa kịp chạy, nên kiểm lại hạn TẠI THỜI ĐIỂM hành động.
          this.assertNotExpired(invite);
          this.assertTransition(invite, MatchInviteStatus.Accepted);

          // Step 4: Kiểm lại block/report, không ghép được thì ghi Declined
          // Kiểm lại block/report TẠI THỜI ĐIỂM accept, không chỉ lúc mời: block có thể phát sinh
          // SAU khi mời, TRƯỚC khi accept.
          const canPair = await this.interactionPolicy.canPair(
            invite.inviterUserId,
            invite.inviteeUserId,
          );
          if (!canPair) {
            // Không ghép được thì lời mời tự chuyển declined: ghi và COMMIT ngay ở đây, trả
            // "blocked" để tầng gọi throw NGOÀI transaction.
            invite.status = MatchInviteStatus.Declined;
            invite.respondedAt = new Date();
            await manager.save(invite);
            return { kind: 'blocked' as const };
          }
          return { kind: 'proceed' as const };
        });

      // Step 5: Precheck thấy đã accept thì trả lại ticket và session cũ
      // Accept lặp lại (retry mạng) trả lại đúng ticket và session cũ, không lỗi, không tạo đôi.
      if (precheck.kind === 'already_accepted') {
        return {
          invite: precheck.invite,
          session: precheck.session,
          inviteeTicketId: precheck.inviteeTicketId,
        };
      }
      if (precheck.kind === 'blocked') {
        throw new DomainException(
          MatchingErrors.INVITE_TARGET_UNAVAILABLE,
          'Không thể ghép với người này',
          HttpStatus.FORBIDDEN,
        );
      }

      let matchedPair: Array<{
        userId: string;
        ticketId: string;
        sessionId: string;
      }> | null = null;

      // Step 6: Mở transaction chính: tạo ticket, session, đổi lời mời
      // [POSTGRES] Tạo ticket, session và đổi lời mời sang accepted trong 1 transaction: lỗi ở
      // đâu cũng rollback hết, lời mời còn pending để thử lại.
      const result = await this.dataSource.transaction(async (manager) => {

        // Step 7: Khóa lại dòng lời mời; replay giữa chừng thì trả session cũ
        // Khóa dòng lời mời (FOR UPDATE) trước khi đổi gì: 2 accept đồng thời xếp hàng, bên sau
        // thấy lời mời đã accepted và trả sớm ở nhánh dưới, nên không bao giờ insert ticket lần
        // 2.
        const invite = await this.lockInvite(manager, inviteId);
        if (invite.inviteeUserId !== user.userId) {
          throw new DomainException(
            MatchingErrors.INVITE_FORBIDDEN,
            'Lời mời không thuộc về bạn',
            HttpStatus.FORBIDDEN,
          );
        }

        // Replay xảy ra giữa lúc precheck và đây: trả lại session cũ.
        if (invite.status === MatchInviteStatus.Accepted && invite.sessionId) {
          const session = await manager.findOneByOrFail(MatchSession, {
            id: invite.sessionId,
          });
          const inviteeTicket = await manager.findOneByOrFail(MatchTicket, {
            idempotencyKey: inviteAcceptIdempotencyKey(invite.id, 'invitee'),
          });
          return { invite, session, inviteeTicketId: inviteeTicket.id };
        }

        this.assertNotExpired(invite);
        this.assertTransition(invite, MatchInviteStatus.Accepted);

        // Chỉ kiểm 2 bên còn Active, KHÔNG kiểm gender preference như auto-match: người được mời
        // đã chủ động chấp nhận đúng người này (consent trực tiếp).
        const users = await manager.find(User, {
          where: { id: In([invite.inviterUserId, invite.inviteeUserId]) },
        });
        const userById = new Map(users.map((u) => [u.id, u]));
        const inviter = userById.get(invite.inviterUserId);
        const invitee = userById.get(invite.inviteeUserId);
        if (
          !inviter ||
          !invitee ||
          inviter.status !== UserStatus.Active ||
          invitee.status !== UserStatus.Active
        ) {
          throw new DomainException(
            MatchingErrors.USER_BANNED,
            'Một trong hai bên không còn khả dụng để ghép',
            HttpStatus.CONFLICT,
          );
        }

        // Step 8: Tạo 2 ticket matched, unique chặn user đang bận và ticket đôi
        const [ticketInviter, ticketInvitee] = await this.createMatchedTickets(
          manager,
          invite,
          inviter,
          invitee,
        );

          // [SERVICE] InviteService.createMatchedTickets  ·  invite.service.ts L504–544
          try {
            // Insert cả 2 ticket trong CÙNG transaction: vi phạm uq_match_tickets_active_user (1
            // trong 2 bên đang bận queue hoặc session khác) rollback tất cả, lời mời giữ nguyên
            // pending.
            const [ticketInviter, ticketInvitee] = await manager.save([
              this.draftMatchedTicket(manager, invite, inviter, 'inviter'),
              this.draftMatchedTicket(manager, invite, invitee, 'invitee'),
            ]);

              // [SERVICE] InviteService.draftMatchedTicket  ·  invite.service.ts L546–574
              const profile = inviter;
              const role = 'inviter';
              const bandSize = this.config.getOrThrow('MATCHING_AGE_BAND_SIZE', {
                infer: true,
              });
              const perPoint = this.config.getOrThrow(
                'MATCHING_TRUST_PENALTY_MS_PER_POINT',
                { infer: true },
              );
              const maxMs = this.config.getOrThrow('MATCHING_TRUST_PENALTY_MAX_MS', {
                infer: true,
              });
              return manager.create(MatchTicket, {
                userId: profile.id,
                matchType: invite.matchType,
                region: profile.region ?? 'GLOBAL',
                ageBand: ageBandOf(profile.birthDate, bandSize),
                // Tạo ở trạng thái matched để rơi vào phạm vi unique index (queued, matched):
                // user đang bận thì insert lỗi; chỉ sau khi có session mới chuyển sang confirmed.
                status: MatchTicketStatus.Matched,
                enqueuedAt: new Date(),
                priorityBoostMs: 0,
                trustPenaltyMs: trustPenaltyMsOf(profile.trustScore, perPoint, maxMs),
                sessionId: null,
                // Key theo (lời mời, vai trò): accept lặp lại ra đúng 2 key cũ, unique DB chặn
                // tạo ticket đôi.
                idempotencyKey: inviteAcceptIdempotencyKey(invite.id, role),
              });

            return [ticketInviter, ticketInvitee];
          } catch (err) {
            if (violatedConstraint(err, 'uq_match_tickets_active_user')) {
              throw new DomainException(
                MatchingErrors.INVITE_ACCEPT_USER_BUSY,
                'Một trong hai bên đang ở trong hàng đợi/phiên ghép khác',
                HttpStatus.CONFLICT,
              );
            }
            // Unique ở idempotency_key = replay accept cũ: đọc lại 2 ticket đã tạo. Nhánh này
            // thực tế không chạy tới (lời mời đã khóa nên accept thứ 2 trả sớm); nếu bỏ khóa thì
            // phải tách sang transaction riêng, vì Postgres hủy cả transaction khi 1 câu lỗi.
            if (!isUniqueViolation(err)) throw err;

            const existing = await manager.find(MatchTicket, {
              where: {
                idempotencyKey: In([
                  inviteAcceptIdempotencyKey(invite.id, 'inviter'),
                  inviteAcceptIdempotencyKey(invite.id, 'invitee'),
                ]),
              },
            });
            const byKey = new Map(existing.map((t) => [t.idempotencyKey, t]));
            const ticketInviter = byKey.get(
              inviteAcceptIdempotencyKey(invite.id, 'inviter'),
            );
            const ticketInvitee = byKey.get(
              inviteAcceptIdempotencyKey(invite.id, 'invitee'),
            );
            if (!ticketInviter || !ticketInvitee) throw err;
            return [ticketInviter, ticketInvitee];
          }

        // Step 9: Tạo session confirmed, chuyển 2 ticket sang confirmed
        const confirmedAt = new Date();
        const session = await manager.save(
          manager.create(MatchSession, {
            matchType: invite.matchType,
            userAId: invite.inviterUserId,
            userBId: invite.inviteeUserId,
            ticketAId: ticketInviter.id,
            ticketBId: ticketInvitee.id,
            status: MatchSessionStatus.Confirmed,
            confirmedAAt: confirmedAt,
            confirmedBAt: confirmedAt,
          }),
        );
        ticketInviter.sessionId = session.id;
        ticketInvitee.sessionId = session.id;
        ticketInviter.status = MatchTicketStatus.Confirmed;
        ticketInvitee.status = MatchTicketStatus.Confirmed;
        await manager.save([ticketInviter, ticketInvitee]);

        // Step 10: Đổi lời mời sang accepted, lưu session_id
        invite.status = MatchInviteStatus.Accepted;
        invite.respondedAt = new Date();
        invite.sessionId = session.id;
        await manager.save(invite);

        matchedPair = [
          {
            userId: inviter.id,
            ticketId: ticketInviter.id,
            sessionId: session.id,
          },
          {
            userId: invitee.id,
            ticketId: ticketInvitee.id,
            sessionId: session.id,
          },
        ];
        return { invite, session, inviteeTicketId: ticketInvitee.id };
      });

      // Step 11: Sau commit, phát match.confirmed cho cả 2 bên qua Redis
      // [REDIS] Bắn SAU commit cùng event match.confirmed mà auto-match dùng; 2 bên refetch rồi
      // vào phòng ngay.
      if (matchedPair) {
        const pair = matchedPair as Array<{
          userId: string;
          ticketId: string;
          sessionId: string;
        }>;
        await Promise.all(
          pair.map(({ userId, ticketId, sessionId }) => {
            const envelope: RealtimeEnvelope<MatchConfirmedEventData> = {
              event: RealtimeEvents.MatchConfirmed,
              data: { ticketId, sessionId },
            };
            return publishRealtimeEvent(
              this.realtime,
              this.logger,
              userId,
              envelope,
            );
          }),
        );
      }
      return result;

    // Step 12: Controller trả lời mời, session và inviteeTicketId
    return MatchInviteAcceptedDto.from(
      result,
      await this.userService.getByIdOrThrow(result.invite.inviterUserId),
    );
  }
}

// [SERVICE] match-invite.entity.ts  ·  match-invite.entity.ts L18–31
// Bảng chuyển trạng thái duy nhất: mọi thay đổi phải đi qua đây, không tin client gửi trạng thái
// đích. Chỉ pending mới đi tiếp được.
export const MATCH_INVITE_TRANSITIONS: Readonly<
  Record<MatchInviteStatus, readonly MatchInviteStatus[]>
> = {
  [MatchInviteStatus.Pending]: [
    MatchInviteStatus.Accepted,
    MatchInviteStatus.Declined,
    MatchInviteStatus.Expired,
    MatchInviteStatus.Cancelled,
  ],
  [MatchInviteStatus.Accepted]: [],
  [MatchInviteStatus.Declined]: [],
  [MatchInviteStatus.Expired]: [],
  [MatchInviteStatus.Cancelled]: [],
};

// [POSTGRES] CREATE UNIQUE INDEX uq_match_tickets_active_user  ·  1752200000000-matching-core.ts L34–38
await queryRunner.query(`
  CREATE UNIQUE INDEX uq_match_tickets_active_user
    ON match_tickets(user_id)
    WHERE status IN ('queued','matched')
`);

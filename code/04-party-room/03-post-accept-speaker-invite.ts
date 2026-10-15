// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/party/rooms/:id/speaker-invite/accept
//  Audience đồng ý lời mời của host để lên speaker: đếm giới hạn dưới khóa phòng, commit role, rồi mở quyền publish trên LiveKit.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [SERVICE] → [POSTGRES] → [EXTERNAL] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận request của chính audience được mời, trả membership sau khi đổi
//                 role.
//    [SERVICE]    Khóa phòng, kiểm lời mời đang chờ, đếm speaker so với giới hạn, đổi
//                 role; sau commit mới gọi SFU, lỗi thì hoàn role cũ.
//    [POSTGRES]   Khóa dòng phòng (FOR UPDATE) làm điểm tuần tự hóa; role và cờ
//                 speaker_invite_pending nằm ở dòng membership.
//    [EXTERNAL]   LiveKit: updateParticipant đổi quyền publish ngay cho người đang kết
//                 nối, không cần cấp lại token.
//    [REDIS]      Pub/sub báo party.role.changed cho mọi member đang ở phòng.
//
//  Bảng       party_rooms — khóa FOR UPDATE; đọc status và speaker_limit (chụp lúc tạo
//               phòng).
//             party_room_members — dòng của người accept: kiểm role audience và
//               speaker_invite_pending, đếm role speaker, đổi role thành speaker; hoàn
//               role cũ khi SFU lỗi.
//  Redis      realtime:user:{userId} — publish party.role.changed tới từng member đang
//               active, mỗi người một kênh.
//  Liên quan  inviteSpeaker — bước trước: host gửi lời mời, chỉ bật cờ
//               speaker_invite_pending, người được mời vẫn là audience (chưa chép).
//             LiveKit (SFU) — nhận lệnh updateParticipant đổi grant canPublish của
//               participant đang nối.
//             signaling-gateway — subscribe pub/sub và đẩy party.role.changed xuống
//               socket.
//  Cách đọc   Một file, đi theo thứ tự chạy, Step N đánh số từng bước. Hàm được gọi
//             chép ngay tại dòng gọi nó, ngang cấp (chỉ khối { } của chính code mới
//             thụt vào), không chép class hay chữ ký hàm (tên hàm nằm ở dòng tiêu đề //
//             [TẦNG]). Constructor gom hết ở đầu, dưới class của điểm vào, chỉ gồm các
//             dependency mà code bên dưới dùng (this.xxx); các dòng const là tham số
//             truyền vào, do công cụ gán từ chỗ gọi (hàm phụ chỉ trả về một biểu thức
//             được thay thẳng vào giá trị); mọi dòng còn lại là code nguyên văn trong
//             repo.

// [CONTROLLER] PartyRoomController.acceptSpeakerInvite  ·  party-room.controller.ts L46–47, L195–206
@Controller('party/rooms')
export class PartyRoomController {
  // PartyRoomService
  private readonly logger = new Logger(PartyRoomService.name);
  // LivekitSdkClient
  private readonly roomService: RoomServiceClient;
  constructor(
    private readonly partyRoomService: PartyRoomService,
    // PartyRoomService
    @InjectDataSource() private readonly dataSource: DataSource,
    @InjectRepository(PartyRoomMember)
    private readonly memberRepo: Repository<PartyRoomMember>,
    private readonly livekit: PartyLivekitRoomPort,
    @Inject(PARTY_REDIS) private readonly redis: Redis,
  ) {}

  // Step 1: Nhận request (user từ JWT, id phòng từ URL)
  @Post(':id/speaker-invite/accept')
  @HttpCode(HttpStatus.OK)
  async acceptSpeakerInvite(
    @CurrentUser() user: AuthenticatedUser,
    @Param('id', ParseUUIDPipe) id: string,
  ): Promise<PartyRoomMemberDto> {
    return PartyRoomMemberDto.from(
      await this.partyRoomService.acceptSpeakerInvite(user, id),
    );

    // [SERVICE] PartyRoomService.acceptSpeakerInvite  ·  party-room.service.ts L568–633
    const roomId = id;
    const result = await this.dataSource.transaction(
      async (manager): Promise<PartyRoleTransitionResult> => {

        // Step 2: Trong transaction, khóa dòng phòng FOR UPDATE
        // [POSTGRES] Đếm speaker và đổi role dưới CÙNG lock phòng: 2 người accept đồng thời bị
        // xếp hàng, người sau thấy count đã tăng nên bị chặn.
        const room = await this.lockActiveRoom(manager, roomId);

        // [SERVICE] PartyRoomService.lockActiveRoom  ·  party-room.service.ts L1167–1190
        const room = await manager.findOne(PartyRoom, {
          where: { id: roomId },
          lock: { mode: 'pessimistic_write' },
        });
        if (!room) {
          throw new DomainException(
            PartyRoomErrors.ROOM_NOT_FOUND,
            'Không tìm thấy phòng',
            HttpStatus.NOT_FOUND,
          );
        }
        if (room.status !== PartyRoomStatus.Active) {
          throw new DomainException(
            PartyRoomErrors.ROOM_CLOSED,
            'Phòng đã đóng',
            HttpStatus.CONFLICT,
          );
        }
        return room;

        const target = await manager.findOneBy(PartyRoomMember, {
          roomId,
          userId: user.userId,
          leftAt: IsNull(),
        });
        if (!target) {
          throw new DomainException(
            PartyRoomErrors.NOT_A_MEMBER,
            'Bạn không ở trong phòng',
            HttpStatus.FORBIDDEN,
          );
        }

        // Step 3: Kiểm tra lời mời đang chờ (đã là speaker thì trả luôn)
        // Đã là speaker thì trả về luôn: retry không lỗi và không gọi SFU thêm lần nữa.
        if (target.role === PartyRole.Speaker)
          return { member: target, changed: false };
        if (
          target.role !== PartyRole.Audience ||
          !target.speakerInvitePending
        ) {
          // Không có lời mời đang chờ thì từ chối: lời mời là bước đồng ý bắt buộc, vì API đổi
          // role của host chỉ thu quyền (không cấp), nên client không bypass được.
          throw new DomainException(
            PartyRoomErrors.SPEAKER_INVITE_NOT_PENDING,
            'Không có lời mời speaker đang chờ',
            HttpStatus.CONFLICT,
          );
        }

        // Step 4: Đếm speaker so với giới hạn, rồi đổi role thành speaker
        const speakerCount = await manager.countBy(PartyRoomMember, {
          roomId,
          role: PartyRole.Speaker,
          leftAt: IsNull(),
        });
        // So với speakerLimit đã chụp lúc tạo phòng; host không chiếm slot vì chỉ đếm role
        // speaker.
        if (speakerCount >= room.speakerLimit) {
          throw new DomainException(
            PartyRoomErrors.SPEAKER_LIMIT_REACHED,
            `Phòng đã đủ ${room.speakerLimit} speaker`,
            HttpStatus.CONFLICT,
          );
        }
        target.role = PartyRole.Speaker;
        target.speakerInvitePending = false;
        const saved = await manager.save(target);
        return {
          member: saved,
          changed: true,
          previousRole: PartyRole.Audience,
          previousInvitePending: true,
        };
      },
    );
    if (!result.changed) return result.member;

    // Step 5: Sau commit, gọi LiveKit mở quyền publish cho người này
    await this.syncPublishPermissionAfterCommit({
      roomId,
      userId: user.userId,
      canPublish: true,
      expectedRole: PartyRole.Speaker,
      previousRole: result.previousRole,
      previousInvitePending: result.previousInvitePending,
    });

    // [SERVICE] PartyRoomService.syncPublishPermissionAfterCommit  ·  party-room.service.ts L1074–1142
    const input = {
      roomId,
      userId: user.userId,
      canPublish: true,
      expectedRole: PartyRole.Speaker,
      previousRole: result.previousRole,
      previousInvitePending: result.previousInvitePending,
    };
    try {
      // [EXTERNAL] Gọi SFU SAU commit, không giữ lock DB lúc chờ network: nếu gọi khi đang khóa
      // rồi DB rollback thì DB và SFU lệch nhau.
      await this.livekit.updateParticipantPublish(
        partyRoomName(input.roomId),
        input.userId,
        input.canPublish,
      );

      // [EXTERNAL] LivekitSdkClient.updateParticipantPublish  ·  livekit-sdk.client.ts L85–101
      const roomName = partyRoomName(input.roomId);
      const identity = input.userId;
      const canPublish = input.canPublish;
      try {
        await this.roomService.updateParticipant(roomName, identity, undefined, {
          canPublish,
          canSubscribe: true,
          canPublishData: true,
        });
        return 'updated';
      } catch (error) {
        // 404 = participant chưa nối SFU: coi như xong, vì không nối thì không publish được; lần
        // join sau token lấy role từ DB.
        if (this.isNotFound(error)) return 'not_connected';
        throw error;
      }

      return;
    } catch (error) {
      let compensated = false;
      try {

        // Step 6: LiveKit lỗi: hoàn role cũ bằng transaction mới
        // [POSTGRES] SFU lỗi thì hoàn role cũ bằng 1 transaction mới (lock phòng lại), vì lệnh
        // SFU đã nằm ngoài transaction đầu.
        compensated = await this.dataSource.transaction(async (manager) => {
          await this.lockActiveRoom(manager, input.roomId);
          const current = await manager.findOneBy(PartyRoomMember, {
            roomId: input.roomId,
            userId: input.userId,
            leftAt: IsNull(),
          });
          // Chỉ hoàn khi role hiện tại vẫn là role vừa đặt; đã có chuyển đổi mới thắng thì không
          // đè lên.
          if (!current || current.role !== input.expectedRole) return false;
          current.role = input.previousRole;
          current.speakerInvitePending = input.previousInvitePending;
          await manager.save(current);
          return true;
        });
      } catch (compensationError) {
        this.logger.error(
          `Không thể rollback role sau khi LiveKit đổi quyền thất bại: ${
            compensationError instanceof Error
              ? compensationError.message
              : String(compensationError)
          }`,
        );
      }

      // [EXTERNAL] DB đã hoàn role thì mới trả grant cũ trên SFU, best-effort (lỗi chỉ log).
      if (compensated) {
        await this.livekit
          .updateParticipantPublish(
            partyRoomName(input.roomId),
            input.userId,
            input.previousRole !== PartyRole.Audience,
          )
          .catch((restoreError) =>
            this.logger.error(
              `Khôi phục grant LiveKit sau lỗi quyền thất bại: ${
                restoreError instanceof Error
                  ? restoreError.message
                  : String(restoreError)
              }`,
            ),
          );
      }

      this.logger.error(
        `Đổi quyền publish LiveKit thất bại: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw new ServiceUnavailableException(
        'PARTY_MEDIA_PERMISSION_UPDATE_FAILED',
      );
    }

    // Step 7: Báo party.role.changed cho mọi member qua Redis
    await this.publishRoleChanged(roomId, result.member);

    // [SERVICE] PartyRoomService.publishRoleChanged  ·  party-room.service.ts L662–674
    const member = result.member;
    await this.publishToRoomMembers(roomId, {
      event: RealtimeEvents.PartyRoleChanged,
      data: {
        roomId,
        userId: member.userId,
        role: member.role,
      } satisfies PartyRoleChangedEventData,
    });

    // [SERVICE] PartyRoomService.publishToRoomMembers  ·  party-room.service.ts L1316–1329
    const envelope = {
      event: RealtimeEvents.PartyRoleChanged,
      data: {
        roomId,
        userId: member.userId,
        role: member.role,
      } satisfies PartyRoleChangedEventData,
    };
    const opts = {};
    const members = await this.memberRepo.findBy({ roomId, leftAt: IsNull() });
    await Promise.all(
      members
        .filter((m) => m.userId !== opts.excludeUserId)
        .map((m) =>
          // [REDIS] Mỗi member nhận event qua kênh riêng realtime:user:{userId}; publish lỗi chỉ
          // log, role đã commit không bị ảnh hưởng.
          publishRealtimeEvent(this.redis, this.logger, m.userId, envelope),
        ),
    );

    // Step 8: Service trả membership, controller trả DTO
    return result.member;
  }
}

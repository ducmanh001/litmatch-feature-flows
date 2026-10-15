// ════════════════════════════════════════════════════════════════════════════
//  POST /api/v1/calling/webhooks/livekit
//  LiveKit báo ai vào hoặc rời phòng, ticker nền canh giờ (reconnect, tối đa 420 giây) rồi kết thúc call.
// ════════════════════════════════════════════════════════════════════════════
//  Luồng      [CONTROLLER] → [EXTERNAL] → [SERVICE] → [POSTGRES] → [WORKER] → [SERVICE]
//             → [EXTERNAL] → [REDIS]
//
//  Vai trò từng tầng
//    [CONTROLLER] Nhận webhook công khai của LiveKit, lấy nguyên văn body và header
//                 Authorization, kiểm chữ ký rồi mới giao event cho service; sau verify
//                 luôn trả 200.
//    [EXTERNAL]   LiveKit SDK kiểm chữ ký JWT của webhook; khi call kết thúc, core-api
//                 gọi ngược LiveKit để xóa phòng.
//    [SERVICE]    Biến event vào hoặc rời phòng thành trạng thái CallSession (pending,
//                 active, đang reconnect, ended); mọi nhánh kết thúc dùng chung một hàm
//                 end idempotent.
//    [POSTGRES]   CallSession là nguồn sự thật của thời gian: mốc vào phòng, mốc bắt
//                 đầu, cửa sổ reconnect; khóa dòng FOR UPDATE tuần tự hóa webhook,
//                 ticker và nút end.
//    [WORKER]     CallTickerService chạy nền theo chu kỳ ngắn (mặc định 1 giây): đóng
//                 call pending quá hạn, call rớt mạng quá cửa sổ reconnect, và call
//                 Voice Match chạm 420 giây.
//    [REDIS]      Pub/sub báo call.ended cho 2 người để client đóng màn hình gọi.
//
//  Bảng       call_sessions — 1 dòng cho mỗi cuộc gọi: trạng thái, mốc vào phòng và rớt
//               của từng bên, mốc bắt đầu reconnect, lý do và thời lượng khi kết thúc.
//             match_sessions — chỉ được Matching sửa: chuyển sang ended khi call kết
//               thúc để 2 người không bị session cũ giữ lại.
//  Redis      realtime:user:{userId} — kênh pub/sub theo người nhận; core-api phát
//               call.ended, signaling-gateway chuyển xuống socket.
//  Liên quan  Matching — endVoiceSessionForCall: chốt MatchSession sang ended khi call
//               kết thúc.
//             LiveKit — SFU gửi webhook participant_joined, participant_left,
//               room_finished; core-api xóa phòng sau khi kết thúc.
//             signaling-gateway — subscribe kênh realtime của user, đẩy call.ended
//               xuống client.
//             Calling (GET /calling/calls/:id) — đường đối soát dự phòng khi webhook bị
//               mất: hỏi LiveKit ai đang trong phòng (chưa chép).

// [CONTROLLER] livekit-webhook.controller.ts · L34–61
@Public()
@Post('livekit')
@HttpCode(HttpStatus.OK)
async handle(
  @Req() req: RawBodyRequest<Request>,
  @Headers('authorization') authHeader?: string,
): Promise<{ ok: true }> {
  // Chữ ký được kiểm trên NGUYÊN VĂN body (không phải JSON đã parse), TRƯỚC khi đọc bất
  // cứ nội dung nào. Sai chữ ký thì 401.
  const rawBody = req.rawBody?.toString('utf8');
  if (!rawBody || !authHeader) {
    throw new DomainException(
      CallingErrors.WEBHOOK_INVALID,
      'Thiếu body hoặc Authorization header',
      HttpStatus.UNAUTHORIZED,
    );
  }
  let event;
  try {
    event = await this.livekit.receiveWebhook(rawBody, authHeader);
  } catch {
    throw new DomainException(
      CallingErrors.WEBHOOK_INVALID,
      'Chữ ký webhook không hợp lệ',
      HttpStatus.UNAUTHORIZED,
    );
  }
  await this.callingService.handleWebhookEvent(event);
  return { ok: true };
}

// [EXTERNAL] livekit-sdk.client.ts · L126–136
async receiveWebhook(
  rawBody: string,
  authHeader: string,
): Promise<LivekitSdkWebhookEvent> {
  // WebhookReceiver của LiveKit SDK kiểm JWT trong header Authorization bằng API key và
  // secret; sai thì ném lỗi, controller đổi thành 401.
  const event = await this.receiver.receive(rawBody, authHeader);
  return {
    event: event.event,
    roomName: event.room?.name ?? null,
    participantIdentity: event.participant?.identity ?? null,
  };
}

// [SERVICE] calling.service.ts · L490–515
async handleWebhookEvent(event: LivekitWebhookEvent): Promise<void> {
  // Chỉ nhận phòng có prefix call-: đó là prefix cố định để lọc phòng không thuộc
  // calling. Phòng lạ hoặc không có dòng CallSession thì bỏ qua.
  if (!event.roomName?.startsWith('call-')) return;
  const call = await this.callRepo.findOneBy({ roomName: event.roomName });
  if (!call) return;

  switch (event.event) {
    case 'participant_joined':
      if (event.participantIdentity) {
        await this.markJoined(call.id, event.participantIdentity);
      }
      return;
    // participant_left và room_finished không kết thúc call ngay mà chỉ mở cửa sổ
    // reconnect: mất mạng hay connection abort có thể quay lại. Quá hạn mới end.
    case 'participant_left':
    case 'participant_connection_aborted':
      if (event.participantIdentity) {
        await this.markDisconnected(call.id, event.participantIdentity);
      }
      return;
    case 'room_finished':
      await this.markRoomDisconnected(call.id);
      return;
    default:
      return;
  }
}

// [SERVICE] calling.service.ts · L600–645, L647–665, L667–681
private async markJoined(callId: string, identity: string): Promise<void> {
  await this.dataSource.transaction(async (manager) => {
    const call = await manager.findOne(CallSession, {
      where: { id: callId },
      // [POSTGRES] Khóa dòng CallSession FOR UPDATE rồi mới đọc và ghi: webhook của 2
      // bên, ticker và nút end cùng chạm một dòng. Có khóa rồi mới kiểm lại status.
      lock: { mode: 'pessimistic_write' },
    });
    if (!call || call.status === CallSessionStatus.Ended) return;

    const now = new Date();
    if (identity === call.userAId) call.joinedAAt = call.joinedAAt ?? now;
    else if (identity === call.userBId)
      call.joinedBAt = call.joinedBAt ?? now;
    else return;

    if (
      call.joinedAAt &&
      call.joinedBAt &&
      call.status === CallSessionStatus.Pending
    ) {
      call.status = CallSessionStatus.Active;
      // Chỉ khi CẢ 2 đã vào phòng, call mới sang active và đặt startedAt. Đây là mốc
      // tính giới hạn 7 phút của Voice Match; set-if-null nên webhook gửi lại không ghi
      // đè.
      call.startedAt = call.startedAt ?? now;
    }

    if (call.status === CallSessionStatus.Active) {
      // Bên nào quay lại thì gỡ dấu rớt của bên đó. Chỉ khi cả 2 đã quay lại mới chạy
      // tiếp đồng hồ: startedAt dời tiến đúng bằng khoảng gián đoạn nên thời gian rớt
      // mạng không bị tính vào 7 phút.
      if (identity === call.userAId) call.disconnectedAAt = null;
      if (identity === call.userBId) call.disconnectedBAt = null;

      if (
        call.reconnectStartedAt &&
        call.disconnectedAAt === null &&
        call.disconnectedBAt === null &&
        call.startedAt
      ) {
        const pausedMs = Math.max(
          0,
          now.getTime() - call.reconnectStartedAt.getTime(),
        );
        call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
        call.reconnectStartedAt = null;
      }
    }
    await manager.save(call);
  });
}

private async markDisconnected(
  callId: string,
  identity: string,
): Promise<void> {
  await this.dataSource.transaction(async (manager) => {
    const call = await manager.findOne(CallSession, {
      where: { id: callId },
      lock: { mode: 'pessimistic_write' },
    });
    if (!call || call.status !== CallSessionStatus.Active) return;

    const now = new Date();
    if (identity === call.userAId) call.disconnectedAAt ??= now;
    else if (identity === call.userBId) call.disconnectedBAt ??= now;
    else return;
    // Chỉ ghi mốc rớt đầu tiên (??=): event rớt lặp lại không kéo dài cửa sổ reconnect.
    // Từ mốc này đồng hồ giới hạn tạm dừng.
    call.reconnectStartedAt ??= now;
    await manager.save(call);
  });
}

private async markRoomDisconnected(callId: string): Promise<void> {
  await this.dataSource.transaction(async (manager) => {
    const call = await manager.findOne(CallSession, {
      where: { id: callId },
      lock: { mode: 'pessimistic_write' },
    });
    if (!call || call.status !== CallSessionStatus.Active) return;

    const now = new Date();
    call.reconnectStartedAt ??= now;
    call.disconnectedAAt ??= now;
    call.disconnectedBAt ??= now;
    await manager.save(call);
  });
}

// [WORKER] call-ticker.service.ts · L48–59, L66–75, L77–92, L94–112, L114–149
onApplicationBootstrap(): void {
  this.job.start(this.scheduler, {
    jobName: TICKER_JOB,
    intervalMs: this.config.getOrThrow('CALLING_TICKER_INTERVAL_MS', {
      infer: true,
    }),
    task: () => this.runOnce(),
    logger: this.logger,
    errorMessage: 'Call ticker tick lỗi',
    // [POSTGRES] Mỗi tick chỉ 1 pod làm nhờ advisory lock của Postgres. Nếu 2 pod vẫn
    // chạy trùng thì vẫn an toàn vì endById khóa dòng và chỉ end khi chưa ended.
    clusterSingleton: { dataSource: this.dataSource },
  });
}

async runOnce(): Promise<void> {
  await this.job.runExclusive(async () => {
    await withSpan('litmatch.calling', 'calling.ticker.tick', async () => {
      await this.sweepPending();
      await this.sweepReconnectTimeouts();
      await this.processActiveCalls();
    });
  }, undefined);
}

private async sweepPending(): Promise<void> {
  const timeoutSeconds = this.config.getOrThrow(
    'CALLING_PENDING_TIMEOUT_SECONDS',
    { infer: true },
  );
  const cutoff = new Date(Date.now() - timeoutSeconds * 1000);
  const stale = await this.dataSource.getRepository(CallSession).find({
    select: { id: true },
    where: { status: CallSessionStatus.Pending, createdAt: LessThan(cutoff) },
    order: { createdAt: 'ASC', id: 'ASC' },
    // Mỗi tick chỉ lấy id theo lô; phần tồn đọng xử lý dần ở các tick sau. Truy vấn đi
    // theo index (status, created_at).
    take: TICK_BATCH_SIZE,
  });
  for (const call of stale) {
    await this.callingService.endById(call.id, CallEndReason.PendingTimeout);
  }
}

private async sweepReconnectTimeouts(): Promise<void> {
  const reconnectSeconds = this.config.getOrThrow(
    'CALLING_RECONNECT_WINDOW_SECONDS',
    { infer: true },
  );
  const cutoff = new Date(Date.now() - reconnectSeconds * 1000);
  const stale = await this.dataSource.getRepository(CallSession).find({
    select: { id: true },
    where: {
      status: CallSessionStatus.Active,
      // Rớt mạng ngắn không kết thúc call ngay: có CALLING_RECONNECT_WINDOW_SECONDS để
      // quay lại. Quá hạn mà chưa quay lại mới end completed.
      reconnectStartedAt: LessThan(cutoff),
    },
    order: { reconnectStartedAt: 'ASC', id: 'ASC' },
    take: TICK_BATCH_SIZE,
  });
  for (const call of stale) {
    await this.callingService.endById(call.id, CallEndReason.Completed);
  }
}

private async processActiveCalls(): Promise<void> {
  const freeSeconds = this.config.getOrThrow('CALLING_FREE_CALL_SECONDS', {
    infer: true,
  });
  const activeQuery = this.dataSource
    .getRepository(CallSession)
    .createQueryBuilder('call')
    .select(['call.id'])
    .where('call.status = :status', { status: CallSessionStatus.Active })
    // Call đang trong cửa sổ reconnect bị loại khỏi đợt kiểm này: đồng hồ 420 giây tạm
    // dừng cho tới khi cả 2 quay lại hoặc quá hạn.
    .andWhere('call.reconnect_started_at IS NULL')
    .andWhere('call.started_at IS NOT NULL')
    .orderBy('call.updated_at', 'ASC')
    .addOrderBy('call.id', 'ASC')
    .take(TICK_BATCH_SIZE);

  activeQuery
    // Chỉ Voice Match bị giới hạn: call bạn bè gọi lâu dài, không đi qua đồng hồ này.
    // Hạn 420 giây tính ngay trong SQL theo đồng hồ của Postgres, từ startedAt đã bù
    // thời gian reconnect; không tin timer của client.
    .andWhere("call.call_kind = 'voice_match'")
    .andWhere(
      'call.started_at <= now() - make_interval(secs => :freeSeconds)',
      { freeSeconds },
    );

  const active = await activeQuery.getMany();
  for (const call of active) {
    await this.callingService
      .endById(call.id, CallEndReason.FreeLimit)
      .catch((err) =>
        this.logger.error(
          { err: `${err}` },
          `Xử lý call active ${call.id} lỗi — thử lại ở tick sau`,
        ),
      );
  }
}

// [POSTGRES] call-session.entity.ts · L35–43
@Entity({ name: 'call_sessions' })
@Index('uq_call_sessions_match_session', ['matchSessionId'], { unique: true })
@Index('idx_call_sessions_status_created', ['status', 'createdAt'])
@Index('idx_call_sessions_active_updated', ['updatedAt', 'id'], {
  where: "status = 'active'",
})
@Index('idx_call_sessions_reconnect_started', ['reconnectStartedAt', 'id'], {
  where: "status = 'active' AND reconnect_started_at IS NOT NULL",
})

// [SERVICE] calling.service.ts · L442–483, L684–713
async endById(callId: string, reason: CallEndReason): Promise<EndCallResult> {
  const result = await this.dataSource.transaction(async (manager) => {
    const call = await manager.findOne(CallSession, {
      where: { id: callId },
      // [POSTGRES] Khóa dòng rồi kiểm lại status: chốt chặn race end-vs-tick (nút end,
      // webhook, ticker cùng gọi). Đã ended thì trả lại như cũ, không end lần 2.
      lock: { mode: 'pessimistic_write' },
    });
    if (!call) return { call: null, justEnded: false };
    if (call.status === CallSessionStatus.Ended) {
      return { call, justEnded: false };
    }
    call.status = CallSessionStatus.Ended;
    call.endReason = reason;
    call.endedAt = new Date();
    // Thời gian nằm trong cửa sổ reconnect không tính vào thời lượng: dời startedAt
    // tiến lên trước khi chốt duration.
    if (call.startedAt && call.reconnectStartedAt) {
      const pausedMs = Math.max(
        0,
        call.endedAt.getTime() - call.reconnectStartedAt.getTime(),
      );
      call.startedAt = new Date(call.startedAt.getTime() + pausedMs);
    }
    call.durationSeconds = call.startedAt
      ? Math.max(
          0,
          Math.round(
            (call.endedAt.getTime() - call.startedAt.getTime()) / 1000,
          ),
        )
      : 0;
    return { call: await manager.save(call), justEnded: true };
  });

  // Chỉ lời gọi thực hiện transition mới dọn phòng và phát realtime, nên retry hay
  // ticker chạy trùng không bắn event đôi.
  if (result.justEnded && result.call) {
    this.metrics.recordEnded(result.call.endReason ?? reason);
    await this.cleanupEndedCall(result.call);
  }
  return result;
}

private async cleanupEndedCall(call: CallSession): Promise<void> {
  // Chốt MatchSession ở Matching TRƯỚC khi phát event, để user nhận call.ended rồi quay
  // lại tìm ghép không bị session cũ giữ lại. Cả webhook lẫn ticker đều đi qua đây,
  // không chỉ nút UI.
  if (call.callKind !== CallKind.Friend && call.matchSessionId)
    await this.matchingService.endVoiceSessionForCall(
      call.matchSessionId,
      call.userAId,
      call.userBId,
    );
  // [EXTERNAL] Xóa phòng LiveKit best-effort để không rò tài nguyên. Phòng có thể đã tự
  // đóng (room_finished) nên lỗi ở đây không phải lỗi nghiệp vụ.
  await this.livekit.deleteRoom(call.roomName).catch((err) => {
    this.logger.debug(
      `deleteRoom ${call.roomName} bỏ qua: ${err instanceof Error ? err.message : String(err)}`,
    );
  });
  const envelope: RealtimeEnvelope<CallEndedEventData> = {
    event: RealtimeEvents.CallEnded,
    data: {
      callId: call.id,
      matchSessionId: call.matchSessionId,
      reason: call.endReason ?? CallEndReason.Completed,
      durationSeconds: call.durationSeconds ?? 0,
    },
  };
  // [REDIS] Phát call.ended cho cả 2 người qua kênh realtime:user:{id}. Best-effort:
  // client vẫn poll được GET /calling/calls/:id.
  await Promise.all(
    [call.userAId, call.userBId].map((uid) =>
      publishRealtimeEvent(this.redis, this.logger, uid, envelope),
    ),
  );
}

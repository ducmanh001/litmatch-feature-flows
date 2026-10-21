//  WS Redis pub/sub message → SignalingGateway.relay (mỗi sự kiện realtime)
//  Nhận envelope từ Redis, chỉ emit cho socket CỤC BỘ của user, đặt chỗ ngân sách gửi (theo socket và theo cả pod) TRƯỚC khi enqueue, và đóng transport của socket chậm để nó reconnect rồi đọc bù qua REST.
//  Step 1: Một sự kiện từ Redis tới pod: chỉ chuyển cho socket của user ngay trên pod này
//  Step 2: Kiểm room cục bộ TRƯỚC khi parse JSON
//  Step 3: Ước lượng kích thước gói sẽ gửi (JSON + overhead bảo thủ)
//  Step 4: Event lớn hơn cả ngân sách của pod: bỏ, không đuổi ai để nhường chỗ
//  Step 5: Socket đã đầy ngân sách riêng: đóng transport để nó reconnect, các socket khoẻ vẫn được phục vụ
//  Step 6: Đặt chỗ ngân sách TRƯỚC khi enqueue, kể cả event đầu tiên của kết nối
//  Step 7: Pod hết chỗ: đuổi socket có backlog LÂU NHẤT (tối đa 32 lần) để nhường chỗ
//  Step 8: Emit riêng từng socket bằng server.local
//  Step 9: Trả chỗ đã đặt nếu emit không tạo ra gói Engine.IO nào
//  Step 10: Gói thật thay chỗ đã đặt trước nên chỉ cộng phần chênh
//  Step 11: Gói chỉ được tính là đã gửi khi ghi xong thật: drain (WebSocket) hoặc finish của HTTP response (polling)

// [GATEWAY] SignalingGateway.relay · apps/signaling-gateway/src/app/signaling.gateway.ts
@WebSocketGateway({ namespace: '/signaling' })
export class SignalingGateway
  implements
    OnGatewayInit,
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnModuleDestroy
{
  private readonly logger = new Logger(SignalingGateway.name);
  private readonly admissionBudget: ConnectionAdmissionBudget;
  private readonly outboundBudgets = new WeakMap<
    Socket['conn'],
    OutboundBudget
  >();
  private readonly outboundMaxPackets: number;
  private readonly outboundMaxBytes: number;
  private readonly outboundTotal: OutboundTotalBudget;
  @WebSocketServer()
  private readonly server!: Namespace;
  private readonly previousJwtSecret: string | undefined;
  // OutboundBudget
  private packets = 0;
  private bytes = 0;
  private readonly sizes = new WeakMap<OutboundPacket, number>();
  private readonly pendingWrites = new Set<() => void>();
  private reservation?: { bytes: number };
  private disposed = false;
  // OutboundTotalBudget
  packets = 0;
  bytes = 0;
  private readonly backlogs = new Set<OutboundBudget>();
  constructor(
    private readonly config: ConfigService<SignalingEnv, true>,
    private readonly metrics: SignalingMetrics,
    // OutboundBudget
    private readonly connection: Socket['conn'],
    private readonly total?: OutboundTotalBudget,
    // OutboundTotalBudget
    private readonly maxPackets: number,
    private readonly maxBytes: number,
    private readonly pressure: (result: 'evicted' | 'dropped') => void,
  ) {
    // SignalingGateway
    this.admissionBudget = new ConnectionAdmissionBudget(
      config.get('WS_MAX_CONNECTIONS_PER_PROCESS', { infer: true }) ?? 2_000,
      config.get('WS_MAX_CONCURRENT_HANDSHAKES', { infer: true }) ?? 64,
    );
    this.previousJwtSecret =
      config.get('JWT_SECRET_PREVIOUS', { infer: true }) || undefined;
    this.outboundMaxPackets =
      config.get('WS_OUTBOUND_MAX_PACKETS', { infer: true }) ?? 128;
    this.outboundMaxBytes =
      config.get('WS_OUTBOUND_MAX_BYTES', { infer: true }) ?? 1_048_576;
    this.outboundTotal = new OutboundTotalBudget(
      config.get('WS_OUTBOUND_TOTAL_MAX_PACKETS', { infer: true }) ?? 16_384,
      config.get('WS_OUTBOUND_TOTAL_MAX_BYTES', { infer: true }) ?? 134_217_728,
      (result) => {
        this.metrics.outboundPressure(result);
        if (result === 'evicted') this.metrics.slowConsumer();
      },
    );
    this.metrics.bindOutboundGauges(this.outboundTotal);
    // OutboundBudget
    connection.on('packetCreate', this.onPacket);
    connection.on('flush', this.onFlush);
    connection.once('close', this.dispose);
  }

  // Step 1: Một sự kiện từ Redis tới pod: chỉ chuyển cho socket của user ngay trên pod này
  relay(channel: string, raw: string): void {
    const userId = parseRealtimeUserChannel(channel);
    if (!userId) return;
    const room = userRoom(userId);

    // Step 2: Kiểm room cục bộ TRƯỚC khi parse JSON
    // Kiểm room TRƯỚC khi parse JSON: ở chế độ pattern đa số event không dành cho user nào trên
    // pod, còn ở chế độ user đây là lưới an toàn cho event tới muộn sau khi socket cuối của user
    // đã rời.
    if (!this.hasLocalSockets(room)) {
      this.metrics.relay('no_local_room');
      return;
    }
    let envelope: RealtimeEnvelope;
    try {
      envelope = JSON.parse(raw) as RealtimeEnvelope;
    } catch {
      this.logger.warn(`Bỏ qua payload không phải JSON trên ${channel}`);
      this.metrics.relay('invalid');
      return;
    }
    if (typeof envelope?.event !== 'string') {
      this.metrics.relay('invalid');
      return;
    }

    // Step 3: Ước lượng kích thước gói sẽ gửi (JSON + overhead bảo thủ)
    const nextBytes = OutboundBudget.eventBytes(
      this.server.name,
      envelope.event,
      envelope.data,
    );

    // [GATEWAY] OutboundBudget.eventBytes · apps/signaling-gateway/src/app/outbound-budget.ts
    const namespace = this.server.name;
    const event = envelope.event;
    const data = envelope.data;
    return (
      // Envelope từ Redis luôn là JSON nên Socket.IO phát đúng một gói EVENT không nhị phân và
      // không có ACK id; cộng overhead bảo thủ (loại gói Engine.IO + header frame WebSocket lớn
      // nhất) để tính cả polling.
      Buffer.byteLength(`2${namespace},${JSON.stringify([event, data])}`) +
      FRAME_OVERHEAD
    );

    // Step 4: Event lớn hơn cả ngân sách của pod: bỏ, không đuổi ai để nhường chỗ
    // Một event lớn hơn cả ngân sách của pod thì không thể được nhận chỗ bằng cách đuổi ai: bỏ
    // ngay (đếm vào signaling_outbound_pressure_total{result="dropped"}).

    // [GATEWAY] OutboundTotalBudget.fitsAlone · apps/signaling-gateway/src/app/outbound-budget.ts
    const bytes = nextBytes;
    return this.maxPackets >= 1 && bytes <= this.maxBytes;

    if (!this.outboundTotal.fitsAlone(nextBytes)) {
      this.metrics.outboundPressure('dropped');
      return;
    }
    let delivered = false;
    for (const id of this.server.adapter.rooms.get(room) ?? []) {
      const client = this.server.sockets.get(id);
      if (!client || client.conn.readyState !== 'open') continue;
      let budget = this.outboundBudgets.get(client.conn);
      if (!budget) {
        budget = new OutboundBudget(client.conn, this.outboundTotal);
        this.outboundBudgets.set(client.conn, budget);
      }

      // [GATEWAY] OutboundBudget.allows · apps/signaling-gateway/src/app/outbound-budget.ts
      const maxPackets = this.outboundMaxPackets;
      const maxBytes = this.outboundMaxBytes;
      return this.packets + 1 <= maxPackets && this.bytes + nextBytes <= maxBytes;

      if (
        !budget.allows(
          nextBytes,
          this.outboundMaxPackets,
          this.outboundMaxBytes,
        )
      ) {
        this.metrics.slowConsumer();

        // Step 5: Socket đã đầy ngân sách riêng: đóng transport để nó reconnect, các socket khoẻ
        // vẫn được phục vụ
        // Đóng transport chứ không gửi DISCONNECT của namespace, vì client nhận DISCONNECT sẽ
        // ngừng tự nối lại. Socket chậm reconnect rồi đọc bù trạng thái qua REST; socket khoẻ
        // trong cùng room vẫn được phục vụ.
        budget.evict();

        // [GATEWAY] OutboundBudget.evict · apps/signaling-gateway/src/app/outbound-budget.ts
        // Nhả sổ sách gói đang xếp hàng và đang ghi NGAY, kể cả khi việc đóng transport hoàn tất
        // muộn hơn: ngân sách của pod không được kẹt vì một socket đã bị đuổi.
        this.dispose();

        // [GATEWAY] OutboundBudget.dispose
        if (this.disposed) return;
            this.disposed = true;
            this.connection.off('packetCreate', this.onPacket);
            this.connection.off('flush', this.onFlush);
            this.connection.off('close', this.dispose);
            for (const release of this.pendingWrites) release();
            this.reservation = undefined;
            this.update(-this.packets, -this.bytes);

            // [GATEWAY] OutboundBudget.update
            const packets = -this.packets;
            // bytes = -this.bytes
            this.packets += packets;
            this.bytes += bytes;
            this.total?.update(this, packets, bytes, this.packets > 0);

            // [GATEWAY] OutboundTotalBudget.update
            const owner = this;
            const active = this.packets > 0;
            this.packets += packets;
            this.bytes += bytes;
            if (active) this.backlogs.add(owner);
            else this.backlogs.delete(owner);

        this.connection.close(true);

        continue;
      }

      // Step 6: Đặt chỗ ngân sách TRƯỚC khi enqueue, kể cả event đầu tiên của kết nối
      const cancel = budget.reserve(nextBytes);

      // [GATEWAY] OutboundBudget.reserve · apps/signaling-gateway/src/app/outbound-budget.ts
      // bytes = nextBytes
      // Đặt chỗ phải qua ngân sách của cả pod. makeRoom có thể đuổi chính kết nối này nếu nó đang
      // giữ backlog cũ nhất, nên kiểm lại disposed ngay sau đó.

      // [GATEWAY] OutboundTotalBudget.makeRoom
      if (!this.fitsAlone(bytes)) {
        this.pressure('dropped');
        return false;
      }
      // Công việc đuổi socket cho MỖI người nhận bị chặn ở 32 lần: tin realtime là best-effort
      // nên thà bỏ event còn hơn quét cả pod trong một lần relay.
      for (let evicted = 0; evicted < 32; evicted++) {
        if (
          this.packets + 1 <= this.maxPackets &&
          this.bytes + bytes <= this.maxBytes
        )
          return true;

        // Step 7: Pod hết chỗ: đuổi socket có backlog LÂU NHẤT (tối đa 32 lần) để nhường chỗ
        // Tập backlog giữ thứ tự chèn nên chọn socket có backlog lâu nhất là O(1), và socket rảnh
        // không bị giữ lại trong tập.
        const oldest = this.backlogs.values().next().value;
        if (!oldest) break;
        oldest.evict();
        this.pressure('evicted');
      }
      if (
        this.packets + 1 <= this.maxPackets &&
        this.bytes + bytes <= this.maxBytes
      )
        return true;
      this.pressure('dropped');
      return false;

      if (this.disposed || (this.total && !this.total.makeRoom(bytes))) return;
      if (this.disposed) return;
      const reservation = { bytes };
      this.reservation = reservation;
      this.update(1, bytes);
      return () => {
        if (this.reservation !== reservation) return;
        this.reservation = undefined;
        this.update(-1, -bytes);
      };

      if (!cancel) continue;
      try {

        // Step 8: Emit riêng từng socket bằng server.local
        // Emit riêng từng socket để MỖI người nhận đặt chỗ ngân sách trước khi enqueue, kể cả
        // event đầu tiên. `local` là bắt buộc: mọi pod đã nhận bản Redis relay của event này, nên
        // broadcast không-local sẽ được cluster adapter chuyển sang pod khác và socket nhận N
        // bản.
        this.server.local.to(id).emit(envelope.event, envelope.data);
        delivered = true;
      } finally {

        // Step 9: Trả chỗ đã đặt nếu emit không tạo ra gói Engine.IO nào
        // Chỉ trả lại chỗ đặt trước khi emit không tạo ra gói Engine.IO nào (ví dụ socket vừa
        // đóng); nếu có gói thì packetCreate đã chuyển chỗ đặt thành gói thật.
        cancel();
      }
    }
    if (delivered) this.metrics.relay('delivered');
  }
}

// [GATEWAY] OutboundBudget.onPacket · apps/signaling-gateway/src/app/outbound-budget.ts
private readonly onPacket = (packet: OutboundPacket): void => {
    const bytes =
      (typeof packet.data === 'string'
        ? Buffer.byteLength(packet.data)
        : (packet.data?.byteLength ?? 0)) + FRAME_OVERHEAD;
    this.sizes.set(packet, bytes);

    // Step 10: Gói thật thay chỗ đã đặt trước nên chỉ cộng phần chênh
    // Chỗ đặt trước khi emit được chuyển thành gói thật ở packetCreate: chỉ cộng phần chênh, nên
    // mỗi gói được đếm đúng một lần dù đã đặt chỗ trước.
    const reserved = this.reservation;
    this.reservation = undefined;
    this.update(reserved ? 0 : 1, bytes - (reserved?.bytes ?? 0));
  };
}

// [GATEWAY] OutboundBudget.onFlush
private readonly onFlush = (batch: OutboundPacket[]): void => {
    let packets = 0;
    let bytes = 0;
    for (const packet of batch) {
      const size = this.sizes.get(packet);
      // Gói xếp hàng trước khi tracker được gắn (ví dụ handshake) không có kích thước ghi nhận:
      // bỏ qua thay vì đếm sai.
      if (size === undefined) continue; // e.g. handshake queued before this tracker was attached
      packets++;
      bytes += size;
      this.sizes.delete(packet);
    }
    if (packets === 0) return;

    const transport = this.connection.transport;
    // Engine.IO's own 'drain' fires immediately after flush, NOT after writing to the network.
    // Polling transport's 'drain' also fires at res.end(); wait for the HTTP response's finish.
    const response =
      transport.name === 'polling'
        ? (transport as typeof transport & { res?: ServerResponse }).res
        : undefined;

    // Step 11: Gói chỉ được tính là đã gửi khi ghi xong thật: drain (WebSocket) hoặc finish của
    // HTTP response (polling)
    // 'drain' của Engine.IO bắn ngay sau flush chứ không phải sau khi ghi ra mạng, và với polling
    // còn bắn lúc res.end(): nên chờ 'finish' của HTTP response; WebSocket thì chờ 'drain' của
    // transport. Gói chỉ rời bộ đếm khi đó.
    const writer = response ?? transport;
    const completed = response ? 'finish' : 'drain';
    const release = (): void => {
      if (!this.pendingWrites.delete(release)) return;
      writer.off(completed, release);
      writer.off('close', release);
      this.update(-packets, -bytes);
    };
    this.pendingWrites.add(release);
    writer.once(completed, release);
    writer.once('close', release);
  };
}

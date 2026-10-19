//  JOB ReconciliationService.runFast (mỗi ECONOMY_RECONCILIATION_FAST_INTERVAL_MS)
//  Cộng thêm các cửa sổ giao dịch mới vào tổng Nợ − Có cộng dồn, đếm receipt đã credit mà thiếu giao dịch, rồi báo lệch qua log và metric.
//  Step 1: Tier rẻ chạy dày: kiểm bất biến Nợ = Có theo currency và đếm receipt mồ côi
//  Step 2: Cập nhật tổng Nợ − Có cộng dồn rồi lấy các currency đang lệch (tổng khác 0)
//  Step 3: Lưới an toàn: dựng lại tổng từ TOÀN BỘ lịch sử (lần đầu hoặc mỗi FULL_INTERVAL)
//  Step 4: Đường thường: cận trên cửa sổ kế tiếp tính bằng đồng hồ DB (now() − LAG)
//  Step 5: Cộng Nợ − Có của các giao dịch MỚI trong [watermark, to)
//  Step 6: Dịch watermark SAU khi cửa sổ cộng xong
//  Step 7: Đếm receipt đã credit mà chưa có transaction (index một phần gần như rỗng)
//  Step 8: Lệch thì log error (metric đã ghi ở trên để alert fire); job không tự sửa

// [WORKER] ReconciliationService.runFast · economy/jobs/reconciliation.service.ts
@Injectable()
export class ReconciliationService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(ReconciliationService.name);
  private ledgerBalance: LedgerBalanceState | undefined;
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly config: ConfigService<CoreApiEnv, true>,
    private readonly metrics: EconomyMetrics,
  ) {}

  // Step 1: Tier rẻ chạy dày: kiểm bất biến Nợ = Có theo currency và đếm receipt mồ côi
  async runFast(): Promise<FastReconciliationReport> {
    const startedAt = Date.now();

    // Step 2: Cập nhật tổng Nợ − Có cộng dồn rồi lấy các currency đang lệch (tổng khác 0)
    const imbalances = await this.currentImbalances();

    // [WORKER] ReconciliationService.currentImbalances
    const lagSeconds =
      this.config.getOrThrow('ECONOMY_RECONCILIATION_LAG_MS', { infer: true }) /
      1000;
    const fullIntervalMs = this.config.getOrThrow(
      'ECONOMY_RECONCILIATION_FULL_INTERVAL_MS',
      { infer: true },
    );
    // Tổng cộng dồn chỉ nằm trong bộ nhớ của pod đang giữ khoá cluster-singleton: job vẫn
    // READ-ONLY tuyệt đối (không bảng trạng thái, không ghi gì xuống DB). Pod mới giành khoá chưa
    // có trạng thái thì dựng lại từ đầu.
    const current = this.ledgerBalance;
    if (
      current === undefined ||
      Date.now() - current.rebasedAt >= fullIntervalMs
    ) {
      const startedAt = Date.now();
      // Lưới an toàn: mỗi ECONOMY_RECONCILIATION_FULL_INTERVAL_MS (và khi pod lần đầu giành khoá)
      // dựng lại tổng từ TOÀN BỘ lịch sử. Đây là lần duy nhất chi phí tỉ lệ với cả bảng.
      const { to } = await this.nextWindow(null, lagSeconds);

      // Step 3: Lưới an toàn: dựng lại tổng từ TOÀN BỘ lịch sử (lần đầu hoặc mỗi FULL_INTERVAL)
      const imbalance = await this.sumImbalance(null, to);
      this.ledgerBalance = { watermark: to, imbalance, rebasedAt: Date.now() };
      this.logger.log(
        `Đối soát ledger: dựng lại tổng từ toàn bộ lịch sử tới ${to} (${Date.now() - startedAt} ms)`,
      );
    } else {
      // Đường thường: chỉ cộng các cửa sổ mới [watermark, now() − LAG), tối đa 50 cửa sổ × 6 giờ
      // mỗi tick (≈ 12 ngày bắt kịp sau khi job nghỉ lâu). Chi phí mỗi tick tỉ lệ với số giao
      // dịch MỚI, không với cả bảng ledger_entries.
      for (let i = 0; i < MAX_WINDOWS_PER_TICK; i += 1) {

        // Step 4: Đường thường: cận trên cửa sổ kế tiếp tính bằng đồng hồ DB (now() − LAG)
        const { to, advances, final } = await this.nextWindow(
          current.watermark,
          lagSeconds,
        );

        // [WORKER] ReconciliationService.nextWindow
        const from = current.watermark;
        // Cận trên cửa sổ tính bằng ĐỒNG HỒ DB (now() − LAG), không lệch đồng hồ giữa pod và DB,
        // và tối đa 6 giờ sau watermark. transactions.created_at là giờ BẮT ĐẦU transaction nên
        // giao dịch chạy lâu có thể commit sau khi cửa sổ của nó đã qua: LAG
        // (ECONOMY_RECONCILIATION_LAG_MS) phải lớn hơn giao dịch ghi sổ lâu nhất.
        const [row]: Array<{ to: string; advances: boolean; final: boolean }> =
          await this.dataSource.query(
            `SELECT w.hi::text AS "to",
                    ($1::timestamptz IS NULL OR w.hi > $1::timestamptz) AS advances,
                    ($1::timestamptz IS NULL OR w.hi = w.safe) AS final
               FROM (
                 SELECT s.safe,
                        CASE WHEN $1::timestamptz IS NULL THEN s.safe
                             ELSE LEAST($1::timestamptz + make_interval(hours => $3::int), s.safe) END AS hi
                   FROM (SELECT now() - make_interval(secs => $2::double precision) AS safe) s
               ) w`,
            [from, lagSeconds, MAX_WINDOW_HOURS],
          );
        return row;

        if (!advances) break;

        // Step 5: Cộng Nợ − Có của các giao dịch MỚI trong [watermark, to)
        const delta = await this.sumImbalance(current.watermark, to);

        // [WORKER] ReconciliationService.sumImbalance
        // from = current.watermark
        // Cộng THEO TRANSACTION (join qua transactions), không theo từng entry: cửa sổ luôn chứa
        // trọn các giao dịch đã cân Nợ = Có, cắt giữa chừng sẽ báo lệch giả.
        const select = `SELECT le.currency,
                    SUM(CASE WHEN le.direction = 'debit' THEN le.amount ELSE -le.amount END)::text AS delta
               FROM transactions t
               JOIN ledger_entries le ON le.transaction_id = t.id`;
        // Hai câu SQL riêng thay vì một câu `$1 IS NULL OR …` để planner dùng được BRIN cho cửa
        // sổ hẹp.
        const rows: Array<{ currency: string; delta: string }> =
          from === null
            ? await this.dataSource.query(
                `${select} WHERE t.created_at < $1::timestamptz GROUP BY le.currency`,
                [to],
              )
            : await this.dataSource.query(
                `${select} WHERE t.created_at >= $1::timestamptz AND t.created_at < $2::timestamptz
                  GROUP BY le.currency`,
                [from, to],
              );
        return new Map(rows.map((row) => [row.currency, BigInt(row.delta)]));

        for (const [currency, amount] of delta) {
          current.imbalance.set(
            currency,
            (current.imbalance.get(currency) ?? 0n) + amount,
          );
        }

        // Step 6: Dịch watermark SAU khi cửa sổ cộng xong
        // Trạng thái chỉ đổi SAU khi một cửa sổ tính xong: lỗi DB giữa chừng không làm mất hay
        // nhân đôi cửa sổ nào, tick sau tính lại đúng chỗ.
        current.watermark = to;
        if (final) break;
      }
    }
    return [...(this.ledgerBalance?.imbalance ?? [])]
      .filter(([, amount]) => amount !== 0n)
      .map(([currency, amount]) => ({
        currency,
        imbalance: amount.toString(),
      }));

    // Step 7: Đếm receipt đã credit mà chưa có transaction (index một phần gần như rỗng)
    // Receipt đã credit phải có đúng một transaction ledger đi kèm. COUNT này chạy trên index một
    // phần chỉ chứa receipt credited chưa gắn transaction (thường rỗng) nên không quét bảng
    // receipt.
    const [{ count: orphanReceipts }]: Array<{ count: string }> =
      await this.dataSource.query(
        `SELECT COUNT(*)::text AS count FROM iap_receipts WHERE status = 'credited' AND transaction_id IS NULL`,
      );

    const report: FastReconciliationReport = {
      currencyImbalances: imbalances,
      receiptsWithoutTransaction: Number(orphanReceipts),
      ok: imbalances.length === 0 && Number(orphanReceipts) === 0,
    };

    for (const { currency } of imbalances)
      this.metrics.recordReconciliationMismatch('invariant', currency);
    this.metrics.recordReconciliationMismatch(
      'orphan_receipt',
      LedgerCurrency.Diamond,
      report.receiptsWithoutTransaction,
    );
    this.metrics.recordReconciliationRun(
      'fast',
      report.ok,
      (Date.now() - startedAt) / 1000,
    );

    // Step 8: Lệch thì log error (metric đã ghi ở trên để alert fire); job không tự sửa
    // Job này READ-ONLY tuyệt đối: lệch chỉ ghi log và metric để alert, không tự sửa. Sửa lệch
    // thật phải đi qua reversal entry ở write path chuẩn (LedgerService), không bao giờ từ job
    // chẩn đoán.
    if (!report.ok) {
      this.logger.error(
        { report },
        'ĐỐI SOÁT LỆCH (fast) — tổng Nợ/Có hoặc receipt không khớp, cần điều tra ngay',
      );
    } else {
      this.logger.debug('Đối soát fast OK — tổng Nợ = tổng Có, receipt khớp');
    }
    return report;
  }
}

// [POSTGRES] 1758400000000-reconciliation-indexes.ts · database/migrations/1758400000000-reconciliation-indexes.ts
{
  name: 'idx_transactions_created_at_brin',
  // BRIN chỉ dùng cho bảng append-only không bao giờ xoá: transactions chỉ được thêm vào cuối
  // theo thời gian nên BRIN đủ chọn lọc cho cửa sổ created_at và gần như không tốn gì ở đường ghi
  // nóng. Bảng bị retention xoá rồi chèn lại thì BRIN mất chọn lọc (đã đổi sang B-tree, xem trang
  // Vòng đời dữ liệu).
  sql: 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_created_at_brin ON transactions USING brin (created_at)',
},

// [POSTGRES] 1758400000000-reconciliation-indexes.ts
{
  name: 'idx_iap_receipts_orphan',
  // Index một phần chỉ chứa receipt đã credit mà chưa gắn transaction (thường rỗng): câu COUNT
  // orphan của tier fast gần như không tốn gì.
  sql: "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_iap_receipts_orphan ON iap_receipts (id) WHERE status = 'credited' AND transaction_id IS NULL",
},

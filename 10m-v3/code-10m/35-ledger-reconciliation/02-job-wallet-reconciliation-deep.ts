//  JOB ReconciliationService.runDeep (mỗi ECONOMY_RECONCILIATION_INTERVAL_MS)
//  Lấy một mẫu ví kế tiếp theo cursor user_id bền, so snapshot với số dư dẫn xuất từ ledger trong cùng một statement, rồi lưu cursor cùng transaction; ok chỉ phản ánh mẫu vừa kiểm.
//  Step 1: Tier đắt, cadence thưa: đối chiếu mẫu ví với số dư dẫn xuất từ ledger
//  Step 2: Lấy mẫu ví kế tiếp theo cursor bền
//  Step 3: Chặn cỡ mẫu ngoài [1, 10.000]
//  Step 4: Mở một transaction cho khoá, đọc mẫu và lưu cursor
//  Step 5: Khoá dòng checkpoint và đọc cursor afterUserId
//  Step 6: Một statement: đọc tail + head, dẫn xuất số dư từ ledger, liệt kê ví lệch
//  Step 7: Kiểm metadata cursor mà statement trả về
//  Step 8: Lưu cursor mới (ví lớn nhất vừa kiểm)
//  Step 9: Đặt ok theo mẫu vừa kiểm
//  Step 10: Lệch thì log error (metric đã ghi ở trên); job không tự sửa

// [WORKER] ReconciliationService.runDeep · economy/jobs/reconciliation.service.ts
@Injectable()
export class ReconciliationService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(ReconciliationService.name);
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly metrics: EconomyMetrics,
    // WalletReconciliationSampler
    private readonly dataSource: DataSource,
  ) {}

  // Step 1: Tier đắt, cadence thưa: đối chiếu mẫu ví với số dư dẫn xuất từ ledger
  async runDeep(
    sampleWallets = WALLET_SAMPLE_SIZE,
  ): Promise<DeepReconciliationReport> {
    const startedAt = Date.now();

    // Step 2: Lấy mẫu ví kế tiếp theo cursor bền
    const walletMismatches = await new WalletReconciliationSampler(
      this.dataSource,
    ).sample(sampleWallets);

    // [POSTGRES] WalletReconciliationSampler.sample · economy/jobs/wallet-reconciliation-sampler.ts
    const size = sampleWallets;

    // Step 3: Chặn cỡ mẫu ngoài [1, 10.000]
    // Mẫu chạy tay cũng phải là số nguyên trong [1, 10.000]: mỗi ví là một phép cộng trên
    // ledger_entries của ví đó, nên tăng cỡ mẫu vẫn phải đo chi phí lịch sử ledger mỗi ví.
    if (!Number.isSafeInteger(size) || size < 1 || size > MAX_SAMPLE_SIZE) {
      throw new RangeError(
        `Wallet sample size must be an integer in [1, ${MAX_SAMPLE_SIZE}]`,
      );
    }

    // Step 4: Mở một transaction cho khoá, đọc mẫu và lưu cursor
    // Khoá, đọc mẫu và lưu cursor nằm trong CÙNG một transaction: lỗi đọc hay lỗi commit rollback
    // cả cursor, nên pod thay thế kiểm lại đúng mẫu đó thay vì bỏ qua ví nào.
    return this.dataSource.transaction(async (manager) => {
      await manager.query(
        `INSERT INTO runtime_job_checkpoints (job_key, state) VALUES ($1, $2::jsonb)
         ON CONFLICT (job_key) DO NOTHING`,
        [CHECKPOINT_KEY, JSON.stringify({ afterUserId: null })],
      );

      // Step 5: Khoá dòng checkpoint và đọc cursor afterUserId
      // FOR UPDATE: hai sampler chạy cùng lúc phải xếp hàng nên mỗi bên nhận một trang khác nhau.
      const [checkpoint] = (await manager.query(
        'SELECT state FROM runtime_job_checkpoints WHERE job_key = $1 FOR UPDATE',
        [CHECKPOINT_KEY],
      )) as Array<{ state: WalletCursor }>;
      const afterUserId = checkpoint?.state?.afterUserId;
      if (afterUserId !== null && typeof afterUserId !== 'string') {
        throw new Error('Wallet sampling checkpoint has an invalid cursor');
      }

      // Step 6: Một statement: đọc tail + head, dẫn xuất số dư từ ledger, liệt kê ví lệch
      // Keyset theo khoá chính wallets.user_id: không sort cả bảng theo updated_at và không cần
      // index trên cột đổi mỗi lần ghi số dư. Tail (sau cursor) và head (vòng lại từ đầu) là hai
      // dải khoá không giao nhau, đọc trong CÙNG statement: hết tail thì head bù cho đủ cỡ mẫu
      // nên DB nhỏ vẫn được kiểm hết mỗi lần chạy tay. Snapshot ví và ledger của từng ví dùng
      // chung MVCC snapshot của statement.
      const [page] = (await manager.query(
        `WITH tail AS MATERIALIZED (
           SELECT user_id, balance FROM wallets
            ${afterUserId === null ? '' : 'WHERE user_id > $1::uuid'}
            ORDER BY user_id LIMIT $2
         ), head AS MATERIALIZED (
           SELECT user_id, balance FROM wallets WHERE user_id <= $1::uuid
            ORDER BY user_id LIMIT GREATEST(0, $2::integer - (SELECT count(*) FROM tail))
         ), sample_wallets AS MATERIALIZED (
           SELECT * FROM tail UNION ALL SELECT * FROM head
         ), derived AS (
           SELECT la.user_id,
                  SUM(CASE WHEN le.direction = 'credit' THEN le.amount ELSE -le.amount END) AS balance
             FROM sample_wallets sw
             JOIN ledger_accounts la ON la.user_id = sw.user_id AND la.kind = 'user_wallet'
             JOIN ledger_entries le ON le.account_id = la.id
            GROUP BY la.user_id
         ), mismatches AS (
           SELECT sw.user_id AS "userId", sw.balance::text AS snapshot,
                  COALESCE(d.balance, 0)::text AS derived
             FROM sample_wallets sw LEFT JOIN derived d ON d.user_id = sw.user_id
            WHERE sw.balance <> COALESCE(d.balance, 0)
         )
         SELECT (SELECT count(*)::int FROM sample_wallets) AS "sampledWallets",
                COALESCE((SELECT user_id::text FROM head ORDER BY user_id DESC LIMIT 1),
                         (SELECT user_id::text FROM tail ORDER BY user_id DESC LIMIT 1)) AS "nextUserId",
                COALESCE((SELECT jsonb_agg(m) FROM mismatches m), '[]'::jsonb) AS "walletMismatches"`,
        [afterUserId, size],
      )) as WalletSample[];

      // Step 7: Kiểm metadata cursor mà statement trả về
      // Metadata cursor thiếu hoặc lạ thì ném lỗi (rollback): không lưu cursor và không bao giờ
      // coi đó là một mẫu sạch.
      if (
        !page ||
        !Number.isInteger(page.sampledWallets) ||
        page.sampledWallets < 0 ||
        page.sampledWallets > size ||
        (page.sampledWallets === 0) !== (page.nextUserId === null) ||
        !Array.isArray(page.walletMismatches)
      ) {
        throw new Error('Wallet sample returned incomplete cursor metadata');
      }

      // Step 8: Lưu cursor mới (ví lớn nhất vừa kiểm)
      // Cursor chỉ tiến SAU khi cả mẫu đã đọc xong, và là thứ duy nhất được ghi: ví, ledger và
      // receipt không bị chạm.
      await manager.query(
        'UPDATE runtime_job_checkpoints SET state = $2::jsonb, updated_at = now() WHERE job_key = $1',
        [CHECKPOINT_KEY, JSON.stringify({ afterUserId: page.nextUserId })],
      );
      return page.walletMismatches;
    });

    const report: DeepReconciliationReport = {
      walletMismatches,

      // Step 9: Đặt ok theo mẫu vừa kiểm
      // ok chỉ phản ánh MẪU vừa kiểm (mặc định 100 ví), không chứng minh toàn bộ ví đã sạch. Thời
      // gian bao phủ một vòng phụ thuộc số ví, cadence và số lần chạy thành công.
      ok: walletMismatches.length === 0,
    };

    this.metrics.recordReconciliationMismatch(
      'wallet_sample',
      LedgerCurrency.Diamond,
      walletMismatches.length,
    );
    this.metrics.recordReconciliationRun(
      'deep',
      report.ok,
      (Date.now() - startedAt) / 1000,
    );

    // Step 10: Lệch thì log error (metric đã ghi ở trên); job không tự sửa
    if (!report.ok) {
      this.logger.error(
        { report },
        'ĐỐI SOÁT LỆCH (deep) — snapshot ví không khớp ledger, cần điều tra ngay',
      );
    } else {
      this.logger.debug('Đối soát deep OK — snapshot khớp ledger');
    }
    return report;
  }
}

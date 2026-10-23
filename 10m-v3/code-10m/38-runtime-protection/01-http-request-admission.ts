//  HTTP RequestConcurrencyInterceptor.intercept (APP_INTERCEPTOR: mọi request HTTP trừ health)
//  Mỗi request giữ một slot trong ngân sách của pod; hết chỗ thì trả 503 kèm Retry-After: 1 ngay, không xếp hàng; slot được nhả khi request xong, lỗi hoặc bị huỷ.
//  Step 1: Chạy cho mọi request HTTP; health được miễn
//  Step 2: Lấy một slot của pod, không chờ
//  Step 3: Hết chỗ: 503 ngay trước khi handler chạy
//  Step 4: Chạy handler và nhả slot khi xong, lỗi hoặc bị huỷ

// [CONTROLLER] RequestConcurrencyInterceptor.intercept · common/interceptors/request-concurrency.interceptor.ts
@Injectable()
export class RequestConcurrencyInterceptor implements NestInterceptor {
  private readonly budget: ConcurrencyBudget;
  // ConcurrencyBudget
  private inFlight = 0;
  constructor(
    config: ConfigService<CoreApiEnv, true>,
    private readonly reflector: Reflector,
    // ConcurrencyBudget
    private readonly limit: number,
  ) {
    // RequestConcurrencyInterceptor
    const configured = config.getOrThrow('HTTP_MAX_CONCURRENT_REQUESTS', {
      infer: true,
    });
    this.budget = new ConcurrencyBudget(
      configured || config.getOrThrow('DATABASE_POOL_MAX', { infer: true }) * 4,
    );
    // ConcurrencyBudget
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(`ConcurrencyBudget limit không hợp lệ: ${limit}`);
    }
  }

  // Step 1: Chạy cho mọi request HTTP; health được miễn
  intercept(context: ExecutionContext, next: CallHandler) {
    if (
      context.getType() !== 'http' ||
      // Health probe được miễn (@SkipRequestConcurrency): nếu probe cũng bị từ chối khi quá tải
      // thì kubelet sẽ đánh dấu pod không sẵn sàng và gây vòng lặp rollout đúng lúc hệ thống cần
      // ổn định.
      this.reflector.getAllAndOverride<boolean>(SKIP_REQUEST_CONCURRENCY, [
        context.getHandler(),
        context.getClass(),
      ])
    )
      return next.handle();

    return defer(() => {

      // Step 2: Lấy một slot của pod, không chờ
      // Không chờ: đây là admission có trần, không phải một hàng đợi nữa. Hàng đợi ở ứng dụng chỉ
      // che quá tải và làm mọi request chậm theo; PostgreSQL vẫn giữ giới hạn kết nối và truy vấn
      // của riêng nó.
      const release = this.budget.tryAcquire();

      // [SERVICE] ConcurrencyBudget.tryAcquire · common/scheduling/concurrency-budget.ts
      // Không chờ và không xếp hàng: caller HTTP nhận 503 khi hết chỗ, job nền thì bỏ tick và thử
      // lại ở chu kỳ sau.
      if (this.inFlight >= this.limit) return undefined;
      this.inFlight += 1;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        this.inFlight -= 1;
      };

      if (!release) {
        context
          .switchToHttp()
          .getResponse<Response>()
          // Báo cho client đây là bận tạm thời và nên thử lại sau 1 giây, thay vì để request treo
          // trong hàng đợi.
          .setHeader('Retry-After', '1');

        // Step 3: Hết chỗ: 503 ngay trước khi handler chạy
        throw new ServiceUnavailableException(
          'Dịch vụ đang bận, vui lòng thử lại sau.',
        );
      }

      // Step 4: Chạy handler và nhả slot khi xong, lỗi hoặc bị huỷ
      // finalize nhả slot khi request hoàn tất, lỗi hoặc Observable bị huỷ; defer bên trong còn
      // nhả slot cả khi handle() ném lỗi trước khi trả về Observable.
      return defer(() => next.handle()).pipe(finalize(release));
    });
  }
}

// Package waitx 封装了 apimachinery 的 wait，提供回调返回普通 error\n// 而非 (done bool, err error) 对的辅助函数。\n//\n// 此处 error 的语义与上游相反。在 wait.PollUntilContextCancel 中，回调返回的\n// error 会结束等待；而此包中每个辅助函数的 error 都表示“尚未就绪”，辅助函数会继续调用。\n// 返回 nil 代表成功，且只有返回 nil 才会停止重试。\n//\n// 此包中的函数名有意不与上游名称对应。过去曾沿用上游命名，导致调用方看到熟悉的名称时\n// 误将上游契约套用过来：某 DevicePlugin 服务端在记录读取失败后继续执行并返回 nil，\n// 结果在未发送任何数据的情况下以成功结束重试循环。现在的命名明确标识了辅助函数所属的两类语义：\n//\n//   - Retry* 会在 attempt 返回 nil 时立即停止；返回 error 表示继续重试。\n//   - Repeat* 绝不会因 nil 而停止；无论 attempt 返回什么，它都会按间隔持续运行直至 context 结束，\n//     并将最后一次观察到的 error 返回给调用方。
package waitx

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// AttemptFunc 表示对尚未就绪的操作进行的一次尝试。\n//\n// 返回 nil 表示已完成；返回 error 表示尚未完成，具体后续行为由运行它的辅助函数决定——\n// Retry* 会再次调用它，Repeat* 会记录该 error 并继续执行。\n// 将其命名为 attempt 而非 condition，是因为 condition 返回 error 会被理解为\n// “评估条件失败”，这并非此处 error 的含义。
type AttemptFunc = func(context.Context) error

// ErrStopRetrying 由希望放弃而非继续重试的 AttemptFunc 返回。\n// 辅助函数会停止并返回该 attempt 产生的最后一个实际 error。\n//\n// 如果 attempt 在尚未返回任何实际 error 前就放弃，则无错误可报，辅助函数会返回 nil，\n// 调用方会将其视为成功。仅当需要这种语义时，才应在首次尝试时放弃。
var ErrStopRetrying = errors.New("stop retrying")

// RetryUntilSuccess 按指定间隔调用 attempt，直到其返回 nil 后停止。\n//\n// attempt 返回 error 表示“尚未就绪”，因此会继续重试；这与 wait.PollUntilContextCancel 相反，\n// 后者的 condition 返回 error 会结束等待。如果 attempt 仅记录失败日志并返回 nil，\n// 本辅助函数将直接以成功结束。\n//\n// 如果 context 先结束，则返回 attempt 最后一次返回的 error。attempt 也可以通过返回\n// ErrStopRetrying 主动放弃重试。
func RetryUntilSuccess(
	ctx context.Context, interval time.Duration, immediate bool, attempt AttemptFunc,
) error {
	var lastErr error

	return wait.PollUntilContextCancel(ctx, interval, immediate, func(ctx context.Context) (bool, error) {
		err := attempt(ctx)

		switch cerr := ctx.Err(); {
		case cerr != nil && err != nil:
			// 由外部 context 取消，返回真实的错误原因。
			if errors.Is(err, cerr) && lastErr != nil {
				return false, lastErr
			}
			return false, err
		case cerr != nil:
			// 此处返回取消 error，以保持外部行为一致。
			return false, cerr
		case err != nil:
			// attempt 主动放弃而非失败，返回此前记录的失败 error。
			if errors.Is(err, ErrStopRetrying) {
				return true, lastErr
			}
			// 尚未就绪，记录原因并在下一个间隔重试。
			lastErr = err
			return false, nil // nolint:nilerr
		}

		// attempt 已成功，无需继续重试。
		return true, nil
	})
}

// RetryUntilSuccessWithTimeout 与 RetryUntilSuccess 类似，但在传入 context 的基础上\n// 额外附加了自身的超时截止时间。任一条件结束时即停止重试，并返回 attempt 最后一次返回的 error。
func RetryUntilSuccessWithTimeout(
	ctx context.Context, interval, timeout time.Duration, immediate bool, attempt AttemptFunc,
) error {
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, timeout)
	defer deadlineCancel()

	return RetryUntilSuccess(deadlineCtx, interval, immediate, attempt)
}

// RepeatUntilContextCancel 按指定间隔调用 attempt 直至 context 结束，无论 attempt 返回什么。\n//\n// 与 RetryUntilSuccess 不同，返回 nil 不会停止执行：该函数适用于没有终点的任务（例如监控循环）。\n// attempt 返回的 error 仅被记录而不会触发其他动作，当 context 结束时返回最后一次记录的 error。\n// attempt 可通过返回 ErrStopRetrying 提前终止循环。
func RepeatUntilContextCancel(
	ctx context.Context, interval time.Duration, immediate bool, attempt AttemptFunc,
) error {
	var lastErr error

	return wait.PollUntilContextCancel(ctx, interval, immediate, func(ctx context.Context) (bool, error) {
		err := attempt(ctx)

		switch cerr := ctx.Err(); {
		case cerr != nil && err != nil:
			// 由外部 context 取消，返回真实的错误原因。
			if errors.Is(err, cerr) && lastErr != nil {
				return false, lastErr
			}
			return false, err
		case cerr != nil:
			// 由于使用后台 context 检查连接，\n// 此处应返回取消 error，以保持外部行为一致。
			return false, cerr
		case err != nil:
			// attempt 请求停止，返回此前记录的失败 error。
			if errors.Is(err, ErrStopRetrying) {
				return true, lastErr
			}
			// 记录本轮失败原因，不会终止循环。
			lastErr = err
		}

		// 成功也不会终止循环，等待间隔后继续执行。
		return false, nil
	})
}

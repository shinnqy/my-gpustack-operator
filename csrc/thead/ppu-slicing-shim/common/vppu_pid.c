#include "vppu_pid.h"

/* clamp_i64 — 在两侧都有限制，因此一个表达式的值不能超出其被限制的范围。 */
static int64_t clamp_i64(int64_t value, int64_t low, int64_t high)
{
    if (value < low) {
        return low;
    }
    return (value > high) ? high : value;
}

/* min_allow — 窗口永远不能低于此下限；参见 VPPU_PID_MIN_DIVISOR。即使用于无法进行划分的短周期，也
 * 绝不能设为零，因为零只是一种等待状态，而非限制。 */
static uint64_t min_allow(uint64_t period_ns)
{
    uint64_t floor_ns = period_ns / VPPU_PID_MIN_DIVISOR;

    return (floor_ns == 0ULL) ? 1ULL : floor_ns;
}

uint64_t vppu_pid_floor(unsigned int target_percent, uint64_t period_ns)
{
    if (target_percent >= 100U) {
        return period_ns;
    }

    /* 在除法之前进行相乘，否则上述说法就不正确：一个不能被100整除的周期，在目标值的每个点中会损失高达
     * 99纳秒的精度。任何可到达的输入都如此——该周期以整毫秒为单位配置，因此其纳秒值总是能被整除——但被标
     * 记为“精确”的边界值，对于其给定的参数来说必须是精确的，而不是对于其实际得到的参数来说。乘积不会溢
     * 出：此循环能接受的最大周期单位是毫秒，而100倍的数值远不及2^64。 */
    uint64_t share = period_ns * target_percent / 100ULL;

    return (share < min_allow(period_ns)) ? min_allow(period_ns) : share;
}

uint64_t vppu_pid_step(struct vppu_pid_state *state, const struct vppu_pid_gains *gains,
                       unsigned int target_percent, unsigned int measured_percent,
                       uint64_t period_ns)
{
    /* 未分步状态仅占用配额所分配的槽位，其他内容则无：传感器目前还无法报告该容器正在执行什么操作，若将
     * 其尚未升起的数值纳入其中，就会在等待突发情况的那个步骤上突破配额限制。 */
    if (state->allow_ns == 0ULL) {
        state->allow_ns = vppu_pid_floor(target_percent, period_ns);
        return state->allow_ns;
    }

    uint64_t allow = state->allow_ns;
    int64_t error = (int64_t)target_percent - (int64_t)measured_percent;

    /* 被限制后，仅积分项就绝不可能要求超过一个完整的窗口时间。如果没有这种限制，由长内核在底部支撑的容
     * 器会持续进行积分计算，直到工作负载结束，此时窗口会保持开放状态，持续进行所需的步骤——而此时负载早
     * 已消失。 */
    int64_t integral_limit = VPPU_PID_DIVISOR / ((gains->ki > 0) ? gains->ki : 1);
    int64_t integral = clamp_i64((int64_t)state->integral + error, -integral_limit,
                                integral_limit);
    int64_t derivative = error - (int64_t)state->last_error;

    int64_t weighted = (int64_t)gains->kp * error + (int64_t)gains->ki * integral
                       + (int64_t)gains->kd * derivative;
    int64_t delta = (int64_t)(period_ns / VPPU_PID_DIVISOR) * weighted;

    int64_t next = clamp_i64((int64_t)allow + delta, (int64_t)min_allow(period_ns),
                             (int64_t)period_ns);

    state->integral = (int32_t)integral;
    state->last_error = (int32_t)error;
    state->allow_ns = (uint64_t)next;
    return state->allow_ns;
}

/* hggc_compute.c — 配额的计算部分：每张卡对应一个 duty-cycle 窗口，通过由此容器自身使用情况驱动的
 * PID 循环进行开启与关闭。
 *
 * 其控制机制。每张卡都包含一段重复的 HGGC_SM_CONTROL_PERIOD_MS 周期。在周期内的第一个 allow_ns 时
 * 间内启动的进程会直接执行；之后启动的进程则等待下一个周期。在任务负载下，进程连续启动，导致 duty
 * cycle 为 allow_ns / period，即容器对卡功能的贡献比例。该周期属于共享区域，因此容器内的每个进程共
 * 享一个周期，而非各自拥有独立周期——限制条件由容器设定，而非某个进程设定。
 *
 * 每张卡对应一个窗口，即每张卡对应一个独立组件。容纳两张卡的容器可以接收其中一半，以及另一半的全部
 * 内容，因此是否进行启动取决于该卡本身，而非容器。
 *
 * 为什么使用 A 循环而非 A 公式？所采用的 duty 周期并非真正的 duty 周期：在窗口关闭前启动的内核会让
 * 卡保持忙碌状态，其占用程度则取决于工作负载。因此，这一限制由反馈机制控制——测量容器实际使用的量，
 * 然后调整窗口大小以使其符合限制值。相关计算逻辑位于 common/vppu_pid.c 文件中，该文件针对模拟卡进
 * 行测试；此文件正是需要真实卡进行验证的部分。
 *
 * 所测量的数值。hgmlDeviceGetProcessUtilization，仅针对该容器下的进程进行求和。Card-total 会让每个
 * 容器的循环在共享卡上叠加并产生波动，而 Gate 3 在真实硬件上确认了针对每个进程的查询是受支持的，在
 * 负载下不会为空，并且会报告调用者的自身进程 ID——这就是淘汰 Card-total 回退方案的原因。
 *
 * 窗口和循环的运行速度不同，第二种速度属于该传感器的特性，而非用户偏好。在 PPU 上追踪发现：每 100
 * 毫秒内，数值会变化约 10 个百分点，且不会更快，无论是正向还是反向。因此，从空闲状态变为固定状态的
 * 卡会读取 0、10、22、32 等数值，需要整整一秒才能达到 100。每次窗口更新时执行一次循环，因此会对在
 * 循环方向下已失效的数值产生影响，最长可达一秒；第一次构建时，数值会在整个窗口和最低值之间波动，周
 * 期约为几秒。重新设置窗口是免费的，且每周期都会发生一次；循环操作则每 HGGC_SM_CONTROL_STEP_MS 执
 * 行一次，默认值为所测量的秒数。
 *
 * 为什么通过 dlopen 会触发 HGML 问题？所加载库的 DT_NEEDED 可能只指向 libc.so.6——情况一证明了这一
 * 点——因此无论如何方便，都无法链接 libhgml.so。这些类型仍然来自 hgml.h 中的 __typeof__ 机制，所以
 * 此文件调用的指针属于头文件中的定义，而非对四个签名的手工猜测。
 *
 * FAIL-CLOSED，在这三种情况下可能会失败：
 * - 容器配置无法使用，或者当前没有上下文：启动操作被拒绝，这与分配操作的情况一致。
 * - 使用情况完全无法读取：该循环会保留其拥有的配额，绝不会超过配额本身的份额。缺失的反馈信号不应变
 * 成无法达到的上限，而固定的休眠状态或打开的窗口就会造成这种情况；
 * - 账本无法访问：被拒绝，因为非共享的窗口并不限制包含多个进程的容器。
 *
 * 未被拦截的内容。主机回调在 CPU 上运行，因此延迟它们并不会释放任何资源。它们与其他内容一样被统计，
 * 并直接传递过去。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

/* SDK 头文件并不包含其自身声明所需的内容。 */
#include <stdbool.h>
#include <stddef.h>

#include <hgml.h>

#include "common/vppu.h"
#include "common/vppu_ledger.h"
#include "common/vppu_pid.h"
#include "common/vppu_quota.h"
#include "hggc/hggc_quota.h"

/* 一条查询会返回驱动程序仍持有的所有样本，因此缓冲区的大小是容器进程所需的几倍，而非仅满足一个进程
 * 的需求。如果答案被截断，则视为无答案：部分结果意味着使用比实际需要的更少的资源来代表容器。 */
#define VPPU_UTIL_MAX_SAMPLES 128u

/* 此文件所需的供应商条目，应从 hgml.h 中复制，而非重新输入。 */
static __typeof__(hgmlInit_v2) *hgml_init;
static __typeof__(hgmlDeviceGetHandleByIndex_v2) *hgml_handle_by_index;
static __typeof__(hgmlDeviceGetProcessUtilization) *hgml_process_utilization;

static bool hgml_ready;
static bool hgml_tried;
static int hgml_pid;

static hgmlDevice_t device_handle[VPPU_MAX_DEVICES];
static bool device_handle_ok[VPPU_MAX_DEVICES];

/* 容器配置，每个进程仅读取一次。
 *
 * 计算限制在进程运行期间无法改变——该库被预加载到某个容器中，该容器被切片或未被切片——因此此处会被锁
 * 定，而不是在 getenv() 会进行每次启动时的环境线性扫描的路径上重新读取。限制有多严格则会在每个控制
 * 步骤中重新读取一次，因此即使数值发生变化仍会生效：在 HAMi-core 中，避免将配额直接写入账本就是此
 * 设计的优势所在。
 *
 * any_capped 是容器的答案，card_capped 则是每张卡的配额，因为限制是按卡计算的：一个容器可以包含一
 * 张计算能力仅为半张卡的卡，也可以包含一张完整卡的卡。容器级别的锁定机制确保了未设置配额容器的启动
 * 路径不会受到任何设备查询的干扰。 */
static bool config_ready;
static bool any_capped;
static bool card_capped_known[VPPU_MAX_DEVICES];
static bool card_capped[VPPU_MAX_DEVICES];
static unsigned long long period_ns;
static unsigned long long step_ns;
static unsigned int graph_weight;
static struct vppu_pid_gains gains;

/* 当此线程处于控制步骤中时进行设置，因此进入插入的启动入口的利用采样会被计数并传递，而非被限制——且
 * 无法递归进入另一个步骤。initial-exec 出于常见原因，‘共有’账本显示：通用的动态 TLS 模型会将动态链
 * 接器置于 DT_NEEDED 状态。 */
static __thread bool in_control __attribute__((tls_model("initial-exec")));

/* 不同进程和步骤间隔下的账本使用数据背后的分配组合。
 *
 * 该驱动程序会报告一个进程的利用率数值，因此图形显示与非图形显示的利用率无法直接分开读取。可以单独
 * 分离的是测量本身：一个图形启动发生的间隔与没有发生该情况的间隔被分别累积，然后这两个平均值会并列
 * 显示。这就是图形系数所依赖的测量值——如果图形显示超出限制，图形平均值就会高于普通平均值。 */
static unsigned long long interval_launches;
static unsigned long long interval_graph_launches;
static unsigned long long graph_util_sum;
static unsigned long long graph_util_steps;
static unsigned long long plain_util_sum;
static unsigned long long plain_util_steps;

static unsigned long long now_ns(void)
{
    struct timespec ts;

    /* CLOCK_MONOTONIC，因为该窗口是一个持续时间，阶梯式墙钟不能开启或关闭它；它在共享该区域的进程之间
     * 也是可比较的，而进程级时钟则无法做到这一点。 */
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0ULL;
    }
    return (unsigned long long)ts.tv_sec * 1000000000ULL + (unsigned long long)ts.tv_nsec;
}

static void sleep_ns(unsigned long long duration)
{
    struct timespec req;

    req.tv_sec = (time_t)(duration / 1000000000ULL);
    req.tv_nsec = (long)(duration % 1000000000ULL);
    while (nanosleep(&req, &req) != 0 && errno == EINTR) {
        /* 信号并非等待的终点：需要 SIGPROF 进行分析的工作负载，否则会在每个样本中提前启动。 */
    }
}

/* is_capped — 是否某个计算数值让启动路径有需要等待的内容。
 *
 * 100是一个设定的上限，并非不存在，也不会限制任何东西：完全将循环排除在启动路径之外，而不是运行一
 * 个错误只能为正的控制器。0表示无法使用的配置，而vppu_hggc_gate()已经拒绝了这种配置。 */
static bool is_capped(unsigned int target)
{
    return target > 0U && target < 100U;
}

static void load_config(void)
{
    /* 两个竞争的线程读取相同的环境数据并存储相同的值，因此无保护的标志在最坏情况下会引发重复的解析操作
     * ——这与 hggc_quota.c 中入口点缓存所面临的问题相同。同样的原因：启动路径上的锁并非免费的。 */
    if (config_ready) {
        return;
    }
    config_ready = true;

    /* 对表格进行一次扫描后，无限制容器即可根据单个布尔值确定启动路径，就像当限制条件为仅一个容器那样。
     * 下面会针对每个卡询问其是否受到限制，一旦确定了启动方式即可。 */
    for (int i = 0; i < VPPU_MAX_DEVICES && !any_capped; i++) {
        any_capped = is_capped(vppu_quota_sm_percent(i));
    }
    period_ns = (unsigned long long)vppu_quota_sm_period_ms() * 1000000ULL;
    step_ns = (unsigned long long)vppu_quota_sm_step_ms() * 1000000ULL;
    graph_weight = vppu_quota_graph_weight();
    vppu_quota_sm_gains(&gains);
}

/* device_capped — 是否必须等待该卡对应的窗口，根据上述配置要求，按卡进行锁定。 */
static bool device_capped(int device)
{
    if (!card_capped_known[device]) {
        card_capped[device] = is_capped(vppu_quota_sm_percent(device));
        card_capped_known[device] = true;
    }
    return card_capped[device];
}

/* resolve_hgml — 启动管理库，该进程从其中读取使用情况数据。
 *
 * 通过fork出的子进程而非继承进程重新完成：HGML句柄按与NVML句柄相同的方式属于每个进程，如果子进程继
 * 续使用父进程的资源，它就会读取驱动程序对未打开的描述符所做的任何操作。进程ID时间戳与账本使用的格
 * 式一致，原因也是一样的——pthread_atfork需要-lpthread。 */
static bool resolve_hgml(void)
{
    int self = (int)getpid();

    if (hgml_pid != self) {
        hgml_pid = self;
        hgml_tried = false;
        hgml_ready = false;
        for (int i = 0; i < VPPU_MAX_DEVICES; i++) {
            device_handle_ok[i] = false;
        }
    }
    if (hgml_tried) {
        return hgml_ready;
    }
    hgml_tried = true;

    /* RTLD_LOCAL: 这个句柄用于读取图例，若让整个过程通过它进行解析，将会改变工作负载自身符号的绑定方式。 */
    void *lib = dlopen("libhgml.so", RTLD_LAZY | RTLD_LOCAL);
    if (lib == NULL) {
        vppu_log(VPPU_LOG_DENY,
                 "cannot load libhgml.so (%s) — the compute window holds the quota's own share "
                 "and is never opened past it\n",
                 dlerror());
        return false;
    }

    hgml_init = dlsym(lib, "hgmlInit_v2");
    hgml_handle_by_index = dlsym(lib, "hgmlDeviceGetHandleByIndex_v2");
    hgml_process_utilization = dlsym(lib, "hgmlDeviceGetProcessUtilization");
    if (hgml_init == NULL || hgml_handle_by_index == NULL || hgml_process_utilization == NULL) {
        vppu_log(VPPU_LOG_DENY,
                 "libhgml.so lacks the utilisation entries — the compute window holds the "
                 "quota's own share\n");
        return false;
    }

    hgmlReturn_t rc = hgml_init();
    if (rc != HGML_SUCCESS) {
        vppu_log(VPPU_LOG_DENY,
                 "hgmlInit_v2 failed (%d) — the compute window holds the quota's own share\n",
                 (int)rc);
        return false;
    }

    hgml_ready = true;
    return true;
}

static bool device_by_index(int device, hgmlDevice_t *out)
{
    if (!device_handle_ok[device]) {
        /* 容器本地索引与内存配额键中的数字格式完全一致：SDK 会重新对容器内的设备进行编号，因此无论其宿主的
         * 序位如何，通过该设备的槽位始终为 0。 */
        if (hgml_handle_by_index((unsigned int)device, &device_handle[device]) != HGML_SUCCESS) {
            return false;
        }
        device_handle_ok[device] = true;
    }
    *out = device_handle[device];
    return true;
}

/* newest_for_pid — 是指样本 index 是否为驱动程序为其进程所持有的最新项？
 *
 * 查询要求获取所有历史数据，这正是 Gate 3 所测量的内容，因此一个进程可能会出现多次。将每次出现的情
 * 况相加后，容器利用率就会因驱动程序保存样本的时间长短而发生变化。采用扫描而非映射方式，因为缓冲区
 * 较小，且此操作在窗口周期内仅运行一次：在这里使用哈希表会需要更多的代码来避免错误。 */
static bool newest_for_pid(const hgmlProcessUtilizationSample_t *samples, unsigned int count,
                           unsigned int index)
{
    for (unsigned int i = 0; i < count; i++) {
        if (i == index || samples[i].pid != samples[index].pid) {
            continue;
        }
        if (samples[i].timeStamp > samples[index].timeStamp) {
            return false;
        }
        /* 相同的时间戳：保留其中第一个，即第一个。 */
        if (samples[i].timeStamp == samples[index].timeStamp && i < index) {
            return false;
        }
    }
    return true;
}

/* sample_util — 此容器对一张卡的共享份额，若无法读取则返回 false。
 *
 * “这个容器”的判定取决于账本自身的进程表以及调用者的进程ID，而非该进程ID是否在这个命名空间中可解析
 * ：区域文件是按容器划分的，因此其中的进程ID从构造上属于这个容器，而主机进程ID在这里也可能是一个有
 * 效的进程ID。 */
static bool sample_util(int device, unsigned int *out)
{
    if (!resolve_hgml()) {
        return false;
    }

    hgmlDevice_t handle;
    if (!device_by_index(device, &handle)) {
        return false;
    }

    hgmlProcessUtilizationSample_t samples[VPPU_UTIL_MAX_SAMPLES];
    unsigned int count = VPPU_UTIL_MAX_SAMPLES;
    if (hgml_process_utilization(handle, samples, &count, 0ULL) != HGML_SUCCESS) {
        return false;
    }
    if (count > VPPU_UTIL_MAX_SAMPLES) {
        return false;
    }

    int self = (int)getpid();
    unsigned long long total = 0ULL;
    for (unsigned int i = 0; i < count; i++) {
        int pid = (int)samples[i].pid;

        if (pid != self && !vppu_ledger_has_process(device, pid)) {
            continue;
        }
        if (!newest_for_pid(samples, count, i)) {
            continue;
        }
        total += samples[i].smUtil;
    }

    *out = (total > 100ULL) ? 100U : (unsigned int)total;
    return true;
}

/* note_launch_mix — 将某个区间的测量值合并到图/非图平均值中，并开始下一个区间的计数。 */
static void note_launch_mix(unsigned int measured)
{
    unsigned long long launches = __atomic_exchange_n(&interval_launches, 0ULL, __ATOMIC_RELAXED);
    unsigned long long graphs = __atomic_exchange_n(&interval_graph_launches, 0ULL,
                                                    __ATOMIC_RELAXED);

    if (launches == 0ULL) {
        return;
    }
    if (graphs > 0ULL) {
        graph_util_sum += measured;
        graph_util_steps++;
    } else {
        plain_util_sum += measured;
        plain_util_steps++;
    }
}

static unsigned long long average(unsigned long long sum, unsigned long long count)
{
    return (count == 0ULL) ? 0ULL : sum / count;
}

/* control_step — 循环的一次迭代，由声称该步骤的进程中的任意线程执行。会发布新的分配额度，供其他进
 * 程读取。
 *
 * 目标在这里被重新读取，而不是与其余配置一起被缓存，因此更改后的配额会生效：将配额写入账本是
 * HAMi-core 的陷阱，因为更改限制意味着删除缓存文件。 */
static void control_step(int device, struct vppu_pid_state *state)
{
    struct vppu_pid_state local;
    unsigned int target = vppu_quota_sm_percent(device);
    unsigned int measured = 0U;

    in_control = true;
    bool sampled = sample_util(device, &measured);
    in_control = false;

    /* 只有步进过程会写入这些内容，因此直接读取诊断词是安全的；该配额是其他进程在启动路径上会读取的一个
     * 词，因此会被处理并以原子操作的形式发布。这两个时间戳属于此文件，而非算术运算——因此出现了零值。 */
    local.window_start_ns = 0ULL;
    local.last_step_ns = 0ULL;
    local.allow_ns = __atomic_load_n(&state->allow_ns, __ATOMIC_ACQUIRE);
    local.integral = state->integral;
    local.last_error = state->last_error;

    if (sampled) {
        vppu_ledger_note_util(device, measured);
        vppu_pid_step(&local, &gains, target, measured, period_ns);
    } else {
        /* 没有测量，因此没有循环：只能退回到配额在窗口中的自身份额——而且只能向下分配。已经低于该份额的循环
         * 之所以如此，是因为工作负载超出了其窗口限制，而忽略了这一点后，在出现损失样本的情况下，会重新计算
         * 容器所应获得的配额。 */
        unsigned long long floor_ns = vppu_pid_floor(target, period_ns);

        if (local.allow_ns == 0ULL || floor_ns < local.allow_ns) {
            local.allow_ns = floor_ns;
        }
    }
    __atomic_store_n(&state->allow_ns, local.allow_ns, __ATOMIC_RELEASE);
    state->integral = local.integral;
    state->last_error = local.last_error;

    note_launch_mix(measured);
    vppu_log(VPPU_LOG_DEBUG,
             "compute device=%d target=%u measured=%u%s allow_us=%llu period_us=%llu "
             "step_us=%llu graph_util=%llu/%llu plain_util=%llu/%llu\n",
             device, target, measured, sampled ? "" : " (unread)", local.allow_ns / 1000ULL,
             period_ns / 1000ULL, step_ns / 1000ULL, average(graph_util_sum, graph_util_steps),
             graph_util_steps, average(plain_util_sum, plain_util_steps), plain_util_steps);
}

/* maybe_step — 如果必要的话运行循环，在整个容器内的每个步骤间隔最多进行一次。
 *
 * 交换机制就是实现“最多一次”的效果：多个进程在同一时间间隔内尝试执行，其中一个会成功，其余的则继续
 * 执行其发布的操作。而锁定机制则相当于一个系统调用，在已经为每次启动操作支付时钟读取成本的路径上运
 * 行。 */
static void maybe_step(int device, struct vppu_pid_state *state, unsigned long long now)
{
    unsigned long long last = __atomic_load_n(&state->last_step_ns, __ATOMIC_ACQUIRE);

    /* now < last 并非过度担忧：区域文件会持续存在直到重启，之前启动时的单调时钟时间戳也可以保留在账本
     * 中。将其视为正常的话，就会重新生成时间戳，若等待它则会导致程序卡在死循环状态，直到时钟更新完成。 */
    if (last != 0ULL && now >= last && now - last < step_ns) {
        return;
    }
    if (!__atomic_compare_exchange_n(&state->last_step_ns, &last, now, false, __ATOMIC_ACQ_REL,
                                     __ATOMIC_ACQUIRE)) {
        return;
    }
    control_step(device, state);
}

/* wait_for_window — 等待调用者，直到其卡的槽位允许此次启动。 */
static void wait_for_window(int device, struct vppu_pid_state *state, bool graph)
{
    for (;;) {
        unsigned long long now = now_ns();
        unsigned long long start = __atomic_load_n(&state->window_start_ns, __ATOMIC_ACQUIRE);

        /* 如果窗口已关闭，任何注意到此情况的人都会留下痕迹，这很常见且周期性的发生；至于循环是否也会进行，
         * 则是一个更慢的问题，在此询问它正是为了区分这两种情况。now < start 涵盖了因区域文件在重启后仍然存
         * 在而未来留下的痕迹。 */
        if (now < start || now - start >= period_ns) {
            if (__atomic_compare_exchange_n(&state->window_start_ns, &start, now, false,
                                            __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                maybe_step(device, state, now);
            }
            continue;
        }

        unsigned long long allow = __atomic_load_n(&state->allow_ns, __ATOMIC_ACQUIRE);
        if (graph && graph_weight > 1U) {
            /* 图形启动会运行被捕获到该图形中的任意数量的内核，因此它只能在单个内核能获得的窗口时间的一小部分内
             * 被允许使用。 */
            allow /= graph_weight;
        }
        if (now - start < allow) {
            return;
        }
        sleep_ns(start + period_ns - now);
    }
}

bool vppu_hggc_gate(enum vppu_entry entry, bool graph)
{
    if (!vppu_quota_usable()) {
        vppu_log(VPPU_LOG_DENY,
                 "DENIED %s: the container's quota configuration is unusable — see the "
                 "load-time report\n",
                 vppu_hggc_name(entry));
        return false;
    }

    load_config();
    if (!any_capped) {
        return true;
    }

    /* 两个绝对不能进行等待的调用者。持有卡账本锁的线程处于接纳状态中，供应商的分配操作也在该锁下执行：
     * 若进行限制，则会导致整个窗口期间无法使用该锁，从而阻碍卡上其他进程的分配操作。处于控制步骤中的线
     * 程则属于该拦截库对使用情况的采样操作。 */
    if (in_control || vppu_ledger_holding()) {
        return true;
    }

    int device = vppu_hggc_device();
    if (device < 0) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s: no current device\n", vppu_hggc_name(entry));
        return false;
    }

    /* 在命名卡之后进行询问，因为一个容器可能同时包含一张封顶卡和一张未封顶卡——且计数仅针对封顶卡，因为
     * 存在启动混合情况，以便解释循环中的测量使用情况。 */
    if (!device_capped(device)) {
        return true;
    }

    __atomic_fetch_add(&interval_launches, 1ULL, __ATOMIC_RELAXED);
    if (graph) {
        __atomic_fetch_add(&interval_graph_launches, 1ULL, __ATOMIC_RELAXED);
    }

    struct vppu_pid_state *state = vppu_ledger_control(device);
    if (state == NULL) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s device=%d: the ledger is unavailable\n",
                 vppu_hggc_name(entry), device);
        return false;
    }

    wait_for_window(device, state, graph);
    return true;
}

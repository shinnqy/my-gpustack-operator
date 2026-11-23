/* vppu_test.c — 用于 common/的单元测试，也是该库中的第一个测试。
 *
 * 它们存在是因为没有 hg*、hggc* 或 hgml* 类型的通用名称：因此配额计算、键映射和区域可以在没有 SDK、
 * 没有供应商库也没有设备的条件下实现，而此树中其他内容均无法做到这一点。这里的其他内容只能根据供应
 * 商镜像中的 PPU 主机来评判。
 *
 * 由 thead-case-6.sh 构建并运行，它会打印出每个脚本脚本中相同的三列表格，最终以 FAILS=<n> 结尾，因
 * 此脚本直接传递该结果，而不进行重新解释。
 *
 * 涉及该区域的每个测试都会在子进程中进行，而父进程则不会进行映射——只有一个故意的例外，即
 * test_fork_while_locked() 函数，其存在正是出于这个原因。这并非出于繁琐：每次进程都会独立获取映射，
 * 这样损坏的账本只需进行一次 open() 操作，而非每次分配都需操作一次，因此只进行了一次映射的父进程无
 * 法再测试另一个区域——而且跨进程测试本质上需要独立的进程。 */
#define _GNU_SOURCE

#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "vppu_ledger.h"
#include "vppu_pid.h"
#include "vppu_quota.h"

#define MIB (1024ULL * 1024ULL)
#define QUOTA_MIB 4096ULL
#define QUOTA_BYTES (QUOTA_MIB * MIB)

/* 控制器窗口在纳秒级别下，处于默认周期状态。 */
#define PERIOD_NS (VPPU_SM_PERIOD_MS_DEFAULT * 1000000ULL)

static int fails;
static char tmpdir[] = "/tmp/vppu-test-XXXXXX";

/* 在自己的语句中调用任何会写入输出参数的函数，切勿将其放在需要格式化该输出参数的 check_that() 条件
 * 内：调用函数的参数评估顺序未指定，因此可以在调用前根据值来格式化细节。两种顺序都出现过——clang 先
 * 评估条件，gcc 再处理参数——最终结果就是出现了一行数据，却打印出了根本不存在的数字。 */
static void check_that(bool ok, const char *name, const char *fmt, ...)
{
    va_list ap;

    printf("%s | %s | ", ok ? "PASS" : "FAIL", name);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    if (!ok) {
        fails++;
    }
}

/* region_path — 每次测试使用一个新的账本路径，因此没有测试会受其他测试遗留问题的影响。 */
static const char *region_path(int which)
{
    static char path[256];

    snprintf(path, sizeof(path), "%s/ledger-%d", tmpdir, which);
    return path;
}

/* run_child — 在自己的进程中运行一个主体，针对自己的账本进行操作，并报告其选择的退出代码。0表示该
 * 主体完成任务；其他数值则属于其自身的诊断信息。 */
static int run_child(int (*body)(void), const char *path)
{
    pid_t pid = fork();

    if (pid == 0) {
        setenv(VPPU_ENV_LEDGER_PATH, path, 1);
        _exit(body());
    }
    if (pid < 0) {
        return -1;
    }

    int status = 0;
    if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
        return -1;
    }
    return WEXITSTATUS(status);
}

/* --------------------------------------------------------------------------------------- 配额解析
 * ------------------------------------------------------------------------------------- */

static void test_quota_parse(void)
{
    static const struct {
        const char *value;
        unsigned long long unit;
        unsigned long long want;
        const char *name;
    } cases[] = {
        {"4096", MIB, 4096ULL * MIB, "parse: a MiB figure scales"},
        {"25", 1ULL, 25ULL, "parse: a percentage is unscaled"},
        {NULL, MIB, 0ULL, "parse: unset is zero"},
        {"", MIB, 0ULL, "parse: empty is zero"},
        /* 零被拒绝，而非作为无限制的标准被尊重：这就是未设置模板所呈现的结果，而零的配额则出于某种没人会发
         * 现的理由，拒绝所有分配。 */
        {"0", MIB, 0ULL, "parse: zero is not a quota"},
        {"abc", MIB, 0ULL, "parse: a malformed figure is zero"},
        /* 后面的垃圾数据必须被拒绝，不能被截断：“4096MiB”的解析结果为4096，这样会导致调用方从未选择的单位
         * 看起来像是可以正常工作的。 */
        {"4096MiB", MIB, 0ULL, "parse: trailing junk is rejected"},
        {"-1", MIB, 0ULL, "parse: a negative figure is rejected"},
        /* 限制就是核心问题：被回绕的产品要么落在 0 上，那里没有任何约束，要么落在一个微小的数值上，这完全
         * 无法执行任何操作。 */
        {"18014398509481985", MIB, 0ULL, "parse: an overflowing figure is rejected"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        unsigned long long got = vppu_quota_parse(cases[i].value, cases[i].unit);

        check_that(got == cases[i].want, cases[i].name, "\"%s\" x %llu -> %llu, want %llu",
                   (cases[i].value != NULL) ? cases[i].value : "(unset)", cases[i].unit, got,
                   cases[i].want);
    }
}

static void test_quota_knob(void)
{
    static const struct {
        const char *value;
        unsigned int want;
        const char *name;
    } cases[] = {
        {"250", 250U, "knob: a figure in range is taken"},
        /* 零是一个合理的收益——导数项默认为此值——这也是为什么在 vppu_quota_parse() 函数之外存在这个解析逻辑，
         * 因为零表示无法使用。 */
        {"0", 0U, "knob: zero is a value, not an absence"},
        {NULL, 42U, "knob: unset falls back"},
        {"", 42U, "knob: empty falls back"},
        {"abc", 42U, "knob: malformed falls back"},
        {"250x", 42U, "knob: trailing junk falls back"},
        {"-1", 42U, "knob: a negative figure falls back"},
        {"1001", 42U, "knob: above the range falls back"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        unsigned int got = vppu_quota_knob(cases[i].value, 0U, 1000U, 42U);

        check_that(got == cases[i].want, cases[i].name, "\"%s\" -> %u, want %u",
                   (cases[i].value != NULL) ? cases[i].value : "(unset)", got, cases[i].want);
    }

    setenv(VPPU_ENV_SM_PERIOD_MS, "0", 1);
    check_that(vppu_quota_sm_period_ms() == VPPU_SM_PERIOD_MS_DEFAULT,
               "knob: a window of zero is refused", "%s=0 -> %ums, the default",
               VPPU_ENV_SM_PERIOD_MS, vppu_quota_sm_period_ms());
    unsetenv(VPPU_ENV_SM_PERIOD_MS);

    setenv(VPPU_ENV_SM_KI, "77", 1);
    struct vppu_pid_gains gains;
    vppu_quota_sm_gains(&gains);
    check_that(gains.ki == 77 && gains.kp == VPPU_SM_KP_DEFAULT && gains.kd == VPPU_SM_KD_DEFAULT,
               "knob: one gain can be tuned alone", "kp=%d ki=%d kd=%d", gains.kp, gains.ki,
               gains.kd);
    unsetenv(VPPU_ENV_SM_KI);

    /* 0的权重会使图形启动变为免费，这与旋钮的用途相反，因此范围从1开始。 */
    setenv(VPPU_ENV_SM_GRAPH_WEIGHT, "0", 1);
    check_that(vppu_quota_graph_weight() == VPPU_SM_GRAPH_WEIGHT_DEFAULT,
               "knob: a graph weight of zero is refused", "%s=0 -> %u",
               VPPU_ENV_SM_GRAPH_WEIGHT, vppu_quota_graph_weight());
    unsetenv(VPPU_ENV_SM_GRAPH_WEIGHT);
}

static void test_quota_env(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "3", "2048", 1);
    check_that(vppu_quota_memory_bytes(3) == 2048ULL * MIB, "quota: a card's own figure",
               "%s3=2048 -> %llu bytes", VPPU_ENV_MEMORY_LIMIT_PREFIX,
               vppu_quota_memory_bytes(3));
    check_that(vppu_quota_memory_bytes(4) == 0ULL, "quota: an unconfigured card is zero",
               "card 4 carries no figure");
    check_that(vppu_quota_memory_bytes(-1) == 0ULL && vppu_quota_memory_bytes(VPPU_MAX_DEVICES) == 0ULL,
               "quota: an out-of-range index is zero", "-1 and %d both answer 0",
               VPPU_MAX_DEVICES);

    /* 未标注的内存数据，按计算方的方式读取——两个维度上相同的优先级是约定，而非便利。 */
    setenv(VPPU_ENV_MEMORY_LIMIT, "1024", 1);
    check_that(vppu_quota_memory_bytes(3) == 2048ULL * MIB
                   && vppu_quota_memory_bytes(4) == 1024ULL * MIB,
               "quota: a card's own memory figure wins over the un-indexed one",
               "%s3=2048 with %s=1024 -> card 3 %lluMiB, card 4 %lluMiB",
               VPPU_ENV_MEMORY_LIMIT_PREFIX, VPPU_ENV_MEMORY_LIMIT,
               vppu_quota_memory_bytes(3) / MIB, vppu_quota_memory_bytes(4) / MIB);

    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "3", "not-a-number", 1);
    check_that(vppu_quota_memory_bytes(3) == 0ULL,
               "quota: an unparsable per-card memory figure does not fall back",
               "%s3 is set, so %s=1024 must not rescue it", VPPU_ENV_MEMORY_LIMIT_PREFIX,
               VPPU_ENV_MEMORY_LIMIT);
    unsetenv(VPPU_ENV_MEMORY_LIMIT);
    check_that(vppu_quota_memory_bytes(4) == 0ULL,
               "quota: with neither form set a card carries no memory figure",
               "card 4 answers 0 once %s is gone", VPPU_ENV_MEMORY_LIMIT);
    unsetenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "3");

    setenv(VPPU_ENV_SM_LIMIT, "25", 1);
    check_that(vppu_quota_sm_percent(0) == 25U, "quota: a compute cap", "%s=25 -> %u",
               VPPU_ENV_SM_LIMIT, vppu_quota_sm_percent(0));
    setenv(VPPU_ENV_SM_LIMIT, "150", 1);
    check_that(vppu_quota_sm_percent(0) == 0U, "quota: a compute cap above 100 is not a cap",
               "150 is reported unset rather than clamped");
    unsetenv(VPPU_ENV_SM_LIMIT);

    /* 每张卡的运算数据，以及将其与 HAMi-core 区分开来的规则：一个设定好的索引数据决定了该卡的设定，因
     * 此任何错误输入都会使该卡无法使用，而不是采用整个系统的总数据。 */
    setenv(VPPU_ENV_SM_LIMIT, "50", 1);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "1", "25", 1);
    check_that(vppu_quota_sm_percent(1) == 25U && vppu_quota_sm_percent(0) == 50U,
               "quota: a card's own compute cap wins", "%s1=25 with %s=50 -> card 1 %u, card 0 %u",
               VPPU_ENV_SM_LIMIT_PREFIX, VPPU_ENV_SM_LIMIT, vppu_quota_sm_percent(1),
               vppu_quota_sm_percent(0));
    check_that(vppu_quota_sm_percent(2) == 50U, "quota: a card with no figure of its own falls back",
               "card 2 reads %s=50 -> %u", VPPU_ENV_SM_LIMIT, vppu_quota_sm_percent(2));

    setenv(VPPU_ENV_SM_LIMIT_PREFIX "1", "not-a-number", 1);
    check_that(vppu_quota_sm_percent(1) == 0U,
               "quota: an unparsable per-card compute cap does not fall back",
               "%s1 is set, so %s=50 must not rescue it", VPPU_ENV_SM_LIMIT_PREFIX,
               VPPU_ENV_SM_LIMIT);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "1", "150", 1);
    check_that(vppu_quota_sm_percent(1) == 0U,
               "quota: an out-of-range per-card compute cap does not fall back",
               "%s1=150 is not a cap, and %s=50 must not stand in for it",
               VPPU_ENV_SM_LIMIT_PREFIX, VPPU_ENV_SM_LIMIT);
    unsetenv(VPPU_ENV_SM_LIMIT_PREFIX "1");
    unsetenv(VPPU_ENV_SM_LIMIT);
}

/* vppu_log_level() 函数在首次使用时会被缓存，因此每个判断结果也需要对应的进程。该级别决定了是否会
 * 出现拒绝情况，因此格式错误的结果绝不能导致库无法运行：strtol() 对 "abc" 的解析结果为 0，而 0 也
 * 是用来隐藏拒绝情况的级别。 */
static int child_log_level_malformed(void)
{
    setenv(VPPU_ENV_LOG_LEVEL, "abc", 1);
    return (vppu_log_level() == VPPU_LOG_DENY) ? 0 : 1;
}

static int child_log_level_trailing_junk(void)
{
    setenv(VPPU_ENV_LOG_LEVEL, "2x", 1);
    return (vppu_log_level() == VPPU_LOG_DENY) ? 0 : 1;
}

static int child_log_level_set(void)
{
    setenv(VPPU_ENV_LOG_LEVEL, "2", 1);
    return (vppu_log_level() == VPPU_LOG_DEBUG) ? 0 : 1;
}

static int child_log_level_silenced(void)
{
    setenv(VPPU_ENV_LOG_LEVEL, "0", 1);
    return (vppu_log_level() == 0) ? 0 : 1;
}

static void test_log_level(void)
{
    check_that(run_child(child_log_level_malformed, region_path(0)) == 0,
               "log: a malformed level keeps the default", "%s=abc must not silence denials",
               VPPU_ENV_LOG_LEVEL);
    check_that(run_child(child_log_level_trailing_junk, region_path(0)) == 0,
               "log: a level with trailing junk keeps the default", "%s=2x is not a level",
               VPPU_ENV_LOG_LEVEL);
    check_that(run_child(child_log_level_set, region_path(0)) == 0,
               "log: a level that parses is used", "%s=2 raises the level to debug",
               VPPU_ENV_LOG_LEVEL);
    check_that(run_child(child_log_level_silenced, region_path(0)) == 0,
               "log: zero still silences everything", "%s=0 is a choice, not a typo",
               VPPU_ENV_LOG_LEVEL);
}

/* vppu_quota_validate() 每进程调用一次，因此每个判断结果都需要独立的配额。 */
static int child_validate_none(void)
{
    unsetenv(VPPU_ENV_MEMORY_LIMIT);
    unsetenv(VPPU_ENV_SM_LIMIT);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

static int child_validate_bad(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "not-a-number", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

static int child_validate_no_compute(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "4096", 1);
    unsetenv(VPPU_ENV_SM_LIMIT);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

static int child_validate_bad_compute(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "4096", 1);
    setenv(VPPU_ENV_SM_LIMIT, "150", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

static int child_validate_good(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "4096", 1);
    setenv(VPPU_ENV_SM_LIMIT, "100", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 0 : 1;
}

/* 用于实现每卡计算功能的双卡容器。 */
static int child_validate_two_cards(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "4096", 1);
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "1", "6144", 1);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "0", "50", 1);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "1", "25", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 0 : 1;
}

static int child_validate_one_card_no_compute(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "0", "4096", 1);
    setenv(VPPU_ENV_MEMORY_LIMIT_PREFIX "1", "6144", 1);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "0", "50", 1);
    unsetenv(VPPU_ENV_SM_LIMIT);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

/* 仅就那个未标注索引的配对而言，它构成了容器的完整配置——即那些分配器在每个维度上只知道一个数值的分
 * 配器；HAMi自己的NVIDIA分支就是这样的分配器。 */
static int child_validate_shared_only(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT, "4096", 1);
    setenv(VPPU_ENV_SM_LIMIT, "50", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 0 : 1;
}

static int child_validate_shared_no_compute(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT, "4096", 1);
    unsetenv(VPPU_ENV_SM_LIMIT);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

/* 未标记的内存数据适用于所有卡，因此自身计算数据格式错误的卡会按照共享数据进行分配，从而启动无限制
 * 的操作——启动路径将未解决的限制视为“无需等待的内容”。 */
static int child_validate_shared_bad_card_compute(void)
{
    setenv(VPPU_ENV_MEMORY_LIMIT, "4096", 1);
    setenv(VPPU_ENV_SM_LIMIT, "50", 1);
    setenv(VPPU_ENV_SM_LIMIT_PREFIX "1", "not-a-number", 1);
    vppu_quota_validate();
    return vppu_quota_usable() ? 1 : 0;
}

static void test_quota_validate(void)
{
    check_that(run_child(child_validate_none, region_path(0)) == 0,
               "validate: no figure at all is unusable",
               "a sliced container with no %s<i> must not run unlimited",
               VPPU_ENV_MEMORY_LIMIT_PREFIX);
    check_that(run_child(child_validate_bad, region_path(0)) == 0,
               "validate: an unparsable figure is unusable",
               "a typo must not read as a card that was never sliced");
    /* 计算部分是一个分配器必须完全满足的指标，因为其自带的辅助功能默认将请求设置为100%——如果忽略这个变
     * 量，就会悄无声息地占用整张卡的计算资源。只有在控制器强制执行该标准后，才能确定其可用性。 */
    check_that(run_child(child_validate_no_compute, region_path(0)) == 0,
               "validate: a memory figure without a compute figure is unusable",
               "%s absent must not read as uncapped compute", VPPU_ENV_SM_LIMIT);
    check_that(run_child(child_validate_bad_compute, region_path(0)) == 0,
               "validate: an out-of-range compute figure is unusable", "%s=150 is not a cap",
               VPPU_ENV_SM_LIMIT);
    check_that(run_child(child_validate_good, region_path(0)) == 0,
               "validate: both figures are usable", "%s0=4096 with %s=100 is accepted",
               VPPU_ENV_MEMORY_LIMIT_PREFIX, VPPU_ENV_SM_LIMIT);
    check_that(run_child(child_validate_two_cards, region_path(0)) == 0,
               "validate: two cards at their own compute caps are usable",
               "%s0=50 beside %s1=25 is accepted", VPPU_ENV_SM_LIMIT_PREFIX,
               VPPU_ENV_SM_LIMIT_PREFIX);
    /* 每个被GIVEN的容器都需要一个计算量，因此一张卡的量化标准不能替代另一张的——按卡而非按容器进行验证
     * 正是其关键所在。 */
    check_that(run_child(child_validate_one_card_no_compute, region_path(0)) == 0,
               "validate: a card with memory and no compute figure is unusable",
               "%s0=50 must not cover card 1", VPPU_ENV_SM_LIMIT_PREFIX);
    check_that(run_child(child_validate_shared_only, region_path(0)) == 0,
               "validate: the un-indexed pair alone is usable", "%s=4096 with %s=50 is accepted",
               VPPU_ENV_MEMORY_LIMIT, VPPU_ENV_SM_LIMIT);
    check_that(run_child(child_validate_shared_no_compute, region_path(0)) == 0,
               "validate: an un-indexed memory figure without a compute figure is unusable",
               "%s=4096 alone must not read as uncapped compute", VPPU_ENV_MEMORY_LIMIT);
    check_that(run_child(child_validate_shared_bad_card_compute, region_path(0)) == 0,
               "validate: one card's malformed compute figure spoils the un-indexed pair",
               "%s covers every card, so %s1 cannot be left unresolved", VPPU_ENV_MEMORY_LIMIT,
               VPPU_ENV_SM_LIMIT_PREFIX);
}

/* --------------------------------------------------------------------------------------- 计算控制
 * 器
 *
 * 这是无需卡就能实现的计算配额的一半，另一半在硬件上看起来像是“波动不定”的。下面的描述并非随意编造
 * ：在打开窗口内启动的内核会在窗口关闭后继续占用卡资源，因此实际利用率是该使用周期的若干倍——这正是
 * 前馈下限无法察觉的误差，而循环的存在正是为了消除这种误差。
 * ------------------------------------------------------------------------------------- */

/* simulate — 针对一个使用率达到其任务周期 spill_tenths/10倍的卡运行循环，并包含一个测量延迟窗口。
 * 报告最后的测量值以及最后的分配量。 */
static unsigned int simulate(unsigned int target, unsigned int spill_tenths, int steps,
                             unsigned long long *allow_out)
{
    struct vppu_pid_gains gains;
    struct vppu_pid_state state;
    unsigned int measured = 0U;
    unsigned long long allow = 0ULL;

    memset(&state, 0, sizeof(state));
    vppu_quota_sm_gains(&gains);

    for (int i = 0; i < steps; i++) {
        allow = vppu_pid_step(&state, &gains, target, measured, PERIOD_NS);

        unsigned long long util = allow * spill_tenths * 100ULL / (PERIOD_NS * 10ULL);
        measured = (util > 100ULL) ? 100U : (unsigned int)util;
    }
    *allow_out = allow;
    return measured;
}

static void test_pid_feed_forward(void)
{
    check_that(vppu_pid_floor(25U, PERIOD_NS) == PERIOD_NS / 4ULL,
               "pid: the cold-start allowance is the quota's share of the window",
               "25%% of %llums is %lluns", (unsigned long long)VPPU_SM_PERIOD_MS_DEFAULT,
               vppu_pid_floor(25U, PERIOD_NS));
    check_that(vppu_pid_floor(100U, PERIOD_NS) == PERIOD_NS,
               "pid: an uncapped container gets the whole window", "100%% is the whole period");

    /* 第一步必须严格遵循地面测量，忽略其他数据。传感器受到零值的旋转速率限制，因此已固定的卡在第一次采
     * 样时读数几乎为零；将这一情况纳入其中，会在等待突发数据的那一步导致超过限额的窗口出现。在此处设置
     * 测量值为0，就是最糟糕的情况。 */
    struct vppu_pid_gains gains;
    struct vppu_pid_state state;
    memset(&state, 0, sizeof(state));
    vppu_quota_sm_gains(&gains);
    unsigned long long first = vppu_pid_step(&state, &gains, 25U, 0U, PERIOD_NS);
    check_that(first == vppu_pid_floor(25U, PERIOD_NS),
               "pid: the first step is the floor and ignores the measurement",
               "%lluns, floor is %lluns", first, vppu_pid_floor(25U, PERIOD_NS));
    check_that(state.integral == 0 && state.last_error == 0,
               "pid: the first step accumulates nothing",
               "integral=%d last_error=%d, so the second step starts from the measurement",
               (int)state.integral, (int)state.last_error);

    /* 在它实际对测量进行处理的那一步之后，或者如果循环只是单向传递的话。 */
    unsigned long long second = vppu_pid_step(&state, &gains, 25U, 0U, PERIOD_NS);
    check_that(second > first, "pid: the second step follows the measurement",
               "%lluns after %lluns, with the container measured below its cap", second, first);
}

static void test_pid_clamps(void)
{
    struct vppu_pid_gains gains;
    struct vppu_pid_state state;
    unsigned long long allow = 0ULL;

    memset(&state, 0, sizeof(state));
    vppu_quota_sm_gains(&gains);

    /* 无论分配多少资源，都会有一张卡处于 100% 状态——就是一个长的内核在做这件事。窗口必须保持打开状态，
     * 绝不能关闭：拒绝每次启动会让计算限制变成死锁，而且之后无法缩短正在运行的内核的时长。 */
    for (int i = 0; i < 200; i++) {
        allow = vppu_pid_step(&state, &gains, 25U, 100U, PERIOD_NS);
    }
    check_that(allow >= PERIOD_NS / VPPU_PID_MIN_DIVISOR && allow > 0ULL,
               "pid: a pinned card squeezes the window but never closes it",
               "%lluns, floor is %lluns", allow, (unsigned long long)PERIOD_NS / VPPU_PID_MIN_DIVISOR);

    /* 当积分在接触那个地板时必须保持有界，否则那个窗口就会保持敞开状态，直到所有窗口打开为止——而此时负
     * 载早已消失。 */
    long long limit = (long long)VPPU_PID_DIVISOR / (gains.ki > 0 ? gains.ki : 1);
    check_that(state.integral >= -limit && state.integral <= limit,
               "pid: the integral is clamped against windup", "%lld, limit %lld",
               (long long)state.integral, limit);

    /* 即使在整个窗口期间都无法达到上限的工作负载，也必须被赋予整个窗口的时间，必须保持在该水平而不是在
     * 周围波动。 */
    unsigned long long open = 0ULL;
    unsigned int measured = simulate(25U, 2U, 200, &open);
    check_that(open == PERIOD_NS, "pid: a workload under its cap keeps the whole window",
               "measured %u%% against a 25%% cap, allowance %lluns of %lluns", measured, open,
               (unsigned long long)PERIOD_NS);
}

static void test_pid_convergence(void)
{
    static const struct {
        unsigned int target;
        unsigned int spill_tenths;
        const char *name;
    } cases[] = {
        {25U, 20U, "pid: converges on a 25% cap when work spills a window over"},
        {50U, 15U, "pid: converges on a 50% cap"},
        {10U, 40U, "pid: converges on a 10% cap against four windows of spill"},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        unsigned long long allow = 0ULL;
        unsigned int measured = simulate(cases[i].target, cases[i].spill_tenths, 400, &allow);
        int drift = (int)measured - (int)cases[i].target;

        check_that(drift >= -5 && drift <= 5, cases[i].name,
                   "settled at %u%% against a %u%% cap (allowance %lluns of %lluns)", measured,
                   cases[i].target, allow, (unsigned long long)PERIOD_NS);
    }
}

/* --------------------------------------------------------------------------------------- 关键映射
 * 表 ------------------------------------------------------------------------------------- */

static void test_alloc_map(void)
{
    int device = -1;
    unsigned long long bytes = 0ULL;

    vppu_alloc_record(0x10000ULL, 2, 4096ULL);
    bool took = vppu_alloc_take(0x10000ULL, &device, &bytes);
    check_that(took && device == 2 && bytes == 4096ULL,
               "map: a record comes back with its card", "device=%d bytes=%llu", device,
               bytes);
    check_that(!vppu_alloc_take(0x10000ULL, &device, &bytes),
               "map: a key can only be taken once", "the second take reports nothing");
    check_that(!vppu_alloc_take(0xdeadbeefULL, &device, &bytes),
               "map: an unrecorded key is not an error", "nothing to refund, reported as such");

    /* 那个墓碑案例，以及它所保护的缺陷。通过清空槽位来删除条目，会中断所有经过该条目的关键路径的探测链，
     * 因此后续空闲的进程将一无所获，其数据字节也会在进程生命周期内被记账。完整的表使得所有路径都变得很
     * 长，这就是将其从边缘情况转变为常见情况的原因。 */
    const unsigned long long first = 0x1000ULL;
    for (unsigned long long i = 0; i < 1024ULL; i++) {
        vppu_alloc_record(first + i * 0x1000ULL, 0, 1ULL);
    }
    check_that(vppu_alloc_take(first, &device, &bytes),
               "map: a full table still yields the first key", "1024 page-aligned keys stored");

    unsigned int lost = 0;
    for (unsigned long long i = 1; i < 1024ULL; i++) {
        if (!vppu_alloc_take(first + i * 0x1000ULL, &device, &bytes)) {
            lost++;
        }
    }
    check_that(lost == 0, "map: a deletion does not orphan the keys behind it",
               "%u of 1023 remaining keys could not be taken after one deletion", lost);
}

/* --------------------------------------------------------------------------------------- 区域
 * ------------------------------------------------------------------------------------- */

static int child_create_region(void)
{
    if (!vppu_ledger_lock(0)) {
        return 1;
    }
    vppu_ledger_note_config(0, QUOTA_BYTES, 25U);
    if (!vppu_ledger_charge(0, MIB)) {
        vppu_ledger_unlock(0);
        return 2;
    }
    vppu_ledger_unlock(0);

    if (vppu_ledger_used(0) != MIB) {
        return 3;
    }
    if (!vppu_ledger_lock(0)) {
        return 4;
    }
    vppu_ledger_refund(0, MIB);
    vppu_ledger_unlock(0);
    return (vppu_ledger_used(0) == 0ULL) ? 0 : 5;
}

/* read_u32 / read_u64 — 该区域以外部扫描器读取的方式被读取：通过原始文件中已记录的偏移量进行读取，
 * 而非通过此库的结构体。 */
static bool read_at(const char *path, off_t offset, void *out, size_t len)
{
    int fd = open(path, O_RDONLY);

    if (fd < 0) {
        return false;
    }
    bool ok = (pread(fd, out, len, offset) == (ssize_t)len);
    close(fd);
    return ok;
}

static void test_region_layout(void)
{
    const char *path = region_path(1);

    check_that(run_child(child_create_region, path) == 0,
               "region: charge and refund round trip", "1MiB charged then refunded to zero");

    struct stat st;
    memset(&st, 0, sizeof(st));
    bool sized = (stat(path, &st) == 0);
    check_that(sized && (size_t)st.st_size >= sizeof(struct vppu_region),
               "region: the file is at least the layout's size", "%llu bytes, layout is %zu",
               (unsigned long long)st.st_size, sizeof(struct vppu_region));

    char magic[8] = {0};
    uint32_t version = 0;
    uint32_t header_bytes = 0;
    uint32_t device_slots = 0;
    uint32_t process_slots = 0;
    bool got = read_at(path, 0, magic, sizeof(magic))
               && read_at(path, 8, &version, sizeof(version))
               && read_at(path, 12, &header_bytes, sizeof(header_bytes))
               && read_at(path, 16, &device_slots, sizeof(device_slots))
               && read_at(path, 20, &process_slots, sizeof(process_slots));

    check_that(got && memcmp(magic, VPPU_REGION_MAGIC, sizeof(magic)) == 0,
               "region: the magic is at offset 0", "%.8s", got ? magic : "unread");
    check_that(version == VPPU_REGION_VERSION, "region: the layout version is at offset 8",
               "%u", version);
    check_that(header_bytes == 96U, "region: the header size is at offset 12",
               "%u, the documented offset of the device table", header_bytes);
    check_that(device_slots == VPPU_MAX_DEVICES && process_slots == VPPU_MAX_PROCESSES_PER_DEVICE,
               "region: the slot counts are at offsets 16 and 20", "%u cards, %u processes",
               device_slots, process_slots);
}

/* 此处无法计算的数值，因此在文件中找到该数值证明是父进程读取了子进程所写的内容，而非巧合。 */
#define CONTROL_WINDOW_MARK 0x1122334455667788ULL

static int child_control_round_trip(void)
{
    struct vppu_pid_gains gains;

    /* 一次锁定和释放操作，正是用于创建和标记该区域。 */
    if (!vppu_ledger_lock(0)) {
        return 1;
    }
    vppu_ledger_unlock(0);

    struct vppu_pid_state *state = vppu_ledger_control(0);
    if (state == NULL) {
        return 2;
    }

    vppu_quota_sm_gains(&gains);
    state->window_start_ns = CONTROL_WINDOW_MARK;
    vppu_pid_step(state, &gains, 25U, 100U, PERIOD_NS);
    vppu_ledger_note_util(0, 100U);
    return 0;
}

/* 该循环必须能够从运行它的进程外部被读取，因为其收益并不符合任何 PPU 的规范，而在硬件上对其进行调
 * 整也无需对循环自身状态进行分析。这就是那种读取方式，由工具/和抓取器按照文件中的文档化偏移量进行，
 * 而不是通过这个拦截库的结构体来操作。 */
static void test_control_state_published(void)
{
    const char *path = region_path(7);

    check_that(run_child(child_control_round_trip, path) == 0,
               "control: a step is written to the region", "one window and one step recorded");

    off_t control_offset = (off_t)offsetof(struct vppu_region, devices)
                           + (off_t)offsetof(struct vppu_device_usage, control);
    check_that(control_offset == 128,
               "control: the controller words sit at the documented offset",
               "%lld, card 0's controller state", (long long)control_offset);

    uint64_t window_start = 0;
    uint64_t allow = 0;
    uint32_t util = 0;
    bool got = read_at(path, control_offset + (off_t)offsetof(struct vppu_pid_state, window_start_ns),
                       &window_start, sizeof(window_start))
               && read_at(path, control_offset + (off_t)offsetof(struct vppu_pid_state, allow_ns),
                          &allow, sizeof(allow))
               && read_at(path,
                          (off_t)offsetof(struct vppu_region, devices)
                              + (off_t)offsetof(struct vppu_device_usage, sm_util_percent),
                          &util, sizeof(util));

    struct vppu_pid_gains gains;
    struct vppu_pid_state expect;
    memset(&expect, 0, sizeof(expect));
    vppu_quota_sm_gains(&gains);
    unsigned long long want_allow = vppu_pid_step(&expect, &gains, 25U, 100U, PERIOD_NS);

    check_that(got && window_start == CONTROL_WINDOW_MARK,
               "control: the window the loop is in is readable", "0x%llx",
               (unsigned long long)window_start);
    check_that(got && allow == want_allow,
               "control: the allowance the loop decided is readable", "%lluns, want %lluns",
               (unsigned long long)allow, want_allow);
    check_that(got && util == 100U, "control: the measured utilisation is readable", "%u%%",
               util);
}

static int child_expect_refusal(void)
{
    /* 拒绝就必须是拒绝，而不是在不可读区域旁边出现一个新的区域。 */
    return vppu_ledger_lock(0) ? 1 : 0;
}

static void test_region_refusals(void)
{
    const char *path = region_path(2);

    /* 此版本构建中未知晓由布局所标记的区域。拒绝该区域才能避免下一个字段在旧读取器中被无声地错误解析并
     * 加入合约中。 */
    struct vppu_region seed;
    memset(&seed, 0, sizeof(seed));
    memcpy(seed.magic, VPPU_REGION_MAGIC, sizeof(seed.magic));
    seed.layout_version = VPPU_REGION_VERSION + 99U;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    bool wrote = (fd >= 0 && write(fd, &seed, sizeof(seed)) == (ssize_t)sizeof(seed));
    if (fd >= 0) {
        close(fd);
    }
    check_that(wrote && run_child(child_expect_refusal, path) == 0,
               "region: an unknown layout version is refused",
               "version %u refused rather than misparsed", seed.layout_version);

    /* 那个存在但不属于我们的文件永远不会被覆盖——而且由于它的长度比该区域短，也永远不会被调整大小。其大
     * 小是“sharp half”：这种拒绝行为通常发生在 ftruncate 将某人的文件大小增加到 36960 字节之后，无论当
     * 时声明了什么修改内容，这都视为一种修改行为。 */
    const char *foreign = region_path(3);
    fd = open(foreign, O_RDWR | O_CREAT | O_TRUNC, 0600);
    wrote = (fd >= 0 && write(fd, "not a ledger", 12) == 12);
    if (fd >= 0) {
        close(fd);
    }
    bool refused = (wrote && run_child(child_expect_refusal, foreign) == 0);
    struct stat after;
    bool sized = (stat(foreign, &after) == 0);
    check_that(refused, "region: a foreign file is refused", "refused rather than overwritten");
    check_that(refused && sized && after.st_size == 12,
               "region: a foreign file is not even resized", "%llu bytes, was 12",
               (unsigned long long)(sized ? after.st_size : -1));
}

/* --------------------------------------------------------------------------------------- 两个进程，
 * 一个配额 ------------------------------------------------------------------------------------- */

/* contend — 获取卡的锁定，决定是否违反配额，等待，然后进行记账。
 *
 * 等待过程就是测试。它扩大了决策与记账之间的时间窗口，使得调度器无法忽视这一差异。因此，以进程为单
 * 位的账本，而非以容器为单位的账本，或者分配完成前就释放的锁，都能让调用者获得完整的配额。这正是锁
 * 存在的目的——在检查之后再进行分配，从而解决该锁所引发的竞争问题。 */
static int contend(void)
{
    if (!vppu_ledger_lock(0)) {
        return 2;
    }
    unsigned long long used = vppu_ledger_used(0);
    unsigned long long remaining = (used >= QUOTA_BYTES) ? 0ULL : QUOTA_BYTES - used;

    usleep(150000);

    int verdict = 1;
    if (remaining >= QUOTA_BYTES && vppu_ledger_charge(0, QUOTA_BYTES)) {
        verdict = 0;
    }
    vppu_ledger_unlock(0);
    return verdict;
}

static void test_one_quota_two_processes(void)
{
    const char *path = region_path(4);
    pid_t pids[2];

    for (int i = 0; i < 2; i++) {
        pids[i] = fork();
        if (pids[i] == 0) {
            setenv(VPPU_ENV_LEDGER_PATH, path, 1);
            _exit(contend());
        }
    }

    int granted = 0;
    int broken = 0;
    for (int i = 0; i < 2; i++) {
        int status = 0;
        if (pids[i] < 0 || waitpid(pids[i], &status, 0) != pids[i] || !WIFEXITED(status)) {
            broken++;
            continue;
        }
        if (WEXITSTATUS(status) == 0) {
            granted++;
        } else if (WEXITSTATUS(status) != 1) {
            broken++;
        }
    }

    check_that(broken == 0 && granted == 1, "quota: two processes cannot both take it whole",
               "%d of 2 granted the whole %lluMiB (%d could not run the test)", granted,
               QUOTA_MIB, broken);

    uint64_t used = 0;
    off_t used_offset = (off_t)offsetof(struct vppu_region, devices)
                        + (off_t)offsetof(struct vppu_device_usage, memory_used_bytes);
    bool read_used = read_at(path, used_offset, &used, sizeof(used));
    check_that(read_used && used == QUOTA_BYTES, "quota: the ledger holds one charge, not two",
               "%llu bytes accounted, want %llu", (unsigned long long)used,
               (unsigned long long)QUOTA_BYTES);
}

/* --------------------------------------------------------------------------------------- 一个已终
 * 止进程留下的记账记录 */

static int child_charge_and_die(void)
{
    if (!vppu_ledger_lock(0)) {
        return 1;
    }
    bool charged = vppu_ledger_charge(0, QUOTA_BYTES);
    vppu_ledger_unlock(0);
    return charged ? 0 : 2;
}

static int child_reclaim(void)
{
    if (!vppu_ledger_lock(0)) {
        return 1;
    }
    if (vppu_ledger_used(0) != QUOTA_BYTES) {
        vppu_ledger_unlock(0);
        return 2;
    }
    unsigned long long recovered = vppu_ledger_reclaim(0);
    unsigned long long left = vppu_ledger_used(0);
    vppu_ledger_unlock(0);

    if (recovered != QUOTA_BYTES) {
        return 3;
    }
    return (left == 0ULL) ? 0 : 4;
}

static void test_reclaim_after_death(void)
{
    const char *path = region_path(5);

    check_that(run_child(child_charge_and_die, path) == 0,
               "reclaim: a process charges the whole quota and exits",
               "%lluMiB charged, never refunded", QUOTA_MIB);

    /* 如果没有这个拦截，只要账本文件存在，这笔记账就会持续占用容器的配额，而这一配额实际上来自
     * HAMi-core 中过期的缓存，这些缓存是通过其他途径进入的。 */
    check_that(run_child(child_reclaim, path) == 0,
               "reclaim: a dead process's charge is recovered",
               "the next process reclaims %lluMiB and sees an empty card", QUOTA_MIB);
}

/* --------------------------------------------------------------------------------------- 在锁被持
 * 有时调用 fork() */

static int child_after_fork(void)
{
    /* hang是此测试旨在捕获的两种失败类型之一，因此应将其绑定起来，而不是让它使测试套件停滞：继承了自旋
     * 锁标志的子进程会等待本进程中不存在的持有者。 */
    alarm(10);

    if (!vppu_ledger_lock(0)) {
        return 2;
    }
    unsigned long long used = vppu_ledger_used(0);
    vppu_ledger_unlock(0);

    /* 父进程在释放之前会完成整个配额的操作，因此这里的空卡意味着该操作在父进程仍持有锁的情况下被执行了
     * ——这种继承锁被视为允许再次进入的锁。 */
    return (used == QUOTA_BYTES) ? 0 : 1;
}

/* 特意保留的执行逻辑：这是唯一的测试，其中 PARENT 会映射一个区域，且该映射会按进程进行锁定。 */
static void test_fork_while_locked(void)
{
    setenv(VPPU_ENV_LEDGER_PATH, region_path(6), 1);

    if (!vppu_ledger_lock(0)) {
        check_that(false, "fork: a child does not inherit the parent's lock",
                   "the parent could not take a card lock to fork under");
        return;
    }

    pid_t pid = fork();
    if (pid == 0) {
        _exit(child_after_fork());
    }

    usleep(200000);
    bool charged = vppu_ledger_charge(0, QUOTA_BYTES);
    vppu_ledger_unlock(0);

    int status = 0;
    bool exited = (pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status));
    int code = exited ? WEXITSTATUS(status) : -1;

    check_that(charged && exited && code == 0,
               "fork: a child does not inherit the parent's lock",
               "child verdict %d (0 = waited for the release and then saw the quota spent, "
               "1 = let through while the parent still held it, 2 = could not lock, "
               "-1 = hung or was killed)",
               code);
}

int main(void)
{
    if (mkdtemp(tmpdir) == NULL) {
        printf("FAIL | setup | cannot create a temporary directory\n");
        printf("FAILS=1\n");
        return 1;
    }

    /* 二级级别，因此失败的测试会在对应行旁显示该库自身的诊断信息。 */
    setenv(VPPU_ENV_LOG_LEVEL, "2", 1);

    test_quota_parse();
    test_quota_knob();
    test_quota_env();
    test_log_level();
    test_quota_validate();
    test_pid_feed_forward();
    test_pid_clamps();
    test_pid_convergence();
    test_alloc_map();
    test_region_layout();
    test_control_state_published();
    test_region_refusals();
    test_one_quota_two_processes();
    test_reclaim_after_death();
    test_fork_while_locked();

    printf("FAILS=%d\n", fails);
    return (fails == 0) ? 0 : 1;
}

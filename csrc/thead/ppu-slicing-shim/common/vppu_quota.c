#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "vppu_quota.h"

#define VPPU_MIB (1024ULL * 1024ULL)

/* 足够长，以容纳最长的前缀以及上述限制所允许的最宽的索引。内存前缀是两者中较长的那个，因此一个限制
 * 同时适用于两者。 */
#define VPPU_ENV_NAME_MAX (sizeof(VPPU_ENV_MEMORY_LIMIT_PREFIX) + 16)

/* 加载时间报告中卡的名称是“card <i>”，或者是未标注数字的句子所代表的含义。 */
#define VPPU_LABEL_MAX 40

static bool config_usable = true;

unsigned long long vppu_quota_parse(const char *value, unsigned long long unit)
{
    if (value == NULL || *value == '\0' || unit == 0ULL) {
        return 0ULL;
    }

    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);

    /* 被拒绝的尾部垃圾数据不会被容忍：“4096MiB”的解析为 4096会让调用者未选择的单位看起来像是可以正常工
     * 作的。 */
    if (end == value || *end != '\0') {
        return 0ULL;
    }
    if (parsed == 0ULL || parsed > ULLONG_MAX / unit) {
        return 0ULL;
    }
    return parsed * unit;
}

unsigned int vppu_quota_knob(const char *value, unsigned int low, unsigned int high,
                             unsigned int fallback)
{
    if (value == NULL || *value == '\0') {
        return fallback;
    }

    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);

    if (end == value || *end != '\0' || parsed < low || parsed > high) {
        return fallback;
    }
    return (unsigned int)parsed;
}

/* env_name — 该变量包含指定类型的某一张卡的数据，若为超出范围的索引则设为NULL。 */
static const char *env_name(const char *prefix, int device, char *buf, size_t len)
{
    if (device < 0 || device >= VPPU_MAX_DEVICES) {
        return NULL;
    }
    if (snprintf(buf, len, "%s%d", prefix, device) < 0) {
        return NULL;
    }
    return buf;
}

/* quota_source — 哪个变量决定了某张卡的大小以及它所持有的内容。
 *
 * 该模块是优先级生效的地方，针对两个维度而言，因此执行路径所采用的图形以及加载时间报告所打印的名称，
 * 永远不会冲突。buf 在作为答案时携带索引名称，因此它必须比返回的指针更持久。 */
static const char *quota_source(const char *prefix, const char *plain, int device, char *buf,
                                size_t len, const char **value_out)
{
    const char *name = env_name(prefix, device, buf, len);
    const char *value = (name != NULL) ? getenv(name) : NULL;

    /* 被设置为有效状态会阻止搜索操作，无法使用则必须拒绝其卡，而不是跳转到容器级的值。 */
    if (value != NULL && *value != '\0') {
        *value_out = value;
        return name;
    }
    *value_out = getenv(plain);
    return plain;
}

unsigned long long vppu_quota_memory_bytes(int device)
{
    char buf[VPPU_ENV_NAME_MAX];
    const char *value = NULL;

    if (device < 0 || device >= VPPU_MAX_DEVICES) {
        return 0ULL;
    }
    quota_source(VPPU_ENV_MEMORY_LIMIT_PREFIX, VPPU_ENV_MEMORY_LIMIT, device, buf, sizeof(buf),
                 &value);
    return vppu_quota_parse(value, VPPU_MIB);
}

unsigned int vppu_quota_sm_percent(int device)
{
    char buf[VPPU_ENV_NAME_MAX];
    const char *value = NULL;

    if (device < 0 || device >= VPPU_MAX_DEVICES) {
        return 0U;
    }
    quota_source(VPPU_ENV_SM_LIMIT_PREFIX, VPPU_ENV_SM_LIMIT, device, buf, sizeof(buf), &value);

    unsigned long long percent = vppu_quota_parse(value, 1ULL);

    /* 超过 100 的数值不是上限，因此被视为未设置状态，由 vppu_quota_validate() 函数进行报告，而不会被限
     * 制为运营方未要求的任何内容。 */
    if (percent > 100ULL) {
        return 0U;
    }
    return (unsigned int)percent;
}

unsigned int vppu_quota_sm_period_ms(void)
{
    return vppu_quota_knob(getenv(VPPU_ENV_SM_PERIOD_MS), VPPU_SM_PERIOD_MS_MIN,
                           VPPU_SM_PERIOD_MS_MAX, VPPU_SM_PERIOD_MS_DEFAULT);
}

unsigned int vppu_quota_sm_step_ms(void)
{
    return vppu_quota_knob(getenv(VPPU_ENV_SM_STEP_MS), VPPU_SM_STEP_MS_MIN, VPPU_SM_STEP_MS_MAX,
                           VPPU_SM_STEP_MS_DEFAULT);
}

void vppu_quota_sm_gains(struct vppu_pid_gains *gains)
{
    gains->kp = (int32_t)vppu_quota_knob(getenv(VPPU_ENV_SM_KP), 0U, VPPU_SM_GAIN_MAX,
                                         VPPU_SM_KP_DEFAULT);
    gains->ki = (int32_t)vppu_quota_knob(getenv(VPPU_ENV_SM_KI), 0U, VPPU_SM_GAIN_MAX,
                                         VPPU_SM_KI_DEFAULT);
    gains->kd = (int32_t)vppu_quota_knob(getenv(VPPU_ENV_SM_KD), 0U, VPPU_SM_GAIN_MAX,
                                         VPPU_SM_KD_DEFAULT);
}

unsigned int vppu_quota_graph_weight(void)
{
    return vppu_quota_knob(getenv(VPPU_ENV_SM_GRAPH_WEIGHT), 1U, VPPU_SM_GRAPH_WEIGHT_MAX,
                           VPPU_SM_GRAPH_WEIGHT_DEFAULT);
}

/* report_compute — 命名将应用于单张卡及其背后的调优的计算数值，并回答它是否属于限制。
 *
 * 每个卡都要求提供环境名称，因为命名卡就是容器表明其被分配的方式。没有可用计算资源的卡会导致配置无
 * 法使用：没有计算限制运行的切片容器正是 flexai 中缺失配置的结果，而本设计明确禁止这种情况——此外，
 * 分配器自身的辅助工具会将请求设置为 100%，因此被省略的变量无法被解读为“此容器未被切片”。
 *
 * label是报告中对该卡的命名方式：它的索引，或者代表未索引数字所涵盖的每类卡的句子，因为具体数量并
 * 不在环境中被标注。 */
static bool report_compute(int device, const char *label)
{
    char buf[VPPU_ENV_NAME_MAX];
    const char *value = NULL;
    const char *name = quota_source(VPPU_ENV_SM_LIMIT_PREFIX, VPPU_ENV_SM_LIMIT, device, buf,
                                    sizeof(buf), &value);
    unsigned int sm = vppu_quota_sm_percent(device);

    if (value == NULL || *value == '\0') {
        vppu_log(VPPU_LOG_DENY,
                 "no %s%d and no %s configured — this library is only preloaded into sliced "
                 "containers, so every allocation and every launch on %s will be denied\n",
                 VPPU_ENV_SM_LIMIT_PREFIX, device, VPPU_ENV_SM_LIMIT, label);
        return false;
    }
    if (sm == 0U) {
        vppu_log(VPPU_LOG_DENY, "unusable %s=%s\n", name, value);
        return false;
    }

    /* 图中标注的调校力度表明：这些增益并不适用于此硬件，因此诊断受限工作负载的人首先需要知道哪些功能实
     * 际上被使用了。就每张卡而言，因为每张卡都有自己的窗口和自己的限制——对于容器来说，无法用一行文字来
     * 描述。 */
    if (sm >= 100U) {
        vppu_log(VPPU_LOG_DEBUG, "%s=%u — %s's compute is uncapped, launches are counted only\n",
                 name, sm, label);
    } else {
        struct vppu_pid_gains gains;

        vppu_quota_sm_gains(&gains);
        vppu_log(VPPU_LOG_DEBUG,
                 "%s=%u on %s, window %ums, step %ums, gains kp=%d ki=%d kd=%d, graph "
                 "weight %u\n",
                 name, sm, label, vppu_quota_sm_period_ms(), vppu_quota_sm_step_ms(), gains.kp,
                 gains.ki, gains.kd, vppu_quota_graph_weight());
    }
    return true;
}

/* indexed_value — 即某张卡自身维度变量所存储的值，若未设置则为 NULL。区分“未设置”与“设为不可用的数
 * 值”才是整个优先级规则，因此在此处这两种答案也分开处理。 */
static const char *indexed_value(const char *prefix, int device, char *buf, size_t len)
{
    const char *name = env_name(prefix, device, buf, len);
    const char *value = (name != NULL) ? getenv(name) : NULL;

    return (value != NULL && *value != '\0') ? value : NULL;
}

/* fallback_stand_in 表示使用无下标变量取得算力额度的一张卡。用它验证回退值，避免误测某张卡自己的覆
 * 盖配置。所有卡都单独配置时取 -1，此时无下标的算力值不起作用。 */
static int fallback_stand_in(void)
{
    for (int i = 0; i < VPPU_MAX_DEVICES; i++) {
        char buf[VPPU_ENV_NAME_MAX];

        if (indexed_value(VPPU_ENV_SM_LIMIT_PREFIX, i, buf, sizeof(buf)) == NULL) {
            return i;
        }
    }
    return -1;
}

void vppu_quota_validate(void)
{
    static bool done;

    if (done) {
        return;
    }
    done = true;

    int configured = 0;
    int unusable = 0;

    /* 当某个维度通过索引来标识时，该卡就会被处理，因为存在的变量表明容器被分配给了该卡。这两个数值随后
     * 会被一起验证：如果卡解决了内存问题但未解决计算问题，则会导致未限制的计算运行，因为启动路径将未解
     * 决的限制视为“无需等待的内容”。 */
    for (int i = 0; i < VPPU_MAX_DEVICES; i++) {
        char mem_buf[VPPU_ENV_NAME_MAX];
        char sm_buf[VPPU_ENV_NAME_MAX];
        char label[VPPU_LABEL_MAX];
        const char *memory = indexed_value(VPPU_ENV_MEMORY_LIMIT_PREFIX, i, mem_buf,
                                           sizeof(mem_buf));

        if (memory == NULL && indexed_value(VPPU_ENV_SM_LIMIT_PREFIX, i, sm_buf, sizeof(sm_buf))
                                  == NULL) {
            continue;
        }
        if (memory != NULL && vppu_quota_parse(memory, VPPU_MIB) == 0ULL) {
            /* 必须命名为“设置但无法使用”。执行路径将 0 视为“未配置”，因此拼写错误与从未被使用的卡无法区分。 */
            vppu_log(VPPU_LOG_DENY, "unusable %s%d=%s\n", VPPU_ENV_MEMORY_LIMIT_PREFIX, i, memory);
            unusable++;
            continue;
        }
        if (memory != NULL) {
            configured++;
        }
        snprintf(label, sizeof(label), "card %d", i);
        if (!report_compute(i, label)) {
            unusable++;
        }
    }

    /* 然后是未索引的内存数量，该数量决定了每张不携带自身内存的卡。这些卡的数量并未在环境中被列出——此处
     * 没有任何内容说明容器中的卡数量——因此该数值仅被验证一次，代表所有卡的数量，同时还需与计算数量相配
     * 合。 */
    const char *shared = getenv(VPPU_ENV_MEMORY_LIMIT);
    if (shared != NULL && *shared != '\0') {
        if (vppu_quota_parse(shared, VPPU_MIB) == 0ULL) {
            vppu_log(VPPU_LOG_DENY, "unusable %s=%s\n", VPPU_ENV_MEMORY_LIMIT, shared);
            unusable++;
        } else {
            int stand_in = fallback_stand_in();

            configured++;
            if (stand_in >= 0
                && !report_compute(stand_in, "every card with no figure of its own")) {
                unusable++;
            }
        }
    }

    if (configured == 0) {
        vppu_log(VPPU_LOG_DENY,
                 "no %s<i> and no %s configured — this library is only preloaded into sliced "
                 "containers, so every allocation will be denied\n",
                 VPPU_ENV_MEMORY_LIMIT_PREFIX, VPPU_ENV_MEMORY_LIMIT);
    }

    config_usable = (configured > 0 && unusable == 0);
}

bool vppu_quota_usable(void)
{
    return config_usable;
}

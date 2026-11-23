#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "vrocm_log.h"
#include "vrocm_quota.h"

/* env_indexed — 索引形式下单个卡的值，若变量未设置则为NULL。
 *
 * NULL表示“未设置”，这就是优先级所指定的含义；空字符串是一个调用者必须拒绝的值，而非其可以忽略的变
 * 量。 */
static const char *env_indexed(int device)
{
    char name[sizeof(VROCM_ENV_MEMORY_LIMIT_PREFIX) + 16];

    snprintf(name, sizeof(name), VROCM_ENV_MEMORY_LIMIT_PREFIX "%d", device);
    return getenv(name);
}

VROCM_INTERNAL unsigned long long vrocm_quota_parse(const char *value, unsigned long long unit)
{
    char *end = NULL;
    unsigned long long parsed;

    if (value == NULL || *value == '\0' || unit == 0) {
        return 0;
    }
    /* 一个数字开头的字符串，在 strtoull 看到之前，其他内容都会被拒绝。
     *
     * 该规则采用这种写法而非测试形式，因为 strtoull 会跳过前导空白字符，然后接受符号：“-10”因此不会在
     * 第一个字符上触发测试，而是被转换为巨大的正数。下面的溢出测试确实能检测到这种情况——被回绕的负数数
     * 值远高于 ULLONG_MAX/MiB——但仅因为单位较大，且配额并非依赖与整个输入类别无关的检查项的地方。直接
     * 拒绝也符合对尾部数据的处理方式：末尾的垃圾数据会被拒绝，因此前端的垃圾数据也应被拒绝。 */
    if (*value < '0' || *value > '9') {
        return 0;
    }

    errno = 0;
    parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == NULL || *end != '\0' || parsed == 0) {
        return 0;
    }
    if (parsed > ULLONG_MAX / unit) {
        return 0;
    }
    return parsed * unit;
}

VROCM_INTERNAL unsigned long long vrocm_quota_memory_bytes(int device)
{
    const char *value;

    if (device < 0 || device >= VROCM_MAX_DEVICES) {
        return 0;
    }

    value = env_indexed(device);
    if (value == NULL) {
        value = getenv(VROCM_ENV_MEMORY_LIMIT);
    }
    return vrocm_quota_parse(value, VROCM_MEMORY_LIMIT_UNIT);
}

VROCM_INTERNAL const char *vrocm_quota_ledger_path(void)
{
    const char *path = getenv(VROCM_ENV_LEDGER_PATH);

    /* 空值也会回退，而不是被当作路径被遵循：VROCM_LEDGER_PATH=就是当需要替换该变量的值时，该变量如何出
     * 现；而打开“”会失败，因为没人能读懂其中的内容。 */
    return (path != NULL && *path != '\0') ? path : VROCM_LEDGER_DEFAULT_PATH;
}

static bool usable;

/* any_figure_named — 是否这两种内存名称形式会记录任何内容。
 *
 * “没有配额”和“有配额时本次构建无法使用”是两种不同的状态，只有后者在默认级别下值得关注。该问题的解
 * 答是通过询问而非记忆，因为每次进程在加载时仅会被读取一次，绝不会在分配过程中被处理。 */
static bool any_figure_named(void)
{
    int device;

    if (getenv(VROCM_ENV_MEMORY_LIMIT) != NULL) {
        return true;
    }
    for (device = 0; device < VROCM_MAX_DEVICES; device++) {
        if (env_indexed(device) != NULL) {
            return true;
        }
    }
    return false;
}

/* 账本会记录所有索引形式以及非索引形式的数据，这样容器就能知道它实际收到的每一个数据，而不仅仅是这
 * 个处理流程首先查询的那个数据。没有任何内容的卡不会被记录：只要设置了非索引形式的数据，每个卡都算
 * 是容器收到的数据，而列出64个这样的卡则会掩盖重要的信息。 */
VROCM_INTERNAL void vrocm_quota_validate(void)
{
    const char *path = vrocm_quota_ledger_path();
    const char *shared = getenv(VROCM_ENV_MEMORY_LIMIT);
    bool any_figure = false;
    bool any_broken = false;
    int device;

    usable = false;

    /* 没有明确的名称，因此这里也没有需要被检查的内容，在默认级别下这并不属于值得关注的错误。这个库会被
     * 加载到容器中的每个进程中——/etc/ld.so.preload 无法区分工作负载与启动脚本中的 sed 操作——因此，如果
     * 容器仅包含该库且没有配置，每个进程会仅打印一次内容，操作员无法从中获得任何操作依据。而一个有名称
     * 但无法使用的工具则属于相反情况，会持续发出噪音。
     *
     * 保持沉默也不会有任何损失：那些实际请求内存但被拒绝的进程，会在 vrocm_ledger_admit 中输出一行自己
     * 的内容。 */
    if (!any_figure_named()) {
        vrocm_log(VROCM_LOG_DEBUG, "no %s figure in either form; every allocation will be refused\n",
                  VROCM_ENV_MEMORY_LIMIT);
        return;
    }

    /* 当前区域是否有效，以及是否正是分配器所选择的区域。默认设置是为了手动运行树形结构时的便利；在
     * .sliced 路径上该变量始终被设置，因此看到默认值意味着插入操作尚未执行。 */
    if (getenv(VROCM_ENV_LEDGER_PATH) == NULL) {
        vrocm_log(VROCM_LOG_DEBUG, "%s is unset; using the default region %s\n",
                  VROCM_ENV_LEDGER_PATH, VROCM_LEDGER_DEFAULT_PATH);
    }

    if (shared != NULL) {
        unsigned long long bytes = vrocm_quota_parse(shared, VROCM_MEMORY_LIMIT_UNIT);

        if (bytes == 0) {
            vrocm_log(VROCM_LOG_DENY, "%s=\"%s\" is not a usable figure\n",
                      VROCM_ENV_MEMORY_LIMIT, shared);
            any_broken = true;
        } else {
            any_figure = true;
            vrocm_log(VROCM_LOG_DEBUG, "%s = %llu MiB (every card carrying no figure of its own)\n",
                      VROCM_ENV_MEMORY_LIMIT, bytes / VROCM_MEMORY_LIMIT_UNIT);
        }
    }

    for (device = 0; device < VROCM_MAX_DEVICES; device++) {
        const char *value = env_indexed(device);
        unsigned long long bytes;

        if (value == NULL) {
            continue;
        }
        bytes = vrocm_quota_parse(value, VROCM_MEMORY_LIMIT_UNIT);
        if (bytes == 0) {
            vrocm_log(VROCM_LOG_DENY, "%s%d=\"%s\" is not a usable figure; card %d is refused\n",
                      VROCM_ENV_MEMORY_LIMIT_PREFIX, device, value, device);
            any_broken = true;
            continue;
        }
        any_figure = true;
        vrocm_log(VROCM_LOG_DEBUG, "%s%d = %llu MiB\n", VROCM_ENV_MEMORY_LIMIT_PREFIX, device,
                  bytes / VROCM_MEMORY_LIMIT_UNIT);
    }

    if (!any_figure) {
        vrocm_log(VROCM_LOG_DENY, "no usable %s figure in either form; "
                                  "every allocation will be refused\n",
                  VROCM_ENV_MEMORY_LIMIT);
        return;
    }

    /* 只要包含至少一个可使用的数值的容器就是可用的，即使另一张卡的数值损坏了：该卡在
     * vrocm_quota_memory_bytes() 函数中返回0，因此单独使用时会被拒绝，这种限制比因为一个错误拒绝整个容
     * 器要严格。 */
    usable = true;
    vrocm_log(VROCM_LOG_DEBUG, "ledger %s%s\n", path,
              any_broken ? " (some cards carry an unusable figure and are refused)" : "");
}

VROCM_INTERNAL bool vrocm_quota_usable(void)
{
    return usable;
}

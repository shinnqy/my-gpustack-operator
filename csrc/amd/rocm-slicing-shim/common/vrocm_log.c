#include <stdlib.h>

#include "vrocm_log.h"

/* 超出范围和格式错误的数值会回退到默认值，而不会使容器无法使用：详细度属于诊断信息，而非配额限制，
 * 错误输入的日志级别导致无法进行任何分配，后果将比正常记录时错误输入的日志级别更为严重。 */
VROCM_INTERNAL int vrocm_log_level(void)
{
    static int level = -1;

    const char *value;
    char *end = NULL;
    long parsed;
    int answer = __atomic_load_n(&level, __ATOMIC_RELAXED);

    if (answer >= 0) {
        return answer;
    }

    /* 计算后存储在本地并仅发布一次，这相当于两项保障而非一项。
     *
     * 没有任何因素能吸引单个读取方，因此两个线程可以同时到达这里并各自进行存储。两个简单的存储对象指向
     * 同一个对象，无论它们存储什么内容都属于数据竞争；这两个线程读取相同的环境并得出相同的答案，但这并
     * 不能定义其正确性。宽松性正是整个要求的核心：没有其他内存对这个值进行约束。
     *
     * 并且默认的配置不再经过锁机制，在替换过程中保持不变。直接写入 level 中后，它就会作为固定的数值存
     * 在，持续时间为 getenv 和 strtol 的执行时间；在该时间段内读取数据的线程会将其视为已配置的数值。 */
    answer = VROCM_LOG_DENY;
    value = getenv(VROCM_ENV_LOG_LEVEL);
    if (value != NULL && *value != '\0') {
        parsed = strtol(value, &end, 10);
        if (end != NULL && *end == '\0' && parsed >= VROCM_LOG_QUIET &&
            parsed <= VROCM_LOG_DEBUG) {
            answer = (int)parsed;
        }
    }
    __atomic_store_n(&level, answer, __ATOMIC_RELAXED);
    return answer;
}

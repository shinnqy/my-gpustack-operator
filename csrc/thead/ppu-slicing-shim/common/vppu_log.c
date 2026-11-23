/* vppu_log.c — 详细程度级别，仅读取一次。
 *
 * 以懒散的方式读取，而非从构造函数中读取：此库已预加载，因此其构造函数按供应商的顺序执行顺序由我们
 * 无法决定，第一次使用时缓存的级别无论哪种情况都会正确。 */
#include <stdlib.h>

#include "vppu.h"

int vppu_log_level(void)
{
    static int level = -1;

    int answer = __atomic_load_n(&level, __ATOMIC_RELAXED);

    if (answer >= 0) {
        return answer;
    }

    /* 计算后存储在本地并仅发布一次，这相当于两项保障而非一项。
     *
     * 没有任何因素能吸引单个读取方，因此该拦截库的两个线程可以同时到达此处并进行存储。两个简单的存储操
     * 作针对同一个对象来说属于数据竞争，无论它们存储什么内容——这两个线程读取相同的环境数据并得出相同的
     * 结论，但这并不能被定义为一个明确的概念。宽松性正是整个要求的核心：没有其他内存对这个值进行约束。
     *
     * 并且默认的配置不再经过锁机制，在替换过程中保持不变。直接写入 level 中后，它就会作为固定的数值存
     * 在，持续时间为 getenv 和 strtol 的执行时间；在该时间段内读取数据的线程会将其视为已配置的数值。 */
    answer = VPPU_LOG_DENY;
    {
        const char *value = getenv(VPPU_ENV_LOG_LEVEL);
        char *end = NULL;

        if (value != NULL && *value != '\0') {
            long parsed = strtol(value, &end, 10);

            /* 无法解析的层级会保留默认设置，而不会停止库的运行。strtol() 对于“abc”会返回 0，而 0 也是那种会隐
             * 藏拒绝情况的层级——因此如果没有这个检查，操作选项中的输入错误会导致失败性诊断信息被忽略，而拒绝所
             * 有分配的容器也不会说明原因。末尾的垃圾数据会被拒绝，原因是 vppu_quota_parse() 会拒绝它：“2x”不是
             * 任何人选择的层级，负数数字也不属于此类。 */
            if (end != value && *end == '\0' && parsed >= 0) {
                answer = (int)parsed;
            }
        }
    }
    __atomic_store_n(&level, answer, __ATOMIC_RELAXED);
    return answer;
}

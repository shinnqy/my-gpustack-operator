/* vrocm_log.h — 那个输出通道，以及控制其访问级别的层级。
 *
 * 这些级别是与验证案例相关的约定，而非优先要求。第1级是按拒绝次数计算的，而非按调用次数计算：它只
 * 包含一行内容，用于回答“为何我的分配被拒绝”，且这是默认设置，因为没有说明就超出配额的工作负载正是
 * 本库旨在防止的情况。第0级适用于需要完全安静的工作负载。第2级增加了负载标记和计数器记录功能，且案
 * 例会依据这些标记来确定行号，因此无法达到第2级的构建将使大多数行停止工作。
 *
 * 该 TAG 指的是库名称，而非项目名称，因为无论何种情况，它都能被识别出来，其输出内容会与 ROCm 自身
 * 的输出混杂在一起——CANN 和 ROCm 运行时都会无提示地将输出写入 stderr，因此简洁且独特的标识比品牌名
 * 称更为重要。 */
#ifndef VROCM_COMMON_VROCM_LOG_H
#define VROCM_COMMON_VROCM_LOG_H

#include <stdio.h>

#include "vrocm.h"

#define VROCM_TAG "[vrocm] "

#define VROCM_ENV_LOG_LEVEL "LIBVROCM_LOG_LEVEL"

#define VROCM_LOG_QUIET 0
#define VROCM_LOG_DENY 1
#define VROCM_LOG_DEBUG 2

/* vrocm_log_level — 当前级别，从环境中读取并保存下来。
 *
 * 由于这位于每次分配的拒绝路径上，因此采用锁定而非重新读取的方式；此外，在运行中的进程下发生变化的
 * 级别会导致案例的 grep 行为取决于其查看时间。 */
VROCM_INTERNAL int vrocm_log_level(void);

#define vrocm_log(lvl, ...)                             \
    do {                                                \
        if (vrocm_log_level() >= (lvl)) {               \
            fprintf(stderr, VROCM_TAG __VA_ARGS__);     \
        }                                               \
    } while (0)

#endif /* VROCM_COMMON_VROCM_LOG_H */

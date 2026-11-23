/* vppu.h — 这是 libvppu.so 所有部分共有的词汇表：日志通道、控制该通道的级别，以及防止供应商提供的
 * 索引变成数组偏移量的设备绑定。
 *
 * 这里没有任何内容可以指向 hg*、hggc* 或 hgml* 类型。正是这一规则使得该目录能够在没有 SDK 和没有设
 * 备的情况下进行编译和运行，这也使得账本中的运算以及配额解析测试能够正常进行——该库中的其他内容只能
 * 在使用厂商镜像中的 PPU 主机上进行测试。 */
#ifndef VPPU_COMMON_VPPU_H
#define VPPU_COMMON_VPPU_H

#include <stdio.h>

/* 隐藏的，未导出。此代码被链接到预加载的库中，该库的可见接口必须完全与供应商的入口点一致：任何进入
 * 全局命名空间的通用符号，都会被工作负载进行拦截；当库的两部分同时加载到同一个进程中时，它们会互相
 * 拦截对方的副本。情况 1 则假设不存在这种情况，而非依赖该属性。 */
#define VPPU_INTERNAL __attribute__((visibility("hidden")))

/* 受选择限制：索引来自设备句柄，因此错误的值绝不能成为数组偏移量。目前见过的容量最大的 PPU 主机可
 * 承载 16 张卡。
 *
 * 受共享区域的布局约定限制。卡锁区域的大小由这个常量决定，其字节偏移量则是两个进程进行锁定的位置，
 * 因此如果在某个构建中提高该值而在另一个构建中不提高，会导致它们锁定不同的字节，从而无法实现互斥。 */
#define VPPU_MAX_DEVICES 64

/* 该标签是库名称而非项目名称——HAMi-core标签与其项目（[HAMI-core Msg …]）——因为每个门案例都通过拦截
 * 此标签来决定其行，这些行与供应商自己的输出交错排列，其中简短且独特的元素更为重要。 */
#define VPPU_TAG "[vppu] "

#define VPPU_ENV_LOG_LEVEL "LIBHGGC_LOG_LEVEL"

/* 1是默认设置，因为“拒绝”这一行代码用于解答“我的分配为何被拒绝”的问题，这种情况很罕见——与HAMi-core
 * 的1级功能不同，后者是按通话进行的，因此GPUStack会将其关闭。2级功能包含负载标记和计数器记录，这些
 * 内容会被门控案例读取，从而被固定下来。 */
#define VPPU_LOG_DENY 1
#define VPPU_LOG_DEBUG 2

VPPU_INTERNAL int vppu_log_level(void);

#define vppu_log(lvl, ...)                          \
    do {                                            \
        if (vppu_log_level() >= (lvl)) {            \
            fprintf(stderr, VPPU_TAG __VA_ARGS__);  \
        }                                           \
    } while (0)

#endif /* VPPU_COMMON_VPPU_H */

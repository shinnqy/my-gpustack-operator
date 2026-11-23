/* hggc_quota.h — hggc 模块的内部协议：插入的条目列表、其背后的两项准入决策（内存和计算），以及每条
 * 条目的调用计数器。
 *
 * 为什么使用多个模块而不是一个文件？一旦统计所有后缀变体，驱动层的内存表面包含 35 个导出名称，其启
 * 动表面包含另外 16 个，所有都存储在一个共享对象中。对这些名称的包装函数只是机械地执行操作，而它们
 * 所引导的两个决策却并非如此，这些决策才是值得阅读的部分。将它们分开后，每个决策都位于一个小的文件
 * 中——内存相关的是 hggc_quota.c，计算相关的是 hggc_compute.c——这样机械的表面就能按名称逐一与 SDK
 * 的符号清单进行比对了。
 *
 * 此处没有任何内容被导出。每个声明都带有 VPPU_INTERNAL 标识，因此该模块自身的边界在此库加载的容器
 * 内部不会成为可交互的界面——案例 1 则表明不应依赖它。 */
#ifndef VPPU_HGGC_HGGC_QUOTA_H
#define VPPU_HGGC_HGGC_QUOTA_H

#include <stdbool.h>

#include "common/vppu.h"

/* 插入的条目，每个导出 ABI 名称对应一个。
 *
 * 该列表属于 SDK 的组成部分，而非某种选择：所有在内存路径上导出的名称 libhggc.so，以及包含的传统
 * v1 版本和 _ptsz 变体都在此列出。因为当工作负载达到未覆盖的边界时，会消耗内存，而此配额永远不会被
 * 看到。按包装函数所执行的操作顺序排列——记账、退款、报告、计数——因为这是读取方需要针对每个名称检查
 * 的唯一属性。 */
enum vppu_entry {
    /* 分配情况。在供应商查看请求之前，会针对卡的配额进行记账。 */
    VPPU_MEM_ALLOC = 0,
    VPPU_MEM_ALLOC_V1,
    VPPU_MEM_ALLOC_ASYNC,
    VPPU_MEM_ALLOC_ASYNC_PTSZ,
    VPPU_MEM_ALLOC_FROM_POOL_ASYNC,
    VPPU_MEM_ALLOC_FROM_POOL_ASYNC_PTSZ,
    VPPU_MEM_ALLOC_MANAGED,
    VPPU_MEM_ALLOC_PITCH,
    VPPU_MEM_ALLOC_PITCH_V1,
    VPPU_MEM_CREATE,

    /* 释放。资金会退还给进行记账的卡，从密钥映射表中获取。 */
    VPPU_MEM_FREE,
    VPPU_MEM_FREE_V1,
    VPPU_MEM_FREE_ASYNC,
    VPPU_MEM_FREE_ASYNC_PTSZ,
    VPPU_MEM_RELEASE,

    /* 查询内容。以配额规定的数字作为答复，而非卡的余额。 */
    VPPU_MEM_GET_INFO,
    VPPU_MEM_GET_INFO_V1,

    /* 主机内存。已统计但未进行记账：固定分配的主机页面并非设备 VRAM，因此对它们进行记账会拒绝那些对卡
     * 毫无影响的分配。 */
    VPPU_MEM_ALLOC_HOST,
    VPPU_MEM_ALLOC_HOST_V1,
    VPPU_MEM_FREE_HOST,

    /* 地址映射。已计数且未进行记账：VMM路径使用 hgMemCreate来分配物理内存，这些仅绑定已支付费用的句柄。 */
    VPPU_MEM_MAP,
    VPPU_MEM_MAP_ARRAY_ASYNC,
    VPPU_MEM_MAP_ARRAY_ASYNC_PTSZ,
    VPPU_MEM_UNMAP,

    /* Pools. 已计数且从未进行记账：一个池的内存被 hgMemAllocFromPoolAsync 占用，该函数会进行记账，随后
     * 通过修剪操作释放。这些拦截库的存在本质上是为了确保池路径进入 libhggc.so 是既定事实而非假设。 */
    VPPU_MEM_POOL_CREATE,
    VPPU_MEM_POOL_DESTROY,
    VPPU_MEM_POOL_EXPORT_POINTER,
    VPPU_MEM_POOL_EXPORT_TO_SHAREABLE_HANDLE,
    VPPU_MEM_POOL_GET_ACCESS,
    VPPU_MEM_POOL_GET_ATTRIBUTE,
    VPPU_MEM_POOL_IMPORT_FROM_SHAREABLE_HANDLE,
    VPPU_MEM_POOL_IMPORT_POINTER,
    VPPU_MEM_POOL_SET_ACCESS,
    VPPU_MEM_POOL_SET_ATTRIBUTE,
    VPPU_MEM_POOL_TRIM_TO,

    /* 入口点解析器。调用者请求分配条目时，会直接跳过该条目的拦截库，因此它们也会被包含在内。 */
    VPPU_GET_PROC_ADDRESS,
    VPPU_GET_PROC_ADDRESS_V1,
    VPPU_GET_EXPORT_TABLE,

    /* 启动过程。在供应商看到之前，会限制该卡的计算窗口：这就是计算配额被消耗的地方，而未被标记的启动条
     * 目则是一种无限制消耗配额的方式。在启动路径上导出的每个名为 libhggc.so 的模块，都是基于内存列表所
     * 说明的原因——覆盖度就是其定义，而悄然缺失的模块则完全类似于那些工作负载从未调用的模块。 */
    VPPU_LAUNCH_KERNEL,
    VPPU_LAUNCH_KERNEL_PTSZ,
    VPPU_LAUNCH_KERNEL_EX,
    VPPU_LAUNCH_KERNEL_EX_PTSZ,
    VPPU_LAUNCH_KERNEL_EX_AD,
    VPPU_LAUNCH_KERNEL_EX_AD_PTSZ,
    VPPU_LAUNCH_COOPERATIVE_KERNEL,
    VPPU_LAUNCH_COOPERATIVE_KERNEL_PTSZ,
    VPPU_LAUNCH_COOPERATIVE_KERNEL_MULTI_DEVICE,
    VPPU_LAUNCH,
    VPPU_LAUNCH_GRID,
    VPPU_LAUNCH_GRID_ASYNC,

    /* 图结构启动过程。像其他部分一样受到限制，还会根据窗口大小乘以一个可配置的倍数进行记账：这些运行会
     * 记录被纳入图结构的多少个内核。 */
    VPPU_GRAPH_LAUNCH,
    VPPU_GRAPH_LAUNCH_PTSZ,

    /* Host 拦截函数。被计数且不会被限制：这些函数在 CPU 上运行，因此限制它们的运行会延迟工作负载，同时
     * 也不会释放卡上的任何资源。 */
    VPPU_LAUNCH_HOST_FUNC,
    VPPU_LAUNCH_HOST_FUNC_PTSZ,

    VPPU_ENTRY_COUNT
};

/* vppu_hggc_name — 该条目导出的 ABI 名称，因为需要由 dlsym() 提供。 */
VPPU_INTERNAL const char *vppu_hggc_name(enum vppu_entry entry);

/* vppu_hggc_count — 记录下此条目被调用的事实。程序结束时的转储文件通过计数来判断“调用是否到达
 * libhggc.so”，而非通过从链接关系推断得出。 */
VPPU_INTERNAL void vppu_hggc_count(enum vppu_entry entry);

/* vppu_hggc_next — 供应商对此条记录的定义，已解析并缓存。
 *
 * NULL表示在搜索顺序中libhggc.so并不位于此库之后。每个包装器都必须将此情况视为失败，而非无操作的静
 * 默状态：在无需调用任何函数的情况下返回成功，会让调用者得到未定义的输出参数。 */
VPPU_INTERNAL void *vppu_hggc_next(enum vppu_entry entry);

/* vppu_hggc_self — 这是该拦截库对此条目的定义，已一次性解析并缓存起来。这些解析器所输出的内容，可
 * 替代供应商的地址使用。 */
VPPU_INTERNAL void *vppu_hggc_self(enum vppu_entry entry);

/* vppu_hggc_admit — 决定对某个分配进行分配，并基于该卡的配额进行限制；若成功，则使该卡被锁定，同时
 * 保留已记录的字节数据。调用方必须随后调用 vppu_hggc_commit()、vppu_hggc_commit_sized() 或
 * vppu_hggc_rollback() 中的恰好一个。
 *
 * 在每种失败模式下均采用失败关闭机制，而“false”则始终表示调用方必须拒绝：此拦截库仅预加载到正在被
 * 分割的容器中，因此无法解决的设备、无法使用的配置、没有数据的卡以及无法访问的账本都只是配置错误，
 * 而非允许分配通过的理由。 */
VPPU_INTERNAL bool vppu_hggc_admit(enum vppu_entry entry, unsigned long long bytes,
                                   int *device_out);

/* vppu_hggc_admit_on — 对应 vppu_hggc_admit()，用于一种入口点，该入口点指定其自身卡，而非继承调用
 * 线程的上下文。VMM路径包含 prop->location.id，而池式分配属于创建该池的卡，两者均不必为当前上下文
 * ：将这些内容计入上下文会消耗一张卡的配额用于另一张卡的内存，而只包含一张卡的容器无法察觉此情况，
 * 包含两张卡的容器则会被破坏。
 *
 * device 值低于 0 表示“未指定卡号”，因此调用者可以自行选择使用其发现的内容，由该值来决定结果；答案
 * 取决于上下文，与之前相同。在布局边界之上命名的卡号将被拒绝，不会重新添加到上下文中——该卡号来自调
 * 用者自己的结构体，而错误在于向请求的非目标卡进行记账，这就是需要修复的问题。 */
VPPU_INTERNAL bool vppu_hggc_admit_on(int device, enum vppu_entry entry, unsigned long long bytes,
                                      int *device_out);

/* vppu_hggc_commit — 供应商的分配成功：记住这个句柄所欠的，释放卡。 */
VPPU_INTERNAL void vppu_hggc_commit(int device, unsigned long long key,
                                    unsigned long long bytes);

/* vppu_hggc_commit_sized — 执行一个分配操作，其实际大小在调用之后才能确定，需将账本与这一大小进行
 * 对齐。对于倾斜式条目，驱动程序会选择行步长：准入条件只能根据调用者的宽度来决定，而记账结果则必须
 * 遵循驱动程序实际返回的步长。 */
VPPU_INTERNAL void vppu_hggc_commit_sized(int device, unsigned long long key,
                                          unsigned long long admitted,
                                          unsigned long long actual);

/* vppu_hggc_rollback — 供应商的分配没有发生：将临时记账款退还，并释放卡。 */
VPPU_INTERNAL void vppu_hggc_rollback(int device, unsigned long long bytes);

/* vppu_hggc_refund — 将被释放的句柄所涉及的字节数据归还给产生记账的卡。从未被记录下来的关键数据无
 * 需退款，这并非错误：在此拦截库加载之前进行的分配正是这种情况。 */
VPPU_INTERNAL void vppu_hggc_refund(unsigned long long key);

/* vppu_hggc_device — 调用线程上下文所依赖的卡，或 -1。
 *
 * 两个配额都需要它，但都没有进行分配：没有分配条目，也没有启动条目会接受设备参数，因此判断某个操作
 * 属于哪张卡的唯一方法就是查询当前上下文。-1表示拒绝，绝不会是“无配额”。 */
VPPU_INTERNAL int vppu_hggc_device(void);

/* vppu_hggc_gate — 在调用线程的卡允许之前，先等待启动完成，并报告是否可以进行。
 *
 * 错误的答案意味着拒绝，调用者必须返回：无法使用的配置、没有当前上下文或无法访问的账本，这些都属于
 * 配置错误，而不是允许工作负载使用被限制的资源的原因。流量限制并非拒绝——需要等待分配到的资源才能启
 * 动的启动过程会在此调用中等待，然后返回 true，因为拒绝会破坏正常运行的工作负载，而不是使其变慢。
 *
 * graph标记该启动过程为捕获的账本，会按照窗口大小的可配置倍数进行记账，因为它运行了被捕获进该进程
 * 中的任意数量的核心单元。 */
VPPU_INTERNAL bool vppu_hggc_gate(enum vppu_entry entry, bool graph);

/* vppu_hggc_view — 调用线程的卡需要报告的内存查询数据。
 *
 * 如果结果为假，则意味着没有可替代的替代方案——没有当前设备，也没有为其配置的配额——调用者必须以原样
 * 形式传递供应商的答复。查询并不属于分配：拒绝说明存在多少内存会破坏那些从未超过配额的设备工作负载。 */
VPPU_INTERNAL bool vppu_hggc_view(unsigned long long *quota_out,
                                  unsigned long long *free_out);

#endif /* VPPU_HGGC_HGGC_QUOTA_H */

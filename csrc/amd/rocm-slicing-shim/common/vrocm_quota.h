/* vrocm_quota.h — 这是 common/: 中的配置部分：它列出了容器被赋予的内容、其账本所在位置，以及其中是
 * 否有任何内容可以实际使用。
 *
 * 仅有的内存资源就在这里。计算配额由平台在硬件层面通过 CU 掩码强制执行，此库根本不会读取该掩码，也
 * 无法覆盖它；使该掩码正确的，是属于分配器的衍生合同，而非预加载对象。因此没有需要解析的计算数据，
 * 也没有需要调优的控制循环——此库所要求的唯一维度是设备内存。
 *
 * 这里统一处理两个容易出错的边界条件。
 * - 配额在每次接入时从环境中读取，绝不会被创建该区域的进程写入到该区域中。如果某个区域缓存了第一个
 * 写入者的数据，意味着以较低配额重新启动的容器会默默保留较高的配额，唯一的解决方式就是删除一个无人
 * 知晓的文件；
 * 被设置但无法解析的图式，会导致所关联卡出现错误，绝不会出现“无限制”的情况，也绝不会跳转到上一层。
 * 将缺失或损坏的配置视为“无限制”，正是该设计明确禁止的一种情况，因为这将错误转化为整个卡的问题。 */
#ifndef VROCM_COMMON_VROCM_QUOTA_H
#define VROCM_COMMON_VROCM_QUOTA_H

#include <stdbool.h>

#include "vrocm.h"

/* 该库自己的命名空间，特意没有放在 HIP_、HSA_ 或 ROCR_ 下。后三个属于 ROCm，由其运行时主动解析，且
 * 仍在不断发展中；若在此处放置私有变量，将会引发未来的冲突。ROCm自己的变量在其他地方仍使用其真实名
 * 称，因为它们属于 ROCm，而非我们自己的。
 *
 * 两种形式都存在，而这一对关系使得它们成为一种合约，而非两种不同规范。
 * - 带有 LOCAL 设备索引后缀的 INDEXED 形式——这是一张卡在 ROCR_VISIBLE_DEVICES 过滤并重新排序列表后
 * 所拥有的索引；该索引空间与计算遮罩的 GPU_list 所使用的空间相同，因此按该列表中的位置对一切进行编
 * 号，就能使两者自动匹配。
 * - 那种UN-INDEXED形式，它决定了每一张没有自身索引数字的卡。
 *
 * 这些前缀是从普通名称中衍生出来的，而不是重复书写两次，因此维度的这两种形式不会偏离。 */
#define VROCM_ENV_MEMORY_LIMIT "VROCM_DEVICE_MEMORY_LIMIT"
#define VROCM_ENV_MEMORY_LIMIT_PREFIX VROCM_ENV_MEMORY_LIMIT "_"

/* 跨进程区域的路径。分配器会为每个容器提供独立的配置，因为该区域是根据卡在 ROCR_VISIBLE_DEVICES 中
 * 的位置来指定的，而该位置属于容器局部范围：两个共享同一区域的容器会将两张不同的物理卡放入它们都称
 * 为索引 0 的槽位中。在 .sliced 路径下，该变量始终被设置，指向设备插件创建并垃圾回收的 pod 工作目
 * 录下的一个目录。
 *
 * 默认设置适用于其他路径——手动运行此树结构，这也是验证方式——而 /dev/shm 的选择原因与 THEAD shim 的
 * 设定一致：每个容器中都存在 tmpfs 文件系统，由容器内的所有进程共享，当容器消失时该文件系统也会消
 * 失。
 *
 * 默认情况下它仅为容器私有，这就是问题所在。采用两种常见的配置方式会使 /dev/shm 成为共享资源，这两
 * 种方式都会破坏上述地址分配机制。
 * - hostIPC: true 表示属于主机，因此节点上的每个容器都会使用它；
 * - 挂载在 /dev/shm上的 emptyDir{medium: Memory} 由 Pod 中的容器共享，这是数据加载器认为默认的 64
 * MiB 大小太小的常见解决方案。无论哪种情况，两个容器都会将不同的物理显卡分配到同一个槽位中，显然这
 * 不符合配额设置的逻辑。默认设置只是便于手动运行单个容器时的便利措施；它不能替代分配器设置，且
 * .sliced 路径必须始终进行设置。 */
#define VROCM_ENV_LEDGER_PATH "VROCM_LEDGER_PATH"
#define VROCM_LEDGER_DEFAULT_PATH "/dev/shm/vrocm-ledger"

/* 分配器所输出的单位包含这两种形式。以 Mebibytes 而非 bytes 为单位，因为请求 API 的
 * .sliced.memory-* 维度就是以该单位来命名的，而分配器中的单位转换只可能出现一个错误，而不是两个。 */
#define VROCM_MEMORY_LIMIT_UNIT (1024ULL * 1024ULL)

/* vrocm_quota_parse — 一个按 unit 缩放的正数值，或对于“未设置或不可使用”则为 0。
 *
 * 完全且刻意保持沉默，因此调用者可以自行决定如何大声地报告每个变量，同时单元测试可以在没有任何环境
 * 的情况下进行算术运算。溢出限制并非装饰性设计：被回绕的乘积结果会默默地变成 0（没有任何约束）或一
 * 个微小的数值，该数值会否定一切，这两种情况都被视为产品缺陷而非不良配置。 */
VROCM_INTERNAL unsigned long long vrocm_quota_parse(const char *value, unsigned long long unit);

/* vrocm_quota_memory_bytes — 一张显卡的 VRAM 容量以字节为单位，若无则记为0。
 *
 * 优先级：VROCM_DEVICE_MEMORY_LIMIT_<i>。当该值被设定时，则使用无索引形式。是否设定决定了结果，若
 * 未设定则无效——错误的索引数值会将该卡的对应值设为 0，且不会传播到整个容器层面，因此错误的单张卡数
 * 值会拒绝该卡，而不会默默让其他用户使用其配额。
 *
 * 超出范围的索引会返回 0，而不是跳过表格进行读取。 */
VROCM_INTERNAL unsigned long long vrocm_quota_memory_bytes(int device);

/* vrocm_quota_ledger_path — 当前生效的账本路径。绝不会为 NULL：未设置或空变量相当于
 * VROCM_LEDGER_DEFAULT_PATH，因此调用者只能获得打开的路径，而非需要处理的异常情况。想要知道当前是
 * 否使用默认路径的调用者，需调用 getenv 函数。 */
VROCM_INTERNAL const char *vrocm_quota_ledger_path(void);

/* vrocm_quota_validate — 在加载时报告一次配置情况，并锁定该配置是否可用。
 *
 * 调用的是发行版库自身的构造函数，而非此处的构造函数，因此顺序清晰明确，不依赖于链接顺序。
 *
 * 它不会终止进程，这是有意偏离“加载失败”机制的做法：这个拦截库通过 /etc/ld.so.preload 加载，因此退
 * 出会杀死容器中的所有进程——包括人类用来诊断配置错误的 shell。在加载时报告问题并随后拒绝所有分配，
 * 与不关闭容器只会导致相同的“失败即终止”结果。 */
VROCM_INTERNAL void vrocm_quota_validate(void);

/* vrocm_quota_usable — 当 vrocm_quota_validate() 发现配置不可用时为 false：没有账本路径，或者以任
 * 意形式完全没有内存数据，或者设置了但无法解析。此时强制路径必须拒绝。 */
VROCM_INTERNAL bool vrocm_quota_usable(void);

#endif /* VROCM_COMMON_VROCM_QUOTA_H */

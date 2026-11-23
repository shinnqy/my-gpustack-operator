/* vrocm_ledger.h — 跨进程账本、其锁、将它们联系起来的约束条件，以及使用区域的布局。
 *
 * 为什么使用 A 区域而不是 A 本地表？容器的配额属于整个容器，而非其中的某个进程：在进程本地账本机制
 * 下，共享同一张卡的两个进程各自获得全部配额，因此 4 GiB 的配额会分配 8 GiB 的存储空间。实测结果显
 * 示：使用 A 区域时，两个进程在 4 GiB 的配额下分别使用 3 GiB 和 1 GiB 的存储空间；而不使用该区域时，
 * 每个进程各使用 4 GiB 的存储空间。
 *
 * 为什么是文件而不是仅内存？该区域同时充当使用表面。某切片的配额及其消耗情况并未出现在任何供应商字
 * 段中——rocm-smi 读取的是 sysfs 和 DRM 节点，而非 HIP 数据，因此该库根本无法获取这些信息，且会报告
 * 物理卡组的任何配额——因此，如果没有任何可读取的数据，内存限制只能从初始化日志中推断出来。
 * tools/rocm-monitor 会读取此文件，而指标抓取工具 later 也会读取相同的字节数据。
 *
 * 该布局结构类似于一份契约。读取器解析这一区域，且无法访问该库的符号，因此它具有特殊意义以及特定的
 * 布局版本；下面的断言会约束每一个偏移量。如果读取器发现一个自己不知道的版本，就必须拒绝处理而非错
 * 误解析——这就是确保下一个新增字段不会引发破坏性变更的原因。
 *
 * BYTE ORDER 属于主机环境。写入者和读取者都是位于同一台机器上某个容器中的进程；一种可移植的编码方
 * 式不仅毫无用处，还会让每个字段都需要一个访问器。
 *
 * 该锁定是按卡进行的，可视为在下方 arena 的 một 个字节上使用的 fcntl() 记录锁定。
 * - 在该区域使用 fcntl 而非 pthread 互斥锁或 POSIX 信号量：这些函数会携带 GLIBC_2.34 的版本信息，
 * 这将使产品的底层支持从 GLIBC_2.4 升级，从而无法在较旧的工作负载镜像中运行。flock() 也可以使用，
 * 但它会锁定整个文件——即所有卡。
 * - 按卡而非按容器处理，因为锁是在运行时分配空间中保持的——这就是能够消除“检查-分配”竞争的原因——因
 * 此，挂起的驱动调用只会暂停某张卡的分配，而非容器中所有进程的分配。持有者将其进程ID写入卡的槽位中，
 * 以便读取者可以识别它。
 * - 进程级别的记录锁定机制，因此不会排除兄弟线程。进程内自旋锁解决了这个问题；两者都遵循同一顺序，
 * 运行时自身回调从插入的进入点重新进入时，会被计数，而非导致死锁。
 * - 读取端完全不持有锁。一个属于一次分配滞后的使用数据量，比一个能占据空闲分配槽位的读取器要重要得
 * 多。
 * - 当持有锁时调用 fork()操作，会让子进程获得自旋锁标志的 COPY 副本，但无法获得锁本身：记录锁不会
 * 被继承，而fork操作仅会传递调用线程的副本，因此子进程的副本会被设置为“无人可释放”状态。账本会检测
 * 到进程ID已改变，会在处理任何相关内容之前重置其进程本地状态——包括标志、密钥映射、重新进入计数器等。
 * 该区域被设置为 MAP_SHARED 状态，并被刻意保留。 */
#ifndef VROCM_COMMON_VROCM_LEDGER_H
#define VROCM_COMMON_VROCM_LEDGER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "vrocm.h"

/* 8个字节，没有终止符，因此区域文件中的strings部分可以识别它。其长度被命名，因为这也是创建锁定所采
 * 用的字节范围。 */
#define VROCM_REGION_MAGIC "VROCMRGN"
#define VROCM_REGION_MAGIC_BYTES 8
#define VROCM_REGION_VERSION 1u

/* 按卡划分，因为每个槽位仅在持有该卡的锁时会被写入数据，这使得整个表完全不存在跨卡同步的情况。该拦
 * 截库是为容器进程设计的，而非主机：即工作负载的主进程及其数据加载器。 */
#define VROCM_MAX_PROCESSES_PER_DEVICE 32

/* 一个进程对一张卡的记账行为。对于空闲槽位，pid 为 0；如果进程不再存在，该槽位会被回收，而不会永远
 * 保持配额。 */
struct vrocm_process_charge {
    int32_t pid;
    uint32_t reserved;
    uint64_t memory_bytes;
};

/* 一张卡的配额、其消耗量以及由谁进行记账。
 *
 * memory_used_bytes 是 processes 中活跃条目总和。保留两者是有意为之：这种汇总形式使得准入决策能够
 * 一次性在热路径上完成，而按进程划分的明细则有助于识别并删除死进程中的记账记录，也便于读取者进行打
 * 印。
 *
 * memory_quota_bytes 在每次接入时都会从环境中更新，绝不会被创建该区域的进程写入——详见
 * vrocm_quota.h 了解为何这种处理方式很重要。 */
struct vrocm_device_usage {
    uint64_t memory_quota_bytes;
    uint64_t memory_used_bytes;
    int32_t lock_holder_pid;
    uint32_t reserved0;
    uint64_t reserved1;
    struct vrocm_process_charge processes[VROCM_MAX_PROCESSES_PER_DEVICE];
};

struct vrocm_region {
    char magic[VROCM_REGION_MAGIC_BYTES];
    uint32_t layout_version;
    uint32_t header_bytes; /* devices[]的偏移量，因此较新的头文件可以被跳过 */
    uint32_t device_slots;
    uint32_t process_slots;
    uint32_t reserved0;
    uint32_t reserved1;
    /* 账本区域：每张卡对应一个字节，通过偏移量锁定且永远不作为数据读取。在布局版本1中，其位置被标记为
     * FROZEN，无法移动，因为运行不同布局版本的两个进程必须使用同一字节来处理同一张卡——否则它们会锁定不
     * 同的偏移量，导致无法协同工作。 */
    unsigned char lock_arena[VROCM_MAX_DEVICES];
    struct vrocm_device_usage devices[VROCM_MAX_DEVICES];
};

/* 合同内容只是断言而非描述。每一个这样的内容都是一份有记录的偏移量，其他读取方可能会将其硬编码，因
 * 此如果字段被意外重新排序，就会导致构建失败。 */
_Static_assert(sizeof(struct vrocm_process_charge) == 16, "process charge layout changed");
_Static_assert(sizeof(struct vrocm_device_usage) == 544, "device usage layout changed");
_Static_assert(offsetof(struct vrocm_device_usage, processes) == 32,
               "process table offset changed");
_Static_assert(offsetof(struct vrocm_region, magic) == 0, "magic must open the region");
_Static_assert(offsetof(struct vrocm_region, lock_arena) == 32, "the lock arena may not move");
_Static_assert(offsetof(struct vrocm_region, devices) == 96, "device table offset changed");
_Static_assert(sizeof(struct vrocm_region) == 34912, "region size changed");

/* 记账如何结束。因为三次拒绝是三种不同的操作问题——配额太小、配置从未到位，以及构建规模过紧的表格——
 * 所以没有缩回为布尔值，如果调用者无法区分这些问题，就会为所有问题记录一条消息。 */
enum vrocm_admit {
    VROCM_ADMIT_OK = 0,
    VROCM_ADMIT_DENIED_QUOTA,     /* 记账量将超过卡的限额 */
    VROCM_ADMIT_DENIED_CONFIG,    /* 没有可用的配置，或者该区域无法使用 */
    VROCM_ADMIT_DENIED_TRACKING,  /* 没有地方可以进行记账了——参见下面的失败关闭部分 */
    VROCM_ADMIT_ALLOC_FAILED,     /* 运行时本身拒绝了；没有进行任何记账 */
};

/* 调用者的实际分配操作，在持有卡锁的情况下执行。
 *
 * 一个函数指针，而非调用者自身的排序锁/检查/分配/记账机制，因为这一排序是设计中的一部分，评审者无
 * 法通过阅读调用者来验证；在检查与记账之间释放锁，会重新引发原本由锁所解决的竞争问题，且这种竞争以
 * 隐秘的方式发生。保持当前的排序方式，还能避免所有 hip* 类型的问题——回调函数会占用它们，但本文件从
 * 未提及此类情况。
 *
 * key 是调用者稍后用于记账的标识符；对于 HIP 来说，就是返回的设备指针。返回 false 意味着运行时拒绝。
 *
 * 零值密钥表示分配成功，且不会产生需要退还的内容，这正是零尺寸请求的行为：运行时会返回成功结果以及
 * 一个空指针，而释放空指针被定义为不会执行任何操作。没有产生任何费用，也没有保留任何槽位——为分配而
 * 保留的槽位若没有空闲资源，则只能保留到进程结束为止。
 *
 * bytes 是输入输出类型：用于 admission 阶段的限制条件；用于实际分配时的实际取值。在分片分配中，两
 * 者会有所不同，因为运行时决定步长，调用者仅知道所要求的宽度。通过此处修改而非再次调用，可以保持对
 * 账过程与检查操作使用同一锁——若再次调用则必须重新获取锁，而此时另一个进程可能已经决定拒绝该旧总数。 */
typedef bool (*vrocm_alloc_fn)(void *ctx, unsigned long long *key, unsigned long long *bytes);

/* vrocm_ledger_admit — 整个记账过程，在卡锁定时一次性完成。
 *
 * 按以下顺序操作：先预留一个地方用于记账，从环境中重新读取卡的配额，如果记账内容超出配额则清理死亡
 * 进程，然后做出决定，调用 alloc 函数，最后根据该函数生成的密钥对记账进行提交。
 *
 * 追踪预留机制优先执行，这种排序规则属于故障关闭机制。此构建版本无法记录的分配，也永远无法退款，因
 * 此未经记录就放行，使得配额机制悄然失效；在分配之前进行预留，意味着此类请求在进入运行时之前就被拒
 * 绝，因此没有可以撤销的操作。 */
VROCM_INTERNAL enum vrocm_admit vrocm_ledger_admit(int device, unsigned long long bytes,
                                                   vrocm_alloc_fn alloc, void *ctx);

/* vrocm_ledger_release — 退还针对 key 记录的记账项，并报告其持有量。
 *
 * 如果密钥从未被记录，则结果为假，调用者必须将其视为“无需退款”，而非错误：在拦截库加载之前进行的分
 * 配就是这种情况，同样适用于由工作负载生成的指针。 */
VROCM_INTERNAL bool vrocm_ledger_release(unsigned long long key, int *device,
                                         unsigned long long *bytes);

/* vrocm_ledger_used — 一张卡的总记账额。设计上为无锁机制；详见头部注释。 */
VROCM_INTERNAL unsigned long long vrocm_ledger_used(int device);

/* vrocm_ledger_quota — 这是该卡上次被接纳的记录，以读取方所看到的形式呈现。无需加锁，当该区域无法
 * 使用或该卡未有过接纳记录时则为0。 */
VROCM_INTERNAL unsigned long long vrocm_ledger_quota(int device);

/* vrocm_ledger_reclaim — 删除已不存在进程的记账记录，并根据剩余数据重新计算卡的总量；返回恢复的字
 * 节数。
 *
 * 仅在出现其他拒绝情况时才会被调用，这既至关重要，又十分罕见，以至于热路径上的存活性扫描不会产生任
 * 何成本。如果没有这个机制，在分配过程中被终止的进程将一直保留其记账记录，直到区域文件存在——具体表
 * 现为：一个进程被 SIGKILL 终止时仍持有 4 GiB 的 6 GiB 配额，而下一个进程只能获得 2 GiB。必须持有
 * 关键锁的情况下调用。 */
VROCM_INTERNAL unsigned long long vrocm_ledger_reclaim(int device);

/* vrocm_ledger_holding — 当前该线程是否持有卡的锁，以及是哪一个。
 *
 * 用于回调的实现：一个能够重新进入被拦截项下的分配回调，在锁机制下会再次执行，该拦截项需要知道这一
 * 点，以避免死锁或重复记账的情况发生。 */
VROCM_INTERNAL bool vrocm_ledger_holding(int *device);

/* vrocm_ledger_lock_epochs — 在整个进程生命周期中，某卡的锁从未持有状态被获取的次数。
 *
 * 在生产环境中进行诊断，在测试环境中进行负载测试：这是让单元测试能够验证整体接入成本恰好对应一次获
 * 取操作的关键，也是证明“检查”与“记账”未被分开的唯一方法。重复进行的获取操作不计入统计。 */
VROCM_INTERNAL unsigned long long vrocm_ledger_lock_epochs(void);

/* vrocm_ledger_tracking_refusals — 因缺乏槽位而被拒绝的入学申请。该数据在计数器转储中有所报告，因
 * 此超出表格容量的容器与仅处于配额限制下的容器能够被区分开。 */
VROCM_INTERNAL unsigned long long vrocm_ledger_tracking_refusals(void);

#endif /* VROCM_COMMON_VROCM_LEDGER_H */

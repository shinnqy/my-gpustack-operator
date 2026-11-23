/* vppu_ledger.h — 跨进程账本、其锁机制以及使用区域的布局。
 *
 * 为什么使用 A 区域而不是 A 本地表？容器的配额属于整个容器，而非其中的某个进程：在进程本地账本中，
 * 共享同一张卡的两个进程会各自获得全部配额，因此 4 GiB 的切片会分配 8 GiB 的存储空间。因此，记账操
 * 作发生在文件映射的区域中，容器中的每个进程都会映射到该区域。
 *
 * 为什么是文件而不是仅使用内存？该区域同时充当使用界面。某切片的配额及其消耗情况没有出现在任何供应
 * 商字段中——ppu-smi 也没有“最大 SM 值”列，与 nvidia-smi 完全一样——因此如果没有任何可读取的信息，计
 * 算限制只能从初始化日志或压力测试中推断出来。flexai 的账本是无状态的，其文档中说明的答案是一个独
 * 立的 CLI 工具；没有可获取的信息，因此这种形式在此被拒绝。
 *
 * 这种布局是一种契约。包括 tools/ 目录，以及后续的指标抓取工具。解析这个区域时无法访问该库的符号，
 * 因此它带有某种魔力，还有布局版本；下面的断言会确定每个偏移量。如果读取方发现一个自己不知道的版本，
 * 就必须拒绝而不是错误解析——这就是防止下一个新增字段造成破坏性变更的关键。
 *
 * BYTE ORDER 属于主机环境。写入者和读取者都是位于同一台机器上某个容器中的进程；一种可移植的编码方
 * 式不仅毫无用处，还会让每个字段都需要一个访问器。
 *
 * 该锁定是按卡进行的，可视为在下方 arena 的 một 个字节上使用的 fcntl() 记录锁定。
 * - 在该区域使用 fcntl 而非 pthread 互斥锁或 POSIX 信号量：这些需要在 glibc 2.17 版本上禁用
 * -lpthread，而本库必须加载这些功能，否则会在 DT_NEEDED 中产生第二个条目，破坏“只有 libc.so.6”的保
 * 证情况。情况1中使用 flock()也可以，但会锁定整个文件，即整个容器。
 * - 按卡而非按容器处理，因为锁是在供应商的实际分配区域之外被持有的——这就是能够消除“检查-分配”竞争
 * 的原因——因此，挂起的驱动程序调用只会停止某张卡的分配，而非容器中所有进程的分配。持有者将其进程ID
 * 写入卡的槽位中，以便 tools/ 能够对其进行标识。
 * - 记录锁定是按进程进行的，因此不会排除兄弟线程。进程内自旋锁解决了这个问题；两者都按固定顺序执行，
 * 从供应商自己的回调中重新进入被插入的入口处不会导致死锁。
 * - 读取端完全不持有锁。一个属于一次分配滞后的使用数据量，比一个能够占据空闲分配槽位的读取器要重要
 * 得多。 */
#ifndef VPPU_COMMON_VPPU_LEDGER_H
#define VPPU_COMMON_VPPU_LEDGER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "vppu.h"
#include "vppu_pid.h"

/* 8个字节，没有终止符，因此区域文件中的strings部分可以识别它。其长度被命名，因为这也是创建锁定所采
 * 用的字节范围。 */
#define VPPU_REGION_MAGIC "VPPUREGN"
#define VPPU_REGION_MAGIC_BYTES 8
#define VPPU_REGION_VERSION 1u

/* 按卡划分，因为每个槽位仅在持有该卡的锁时会被写入数据，这使得整个表完全不存在跨卡同步的情况。该拦
 * 截库是为容器进程设计的，而非主机：即工作负载的主进程及其数据加载器。 */
#define VPPU_MAX_PROCESSES_PER_DEVICE 32

#define VPPU_ENV_LEDGER_PATH "HGGC_LEDGER_PATH"

/* /dev/shm，因为它是每个容器中存在的 tmpfs 存储，被其中的每个进程共享，而且分配器已经将其挂载为可
 * 读写模式，用于 Ascend 读取工具。
 *
 * 默认设置仅适用于一个容器，因此分配器总是将变量设置为Pod工作目录下的每个容器专属目录，正如设备插
 * 件注释中所述：该区域由容器LOCAL卡索引所指向，因此共享位置使得两个容器的索引0能够占用一个槽位。
 *
 * /dev/shm 默认是容器私有数据，通过两种普通配置可以使其共享：hostIPC: true 使其属于主机环境，因此
 * 节点上的所有容器会共享同一区域；在 /dev/shm 上挂载的 emptyDir{medium: Memory} 会被 Pod 中的容器
 * 共享，这是针对数据加载器那种认为默认 64 MiB 大小过小的常见解决方案。无论哪种情况，两个容器都会将
 * 不同的物理卡位放入同一个槽位中，这并非因为配额错误而是因为实际存在但未被显示出来。请将此默认设置
 * 视为手动运行单个容器的便利方案，而非替代分配器设置变量的方式。AMD shim 中的 VROCM_LEDGER_PATH 具
 * 有相同的结构及警告信息。 */
#define VPPU_LEDGER_DEFAULT_PATH "/dev/shm/vppu-ledger"

/* 一个进程对一张卡的记账行为。对于空闲槽位，pid 为 0；如果进程不再存在，该槽位会被回收，而不会永远
 * 保留配额。 */
struct vppu_process_charge {
    int32_t pid;
    uint32_t reserved;
    uint64_t memory_bytes;
};

/* 一张卡的配额、其消耗量以及由谁进行记账。
 *
 * memory_used_bytes 是 processes 中活跃条目总和。保留两者是有意为之：这种汇总形式使得准入决策更加
 * 容易理解，而按进程划分的详细数据则有助于识别并回收已终止进程的记账数据，这也是 tools/ 输出结果所
 * 展示的内容。
 *
 * memory_quota_bytes和sm_limit_percent会在每次接入时从环境中更新，绝不会被创建该区域的进程写入。这
 * 就是与HAMi-core不同的地方，在HAMi-core中，第一个创建者的限制会被冻结在缓存中，更改它意味着删除该
 * 文件。
 *
 * sm_util_percent 是计算控制器上次针对该容器测量的数值，而 control 则是产生该数值的循环。这两个值
 * 都会被公开，不会保持私密状态：这些改进刻意没有从参考实现中继承下来，因此该循环必须在没有进行性能
 * 分析硬件上的可观测性。control 占用了版本 1 中专门用于此目的的四个字样的空间，因此填入后不会产生
 * 任何偏移。 */
struct vppu_device_usage {
    uint64_t memory_quota_bytes;
    uint64_t memory_used_bytes;
    uint32_t sm_limit_percent;
    uint32_t sm_util_percent;
    int32_t lock_holder_pid;
    uint32_t reserved0;
    struct vppu_pid_state control;
    struct vppu_process_charge processes[VPPU_MAX_PROCESSES_PER_DEVICE];
};

struct vppu_region {
    char magic[VPPU_REGION_MAGIC_BYTES];
    uint32_t layout_version;
    uint32_t header_bytes; /* devices[]的偏移量，因此较新的头文件可以被跳过 */
    uint32_t device_slots;
    uint32_t process_slots;
    uint32_t reserved0;
    uint32_t reserved1;
    /* 账本区域：每张卡对应一个字节，通过偏移量锁定且永远不作为数据读取。在布局版本1中，其位置被标记为
     * FROZEN，无法移动，因为运行不同布局版本的两个进程必须使用同一字节来处理同一张卡——否则它们会锁定不
     * 同的偏移量，导致无法协同工作。 */
    unsigned char lock_arena[VPPU_MAX_DEVICES];
    struct vppu_device_usage devices[VPPU_MAX_DEVICES];
};

/* 合同内容只是断言而非描述。每一个这样的内容都是一份有记录的偏移量，其他读取方可能会将其硬编码，因
 * 此如果字段被意外重新排序，就会导致构建失败。 */
_Static_assert(sizeof(struct vppu_process_charge) == 16, "process charge layout changed");
_Static_assert(sizeof(struct vppu_device_usage) == 576, "device usage layout changed");
_Static_assert(offsetof(struct vppu_device_usage, control) == 32,
               "the controller words may not move");
_Static_assert(offsetof(struct vppu_device_usage, processes) == 64,
               "process table offset changed");
_Static_assert(offsetof(struct vppu_region, magic) == 0, "magic must open the region");
_Static_assert(offsetof(struct vppu_region, lock_arena) == 32, "the lock arena may not move");
_Static_assert(offsetof(struct vppu_region, devices) == 96, "device table offset changed");
_Static_assert(sizeof(struct vppu_region) == 36960, "region size changed");

/* vppu_ledger_lock — 获取一张卡的锁定，首次使用时映射对应区域。
 *
 * 当该区域无法使用（无法映射、为外部区域，或本构建版本不支持的布局版本）或调用者已持有其他卡时，返
 * 回 false。返回 false 表示拒绝，绝不会允许使用：调用者必须拒绝操作。
 *
 * 同一卡上的重新进入会被计算在内，因此在此锁定状态下进行的供应商调用，若再次进入另一个被拦截的条目，
 * 就不会导致自身死锁。每个成功的调用必须对应一个准确的 vppu_ledger_unlock()()操作。 */
VPPU_INTERNAL bool vppu_ledger_lock(int device);
VPPU_INTERNAL void vppu_ledger_unlock(int device);

/* vppu_ledger_holding — 当前该线程是否持有卡的锁。
 *
 * 计算控制器会进行干预，因为绝不能让调用者处于资源限制状态中；供应商自己的分配操作在卡的锁机制下执
 * 行，如果达到拦截机制，在锁机制上进行限制会占用整个控制窗口，从而阻碍该卡上其他进程的分配操作。 */
VPPU_INTERNAL bool vppu_ledger_holding(void);

/* vppu_ledger_used — 一张卡的总记账额。设计上为无锁机制；详见头部注释。 */
VPPU_INTERNAL unsigned long long vppu_ledger_used(int device);

/* vppu_ledger_note_config — 记录此次批准被否决的具体数字，以便读取方能看到实际生效的配额，而非该区
 * 域创建时的配额。必须持卡锁定状态调用。 */
VPPU_INTERNAL void vppu_ledger_note_config(int device, unsigned long long quota_bytes,
                                           unsigned int sm_percent);

/* vppu_ledger_note_util — 记录计算控制器针对此容器最后测量的数值。像读取端一样采用无锁机制：这是一
 * 个诊断数据，一个窗口的失效比可能因分配而受阻的控制操作更有价值。 */
VPPU_INTERNAL void vppu_ledger_note_util(int device, unsigned int sm_percent);

/* vppu_ledger_control — 一张卡的控制器相关字眼，当该区域无法使用时则为NULL。
 *
 * 指向共享映射的指针，这是故意设计的：计算控制器在每次启动时读取它，并在一个窗口周期后更新一次，因
 * 此无法承受该文件的锁定——记录锁定属于系统调用，而启动过程则不是。这些词属于共享内存，因此控制器通
 * 过原子操作与它们进行同步；这个目录所拥有的，就是它们的存在，它们被容器内的每个进程共享，且不会移
 * 动。它们的含义属于 vppu_pid.h。 */
VPPU_INTERNAL struct vppu_pid_state *vppu_ledger_control(int device);

/* vppu_ledger_has_process — 是否某个进程在该卡的表中占有一个槽位。
 *
 * 这就是计算控制器如何从邻近容器的账本中获取自身容器的使用样本：该区域是按容器划分的，因此其中的进
 * 程ID按构造属于该容器。进程ID命名空间对比无法解答这个问题——主机进程ID在该命名空间中也可能是一个有
 * 效的进程ID。 */
VPPU_INTERNAL bool vppu_ledger_has_process(int device, int pid);

/* vppu_ledger_charge — 将此进程的记账添加到针对一张卡的记账中。
 *
 * 当卡的进程表已满时，返回值为假，调用者必须将其视为拒绝：无法归因于某次的分配也无法被收回，任由它
 * 通过而不进行记账，正是配额悄然失去意义的方式。必须在持有卡锁的情况下调用。 */
VPPU_INTERNAL bool vppu_ledger_charge(int device, unsigned long long bytes);

/* vppu_ledger_refund — 返还字节数据。被限制为零，而非循环，因为双重释放不得给容器一个无限制配额。
 * 必须在持有卡锁的情况下调用。 */
VPPU_INTERNAL void vppu_ledger_refund(int device, unsigned long long bytes);

/* vppu_ledger_reclaim — 删除已不存在进程的记账记录，根据剩余数据重新计算卡的总量；返回恢复的字节数。
 *
 * 仅在某种准入被拒绝时才会被调用，这既至关重要，又十分罕见，以至于在热路径上执行存活性扫描不会产生
 * 任何成本。如果没有这一操作，在分配过程中被终止的进程会一直保留其记账记录，直到区域文件存在，进而
 * 引发 HAMi-core 的陈旧缓存问题。必须持有所锁的卡来调用此操作。 */
VPPU_INTERNAL unsigned long long vppu_ledger_reclaim(int device);

/* 允许自由实体返回其字节的 key -> (card, bytes) 映射。
 *
 * 设备指针只在本进程地址空间内有意义。不同进程可能在不同卡上得到数值相同的指针，共用一张指针表会导
 * 致某进程释放时退还另一进程的配额。跨进程共享的是区域中的总使用量。 */
VPPU_INTERNAL void vppu_alloc_record(unsigned long long key, int device,
                                     unsigned long long bytes);

/* vppu_alloc_take — 移除某个键的记录并报告其包含的内容。如果该键从未被记录，则结果为假，调用者必须
 * 将其视为“无需退款”，而非错误：当此拦截库加载前进行的分配操作就属于这种情况。 */
VPPU_INTERNAL bool vppu_alloc_take(unsigned long long key, int *device,
                                   unsigned long long *bytes);

/* vppu_alloc_overflows — 记录因表格已满而被丢弃的数据。在计数器转储中有所报告，因此从未完成的退款
 * 与配额限制情况相区分。 */
VPPU_INTERNAL unsigned long long vppu_alloc_overflows(void);

#endif /* VPPU_COMMON_VPPU_LEDGER_H */

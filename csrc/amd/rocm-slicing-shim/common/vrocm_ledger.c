#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "vrocm_ledger.h"
#include "vrocm_log.h"
#include "vrocm_quota.h"

/* 进程内记录分配标识到（设备编号，字节数）的映射。
 *
 * 设备指针只在本进程地址空间内有意义。不同进程可能在不同卡上得到数值相同的指针，共用一张指针表会导
 * 致某进程释放时退还另一进程的配额。跨进程共享的是区域中的总使用量。
 *
 * 跟踪表使用固定容量，避免为了登记一次分配又通过运行时动态分配内存，引入递归。表满时明确拒绝请求，
 * 禁止无记录地放行。 */
#define KEY_SLOTS 16384

enum { SLOT_FREE = 0, SLOT_RESERVED, SLOT_LIVE };

struct key_slot {
    unsigned long long key;
    unsigned long long bytes;
    int device;
    int state;
};

static struct key_slot keys[KEY_SLOTS];
static volatile int keys_lock;

static unsigned long long lock_epochs;
static unsigned long long tracking_refusals;

/* 被拦截，因此“此进程没有可用的配置”这一行仅会在每次分配时出现一次，而非每次都会出现——请参考
 * vrocm_ledger_admit 中的拦截逻辑。 */
static int config_refusal_said;

/* 每张卡对应一个标志位，因为 fcntl() 的记录锁由进程持有：一个进程中的两个线程都能成功处理同一字节
 * 数据，因此仅使用记录锁无法排除其他线程。 */
static volatile int device_lock[VROCM_MAX_DEVICES];

/* 重新进入的计数按线程进行，因此在卡锁定状态下进行的运行时调用，若再次进入另一个拦截库中的入口，就
 * 不会导致自身死锁。 */
static __thread int held_device = -1;
static __thread unsigned held_depth;

static void spin_lock(volatile int *flag)
{
    while (__sync_lock_test_and_set(flag, 1)) {
        /* 进行原子读取是因为数据会被原子地写入：volatile 指令会指定此线程的访问行为，但对其他线程的访问则
         * 不做任何限制，因此另一个线程使用原子 RMW 操作设置值的普通读取会引发数据竞争，无论该指令编译后包
         * 含多少条指令。这是宽松的约束条件——获取操作就是测试并设置的操作，此循环会返回该结果。 */
        while (__atomic_load_n(flag, __ATOMIC_RELAXED)) {
            /* 一种等待而非真正的停滞，因为sched_yield()在采用这种设计方式使其无系统调用的情况下会造成成本——但
             * 带有暂停的等待，而这里并非形式上的问题。等待者可以持续存在直到卡整个关键部分的结束，因为持有者本
             * 身可能也在另一个进程驱动调用后的F_SETLKW中处于阻塞状态：这个循环并不总是像计数器锁机制那样只需少
             * 量周期就能完成。 */
            VROCM_SPIN_PAUSE();
        }
    }
}

static void spin_unlock(volatile int *flag)
{
    __sync_lock_release(flag);
}

/* ---- 区域 ---------------------------------------------------------------------- */

static struct vrocm_region *region_map;
static int region_fd = -1;

/* 这个锁机制采用状态机而非标志，因为打开操作并非瞬间完成：在 F_SETLKW 函数中，当另一个进程需要时间
 * 初始化文件时，主体会长时间处于阻塞状态。在该时间段内到达的线程必须能够区分“尚未决定”和“已决定且
 * 答案为否”两种情况——将前者理解为该线程拒绝在空卡上分配资源。 */
enum { REGION_UNTRIED = 0, REGION_USABLE, REGION_UNUSABLE, REGION_OPENING };

static volatile int region_state;

/* fork 后的状态 */

/* 上述声明的所有内容均为进程本地内容，而 fork() 会将其中所有内容的一个副本传递给子进程。其中两个副
 * 本具有实际危害性。
 *
 * 继承的 SPINLOCK 在被继承对象中没有拥有者，因为 fork 仅会传递调用线程。使用 spin_lock 会无限循环，
 * 占用 100% 的核心资源，且不会产生错误或日志记录。窗口范围并不狭窄：device_lock 在运行时真正的分配
 * 过程中被持有，keys_lock 则贯穿每次分配和释放时的 16384 个槽位扫描过程。重新进入的计数比死机还要
 * 糟糕——一个继承了 held_depth > 0 的子进程认为自己已经持有了一个本不应持有的卡锁，于是直接进行记账
 * 操作，实际上没有任何锁保护。该区域的锁定机制也是一样的：当另一个线程在 region_create 中时，fork
 * 操作会留下一个显示“进行中”的锁定状态，没有线程可以完成操作，因此保持该锁的子进程会等待一个永远无
 * 法到来的答案。被返回给未尝试的子进程后，它会打开自己的锁。
 * - 继承的 KEY 映射与父进程的活跃条目相关联，允许子进程退还其从未产生的费用：在子进程中释放的继承
 * 指针会移除父进程在总金额及某个槽位上的数据，随后子进程会将其计入自己的 PID 中，下一次扫描会从这
 * 些槽位重新计算总金额，导致损失永久化。
 *
 * 通常的答案是 pthread_atfork() 函数，该库无法调用此函数：因为 pthread 名称会将其 glibc 版本提升至
 * .symver 文件所支持的 GLIBC_2.4 以上水平。因此，会在状态被使用的地方进行检查。会记录当前进程的
 * pid，如果某个 pid 不再匹配，则表明该进程是一个尚未重置的子进程。
 *
 * 该区域本身并未被重置，仅其锁定机制被清除。映射方式为 MAP_SHARED，因此子进程能够看到与父进程相同
 * 的字节数据，这是设计上的特性而非错误，且其描述符在fork操作后仍然有效。POSIX标准不会将记录锁传递
 * 给子进程，因此子进程实际上并不持有任何记录锁——而这正是清除 device_lock 操作所假设的情况。 */
static int owner_pid;

static void fork_reset(void)
{
    int self = (int)getpid();

    if (owner_pid == self) {
        return;
    }
    if (owner_pid == 0) {
        /* 此流程中的首次使用。状态仍保持加载器设置时的样子。 */
        owner_pid = self;
        return;
    }

    if (region_state == REGION_OPENING) {
        region_state = REGION_UNTRIED;
    }
    memset((void *)device_lock, 0, sizeof(device_lock));
    keys_lock = 0;
    memset(keys, 0, sizeof(keys));
    held_device = -1;
    held_depth = 0;
    /* 计数器，而非状态：子进程报告父进程的计数结果会描述出该子进程从未执行过的工作，而这两种数据都会作
     * 为当前进程自己的数据被读取。名为“said once”的拦截机制也会随之存在——行名即代表进程，因此继承该拦
     * 截子的进程若被静音，就会因自身原因被拒绝。 */
    lock_epochs = 0;
    tracking_refusals = 0;
    config_refusal_said = 0;
    owner_pid = self;
}

/* record_lock — 一个 fcntl() 记录锁，位于 len 字节的 offset 处。与调用者分离，因为创建锁和每张卡锁
 * 仅在指向位置上有所不同。 */
static bool record_lock(int fd, int type, off_t offset, off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = (short)type;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = len;

    while (fcntl(fd, F_SETLKW, &fl) < 0) {
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

/* region_create — 映射该区域，如果这是第一次执行此过程，则进行创建和初始化。
 *
 * 每个进程恰好运行一次，由单个线程 region_open 选择执行；它自身不会进行锁定操作。
 *
 * 该文件被创建为 0600 权限，这意味着首先到达路径的容器将拥有该文件：以不同用户 ID 运行的兄弟进程在
 * 调用 open() 时会被拒绝，但不会产生任何影响。这是较为保守的做法——更宽松的模式将允许节点上的任何用
 * 户 ID 重写另一个容器的总计数，而它的结构则是每个账本路径对应一个用户 ID。 */
static struct vrocm_region *region_create(void)
{
    const char *path;
    struct vrocm_region *map;

    /* 绝不会是 NULL——未设置的变量会采用默认值——因此不存在“无路径”的情况，过去处理该情况的分支也已被移
     * 除。路径是经配置还是默认设置，由 vrocm_quota_validate 在加载时一次性确定。 */
    path = vrocm_quota_ledger_path();

    region_fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (region_fd < 0) {
        vrocm_log(VROCM_LOG_DENY, "cannot open ledger %s\n", path);
        return NULL;
    }
    if (ftruncate(region_fd, (off_t)sizeof(struct vrocm_region)) < 0) {
        vrocm_log(VROCM_LOG_DENY, "cannot size ledger %s\n", path);
        close(region_fd);
        region_fd = -1;
        return NULL;
    }

    map = mmap(NULL, sizeof(struct vrocm_region), PROT_READ | PROT_WRITE, MAP_SHARED, region_fd, 0);
    if (map == MAP_FAILED) {
        vrocm_log(VROCM_LOG_DENY, "cannot map ledger %s\n", path);
        close(region_fd);
        region_fd = -1;
        return NULL;
    }

    /* 初始化竞争存在两种情况——两个进程同时创建文件，其中一个读取数据而另一个写入头部信息——因此双方都会
     * 对关键字节使用相同的锁。新创建的文件内容全为零，这正是“尚未涉及特殊处理”的状态。 */
    if (!record_lock(region_fd, F_WRLCK, 0, VROCM_REGION_MAGIC_BYTES)) {
        vrocm_log(VROCM_LOG_DENY, "cannot lock ledger %s for initialisation\n", path);
        munmap(map, sizeof(struct vrocm_region));
        close(region_fd);
        region_fd = -1;
        return NULL;
    }
    if (memcmp(map->magic, VROCM_REGION_MAGIC, VROCM_REGION_MAGIC_BYTES) != 0) {
        static const char zeroes[VROCM_REGION_MAGIC_BYTES];

        /* 一个全零魔法文件就是该进程刚刚创建的文件，初始化它正是其目的所在。其他任何魔法文件都属于其他人的
         * 文件：覆盖它将会悄无声息地破坏其原有内容，而且路径是由操作器提供的，因此必须拒绝错误输入，而不是
         * 执行它。 */
        if (memcmp(map->magic, zeroes, VROCM_REGION_MAGIC_BYTES) != 0) {
            record_lock(region_fd, F_UNLCK, 0, VROCM_REGION_MAGIC_BYTES);
            vrocm_log(VROCM_LOG_DENY, "%s is not a ledger; refusing to overwrite it\n", path);
            munmap(map, sizeof(struct vrocm_region));
            close(region_fd);
            region_fd = -1;
            return NULL;
        }
        map->layout_version = VROCM_REGION_VERSION;
        map->header_bytes = (uint32_t)offsetof(struct vrocm_region, devices);
        map->device_slots = VROCM_MAX_DEVICES;
        map->process_slots = VROCM_MAX_PROCESSES_PER_DEVICE;
        /* 最后编写的，因此阅读取方看到这一内容时，会认为头文件已经完成。 */
        memcpy(map->magic, VROCM_REGION_MAGIC, VROCM_REGION_MAGIC_BYTES);
    }
    record_lock(region_fd, F_UNLCK, 0, VROCM_REGION_MAGIC_BYTES);

    /* 此构建版本无法识别的版本属于拒绝行为，而非尽力而为：它读取的偏移量正是可能发生变化的那些。 */
    if (map->layout_version != VROCM_REGION_VERSION) {
        vrocm_log(VROCM_LOG_DENY, "ledger %s is layout version %u, this build speaks %u\n", path,
                  (unsigned)map->layout_version, VROCM_REGION_VERSION);
        munmap(map, sizeof(struct vrocm_region));
        close(region_fd);
        region_fd = -1;
        return NULL;
    }

    vrocm_log(VROCM_LOG_DEBUG, "ledger %s mapped (layout %u)\n", path, VROCM_REGION_VERSION);
    return map;
}

/* region_open — 该区域，如果此进程无法使用，则为NULL。
 *
 * 被锁住的状态下，错误的配置会导致一次 open()调用失败，而不是每次分配都失败；而可用的状态则不会在
 * 热路径上引发重复的系统调用。一个线程被选中执行工作，其余线程则等待其响应：如果调用两次 open()函
 * 数，就会对该区域进行两次映射操作；此外，由于 POSIX 规范要求一旦关闭该文件上的任何描述符，整个进
 * 程就会释放文件记录锁，因此如果失败的线程关闭自己的描述符，就会释放获胜者所持有的卡锁。 */
static struct vrocm_region *region_open(void)
{
    struct vrocm_region *map;
    int state;

    for (;;) {
        state = __sync_val_compare_and_swap(&region_state, REGION_UNTRIED, REGION_OPENING);
        if (state == REGION_UNTRIED) {
            break; /* 被选中的 */
        }
        if (state == REGION_USABLE) {
            return region_map;
        }
        if (state == REGION_UNUSABLE) {
            return NULL;
        }
        /* 另一个线程位于 region_create()函数中，它可能会停留在 F_SETLKW状态，直到其他进程完成文件初始化。
         * 该线程想要的就是这个结果，因此应该等待它，而不是将“未决定”视为“无法使用”。 */
        while (region_state == REGION_OPENING) {
        }
    }

    map = region_create();
    region_map = map;
    /* 完整的屏障，因此看到已确定状态的线程也会看到 region_map。 */
    __sync_val_compare_and_swap(&region_state, REGION_OPENING,
                                map != NULL ? REGION_USABLE : REGION_UNUSABLE);
    return map;
}

/* ---- 每张卡的锁定机制 --------------------------------------------------------------- */

static bool device_valid(int device)
{
    return device >= 0 && device < VROCM_MAX_DEVICES;
}

static bool ledger_lock(int device)
{
    if (!device_valid(device)) {
        return false;
    }
    if (held_depth > 0) {
        /* 同一卡上的重新进入会被计数。不同卡上的重新进入则被拒绝，而非视为嵌套操作，因为两个线程以两种顺序
         * 占用两个卡会导致死锁，本库中没有任何路径需要同时使用两者。 */
        if (held_device != device) {
            return false;
        }
        held_depth++;
        return true;
    }

    spin_lock(&device_lock[device]);
    if (!record_lock(region_fd, F_WRLCK,
                     (off_t)offsetof(struct vrocm_region, lock_arena) + device, 1)) {
        spin_unlock(&device_lock[device]);
        return false;
    }

    held_device = device;
    held_depth = 1;
    lock_epochs++;
    region_map->devices[device].lock_holder_pid = (int32_t)getpid();
    return true;
}

static void ledger_unlock(int device)
{
    if (held_depth == 0 || held_device != device) {
        return;
    }
    if (--held_depth > 0) {
        return;
    }

    region_map->devices[device].lock_holder_pid = 0;
    held_device = -1;
    record_lock(region_fd, F_UNLCK, (off_t)offsetof(struct vrocm_region, lock_arena) + device, 1);
    spin_unlock(&device_lock[device]);
}

VROCM_INTERNAL bool vrocm_ledger_holding(int *device)
{
    fork_reset();
    if (held_depth == 0) {
        return false;
    }
    if (device != NULL) {
        *device = held_device;
    }
    return true;
}

/* ---- 关键映射 --------------------------------------------------------------------- */

static int slot_reserve(void)
{
    int i;

    spin_lock(&keys_lock);
    for (i = 0; i < KEY_SLOTS; i++) {
        if (keys[i].state == SLOT_FREE) {
            keys[i].state = SLOT_RESERVED;
            spin_unlock(&keys_lock);
            return i;
        }
    }
    spin_unlock(&keys_lock);
    return -1;
}

static void slot_commit(int index, unsigned long long key, int device, unsigned long long bytes)
{
    spin_lock(&keys_lock);
    keys[index].key = key;
    keys[index].device = device;
    keys[index].bytes = bytes;
    keys[index].state = SLOT_LIVE;
    spin_unlock(&keys_lock);
}

static void slot_free(int index)
{
    spin_lock(&keys_lock);
    memset(&keys[index], 0, sizeof(keys[index]));
    spin_unlock(&keys_lock);
}

/* 线性设计，也是故意如此：一个自由体可以依次使用 16384 个槽位中的每两个单词，这比调用驱动程序慢几
 * 个数量级要快得多，而且索引又是一个需要维护的结构，在相同的锁下保持一致性并无实际收益。 */
static bool slot_take(unsigned long long key, int *device, unsigned long long *bytes)
{
    int i;

    spin_lock(&keys_lock);
    for (i = 0; i < KEY_SLOTS; i++) {
        if (keys[i].state == SLOT_LIVE && keys[i].key == key) {
            *device = keys[i].device;
            *bytes = keys[i].bytes;
            memset(&keys[i], 0, sizeof(keys[i]));
            spin_unlock(&keys_lock);
            return true;
        }
    }
    spin_unlock(&keys_lock);
    return false;
}

/* ---- 账本 ------------------------------------------------------------------------- */

/* charge_slot — 此进程ID在某一卡上的槽位，如果该卡没有空闲槽位，就会申请一个。当卡的进程表已满时返
 * 回NULL，调用者必须将其视为拒绝，原因与键映射表已满的情况相同。必须在持有卡锁的情况下调用此函数。 */
static struct vrocm_process_charge *charge_slot(struct vrocm_device_usage *usage, int pid)
{
    struct vrocm_process_charge *free_slot = NULL;
    int i;

    for (i = 0; i < VROCM_MAX_PROCESSES_PER_DEVICE; i++) {
        if (usage->processes[i].pid == pid) {
            return &usage->processes[i];
        }
        if (free_slot == NULL && usage->processes[i].pid == 0) {
            free_slot = &usage->processes[i];
        }
    }
    if (free_slot != NULL) {
        free_slot->pid = (int32_t)pid;
        free_slot->memory_bytes = 0;
    }
    return free_slot;
}

VROCM_INTERNAL unsigned long long vrocm_ledger_reclaim(int device)
{
    struct vrocm_device_usage *usage;
    unsigned long long before, total = 0;
    int i;

    if (region_map == NULL || !device_valid(device)) {
        return 0;
    }
    usage = &region_map->devices[device];
    before = usage->memory_used_bytes;

    for (i = 0; i < VROCM_MAX_PROCESSES_PER_DEVICE; i++) {
        struct vrocm_process_charge *slot = &usage->processes[i];

        if (slot->pid == 0) {
            continue;
        }
        /* kill(pid, 0) 的调用会在该进程的 pid 命名空间内得到响应，除非 Pod 请求共享 processNamespace，否则
         * 每个 Pod 的容器都会拥有自己的命名空间。因此，对于该进程所写的条目来说，结果是正确的；而对于来自
         * 其他命名空间的条目来说，则只是猜测：此处没有该条目，因此已记录的账目会被恢复；此处恰好有该条目，
         * 因此已死亡的条目会被保留。因此，在命名空间内进行的扫描是准确的，这正是每个容器对应的 ledger 路径
         * 所呈现的格局。 */
        if (kill((pid_t)slot->pid, 0) < 0 && errno == ESRCH) {
            vrocm_log(VROCM_LOG_DEBUG, "card %d: reclaiming %llu bytes from dead pid %d\n", device,
                      (unsigned long long)slot->memory_bytes, (int)slot->pid);
            memset(slot, 0, sizeof(*slot));
            continue;
        }
        total += slot->memory_bytes;
    }

    /* 从实时槽位中重新推导出来，而非递减，因此已经流失的总量由扫描操作修正，而不是继续传递下去。 */
    usage->memory_used_bytes = total;
    return before > total ? before - total : 0;
}

/* over_quota — used + bytes > quota，未进行求和即进行判断。
 *
 * 这种总和问题就是核心缺陷。bytes 是运行时所接收的工作负载量，因此位于范围顶端的请求会导致计算溢出，
 * 结果会低于预期数值——而“符合要求”这一结论，是这种设计方式绝不可出现的错误结果。减法操作则不会发生
 * 溢出，因为只有在 used <= quota 条件得到满足后才会发生。 */
static bool over_quota(unsigned long long used, unsigned long long bytes, unsigned long long quota)
{
    return used > quota || bytes > quota - used;
}

VROCM_INTERNAL enum vrocm_admit vrocm_ledger_admit(int device, unsigned long long bytes,
                                                   vrocm_alloc_fn alloc, void *ctx)
{
    struct vrocm_device_usage *usage;
    struct vrocm_process_charge *slot;
    unsigned long long quota, key = 0, charged;
    int index;

    fork_reset();
    if (!vrocm_quota_usable()) {
        /* 只在确实发起分配且因配置无效被拒绝时记录原因，每进程一次。预加载库会进入容器内每个进程，启动脚本
         * 中的 sed 等工具可能从不申请设备显存。 */
        if (__atomic_exchange_n(&config_refusal_said, 1, __ATOMIC_RELAXED) == 0) {
            vrocm_log(VROCM_LOG_DENY,
                      "no usable quota configuration; every allocation in this process is"
                      " refused\n");
        }
        return VROCM_ADMIT_DENIED_CONFIG;
    }
    if (!device_valid(device) || alloc == NULL) {
        return VROCM_ADMIT_DENIED_CONFIG;
    }
    quota = vrocm_quota_memory_bytes(device);
    if (quota == 0) {
        return VROCM_ADMIT_DENIED_CONFIG;
    }
    if (region_open() == NULL) {
        return VROCM_ADMIT_DENIED_CONFIG;
    }

    /* 先于加锁和调用运行时预留跟踪槽位。如果当前构建无法记下此次分配，就提前拒绝，避免事后撤销已成功的
     * 驱动调用。 */
    index = slot_reserve();
    if (index < 0) {
        tracking_refusals++;
        vrocm_log(VROCM_LOG_DENY, "card %d: no tracking slot left for %llu bytes; refusing\n",
                  device, bytes);
        return VROCM_ADMIT_DENIED_TRACKING;
    }

    if (!ledger_lock(device)) {
        slot_free(index);
        return VROCM_ADMIT_DENIED_CONFIG;
    }

    usage = &region_map->devices[device];
    /* 每次重新读取当前配额。容器重启后若使用新额度，应以本次配置决定能否分配。 */
    usage->memory_quota_bytes = quota;

    if (over_quota(usage->memory_used_bytes, bytes, quota)) {
        /* 只有额度紧张时才回收退出进程的记账。扫描每个活跃槽位都需要一次系统调用，有充足空间时无需承担这一
         * 开销。 */
        vrocm_ledger_reclaim(device);
    }

    if (over_quota(usage->memory_used_bytes, bytes, quota)) {
        unsigned long long used = usage->memory_used_bytes;

        ledger_unlock(device);
        slot_free(index);
        vrocm_log(VROCM_LOG_DENY, "card %d: %llu bytes refused, %llu of %llu already held\n",
                  device, bytes, used, quota);
        return VROCM_ADMIT_DENIED_QUOTA;
    }

    slot = charge_slot(usage, (int)getpid());
    if (slot == NULL) {
        ledger_unlock(device);
        slot_free(index);
        tracking_refusals++;
        vrocm_log(VROCM_LOG_DENY, "card %d: process table full; refusing %llu bytes\n", device,
                  bytes);
        return VROCM_ADMIT_DENIED_TRACKING;
    }

    /* 此时仍持有该卡的锁。先核对余量，调用真正的运行时分配，再记账。其他进程无法在这三步之间按同一笔余
     * 量重复分配。 */
    charged = bytes;
    if (!alloc(ctx, &key, &charged)) {
        ledger_unlock(device);
        slot_free(index);
        return VROCM_ADMIT_ALLOC_FAILED;
    }

    /* 分配成功却没有指针键，表示零字节请求。运行时返回空指针，而与之对应的 hipFree(NULL) 不做任何事，
     * 因此应立刻归还槽位，也没有字节需要记账。否则空批次反复申请会用尽 16,384 个跟踪槽位。原实现实测在
     * 17,001 次零字节申请后，一张无人使用、额度 4096 MiB 的卡连 64 MiB 都分不到，返回
     * hipErrorOutOfMemory。 */
    if (key == 0) {
        ledger_unlock(device);
        slot_free(index);
        return VROCM_ADMIT_OK;
    }

    /* 实际分配量可能比准入估计大。例如带行距的分配按调用方的宽度预估，按驱动返回的 stride 结算。使用实
     * 际字节数记账，报告因对齐产生的超额，但不在调用方不知情时撤销已成功的分配。下一次准入会看到超额并
     * 予以拒绝。 */
    if (charged > bytes) {
        vrocm_log(VROCM_LOG_DENY,
                  "card %d: admitted %llu bytes, allocation took %llu; charging what it took\n",
                  device, bytes, charged);
    }

    usage->memory_used_bytes += charged;
    slot->memory_bytes += charged;
    slot_commit(index, key, device, charged);
    ledger_unlock(device);
    return VROCM_ADMIT_OK;
}

VROCM_INTERNAL bool vrocm_ledger_release(unsigned long long key, int *device,
                                         unsigned long long *bytes)
{
    struct vrocm_device_usage *usage;
    struct vrocm_process_charge *slot;
    unsigned long long held;
    int card;

    fork_reset();
    if (!slot_take(key, &card, &held)) {
        return false;
    }
    if (device != NULL) {
        *device = card;
    }
    if (bytes != NULL) {
        *bytes = held;
    }
    if (region_open() == NULL || !ledger_lock(card)) {
        /* 无论怎样，密钥都丢失了；唯一损失的就是上报退款信息，而将其上报给我们无法锁定的区域只会更糟。 */
        return true;
    }

    usage = &region_map->devices[card];
    /* 将释放后的数值钳制在下界，防止重复释放导致无符号整数回绕，意外给容器增加几乎无限的额度。 */
    usage->memory_used_bytes = usage->memory_used_bytes > held ? usage->memory_used_bytes - held : 0;
    slot = charge_slot(usage, (int)getpid());
    if (slot != NULL) {
        slot->memory_bytes = slot->memory_bytes > held ? slot->memory_bytes - held : 0;
        if (slot->memory_bytes == 0) {
            memset(slot, 0, sizeof(*slot));
        }
    }
    ledger_unlock(card);
    return true;
}

VROCM_INTERNAL unsigned long long vrocm_ledger_used(int device)
{
    fork_reset();
    if (region_open() == NULL || !device_valid(device)) {
        return 0;
    }
    return region_map->devices[device].memory_used_bytes;
}

VROCM_INTERNAL unsigned long long vrocm_ledger_quota(int device)
{
    fork_reset();
    if (region_open() == NULL || !device_valid(device)) {
        return 0;
    }
    return region_map->devices[device].memory_quota_bytes;
}

VROCM_INTERNAL unsigned long long vrocm_ledger_lock_epochs(void)
{
    return lock_epochs;
}

VROCM_INTERNAL unsigned long long vrocm_ledger_tracking_refusals(void)
{
    return tracking_refusals;
}

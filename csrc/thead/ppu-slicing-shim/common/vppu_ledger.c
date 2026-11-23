#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "vppu_ledger.h"

/* --------------------------------------------------------------------------------------- 进程内锁
 * 定
 *
 * fcntl() 记录锁由进程持有，因此不会排除同级线程。这些标志会生效。它们是 GCC 的原子变量，存储在一
 * 个字节中，而非 pthread 互斥锁，正如头文件所说明的：在 glibc 2.17 版本中，使用 -lpthread 会出现在
 * DT_NEEDED 中。
 * ------------------------------------------------------------------------------------- */

static void spin_take(unsigned char *flag)
{
    unsigned spins = 0;

    while (__atomic_test_and_set(flag, __ATOMIC_ACQUIRE)) {
        /* 首先产出，再睡眠一次，但产出显然没有起到作用：持有者可能位于供应商的分配范围内，而该范围足够长，
         * 以至于旋转会导致核心烧毁。 */
        if (++spins < 128u) {
            sched_yield();
        } else {
            usleep(200);
        }
    }
}

static void spin_drop(unsigned char *flag)
{
    __atomic_clear(flag, __ATOMIC_RELEASE);
}

/* --------------------------------------------------------------------------------------- 进程本地
 * 状态以及剩余的 fork()
 *
 * 这个代码块中的所有内容都通过 fork() 被复制——包括一个在 fork 发生时被其他线程持有的自旋锁，子进程
 * 将永远等待这个锁，因为持有者并不存在于子进程中。这并非什么特殊机制：使用 fork 方法启动的数据加载
 * 器在每次更新时都会执行此操作，而此时其他线程正在进行资源分配。
 *
 * pthread_atfork 是常见的解决方案，但在此处不可用——使用 -lpthread 会使程序进入 DT_NEEDED 状态，而
 * 发布的库可能不包含该状态。因此，该状态会标记出拥有它的进程ID，并在新进程首次使用时被重置。
 *
 * 该重置操作是SERIALISED的，必须如此：多个线程同时执行第一次账本调用时，都会看到不属于自己的进程ID
 * ；而第二个线程在第一个线程已持有锁的情况下清除自旋锁，就会破坏锁的完整性，而不是获得任何资源。这
 * 不仅仅是一个分叉问题——在启动时线程并发分配分配资源的新进程结构也是一样的，因此仅依靠进程ID是不够
 * 的。恰好有一个线程赢得机会进行重置；其他线程则等待它表明立场。
 *
 * 这种映射本身并未被特意重置：它是在继承的描述符上使用 MAP_SHARED，因此仍然属于同一区域且正确无误。
 * 记录锁也不被继承，这也就使得清除进程内标志的操作是安全的——子进程下一次锁尝试会因父进程的记录锁而
 * 受阻，就像其他进程一样。
 * ------------------------------------------------------------------------------------- */

static unsigned char region_spin;
static unsigned char card_spin[VPPU_MAX_DEVICES];
static unsigned char alloc_spin;

/* 此线程所持有的卡件及深度。属于线程本地变量，因为记录锁是进程级锁：在调用厂商的嵌套函数时解锁，会
 * 释放外部分配仍依赖的锁。按线程而非按进程分配，因为兄弟线程不得将其他线程的锁误认为是自己的重入锁
 * 而跳过获取。
 *
 * 初始执行模式，而非默认的通用动态模型，这并非微优化：共享对象中的通用动态 TLS 通过 __tls_get_addr
 * 进行解析，这导致 ld-linux-x86-64.so.2 处于 DT_NEEDED 状态——而 DT_NEEDED 本质上就是 libc.so.6，这
 * 是该库所面临的硬性限制。此处使用初始执行模式是正确的，因为该库仅通过 LD_PRELOAD 或
 * /etc/ld.so.preload 引入，因此始终属于初始执行集合；它永远不会被进行 dlopen() 操作，而这是该模型
 * 会失效的一种情况。 */
static __thread int held_device __attribute__((tls_model("initial-exec"))) = -1;
static __thread unsigned held_depth __attribute__((tls_model("initial-exec")));

/* 该过程的槽位在每张卡的表中，一旦拥有一个，就会存在。通过进程ID而非直接确认，因此通过 fork()继承
 * 的槽位或已被重新使用的进程所留下的槽位，不会与我们的槽位相混淆。 */
static int my_slot_index[VPPU_MAX_DEVICES];
static int32_t my_slot_pid[VPPU_MAX_DEVICES];

static struct vppu_region *region_map;
static bool region_failed;
static int region_fd = -1;

static void reset_alloc_table(void);

static void adopt_state(void)
{
    /* 两个词而非一个：claimed表示恰好一个线程获胜的比赛结果；adopted则表示下方重置操作已完成的公告。如
     * 果只使用一个词，则输掉比赛的线程仍会返回，而获胜者仍在释放自旋锁，随后会获取即将被清零的锁。 */
    static int claimed;
    static int adopted;
    int self = (int)getpid();

    if (__atomic_load_n(&adopted, __ATOMIC_ACQUIRE) == self) {
        return;
    }

    int seen = __atomic_load_n(&claimed, __ATOMIC_RELAXED);
    if (seen != self
        && __atomic_compare_exchange_n(&claimed, &seen, self, false, __ATOMIC_ACQ_REL,
                                       __ATOMIC_RELAXED)) {
        region_spin = 0;
        alloc_spin = 0;
        memset(card_spin, 0, sizeof(card_spin));
        held_device = -1;
        held_depth = 0;
        memset(my_slot_pid, 0, sizeof(my_slot_pid));

        /* 父进程的分配并非本进程可以释放的：通过 fork()继承的设备指针所指向的上下文，是子进程没有的。删除
         * 这些记录就能避免其中一项被计入子进程从未进行的记账。 */
        reset_alloc_table();

        __atomic_store_n(&adopted, self, __ATOMIC_RELEASE);
        return;
    }

    /* 另一个线程持有该任务。目前还剩下的工作只有少数几个没有系统调用也没有锁的存储操作，因此等到它宣布
     * 结果比任何能更礼貌等待的结构更划算——而且这是每个进程运行一次，而非每次分配都运行。 */
    while (__atomic_load_n(&adopted, __ATOMIC_ACQUIRE) != self) {
        sched_yield();
    }
}

static const char *ledger_path(void)
{
    const char *path = getenv(VPPU_ENV_LEDGER_PATH);

    return (path != NULL && *path != '\0') ? path : VPPU_LEDGER_DEFAULT_PATH;
}

/* range_lock — 阻塞等待此进程独占持有账本文件的某个字节范围。仅支持独占模式：该拦截库的读取端不持
 * 有锁。 */
static bool range_lock(int fd, off_t offset, off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = len;

    while (fcntl(fd, F_SETLKW, &fl) != 0) {
        if (errno != EINTR) {
            return false;
        }
    }
    return true;
}

static void range_unlock(int fd, off_t offset, off_t len)
{
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = len;
    (void)fcntl(fd, F_SETLK, &fl);
}

static bool all_zero(const void *base, size_t len)
{
    const unsigned char *bytes = base;

    for (size_t i = 0; i < len; i++) {
        if (bytes[i] != 0) {
            return false;
        }
    }
    return true;
}

/* region_open — 打开账本，如果是第一个进程，会创建并盖章标记它。
 *
 * “魔法与版本”检查是能够针对此文件编写抓取工具的原因：本构建版本中未识别到的、由布局工具标记的区域
 * 被标记为REFUSED，而非误解析结果；此外，存在但非我们拥有的文件也永远不会被覆盖。 */
/* opens_with_magic — 即一个文件虽然长度不足无法容纳该区域，但是否以我们的“魔法”开头。这就是
 * region_open()可能增长的那个短文件，且这不是假设性的：一个在写入标记之前就结束并删除该文件的进程
 * 就会产生完全相同的结果。 */
static bool opens_with_magic(int fd)
{
    char magic[VPPU_REGION_MAGIC_BYTES];

    return pread(fd, magic, sizeof(magic), 0) == (ssize_t)sizeof(magic)
           && memcmp(magic, VPPU_REGION_MAGIC, sizeof(magic)) == 0;
}

static struct vppu_region *region_open(void)
{
    const char *path = ledger_path();

    /* 最初是窄槽位，后来扩大为宽槽位，因此该模式不依赖于创建过程的 umask：容器中的每个进程都必须能够映
     * 射到它，且这些进程不必都以相同的用户身份运行。 */
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd >= 0) {
        (void)fchmod(fd, 0666);
    } else if (errno == EEXIST) {
        fd = open(path, O_RDWR | O_CLOEXEC);
    }
    if (fd < 0) {
        vppu_log(VPPU_LOG_DENY, "cannot open the ledger %s: %s\n", path, strerror(errno));
        return NULL;
    }

    /* 魔法的自身字节就是创建锁定。偏移量0由布局机制冻结，因此两个急于创建该区域的进程会就如何串行化处
     * 理达成一致，即使它们基于不同的版本构建。 */
    if (!range_lock(fd, 0, VPPU_REGION_MAGIC_BYTES)) {
        vppu_log(VPPU_LOG_DENY, "cannot lock the ledger %s: %s\n", path, strerror(errno));
        close(fd);
        return NULL;
    }

    struct vppu_region *region = NULL;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        vppu_log(VPPU_LOG_DENY, "cannot size the ledger %s: %s\n", path, strerror(errno));
        goto done;
    }
    /* 已经包含某些内容的文件在重新调整大小之前会被判定。先扩展它，再拒绝修改它，这仍然会对其进行修改；
     * 而 HGGC_LEDGER_PATH 是一个配置好的路径——如果输入错误的名称，就会影响其他重要内容。可能只会被扩展
     * 的短文件只有那些为空的文件（此过程刚刚创建了它），或者那些已经以我们的特殊方式打开的文件（即创建
     * 者于 ftruncate 和 stamp 之间已去世的区域）。 */
    if ((size_t)st.st_size < sizeof(*region)) {
        if (st.st_size > 0 && !opens_with_magic(fd)) {
            vppu_log(VPPU_LOG_DENY, "%s is not a vppu ledger — refusing to resize it\n", path);
            goto done;
        }
        if (ftruncate(fd, (off_t)sizeof(*region)) != 0) {
            vppu_log(VPPU_LOG_DENY, "cannot grow the ledger %s: %s\n", path, strerror(errno));
            goto done;
        }
    }

    void *mapped = mmap(NULL, sizeof(*region), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        vppu_log(VPPU_LOG_DENY, "cannot map the ledger %s: %s\n", path, strerror(errno));
        goto done;
    }
    region = mapped;

    if (memcmp(region->magic, VPPU_REGION_MAGIC, sizeof(region->magic)) == 0) {
        if (region->layout_version != VPPU_REGION_VERSION) {
            vppu_log(VPPU_LOG_DENY,
                     "the ledger %s is layout version %u and this build speaks %u — "
                     "refusing it rather than misparsing it\n",
                     path, region->layout_version, VPPU_REGION_VERSION);
            munmap(region, sizeof(*region));
            region = NULL;
        }
        goto done;
    }

    if (!all_zero(region, sizeof(*region))) {
        vppu_log(VPPU_LOG_DENY, "%s is not a vppu ledger — refusing to overwrite it\n", path);
        munmap(region, sizeof(*region));
        region = NULL;
        goto done;
    }

    region->layout_version = VPPU_REGION_VERSION;
    region->header_bytes = (uint32_t)offsetof(struct vppu_region, devices);
    region->device_slots = VPPU_MAX_DEVICES;
    region->process_slots = VPPU_MAX_PROCESSES_PER_DEVICE;
    /* 最后的魔法出现在屏障之后：它标志着某个区域对遵循文档布局的读取方是有效的，无需使用此锁定。 */
    __atomic_thread_fence(__ATOMIC_RELEASE);
    memcpy(region->magic, VPPU_REGION_MAGIC, sizeof(region->magic));

done:
    range_unlock(fd, 0, VPPU_REGION_MAGIC_BYTES);
    if (region == NULL) {
        close(fd);
        return NULL;
    }
    region_fd = fd;
    return region;
}

/* region — 映射后的账本，或者映射失败时的 NULL。
 *
 * 通过延迟映射而非从构造函数中加载：此库被预加载到容器内的每个进程中，而那种从不进行分配的情况不应
 * 创建文件。这种失败会被记录下来，因此损坏的账本会导致每个进程需要一次 open()操作，而不是每次分配
 * 时都如此。 */
static struct vppu_region *region(void)
{
    adopt_state();
    spin_take(&region_spin);
    if (region_map == NULL && !region_failed) {
        region_map = region_open();
        region_failed = (region_map == NULL);
    }
    struct vppu_region *mapped = region_map;
    spin_drop(&region_spin);
    return mapped;
}

/* --------------------------------------------------------------------------------------- 每张卡的
 * 锁定机制 ------------------------------------------------------------------------------------- */

static off_t lock_offset(int device)
{
    return (off_t)offsetof(struct vppu_region, lock_arena) + device;
}

bool vppu_ledger_lock(int device)
{
    adopt_state();
    if (device < 0 || device >= VPPU_MAX_DEVICES) {
        return false;
    }
    if (held_device == device) {
        held_depth++;
        return true;
    }
    /* 持有一张卡并请求另一张卡时，需要一个锁定顺序，这个拦截库没有理由定义这一顺序——没有中间插入的条目
     * 会触及两张卡。拒绝操作则会导致失败关闭。 */
    if (held_device >= 0) {
        vppu_log(VPPU_LOG_DEBUG, "declining the ledger for device %d while holding device %d\n",
                 device, held_device);
        return false;
    }
    if (region() == NULL) {
        return false;
    }

    spin_take(&card_spin[device]);
    if (!range_lock(region_fd, lock_offset(device), 1)) {
        vppu_log(VPPU_LOG_DENY, "cannot lock device %d in the ledger: %s\n", device,
                 strerror(errno));
        spin_drop(&card_spin[device]);
        return false;
    }

    held_device = device;
    held_depth = 1;
    region_map->devices[device].lock_holder_pid = (int32_t)getpid();
    return true;
}

bool vppu_ledger_holding(void)
{
    /* 首先采用：fork()会将此线程的“我持有卡N”标志复制到子进程中，而子进程中的锁可能不存在；如果调用者
     * 在首次锁定之前询问这个问题，否则就会被告知自己已经持有该锁。 */
    adopt_state();
    return held_device >= 0;
}

void vppu_ledger_unlock(int device)
{
    if (held_device != device || held_depth == 0) {
        return;
    }
    if (--held_depth > 0) {
        return;
    }

    region_map->devices[device].lock_holder_pid = 0;
    range_unlock(region_fd, lock_offset(device), 1);
    held_device = -1;
    spin_drop(&card_spin[device]);
}

/* --------------------------------------------------------------------------------------- 记账
 * ------------------------------------------------------------------------------------- */

static bool pid_alive(int32_t pid)
{
    if (pid == (int32_t)getpid()) {
        return true;
    }
    if (kill((pid_t)pid, 0) == 0) {
        return true;
    }
    /* EPERM表示它存在且属于其他人；只有ESRCH表示它已消失。 */
    return errno != ESRCH;
}

/* forget_slot — 释放一个槽位，并将该槽位的字节从卡的总容量中移除。当该槽位属于已不存在的进程时使用，
 * 因此这些字节不归属于任何人。 */
static void forget_slot(struct vppu_device_usage *device, struct vppu_process_charge *slot)
{
    device->memory_used_bytes = (slot->memory_bytes > device->memory_used_bytes)
                                    ? 0ULL
                                    : device->memory_used_bytes - slot->memory_bytes;
    slot->pid = 0;
    slot->memory_bytes = 0ULL;
}

/* my_slot — 此进程用于一张卡的记账槽位。必须在持有卡锁定的情况下调用；当表已满且请求了create操作时，
 * 将返回NULL。 */
static struct vppu_process_charge *my_slot(int device, bool create)
{
    struct vppu_device_usage *usage = &region_map->devices[device];
    int32_t self = (int32_t)getpid();

    if (my_slot_pid[device] == self) {
        return &usage->processes[my_slot_index[device]];
    }

    int reusable = -1;
    for (int i = 0; i < VPPU_MAX_PROCESSES_PER_DEVICE; i++) {
        struct vppu_process_charge *slot = &usage->processes[i];

        if (slot->pid == self) {
            /* 按进程 ID 来看属于我们的，但此进程从未记录过它：该进程 ID 被重新使用，因此其背后的记账属于已消失
             * 的进程。 */
            forget_slot(usage, slot);
            slot->pid = self;
            my_slot_pid[device] = self;
            my_slot_index[device] = i;
            return slot;
        }
        if (reusable < 0 && (slot->pid == 0 || !pid_alive(slot->pid))) {
            reusable = i;
        }
    }

    if (!create || reusable < 0) {
        return NULL;
    }

    struct vppu_process_charge *slot = &usage->processes[reusable];
    forget_slot(usage, slot);
    slot->pid = self;
    my_slot_pid[device] = self;
    my_slot_index[device] = reusable;
    return slot;
}

unsigned long long vppu_ledger_used(int device)
{
    if (device < 0 || device >= VPPU_MAX_DEVICES || region() == NULL) {
        return 0ULL;
    }
    return region_map->devices[device].memory_used_bytes;
}

void vppu_ledger_note_config(int device, unsigned long long quota_bytes,
                            unsigned int sm_percent)
{
    if (region_map == NULL || device < 0 || device >= VPPU_MAX_DEVICES) {
        return;
    }
    region_map->devices[device].memory_quota_bytes = quota_bytes;
    region_map->devices[device].sm_limit_percent = sm_percent;
}

void vppu_ledger_note_util(int device, unsigned int sm_percent)
{
    if (device < 0 || device >= VPPU_MAX_DEVICES || region() == NULL) {
        return;
    }
    region_map->devices[device].sm_util_percent = sm_percent;
}

struct vppu_pid_state *vppu_ledger_control(int device)
{
    if (device < 0 || device >= VPPU_MAX_DEVICES || region() == NULL) {
        return NULL;
    }
    return &region_map->devices[device].control;
}

bool vppu_ledger_has_process(int device, int pid)
{
    if (device < 0 || device >= VPPU_MAX_DEVICES || pid <= 0 || region() == NULL) {
        return false;
    }

    const struct vppu_device_usage *usage = &region_map->devices[device];
    for (int i = 0; i < VPPU_MAX_PROCESSES_PER_DEVICE; i++) {
        if (usage->processes[i].pid == (int32_t)pid) {
            return true;
        }
    }
    return false;
}

bool vppu_ledger_charge(int device, unsigned long long bytes)
{
    if (region_map == NULL || device < 0 || device >= VPPU_MAX_DEVICES) {
        return false;
    }

    struct vppu_process_charge *slot = my_slot(device, true);
    if (slot == NULL) {
        vppu_log(VPPU_LOG_DENY,
                 "device %d has no free ledger slot — %d processes already hold a charge\n",
                 device, VPPU_MAX_PROCESSES_PER_DEVICE);
        return false;
    }

    slot->memory_bytes += bytes;
    region_map->devices[device].memory_used_bytes += bytes;
    return true;
}

void vppu_ledger_refund(int device, unsigned long long bytes)
{
    if (region_map == NULL || device < 0 || device >= VPPU_MAX_DEVICES) {
        return;
    }

    struct vppu_device_usage *usage = &region_map->devices[device];
    struct vppu_process_charge *slot = my_slot(device, false);

    /* 对两个数值进行限制而非回绕处理：禁止进行双重释放操作，因为这样会让容器拥有无限制配额，而回绕减法
     * 操作正是如此。 */
    if (slot != NULL) {
        slot->memory_bytes = (bytes > slot->memory_bytes) ? 0ULL : slot->memory_bytes - bytes;
    }
    usage->memory_used_bytes =
        (bytes > usage->memory_used_bytes) ? 0ULL : usage->memory_used_bytes - bytes;
}

unsigned long long vppu_ledger_reclaim(int device)
{
    if (region_map == NULL || device < 0 || device >= VPPU_MAX_DEVICES) {
        return 0ULL;
    }

    struct vppu_device_usage *usage = &region_map->devices[device];
    unsigned long long live = 0ULL;
    for (int i = 0; i < VPPU_MAX_PROCESSES_PER_DEVICE; i++) {
        struct vppu_process_charge *slot = &usage->processes[i];

        if (slot->pid == 0) {
            continue;
        }
        if (!pid_alive(slot->pid)) {
            slot->pid = 0;
            slot->memory_bytes = 0ULL;
            continue;
        }
        live += slot->memory_bytes;
    }

    /* 总数值是重新计算的，而非递减的，因此它与按进程划分的明细之间的任何差异也在这里得到了修正。 */
    unsigned long long before = usage->memory_used_bytes;
    usage->memory_used_bytes = live;
    return (before > live) ? before - live : 0ULL;
}

/* --------------------------------------------------------------------------------------- 进程本地
 * 键 -> (card, bytes) 映射
 * ------------------------------------------------------------------------------------- */

#define VPPU_ALLOC_SLOTS 1024u

/* 两个保留的键值。空值表示结束探测链；DEAD 表示其条目已被占用。释放后的槽位不得变回空状态：由于开
 * 放寻址机制会截断所有经过该槽位的键的探测链，因此下一个无冲突的空闲槽位将找不到任何内容，其字节会
 * 一直被标记为已记账，直至进程结束。 */
#define VPPU_ALLOC_EMPTY 0ULL
#define VPPU_ALLOC_DEAD 0xFFFFFFFFFFFFFFFFULL

struct vppu_alloc_slot {
    unsigned long long key;
    unsigned long long bytes;
    int device;
};

static struct vppu_alloc_slot alloc_table[VPPU_ALLOC_SLOTS];
static unsigned long long alloc_overflows;

static void reset_alloc_table(void)
{
    memset(alloc_table, 0, sizeof(alloc_table));
    alloc_overflows = 0ULL;
}

/* alloc_home — 设备指针和 VMM 处理句柄都是页对齐的，因此基本上每次分配的 % SLOTS 值均为 0，该表就
 * 会变成由槽位组成的单一链状结构
 * 0. 混合密钥，以便槽位得到实际使用。 */
static unsigned int alloc_home(unsigned long long key)
{
    unsigned long long mixed = key * 0x9E3779B97F4A7C15ULL;

    return (unsigned int)((mixed >> 32) % VPPU_ALLOC_SLOTS);
}

static bool alloc_reserved(unsigned long long key)
{
    return key == VPPU_ALLOC_EMPTY || key == VPPU_ALLOC_DEAD;
}

void vppu_alloc_record(unsigned long long key, int device, unsigned long long bytes)
{
    adopt_state();
    if (alloc_reserved(key)) {
        return;
    }

    spin_take(&alloc_spin);
    unsigned int home = alloc_home(key);
    for (unsigned int probe = 0; probe < VPPU_ALLOC_SLOTS; probe++) {
        struct vppu_alloc_slot *slot = &alloc_table[(home + probe) % VPPU_ALLOC_SLOTS];

        if (slot->key == VPPU_ALLOC_EMPTY || slot->key == VPPU_ALLOC_DEAD) {
            slot->key = key;
            slot->bytes = bytes;
            slot->device = device;
            spin_drop(&alloc_spin);
            return;
        }
    }

    /* 完整表格：记账行为有效，记录被删除，因为由该拦截库自身的记账行为导致的否定会被作为配额问题被报告。
     * 如此统计，才能让计数器 dump 显示该行为确实发生。 */
    alloc_overflows++;
    spin_drop(&alloc_spin);
}

bool vppu_alloc_take(unsigned long long key, int *device, unsigned long long *bytes)
{
    adopt_state();
    if (alloc_reserved(key)) {
        return false;
    }

    spin_take(&alloc_spin);
    unsigned int home = alloc_home(key);
    for (unsigned int probe = 0; probe < VPPU_ALLOC_SLOTS; probe++) {
        struct vppu_alloc_slot *slot = &alloc_table[(home + probe) % VPPU_ALLOC_SLOTS];

        if (slot->key == key) {
            *device = slot->device;
            *bytes = slot->bytes;
            slot->key = VPPU_ALLOC_DEAD;
            slot->bytes = 0ULL;
            slot->device = -1;
            spin_drop(&alloc_spin);
            return true;
        }
        if (slot->key == VPPU_ALLOC_EMPTY) {
            break;
        }
    }
    spin_drop(&alloc_spin);
    return false;
}

unsigned long long vppu_alloc_overflows(void)
{
    return alloc_overflows;
}

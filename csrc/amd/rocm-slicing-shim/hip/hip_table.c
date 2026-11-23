#include "common/vrocm_ledger.h"
#include "common/vrocm_log.h"
#include "common/vrocm_quota.h"
#include "hip/hip_resolve.h"
#include "hip/hip_table.h"

/* 一个条目在变得安静之前，会有多少调用者参与。三个就足以判断一个对象或多个对象是否能够触及该条目，
 * 这就是整个问题所在；同时这个数量也要足够小，这样在循环中进行的工作负载分配不会将其淹没。 */
#define TRACE_LIMIT 3

static struct vrocm_entry *entries;
static volatile int entries_lock;

static void link_entry(struct vrocm_entry *entry)
{
    while (__sync_lock_test_and_set(&entries_lock, 1)) {
        /* 这种读取是原子操作，因为写入操作遵循 volatile 规则，这些规则仅针对当前线程的访问，而与其他线程无
         * 关；因此，另一个线程使用原子 RMW 进行数据设置的普通读取操作属于数据竞争，无论该指令被编译成多少
         * 条指令。采用“宽松”模式即可——真正重要的操作是此循环中的测试与设置操作。 */
        while (__atomic_load_n(&entries_lock, __ATOMIC_RELAXED)) {
            /* 与卡锁的锁定暂停时间相同，但保持的时间要短得多：仅三行代码，且仅在条目首次调用时出现。这种竞争状
             * 态更像是一种启动状态，而非稳定状态——一个框架启动其工作线程时，会同时为所有线程进行首次分配——而且
             * 在不浪费持有此锁的线程的槽位的情况下，也不消耗任何资源。 */
            VROCM_SPIN_PAUSE();
        }
    }
    if (entry->next == NULL && entries != entry) {
        entry->next = entries;
        /* 与发布版本一同发布，因为读取方无法锁定该资源。析构函数遍历列表而不进行锁定操作——这种遍历无法确保
         * 锁定，因为如果线程在中间被杀死，会留下标志位被设置的状态，导致进程在退出时卡住——因此，头指针必须
         * 自行承担排序逻辑。next 位于上述位置，因此获取该头指针的读取方会看到一个已经完成的链表；没有任何
         * 元素会被解除链接或释放，这确保了后续遍历的安全性。 */
        __atomic_store_n(&entries, entry, __ATOMIC_RELEASE);
    }
    __sync_lock_release(&entries_lock);
}

VROCM_INTERNAL void vrocm_entry_hit(struct vrocm_entry *entry, void *return_address)
{
    if (__sync_fetch_and_add(&entry->calls, 1) == 0) {
        link_entry(entry);
    }

    /* 采用宽松的原子操作而非简单的读取和递增操作：calls和denials这些操作本身即为原子操作，而这一操作也
     * 是从相同的包装层实现的，这些包装层在包含数十个线程的框架中运行。没有任何限制条件针对它——它仅规定
     * 了一条记录条目会打印多少原始行数——因此整个要求都相当宽松，且上限也几乎是任意的：通过测试的线程会
     * 一起打印内容，最多会超出其数量。 */
    if (vrocm_log_level() < VROCM_LOG_DEBUG ||
        __atomic_load_n(&entry->traced, __ATOMIC_RELAXED) >= TRACE_LIMIT) {
        return;
    }
    __atomic_fetch_add(&entry->traced, 1, __ATOMIC_RELAXED);

    {
        const char *caller = vrocm_caller_of(return_address);

        /* 在 ALLOCATING 条目下将 libamdhip64 命名为一条线，表明预加载无法做到的事情——在运行时内部调用时拒
         * 绝执行——开始变得重要了。今天测量结果显示这种情况从未出现：在 PyTorch 运行过程中，每次调用都来自
         * 框架自身的对象，导致内存溢出（OOM）。详见规范中“风险”部分所述的升级路径。 */
        vrocm_log(VROCM_LOG_DEBUG, "%s <- %s\n", entry->name,
                  caller != NULL ? caller : "(unknown)");
    }
}

VROCM_INTERNAL void vrocm_entry_denied(struct vrocm_entry *entry)
{
    __sync_fetch_and_add(&entry->denials, 1);
}

/* 负载标记器。在此进行验证，而非从通用代码中的构造函数开始，这样就能明确顺序，而不依赖于链接顺序，
 * 这正是 F1 所要求的：这是已发布的库自身的构造函数，它在任何包装器运行之前就执行。 */
__attribute__((constructor)) static void vrocm_load(void)
{
    vrocm_log(VROCM_LOG_DEBUG, "loaded\n");
    vrocm_quota_validate();
}

/* 计数器 dump。仅关联实际执行的条目，因此该 dump 看起来像是“此进程所触及的内容”，而非拦截库可能触
 * 及的所有内容的清单——这正是 cases grep 所呈现的形式。 */
__attribute__((destructor)) static void vrocm_unload(void)
{
    struct vrocm_entry *entry;
    int device;

    if (vrocm_log_level() < VROCM_LOG_DEBUG) {
        return;
    }
    /* 与 link_entry 的释放相配合的拦截机制，以及对这两个计数器的宽松读取，这些计数器由每个包装器原子地
     * 增加：在析构函数执行时，线程仍可以运行，因此此 dump 是唯一一个与整个表同时运行的读取器。这里的每
     * 个数值都代表一个计数，而这一行所代表的含义，也来自于对少量增加的计数读取。 */
    for (entry = __atomic_load_n(&entries, __ATOMIC_ACQUIRE); entry != NULL; entry = entry->next) {
        vrocm_log(VROCM_LOG_DEBUG, "counter %s calls=%llu denials=%llu\n", entry->name,
                  __atomic_load_n(&entry->calls, __ATOMIC_RELAXED),
                  __atomic_load_n(&entry->denials, __ATOMIC_RELAXED));
    }
    for (device = 0; device < VROCM_MAX_DEVICES; device++) {
        unsigned long long quota = vrocm_ledger_quota(device);

        if (quota != 0) {
            vrocm_log(VROCM_LOG_DEBUG, "counter card %d quota=%llu used=%llu\n", device, quota,
                      vrocm_ledger_used(device));
        }
    }
    vrocm_log(VROCM_LOG_DEBUG, "counter tracking_refusals=%llu lock_epochs=%llu\n",
              vrocm_ledger_tracking_refusals(), vrocm_ledger_lock_epochs());
}

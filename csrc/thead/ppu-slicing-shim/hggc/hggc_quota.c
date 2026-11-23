/* hggc_quota.c — libvppu.so中的执行部分：每张卡级别的驱动层VRAM配额，以及用于解决和统计两个配额的
 * 相关表。
 *
 * 产品代码，被注入到切片工作负载容器中。此文件包含内存分配决策；计算相关部分位于 hggc_compute.c 中，
 * 而插入的条目则分别位于 hggc_mem.c、hggc_mem_v1.c、hggc_entry.c 和 hggc_launch.c 中。
 *
 * 与 hgml shim 不同，且必须如此：前者通过 HGML 让 ppu-smi 能够看到配额，而 ppu-smi 既无法执行也无
 * 法监控分配操作。而这一版本在驱动层进行拦截，实际的分配操作就在此处发生。
 *
 * 不需要 dlsym 拦截。libhggcrt（工作负载调用的运行时层）在 DT_NEEDED 中列出 libhggc.so，并通过 PLT
 * 访问它，因此 LD_PRELOAD 定义通过简单的符号插入方式就能生效。HGML 需要 dlsym 拦截，仅因为 ppu-smi
 * 使用 dlopen() 加载它，并且会在指定的句柄上完成解析。
 *
 * 插入的名称属于 DRIVER 层的内容。这就是实际情况，并非疏漏：工作负载会调用 hggcMalloc 函数，该函数
 * 位于 libhggcrt.13.0.so 中，而这里的计数器则证明该调用最终会流入 libhggc.so。如果直接插入
 * hggcMalloc 本身，则无法提供任何答案。
 *
 * 配额按卡计算，由实际分配到的设备决定。这不是命名细节：任何分配条目都不包含设备参数——它们会使用调
 * 用线程当前上下文所依赖的设备进行记账——因此，确定要针对哪张卡进行记账的唯一方法就是询问，这正是
 * vppu_hggc_device() 的功能所在。容纳多个卡的容器会从分配器那里获得每张卡对应的数值，就像 NVIDIA
 * 团队注入 CUDA_DEVICE_MEMORY_LIMIT_<i> 那样，而单个容器级别的总数值则无法体现这一点。
 *
 * 这笔账本属于整个进程区域，该卡的锁定状态从进入决策阶段一直保持到供应商的分配结果返回为止。这两点
 * 都很重要：一个仅属于某个进程的账本会让容器内的每个进程都能获取全部数据；而在分配结果返回之前释放
 * 锁定状态，则会让两个调用者同时通过检查并超出配额。这些字节会在实际调用之前被记账，如果调用失败则
 * 会被退还，因此不存在内存被保留但未被记账的情况。
 *
 * 存在两个输出，因此内存路径门无法因错误原因通过：
 * - 每次调用时计数 COUNTER，在程序退出时输出，因此“该调用经过 libhggc.so”是通过计数调用次数来确定
 * 的，而非从链接或符号证据中推断出来的；
 * - 在拒绝操作中会留下明确的“被拒绝”标记，因此此配额下的拒绝与因其他原因导致的故障不同。这两种情况
 * 都属于诊断信息，而非稳定状态输出，因此它们位于日志级别 2，而被拒绝的标记则位于级别 1 —— 详见
 * common/vppu.h。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>

/* hggc.h仅包含<stdlib.h>和<stdint.h>，因此在其声明中既不使用NULL也不使用bool；请提前引入这两个头文
 * 件，因为hgml.h的消费者需要这些头文件。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>

#include "common/vppu_ledger.h"
#include "common/vppu_quota.h"
#include "hggc/hggc_quota.h"

/* 导出的 ABI 名称，而非源代码级别的名称：hggc.h 中将 hgMemAlloc 映射到 hgMemAlloc_v2，就像 cuda.h
 * 将 cuMemAlloc 映射到 cuda.h 一样；此外，dlsym() 必须指定该库实际导出的符号。下面的纯形式即为 v1
 * ABI，而 libhggc.so 也导出了这些符号。 */
static const char *const entry_names[] = {
    "hgMemAlloc_v2",
    "hgMemAlloc",
    "hgMemAllocAsync",
    "hgMemAllocAsync_ptsz",
    "hgMemAllocFromPoolAsync",
    "hgMemAllocFromPoolAsync_ptsz",
    "hgMemAllocManaged",
    "hgMemAllocPitch_v2",
    "hgMemAllocPitch",
    "hgMemCreate",

    "hgMemFree_v2",
    "hgMemFree",
    "hgMemFreeAsync",
    "hgMemFreeAsync_ptsz",
    "hgMemRelease",

    "hgMemGetInfo_v2",
    "hgMemGetInfo",

    "hgMemAllocHost_v2",
    "hgMemAllocHost",
    "hgMemFreeHost",

    "hgMemMap",
    "hgMemMapArrayAsync",
    "hgMemMapArrayAsync_ptsz",
    "hgMemUnmap",

    "hgMemPoolCreate",
    "hgMemPoolDestroy",
    "hgMemPoolExportPointer",
    "hgMemPoolExportToShareableHandle",
    "hgMemPoolGetAccess",
    "hgMemPoolGetAttribute",
    "hgMemPoolImportFromShareableHandle",
    "hgMemPoolImportPointer",
    "hgMemPoolSetAccess",
    "hgMemPoolSetAttribute",
    "hgMemPoolTrimTo",

    "hgGetProcAddress_v2",
    "hgGetProcAddress",
    "hgGetExportTable",

    "hgLaunchKernel",
    "hgLaunchKernel_ptsz",
    "hgLaunchKernelEx",
    "hgLaunchKernelEx_ptsz",
    "hgLaunchKernelExAD",
    "hgLaunchKernelExAD_ptsz",
    "hgLaunchCooperativeKernel",
    "hgLaunchCooperativeKernel_ptsz",
    "hgLaunchCooperativeKernelMultiDevice",
    "hgLaunch",
    "hgLaunchGrid",
    "hgLaunchGridAsync",

    "hgGraphLaunch",
    "hgGraphLaunch_ptsz",

    "hgLaunchHostFunc",
    "hgLaunchHostFunc_ptsz",
};

/* 那个负责防止枚举和账本发生混乱的拦截器。没有它，添加到其中某一个的新的条目会改变其后的所有名称，
 * 而包装层会默默地计算并解析错误的符号。 */
_Static_assert(sizeof(entry_names) / sizeof(*entry_names) == VPPU_ENTRY_COUNT,
               "every interposed entry needs its exported ABI name");

static unsigned long long entry_calls[VPPU_ENTRY_COUNT];

/* 供应商对每条条目的定义，在首次使用时被解析。被缓存是因为每次分配时调用 dlsym() 是一种热路径不需
 * 要的成本，而且因为解析器条目在单次调用中会与这些条目逐一比较。两个线程在此竞争时会对同一个地址进
 * 行两次解析并存储相同的值；之后会设置 resolved 字段，因此竞争读取器永远不会看到尚未填充的 NULL。 */
static void *next_fn[VPPU_ENTRY_COUNT];
static bool next_resolved[VPPU_ENTRY_COUNT];

/* 这个拦截库对每个条目的定义也采用相同的缓存方式。只有解析器条目需要它，而且这些条目必须针对调用者
 * 询问的每一个符号都可用——如果采用对几百个名称进行绑定遍历的方式，那么每个名称都会重复整个查找表的
 * 操作。 */
static void *self_fn[VPPU_ENTRY_COUNT];
static bool self_resolved[VPPU_ENTRY_COUNT];

/* 这个进程是否曾经到达账本。计数器转储会向账本请求其总计数值，而请求映射操作则涉及该区域——一个没有
 * 进行任何分配的过程（容器中的每个进程都带有这种预加载），在退出过程中不得创建该文件。 */
static bool ledger_touched;

const char *vppu_hggc_name(enum vppu_entry entry)
{
    return entry_names[entry];
}

void vppu_hggc_count(enum vppu_entry entry)
{
    entry_calls[entry]++;
}

void *vppu_hggc_next(enum vppu_entry entry)
{
    if (!next_resolved[entry]) {
        void *fn = dlsym(RTLD_NEXT, entry_names[entry]);

        if (fn == NULL) {
            vppu_log(VPPU_LOG_DENY, "no next %s — libhggc.so not in the search order?\n",
                     entry_names[entry]);
        }
        next_fn[entry] = fn;
        next_resolved[entry] = true;
    }
    return next_fn[entry];
}

void *vppu_hggc_self(enum vppu_entry entry)
{
    if (!self_resolved[entry]) {
        /* RTLD_DEFAULT，不是函数的地址：此文件无法标注定义，这些定义存在于其他编译单元中，而地址表又是需要
         * 与枚举保持一致的另一项内容。全局搜索顺序可以解决这一问题——该拦截库已预加载，因此会优先被处理，其
         * 答案就是它自己的定义，也就是调用者通过链接器访问该符号时得到的那个定义。 */
        self_fn[entry] = dlsym(RTLD_DEFAULT, entry_names[entry]);
        self_resolved[entry] = true;
    }
    return self_fn[entry];
}

/* vppu_hggc_device — 即调用线程的工作即将落地的那张卡。
 *
 * hgCtxGetDevice 报告调用线程当前上下文所对应的设备，这也就是分配条目进行记账的対象，以及启动条目
 * 进行计算所依赖的；这些函数均不接受设备参数。注意这里的普通名称就是 ABI 名称：hggc.h 将
 * hgMemAlloc 映射到 hgMemAlloc_v2，但保留 hgCtxGetDevice 不变，而 hgCtxGetDevice_v2 是一个接受明确
 * 上下文参数的不同函数。如果没有当前上下文或结果超出范围时，将返回 -1，调用者会将此视为拒绝，而非“
 * 没有配额”。 */
int vppu_hggc_device(void)
{
    static HGresult (*real_get_device)(HGdevice *);
    static bool resolved;

    if (!resolved) {
        real_get_device = dlsym(RTLD_NEXT, "hgCtxGetDevice");
        resolved = true;
    }
    if (real_get_device == NULL) {
        return -1;
    }

    HGdevice device = 0;
    if (real_get_device(&device) != HGGC_SUCCESS) {
        return -1;
    }
    if (device < 0 || device >= VPPU_MAX_DEVICES) {
        return -1;
    }
    return (int)device;
}

/* remaining — 卡配额中剩余的部分。
 *
 * 剩余量而非 used + bytes > quota：这种计算方式在足够大的请求下会发生循环，导致结果低于配额限制，
 * 且会被允许通过，没有 denied 标记——这就是检查函数所认为的“该路径不受配额限制”的情况。 */
static unsigned long long remaining(unsigned long long quota, unsigned long long used)
{
    return (used >= quota) ? 0ULL : quota - used;
}

static bool admit_on(int named, enum vppu_entry entry, unsigned long long bytes, int *device_out)
{
    /* 名为的卡在此被限制，因为它来自调用者自己的结构体，而账本按此值对每张卡的锁区域进行索引。 */
    if (named >= VPPU_MAX_DEVICES) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s request=%llu: card %d is beyond this build's %u\n",
                 entry_names[entry], bytes, named, (unsigned int)VPPU_MAX_DEVICES);
        return false;
    }

    /* 在获取锁之前已解决：这会调用供应商的接口，且账本的重新进入计数机制存在，以便在我们在锁下调用供应
     * 商的接口时继续生效，并非为了引发其他调用。 */
    int device = (named >= 0) ? named : vppu_hggc_device();
    if (device < 0) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s request=%llu: no current device\n",
                 entry_names[entry], bytes);
        return false;
    }

    if (!vppu_quota_usable()) {
        vppu_log(VPPU_LOG_DENY,
                 "DENIED %s request=%llu device=%d: the container's quota configuration is "
                 "unusable — see the load-time report\n",
                 entry_names[entry], bytes, device);
        return false;
    }

    unsigned long long quota = vppu_quota_memory_bytes(device);
    if (quota == 0ULL) {
        /* 这两个变量被命名是因为任何一个都可能包含这张卡：无论缺少索引数字、原本会回归的未索引数字，还是将
         * 索引数字设置为不可用的状态，这种拒绝行为都是一样的。 */
        vppu_log(VPPU_LOG_DENY,
                 "DENIED %s request=%llu device=%d: no usable %s%d and no usable %s\n",
                 entry_names[entry], bytes, device, VPPU_ENV_MEMORY_LIMIT_PREFIX, device,
                 VPPU_ENV_MEMORY_LIMIT);
        return false;
    }

    if (!vppu_ledger_lock(device)) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s request=%llu device=%d: the ledger is unavailable\n",
                 entry_names[entry], bytes, device);
        return false;
    }
    ledger_touched = true;
    vppu_ledger_note_config(device, quota, vppu_quota_sm_percent(device));

    unsigned long long used = vppu_ledger_used(device);
    if (bytes > remaining(quota, used)) {
        /* 一次扫描，仅针对那个原本会拒绝的路径：在分配过程中被终止的进程会将其记账留在该区域，而这绝不能导
         * 致账本文件生命周期内容器的配额减少。 */
        if (vppu_ledger_reclaim(device) > 0ULL) {
            used = vppu_ledger_used(device);
        }
    }
    if (bytes > remaining(quota, used)) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s device=%d request=%llu accounted=%llu quota=%llu\n",
                 entry_names[entry], device, bytes, used, quota);
        vppu_ledger_unlock(device);
        return false;
    }

    /* 在供应商看到请求之前就进行了记账，因此不存在内存被保留且未被记账的窗口。被拒绝的记账意味着卡的进
     * 程表已满，这也属于拒绝行为：无人进行记账的分配也永远无法被收回。 */
    if (!vppu_ledger_charge(device, bytes)) {
        vppu_log(VPPU_LOG_DENY,
                 "DENIED %s device=%d request=%llu: the ledger cannot account for it\n",
                 entry_names[entry], device, bytes);
        vppu_ledger_unlock(device);
        return false;
    }

    *device_out = device;
    return true;
}

bool vppu_hggc_admit(enum vppu_entry entry, unsigned long long bytes, int *device_out)
{
    return admit_on(-1, entry, bytes, device_out);
}

bool vppu_hggc_admit_on(int device, enum vppu_entry entry, unsigned long long bytes,
                        int *device_out)
{
    return admit_on(device, entry, bytes, device_out);
}

void vppu_hggc_commit(int device, unsigned long long key, unsigned long long bytes)
{
    vppu_alloc_record(key, device, bytes);
    vppu_ledger_unlock(device);
}

void vppu_hggc_commit_sized(int device, unsigned long long key, unsigned long long admitted,
                            unsigned long long actual)
{
    /* 仍然处于卡锁定状态，因此差异会在其他进程读取总数之前被记录。记账会被修正而非重新接受：内存已经属
     * 于调用者，若在他们不知情的情况下释放内存来固定某个数值，就会破坏驱动程序选择的有效分配机制。因此，
     * 超出配额的操作会被报告而非被拒绝。 */
    if (actual > admitted) {
        unsigned long long extra = actual - admitted;

        if (!vppu_ledger_charge(device, extra)) {
            vppu_log(VPPU_LOG_DENY,
                     "device %d: %llu bytes of row padding not accounted, the ledger is "
                     "full\n",
                     device, extra);
        }
    } else if (admitted > actual) {
        vppu_ledger_refund(device, admitted - actual);
    }

    vppu_alloc_record(key, device, actual);
    vppu_ledger_unlock(device);
}

void vppu_hggc_rollback(int device, unsigned long long bytes)
{
    vppu_ledger_refund(device, bytes);
    vppu_ledger_unlock(device);
}

void vppu_hggc_refund(unsigned long long key)
{
    int device = -1;
    unsigned long long bytes = 0ULL;

    /* 从关键映射表中提取的内容，而非当前上下文中的内容：一个免费资源可能会使用与分配所涉及的当前设备不
     * 同的设备。 */
    if (!vppu_alloc_take(key, &device, &bytes)) {
        return;
    }
    if (!vppu_ledger_lock(device)) {
        /* 把账本放回原处，而不是丢失它：账本无法访问是一种完全无法进行任何分配的状态，而保留的记录则是一种
         * 退款，只要能恢复，仍然有可能得到回报。 */
        vppu_alloc_record(key, device, bytes);
        vppu_log(VPPU_LOG_DENY, "device %d: %llu freed bytes not refunded, the ledger is "
                                "unavailable\n",
                 device, bytes);
        return;
    }
    vppu_ledger_refund(device, bytes);
    vppu_ledger_unlock(device);
}

bool vppu_hggc_view(unsigned long long *quota_out, unsigned long long *free_out)
{
    int device = vppu_hggc_device();
    if (device < 0) {
        return false;
    }

    unsigned long long quota = vppu_quota_memory_bytes(device);
    if (quota == 0ULL) {
        return false;
    }

    /* 无锁定状态。一次分配失效的账本价值，远超过因供应商分配延迟而导致阻塞的查询。 */
    *quota_out = quota;
    *free_out = remaining(quota, vppu_ledger_used(device));
    return true;
}

__attribute__((constructor)) static void announce(void)
{
    /* 报告容器的配置情况，并标记其是否可用，因此错误配置会在加载时而非首次分配时被识别。此处调用而非
     * common/ 中的构造函数调用，因此调用顺序清晰明了。 */
    vppu_quota_validate();

    vppu_log(VPPU_LOG_DEBUG, "hggc_quota loaded, per-card %s<i>, %d entries\n",
             VPPU_ENV_MEMORY_LIMIT_PREFIX, (int)VPPU_ENTRY_COUNT);
}

/* 计数器 dump是证明运行时层调用到达驱动层的证据，因此必须能在正常退出时保留下来：从析构函数中打印
 * 出来，每个条目名称即使为零也需保留——缺失的名称与零计数无法区分。这些条目用标记符拦截在行中，标记
 * 符在每行中重复出现，因为38个条目的其中一行无法阅读，且各情况与行中的标记符一致。 */
__attribute__((destructor)) static void dump_counters(void)
{
    if (vppu_log_level() < VPPU_LOG_DEBUG) {
        return;
    }

    for (int i = 0; i < VPPU_ENTRY_COUNT; i++) {
        if (i % 6 == 0) {
            fprintf(stderr, "%s" VPPU_TAG "hggc_quota counters:", (i == 0) ? "" : "\n");
        }
        fprintf(stderr, " %s=%llu", entry_names[i], entry_calls[i]);
    }

    /* 这些总数属于账本，而非本进程，它们来自账本——本进程只有在达到该状态时才会被要求使用这些数据，因此
     * 没有进行任何分配的进程在退出时不会创建区域文件。只有带有记账的卡会被命名，因此处于空闲状态的表不
     * 会打印出64个零。 */
    if (ledger_touched) {
        for (int i = 0; i < VPPU_MAX_DEVICES; i++) {
            unsigned long long used = vppu_ledger_used(i);

            if (used != 0ULL) {
                fprintf(stderr, " accounted[%d]=%llu", i, used);
            }
        }
    }
    fprintf(stderr, " ledger_overflows=%llu\n", vppu_alloc_overflows());
}

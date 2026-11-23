/* hggc_mem.c — 驱动层的内存条目，当前的 ABI。
 *
 * libhggc.so内存表面中每个导出名称都有一个定义，正如SDK的符号清单所列出的那样。每个名称都包含在其
 * 中，无论是否进行了记账，因为所声称的是覆盖性：被遗漏的名称则是工作负载获取该配额永远无法看到的内
 * 存的方式，从外部无法区分两者。
 *
 * what A WRAPPER 的功能属于以下四种之一，而 hggc_quota.h 中的枚举类型按这些功能名称进行了分类：
 * - 记账：承认针对卡的配额提出的请求，联系供应商，提交或撤销。
 * - refund：联系供应商，然后将 handle 的字节数据返回给被记账的卡。
 * - report：请用配额数值来回答，而非卡数值。
 * - count：以不变的方式调用供应商，已统计过交叉项。
 *
 * 这些签名是头文件中的定义，而非复制内容。hggc.h 将普通源文件名映射到版本化符号（hgMemAlloc ->
 * hgMemAlloc_v2，正如 cuda.h 将 cuMemAlloc 映射到相应符号），因此在此写入普通名称会定义该版本化符
 * 号，并会对每个参数进行头文件类型检查。_ptsz 变体则通过 __typeof__ 而非重新类型化的原型进行相同处
 * 理：在 __HGGC_API_PER_THREAD_DEFAULT_STREAM** 下，头文件仅将普通名称映射到 _ptsz 符号，而本库不
 * 得定义此条件——否则会同时修改所有流项——因此类型是从普通声明中获取的。若参数有任何差异，则构建会失
 * 败，而不会破坏调用结果。
 *
 * v1 ABI 名称位于 hggc_mem_v1.c 文件中，必须使用 #undef 指令取消那些映射，才能访问到它们。 */
#define _GNU_SOURCE

#include <limits.h>

/* hggc.h 仅包含 <stdlib.h> 和 <stdint.h>，因此在其声明中既没有提供 NULL 也不提供 bool。 */
#include <dlfcn.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <hggc.h>

#include "common/vppu.h"
#include "common/vppu_quota.h"
#include "hggc/hggc_quota.h"

/* product — a * b，选择拒绝而非包装。
 *
 * 这些经过处理的条目是仅有的、以二维形式呈现的，而非字节计数形式；一种拦截了该产品的东西会低于配额，
 * 因此被允许使用。拒绝是安全的选择：那些大小不符合64位计数要求的请求，不是该卡能够处理的类型。 */
static bool product(unsigned long long a, unsigned long long b, unsigned long long *out)
{
    if (a != 0ULL && b > ULLONG_MAX / a) {
        return false;
    }
    *out = a * b;
    return true;
}

/* 那些基于其所映射的普通条目的流变体。 */
extern __typeof__(hgMemAllocAsync) hgMemAllocAsync_ptsz;
extern __typeof__(hgMemAllocFromPoolAsync) hgMemAllocFromPoolAsync_ptsz;
extern __typeof__(hgMemFreeAsync) hgMemFreeAsync_ptsz;
extern __typeof__(hgMemMapArrayAsync) hgMemMapArrayAsync_ptsz;

/* named_card — 指某个位置所指定的卡，若没有指定则为 -1。只有 DEVICE 位置可以指定卡：主机和 NUMA 位
 * 置属于主机内存，该配额并不对其生效。 */
static int named_card(const HGmemLocation *location)
{
    if (location == NULL || location->type != HG_MEM_LOCATION_TYPE_DEVICE) {
        return -1;
    }
    return location->id;
}

/* 池 -> 卡表，它必须位于读取它的分配之上。一个池属于其属性中指定的卡，而来自该池的分配会无论调用线
 * 程的上下文如何都出现在那里，因此卡在创建时就被记住了——之后通过句柄无法询问其状态。
 *
 * 小型、固定且无锁：每张卡、每个用途对应一个池，这就是实际工作负载的形态；无法扩展的表无法在插入的
 * 供应商调用中失败，而句柄上的比较交换无需依赖该库所依赖的 glibc 层。不符合要求的池，或在该库加载
 * 前创建的池，根本不存在——而不存在意味着“对调用线程的上下文进行记账”，这是该路径在表存在之前给出的
 * 答案。 */
#define VPPU_MAX_POOLS 64

static struct {
    HGmemoryPool pool;
    int device;
} pool_cards[VPPU_MAX_POOLS];

/* 一个无法由供应商池提供的句柄值，被存储在获取该句柄与填充该句柄之间的槽位中。如果没有这个值，使用
 * 不同池的两个线程会在同一个空槽位上发生冲突：两者都认为该槽位为空，然后各自写入自己的卡，最终胜出
 * 的那个会针对另一个人的卡进行记账——这种分配行为会将其记账内容记到从未接触过的卡上，而这正是本账本
 * 存在的目的，以预防此类问题。 */
#define VPPU_POOL_CLAIMING ((HGmemoryPool)(uintptr_t)1)

/* pool_remember — 用于文件中的池卡。该槽位被优先占用，真正的句柄则最后公布，因此读取方要么完全不匹
 * 配该槽位，要么看到与之对应的卡。 */
static void pool_remember(HGmemoryPool pool, int device)
{
    if (pool == NULL || device < 0) {
        return;
    }
    for (unsigned int i = 0; i < VPPU_MAX_POOLS; i++) {
        HGmemoryPool empty = NULL;

        if (!__atomic_compare_exchange_n(&pool_cards[i].pool, &empty, VPPU_POOL_CLAIMING, false,
                                         __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
            continue;
        }
        pool_cards[i].device = device;
        __atomic_store_n(&pool_cards[i].pool, pool, __ATOMIC_RELEASE);
        return;
    }
}

/* pool_forget — 被销毁的池的槽位会归还，因此循环使用不同池的长生命周期进程不会让供应商的表中充满可
 * 重新用于其他卡的句柄。 */
static void pool_forget(HGmemoryPool pool)
{
    for (unsigned int i = 0; i < VPPU_MAX_POOLS; i++) {
        if (__atomic_load_n(&pool_cards[i].pool, __ATOMIC_ACQUIRE) == pool) {
            __atomic_store_n(&pool_cards[i].pool, NULL, __ATOMIC_RELEASE);
            return;
        }
    }
}

/* default_pool_card — 通过向各个供应商询问并比较，确定此句柄所关联的默认池。默认池是指工作负载在无
 * 需创建任何对象的情况下获得的池，这是常见情况，且它永远不会经过 hgMemPoolCreate 处理——若让系统回
 * 退到调用线程的上下文，就会无法解决本表存在的漏洞。
 *
 * 故意进行干预而非主动询问：hgDeviceGetDefaultMemPool 是一个设备入口项，将其加入干预集合会扩大模块
 * 的导出范围，而这一范围可以简单地通过查询获得。这是调用方的行为，与解决当前上下文的形式相同，它在
 * 卡锁定被获取之前就执行了操作。只有配置了特定数值的卡才会被询问：这些就是容器所持有的对象，而无论
 * 内存池如何规定，对其他对象的分配请求都会被拒绝。 */
static int default_pool_card(HGmemoryPool pool)
{
    static HGresult (*real_default_pool)(HGmemoryPool *, HGdevice);
    static bool resolved;

    if (!resolved) {
        real_default_pool = dlsym(RTLD_NEXT, "hgDeviceGetDefaultMemPool");
        resolved = true;
    }
    if (real_default_pool == NULL) {
        return -1;
    }

    for (int device = 0; device < VPPU_MAX_DEVICES; device++) {
        HGmemoryPool theirs = NULL;

        if (vppu_quota_memory_bytes(device) == 0ULL) {
            continue;
        }
        if (real_default_pool(&theirs, (HGdevice)device) == HGGC_SUCCESS && theirs == pool) {
            return device;
        }
    }
    return -1;
}

/* pool_card — 该池所属于的卡，若表格和供应商都无法确定则为 -1。 */
static int pool_card(HGmemoryPool pool)
{
    if (pool == NULL) {
        return -1;
    }
    for (unsigned int i = 0; i < VPPU_MAX_POOLS; i++) {
        if (__atomic_load_n(&pool_cards[i].pool, __ATOMIC_ACQUIRE) == pool) {
            return pool_cards[i].device;
        }
    }

    /* 在输出过程中进行缓存，因此供应商只需针对每个池进行一次操作，而非每次分配都进行一次。 */
    int device = default_pool_card(pool);
    if (device >= 0) {
        pool_remember(pool, device);
    }
    return device;
}

/* --------------------------------------------------------------------------------------- 分配情况。
 * 在供应商看到请求之前进行记账；如果分配没有发生，这笔记账会被退还，因此不存在内存被未记账保留的时
 * 间窗口。 --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemAlloc(HGdeviceptr *dptr, size_t bytesize)
{
    int device = -1;

    vppu_hggc_count(VPPU_MEM_ALLOC);
    if (!vppu_hggc_admit(VPPU_MEM_ALLOC, bytesize, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr *, size_t) = vppu_hggc_next(VPPU_MEM_ALLOC);
    if (real == NULL) {
        vppu_hggc_rollback(device, bytesize);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, bytesize);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        vppu_hggc_commit(device, (unsigned long long)*dptr, bytesize);
    } else {
        vppu_hggc_rollback(device, bytesize);
    }
    return rc;
}

/* alloc_async / alloc_from_pool — 这些代码与 plain 和 _ptsz 版本共享。两者唯一的区别是驱动程序会默
 * 认使用哪个流，这属于厂商的权限范围；记账逻辑完全相同，因此代码只写一次，且条目会区分计数器与
 * dlsym()。 */
static HGresult alloc_async(enum vppu_entry entry, HGdeviceptr *dptr, size_t bytesize,
                            HGstream hStream)
{
    int device = -1;

    vppu_hggc_count(entry);
    if (!vppu_hggc_admit(entry, bytesize, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr *, size_t, HGstream) = vppu_hggc_next(entry);
    if (real == NULL) {
        vppu_hggc_rollback(device, bytesize);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, bytesize, hStream);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        vppu_hggc_commit(device, (unsigned long long)*dptr, bytesize);
    } else {
        vppu_hggc_rollback(device, bytesize);
    }
    return rc;
}

HGresult HGGCAPI hgMemAllocAsync(HGdeviceptr *dptr, size_t bytesize, HGstream hStream)
{
    return alloc_async(VPPU_MEM_ALLOC_ASYNC, dptr, bytesize, hStream);
}

HGresult HGGCAPI hgMemAllocAsync_ptsz(HGdeviceptr *dptr, size_t bytesize, HGstream hStream)
{
    return alloc_async(VPPU_MEM_ALLOC_ASYNC_PTSZ, dptr, bytesize, hStream);
}

static HGresult alloc_from_pool(enum vppu_entry entry, HGdeviceptr *dptr, size_t bytesize,
                                HGmemoryPool pool, HGstream hStream)
{
    int device = -1;

    vppu_hggc_count(entry);
    /* 池的卡，而非调用者的上下文：在容纳一张卡的容器中，它们是相同的；而容纳两张卡的容器中则不必相同。 */
    if (!vppu_hggc_admit_on(pool_card(pool), entry, bytesize, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr *, size_t, HGmemoryPool, HGstream) = vppu_hggc_next(entry);
    if (real == NULL) {
        vppu_hggc_rollback(device, bytesize);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, bytesize, pool, hStream);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        vppu_hggc_commit(device, (unsigned long long)*dptr, bytesize);
    } else {
        vppu_hggc_rollback(device, bytesize);
    }
    return rc;
}

HGresult HGGCAPI hgMemAllocFromPoolAsync(HGdeviceptr *dptr, size_t bytesize, HGmemoryPool pool,
                                         HGstream hStream)
{
    return alloc_from_pool(VPPU_MEM_ALLOC_FROM_POOL_ASYNC, dptr, bytesize, pool, hStream);
}

HGresult HGGCAPI hgMemAllocFromPoolAsync_ptsz(HGdeviceptr *dptr, size_t bytesize,
                                              HGmemoryPool pool, HGstream hStream)
{
    return alloc_from_pool(VPPU_MEM_ALLOC_FROM_POOL_ASYNC_PTSZ, dptr, bytesize, pool, hStream);
}

/* 管理的内存像其他分配资源一样进行记账：尽管驱动程序可能会迁移页面，但它由卡提供支持。因此，如果以
 * 不记账的方式使用，一个容器可能会将设备内存存储在其配额之外。 */
HGresult HGGCAPI hgMemAllocManaged(HGdeviceptr *dptr, size_t bytesize, unsigned int flags)
{
    int device = -1;

    vppu_hggc_count(VPPU_MEM_ALLOC_MANAGED);
    if (!vppu_hggc_admit(VPPU_MEM_ALLOC_MANAGED, bytesize, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr *, size_t, unsigned int) =
        vppu_hggc_next(VPPU_MEM_ALLOC_MANAGED);
    if (real == NULL) {
        vppu_hggc_rollback(device, bytesize);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, bytesize, flags);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        vppu_hggc_commit(device, (unsigned long long)*dptr, bytesize);
    } else {
        vppu_hggc_rollback(device, bytesize);
    }
    return rc;
}

/* 该调整过的条目是那种分配量，其实际大小在返回之前无法得知：驱动程序会选择行步长，从卡中获得的数值
 * 是步长乘以高度，而不是调用者所要求的宽度。因此，准入决策基于调用者的尺寸，而记账操作则与驱动程序
 * 的数值进行对调——参见 vppu_hggc_commit_sized()。 */
HGresult HGGCAPI hgMemAllocPitch(HGdeviceptr *dptr, size_t *pPitch, size_t WidthInBytes,
                                 size_t Height, unsigned int ElementSizeBytes)
{
    int device = -1;
    unsigned long long request = 0ULL;

    vppu_hggc_count(VPPU_MEM_ALLOC_PITCH);
    if (!product(WidthInBytes, Height, &request)) {
        vppu_log(VPPU_LOG_DENY, "DENIED %s: %zu x %zu overflows a byte count\n",
                 vppu_hggc_name(VPPU_MEM_ALLOC_PITCH), WidthInBytes, Height);
        return HGGC_ERROR_INVALID_VALUE;
    }
    if (!vppu_hggc_admit(VPPU_MEM_ALLOC_PITCH, request, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr *, size_t *, size_t, size_t, unsigned int) =
        vppu_hggc_next(VPPU_MEM_ALLOC_PITCH);
    if (real == NULL) {
        vppu_hggc_rollback(device, request);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, pPitch, WidthInBytes, Height, ElementSizeBytes);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        unsigned long long actual = request;

        /* 没有进行乘法计算的步长会导致所接受的数值被记账，而不是任意数值：分配成功，因此驱动程序自身的产品
         * 不会溢出，猜测还不如坚持已定的结果。 */
        if (pPitch != NULL) {
            (void)product(*pPitch, Height, &actual);
        }
        vppu_hggc_commit_sized(device, (unsigned long long)*dptr, request, actual);
    } else {
        vppu_hggc_rollback(device, request);
    }
    return rc;
}

/* hgMemCreate是VMM路径使用物理内存的地方。它返回的句柄是后续hgMemRelease返回的内容，因此该句柄至关
 * 重要。 */
HGresult HGGCAPI hgMemCreate(HGmemGenericAllocationHandle *handle, size_t size,
                             const HGmemAllocationProp *prop, unsigned long long flags)
{
    int device = -1;

    vppu_hggc_count(VPPU_MEM_CREATE);
    /* VMM在prop中标记其目标卡，该标记不必是调用线程的：对上下文进行记账会从一张卡中获取内存，同时从另
     * 一张卡中获取配额。 */
    if (!vppu_hggc_admit_on(named_card(prop == NULL ? NULL : &prop->location), VPPU_MEM_CREATE,
                            size, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGmemGenericAllocationHandle *, size_t, const HGmemAllocationProp *,
                     unsigned long long) = vppu_hggc_next(VPPU_MEM_CREATE);
    if (real == NULL) {
        vppu_hggc_rollback(device, size);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(handle, size, prop, flags);
    if (rc == HGGC_SUCCESS && handle != NULL) {
        vppu_hggc_commit(device, (unsigned long long)*handle, size);
    } else {
        vppu_hggc_rollback(device, size);
    }
    return rc;
}

/* --------------------------------------------------------------------------------------- 释放。仅
 * 当供应商报告内存已消耗时才会退款，且仅针对该账本记录的关键项——其他情况则属于加载前进行的分配。
 * --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemFree(HGdeviceptr dptr)
{
    vppu_hggc_count(VPPU_MEM_FREE);

    HGresult (*real)(HGdeviceptr) = vppu_hggc_next(VPPU_MEM_FREE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr);
    if (rc == HGGC_SUCCESS) {
        vppu_hggc_refund((unsigned long long)dptr);
    }
    return rc;
}

static HGresult free_async(enum vppu_entry entry, HGdeviceptr dptr, HGstream hStream)
{
    vppu_hggc_count(entry);

    HGresult (*real)(HGdeviceptr, HGstream) = vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, hStream);
    if (rc == HGGC_SUCCESS) {
        vppu_hggc_refund((unsigned long long)dptr);
    }
    return rc;
}

HGresult HGGCAPI hgMemFreeAsync(HGdeviceptr dptr, HGstream hStream)
{
    return free_async(VPPU_MEM_FREE_ASYNC, dptr, hStream);
}

HGresult HGGCAPI hgMemFreeAsync_ptsz(HGdeviceptr dptr, HGstream hStream)
{
    return free_async(VPPU_MEM_FREE_ASYNC_PTSZ, dptr, hStream);
}

HGresult HGGCAPI hgMemRelease(HGmemGenericAllocationHandle handle)
{
    vppu_hggc_count(VPPU_MEM_RELEASE);

    HGresult (*real)(HGmemGenericAllocationHandle) = vppu_hggc_next(VPPU_MEM_RELEASE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(handle);
    if (rc == HGGC_SUCCESS) {
        vppu_hggc_refund((unsigned long long)handle);
    }
    return rc;
}

/* --------------------------------------------------------------------------------------- 查询。配
 * 额的具体数值意味着，基于该卡进行工作负载的分配会看到所获得的槽位，而非整个卡的容量。
 * --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemGetInfo(size_t *free, size_t *total)
{
    vppu_hggc_count(VPPU_MEM_GET_INFO);

    HGresult (*real)(size_t *, size_t *) = vppu_hggc_next(VPPU_MEM_GET_INFO);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(free, total);
    if (rc != HGGC_SUCCESS) {
        return rc;
    }

    unsigned long long quota = 0ULL;
    unsigned long long available = 0ULL;
    if (!vppu_hggc_view(&quota, &available)) {
        return rc;
    }
    if (total != NULL) {
        *total = (size_t)quota;
    }
    if (free != NULL) {
        *free = (size_t)available;
    }
    return rc;
}

/* --------------------------------------------------------------------------------------- 主机内存。
 * 已计数，未进行记账：固定分配的主机页面并非设备 VRAM，因此对它们进行记账会拒绝那些对卡毫无意义的
 * 分配。
 *
 * 这里的释放操作故意不会进行退还。主机指针是本进程自身空间中的地址，并未对其进行记录，因此查询它只
 * 能与恰好携带相同编号的设备句柄匹配——即从未释放的内存的退还。
 * --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemAllocHost(void **pp, size_t bytesize)
{
    vppu_hggc_count(VPPU_MEM_ALLOC_HOST);

    HGresult (*real)(void **, size_t) = vppu_hggc_next(VPPU_MEM_ALLOC_HOST);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pp, bytesize);
}

HGresult HGGCAPI hgMemFreeHost(void *p)
{
    vppu_hggc_count(VPPU_MEM_FREE_HOST);

    HGresult (*real)(void *) = vppu_hggc_next(VPPU_MEM_FREE_HOST);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(p);
}

/* --------------------------------------------------------------------------------------- 地址映射。
 * 已计数但未进行记账：hgMemCreate是VMM路径使用物理内存的地方，这些仅绑定了一个已完成付费的句柄。
 * --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemMap(HGdeviceptr ptr, size_t size, size_t offset,
                          HGmemGenericAllocationHandle handle, unsigned long long flags)
{
    vppu_hggc_count(VPPU_MEM_MAP);

    HGresult (*real)(HGdeviceptr, size_t, size_t, HGmemGenericAllocationHandle,
                     unsigned long long) = vppu_hggc_next(VPPU_MEM_MAP);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(ptr, size, offset, handle, flags);
}

static HGresult map_array_async(enum vppu_entry entry, HGarrayMapInfo *mapInfoList,
                                unsigned int count, HGstream hStream)
{
    vppu_hggc_count(entry);

    HGresult (*real)(HGarrayMapInfo *, unsigned int, HGstream) = vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(mapInfoList, count, hStream);
}

HGresult HGGCAPI hgMemMapArrayAsync(HGarrayMapInfo *mapInfoList, unsigned int count,
                                    HGstream hStream)
{
    return map_array_async(VPPU_MEM_MAP_ARRAY_ASYNC, mapInfoList, count, hStream);
}

HGresult HGGCAPI hgMemMapArrayAsync_ptsz(HGarrayMapInfo *mapInfoList, unsigned int count,
                                         HGstream hStream)
{
    return map_array_async(VPPU_MEM_MAP_ARRAY_ASYNC_PTSZ, mapInfoList, count, hStream);
}

HGresult HGGCAPI hgMemUnmap(HGdeviceptr ptr, size_t size)
{
    vppu_hggc_count(VPPU_MEM_UNMAP);

    HGresult (*real)(HGdeviceptr, size_t) = vppu_hggc_next(VPPU_MEM_UNMAP);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(ptr, size);
}

/* --------------------------------------------------------------------------------------- 池资源。
 * 已计数且未进行记账：某个池的资源被 hgMemAllocFromPoolAsync 占用，该函数会在后续进行记账，随后通
 * 过清除操作归还。这些设置是为了让池路径进入 libhggc.so 成为明确事实而非假设——也为了确保该库中通过
 * 池分配的、未被创建的资源仍属于异常情况，计数工具可以反映这一点。
 *
 * 现在的创建过程也会记录池的卡信息，这是引入该机制的第二个原因：一个池属于其属性中指定的卡，从该池
 * 中分配资源会将其分配到任何调用线程的上下文中，而检查该上下文则会导致错误卡的配额被消耗。句柄在之
 * 后没有办法询问，因此答案必须被保留在知道这一情况的那个调用中。
 *
 * 该表本身包含上述分配内容，因为数据就是在这里被读取的。
 * --------------------------------------------------------------------------------------- */

HGresult HGGCAPI hgMemPoolCreate(HGmemoryPool *pool, const HGmemPoolProps *poolProps)
{
    vppu_hggc_count(VPPU_MEM_POOL_CREATE);

    HGresult (*real)(HGmemoryPool *, const HGmemPoolProps *) =
        vppu_hggc_next(VPPU_MEM_POOL_CREATE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(pool, poolProps);
    if (rc == HGGC_SUCCESS && pool != NULL) {
        pool_remember(*pool, named_card(poolProps == NULL ? NULL : &poolProps->location));
    }
    return rc;
}

HGresult HGGCAPI hgMemPoolDestroy(HGmemoryPool pool)
{
    vppu_hggc_count(VPPU_MEM_POOL_DESTROY);

    HGresult (*real)(HGmemoryPool) = vppu_hggc_next(VPPU_MEM_POOL_DESTROY);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(pool);
    if (rc == HGGC_SUCCESS) {
        pool_forget(pool);
    }
    return rc;
}

HGresult HGGCAPI hgMemPoolExportPointer(HGmemPoolPtrExportData *shareData_out, HGdeviceptr ptr)
{
    vppu_hggc_count(VPPU_MEM_POOL_EXPORT_POINTER);

    HGresult (*real)(HGmemPoolPtrExportData *, HGdeviceptr) =
        vppu_hggc_next(VPPU_MEM_POOL_EXPORT_POINTER);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(shareData_out, ptr);
}

HGresult HGGCAPI hgMemPoolExportToShareableHandle(void *handle_out, HGmemoryPool pool,
                                                  HGmemAllocationHandleType handleType,
                                                  unsigned long long flags)
{
    vppu_hggc_count(VPPU_MEM_POOL_EXPORT_TO_SHAREABLE_HANDLE);

    HGresult (*real)(void *, HGmemoryPool, HGmemAllocationHandleType, unsigned long long) =
        vppu_hggc_next(VPPU_MEM_POOL_EXPORT_TO_SHAREABLE_HANDLE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(handle_out, pool, handleType, flags);
}

HGresult HGGCAPI hgMemPoolGetAccess(HGmemAccess_flags *flags, HGmemoryPool memPool,
                                    HGmemLocation *location)
{
    vppu_hggc_count(VPPU_MEM_POOL_GET_ACCESS);

    HGresult (*real)(HGmemAccess_flags *, HGmemoryPool, HGmemLocation *) =
        vppu_hggc_next(VPPU_MEM_POOL_GET_ACCESS);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(flags, memPool, location);
}

HGresult HGGCAPI hgMemPoolGetAttribute(HGmemoryPool pool, HGmemPool_attribute attr, void *value)
{
    vppu_hggc_count(VPPU_MEM_POOL_GET_ATTRIBUTE);

    HGresult (*real)(HGmemoryPool, HGmemPool_attribute, void *) =
        vppu_hggc_next(VPPU_MEM_POOL_GET_ATTRIBUTE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pool, attr, value);
}

HGresult HGGCAPI hgMemPoolImportFromShareableHandle(HGmemoryPool *pool_out, void *handle,
                                                    HGmemAllocationHandleType handleType,
                                                    unsigned long long flags)
{
    vppu_hggc_count(VPPU_MEM_POOL_IMPORT_FROM_SHAREABLE_HANDLE);

    HGresult (*real)(HGmemoryPool *, void *, HGmemAllocationHandleType, unsigned long long) =
        vppu_hggc_next(VPPU_MEM_POOL_IMPORT_FROM_SHAREABLE_HANDLE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pool_out, handle, handleType, flags);
}

/* 一个导入的指针用于分配其他进程已支付的费用，因此这里不会进行记账——而且也刻意不将其记录下来，正因
 * 如此，之后释放时不会进行退款，也不会将内存配额归还给这个容器，因为它从未占用过内存。 */
HGresult HGGCAPI hgMemPoolImportPointer(HGdeviceptr *ptr_out, HGmemoryPool pool,
                                        HGmemPoolPtrExportData *shareData)
{
    vppu_hggc_count(VPPU_MEM_POOL_IMPORT_POINTER);

    HGresult (*real)(HGdeviceptr *, HGmemoryPool, HGmemPoolPtrExportData *) =
        vppu_hggc_next(VPPU_MEM_POOL_IMPORT_POINTER);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(ptr_out, pool, shareData);
}

HGresult HGGCAPI hgMemPoolSetAccess(HGmemoryPool pool, const HGmemAccessDesc *map, size_t count)
{
    vppu_hggc_count(VPPU_MEM_POOL_SET_ACCESS);

    HGresult (*real)(HGmemoryPool, const HGmemAccessDesc *, size_t) =
        vppu_hggc_next(VPPU_MEM_POOL_SET_ACCESS);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pool, map, count);
}

HGresult HGGCAPI hgMemPoolSetAttribute(HGmemoryPool pool, HGmemPool_attribute attr, void *value)
{
    vppu_hggc_count(VPPU_MEM_POOL_SET_ATTRIBUTE);

    HGresult (*real)(HGmemoryPool, HGmemPool_attribute, void *) =
        vppu_hggc_next(VPPU_MEM_POOL_SET_ATTRIBUTE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pool, attr, value);
}

HGresult HGGCAPI hgMemPoolTrimTo(HGmemoryPool pool, size_t minBytesToKeep)
{
    vppu_hggc_count(VPPU_MEM_POOL_TRIM_TO);

    HGresult (*real)(HGmemoryPool, size_t) = vppu_hggc_next(VPPU_MEM_POOL_TRIM_TO);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pool, minBytesToKeep);
}

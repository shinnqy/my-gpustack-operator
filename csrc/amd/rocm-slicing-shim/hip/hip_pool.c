/* hip_pool.c — 流排序分配器，这是仅由 hipMalloc 操作的账本所留下的空白。
 *
 * 这个 FAMILY 层并非 hipMalloc 的上一层。测量结果显示：在仅使用经典层进行记账的情况下，内存限制为
 * 2 GiB；hipMalloc 能够正确地在 2.000 GiB 处停止分配，而 hipMallocFromPoolAsync 则会在 RDNA 版本中
 * 额外获取 10 GiB，在 CDNA 版本中获取 50 GiB——在两种情况下，测试循环都停止在内存限制处，而非实际内
 * 存使用量。hipDeviceGetDefaultMemPool 会提供一个可使用的内存池，无需任何特殊权限，PyTorch 的内存
 * 池路径也直接经过该内存池。不对这些内存进行记账的配额并不属于配额范畴。
 *
 * 一个导入指针被故意未记录。hipMemPoolImportPointer 会映射另一个进程已分配且已支付费用的内存；对其
 * 进行记账就会重复计费相同的字节数，更糟糕的是——将其记录下来，会让这个容器的免费退款机制计入其从未
 * 支付的费用。 */
#define _GNU_SOURCE

#include <stdint.h>

#include <hip/hip_runtime_api.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_log.h"
#include "hip/hip_resolve.h"
#include "hip/hip_table.h"

VROCM_INTERNAL int vrocm_current_device(void);
VROCM_INTERNAL hipError_t vrocm_admit_status(enum vrocm_admit rc, hipError_t runtime_status);

typedef hipError_t (*fn_malloc_async)(void **, size_t, hipStream_t);
typedef hipError_t (*fn_malloc_from_pool)(void **, size_t, hipMemPool_t, hipStream_t);
typedef hipError_t (*fn_free_async)(void *, hipStream_t);
typedef hipError_t (*fn_import)(void **, hipMemPool_t, hipMemPoolPtrExportData *);

struct pool_ctx {
    hipError_t status;
    void **ptr;
    size_t size;
    hipMemPool_t pool;
    hipStream_t stream;
    fn_malloc_async async;
    fn_malloc_from_pool from_pool;
};

static bool call_async(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct pool_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->async(ctx->ptr, ctx->size, ctx->stream);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

static bool call_from_pool(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct pool_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->from_pool(ctx->ptr, ctx->size, ctx->pool, ctx->stream);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

VROCM_EXPORT hipError_t hipMallocAsync(void **dev_ptr, size_t size, hipStream_t stream)
{
    VROCM_REAL(hipMallocAsync, fn_malloc_async);
    VROCM_ENTRY(hipMallocAsync);

    struct pool_ctx ctx = { .ptr = dev_ptr, .size = size, .stream = stream,
                            .async = real_hipMallocAsync };
    enum vrocm_admit rc;

    VROCM_HIT(hipMallocAsync);
    rc = vrocm_ledger_admit(vrocm_current_device(), size, call_async, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMallocAsync);
    }
    return vrocm_admit_status(rc, ctx.status);
}

VROCM_EXPORT hipError_t hipMallocFromPoolAsync(void **dev_ptr, size_t size, hipMemPool_t mem_pool,
                                  hipStream_t stream)
{
    VROCM_REAL(hipMallocFromPoolAsync, fn_malloc_from_pool);
    VROCM_ENTRY(hipMallocFromPoolAsync);

    struct pool_ctx ctx = { .ptr = dev_ptr, .size = size, .pool = mem_pool, .stream = stream,
                            .from_pool = real_hipMallocFromPoolAsync };
    enum vrocm_admit rc;

    VROCM_HIT(hipMallocFromPoolAsync);
    rc = vrocm_ledger_admit(vrocm_current_device(), size, call_from_pool, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMallocFromPoolAsync);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 当免费额度被发放时则进行退款，而非当流量达到该额度时。另一种做法——将记账行为推迟到本拦截库永远无
 * 法看到的同步阶段——会导致一个容器不断释放并重新分配资源，无理由地不断上升直至达到上限，用户便无法
 * 操作。代价是记账的总额可能短暂低于驱动程序的数值，这倾向于接受而非拒绝，而下一次的校验则仍然限制
 * 着该卡的使用。
 *
 * 被发放仍与被接受不同，因此退款取决于调用情况及其状态：如果运行时拒绝队列，则不会进行任何退款，而
 * 将其记账量退还给容器则意味着容器仍持有该卡。 */
VROCM_EXPORT hipError_t hipFreeAsync(void *dev_ptr, hipStream_t stream)
{
    hipError_t status;

    VROCM_REAL(hipFreeAsync, fn_free_async);
    VROCM_ENTRY(hipFreeAsync);

    VROCM_HIT(hipFreeAsync);
    status = real_hipFreeAsync(dev_ptr, stream);
    if (status == hipSuccess && dev_ptr != NULL) {
        vrocm_ledger_release((unsigned long long)(uintptr_t)dev_ptr, NULL, NULL);
    }
    return status;
}

VROCM_EXPORT hipError_t hipMemPoolImportPointer(void **dev_ptr, hipMemPool_t mem_pool,
                                   hipMemPoolPtrExportData *export_data)
{
    VROCM_REAL(hipMemPoolImportPointer, fn_import);
    VROCM_ENTRY(hipMemPoolImportPointer);

    /* 统计了数据，以便显示已到达条目，且未进行任何记账。参见标题。 */
    VROCM_HIT(hipMemPoolImportPointer);
    return real_hipMemPoolImportPointer(dev_ptr, mem_pool, export_data);
}

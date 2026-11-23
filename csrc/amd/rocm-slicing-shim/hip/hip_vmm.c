/* hip_vmm.c — 虚拟内存管理系列，其分配过程中未包含任何 hipMalloc。
 *
 * 这是 DOOR 真正的框架实现路径。PyTorch 的可扩展段分配器正是基于这一机制设计的——先预留一个地址范围，
 * 然后随着运行环境的扩展，将物理内存映射到该范围内——因此，无视这一机制的配额，并非适用于经过优化的
 * 训练任务配置。与那些包装了所有 hipMalloc* 名称的包装器相比，在 64 MiB 配额下，该分配机制在没有任
 * 何错误的情况下，使用了 512 MiB 的内存空间。
 *
 * 只有 hipMemCreate 会进行分配。该系列中的其他函数会移动地址：hipMemAddressReserve 接受虚拟地址空
 * 间且不需要内存，hipMemMap 将现有的句柄绑定到该空间中，hipMemSetAccess 设置权限，而它们的反操作则
 * 撤销这些操作。因此，一个条目被记账，另一个则被退还，而包装其他四个函数会多次计算相同字节数——在两
 * 个地址上映射的句柄仍属于一次分配。
 *
 * 这笔记账是基于属性，而非上下文。hipMemAllocationProp.location.id 指定了生成内存所依赖的设备，调
 * 用者也可以选择使用与其线程上下文不同的设备。如果读取当前设备，就会对错误的设备进行记账，这正是按
 * 设备分别记账机制所存在的情形。 */
#define _GNU_SOURCE

#include <stdint.h>

#include <hip/hip_runtime_api.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_log.h"
#include "hip/hip_resolve.h"
#include "hip/hip_table.h"

VROCM_INTERNAL int vrocm_current_device(void);
VROCM_INTERNAL hipError_t vrocm_admit_status(enum vrocm_admit rc, hipError_t runtime_status);

typedef hipError_t (*fn_mem_create)(hipMemGenericAllocationHandle_t *, size_t,
                                    const hipMemAllocationProp *, unsigned long long);
typedef hipError_t (*fn_mem_release)(hipMemGenericAllocationHandle_t);

struct vmm_ctx {
    hipError_t status;
    hipMemGenericAllocationHandle_t *handle;
    size_t size;
    const hipMemAllocationProp *prop;
    unsigned long long flags;
    fn_mem_create real;
};

static bool call_mem_create(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct vmm_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->real(ctx->handle, ctx->size, ctx->prop, ctx->flags);
    if (ctx->status != hipSuccess) {
        return false;
    }
    /* 该句柄是关键，因为目前还没有指针：此内存所指向的地址是由 hipMemMap 之后选定的，而 hipMemRelease
     * 则把句柄归还，而不是地址。如果只关注映射过程，就会忽略那些被创建但未被映射的内存，这些内存实际上
     * 也是卡所释放的。 */
    *key = (unsigned long long)(uintptr_t)*ctx->handle;
    return true;
}

/* vmm_device — 属性名称所指向的卡，或者如果调用线程没有指定，则指当前线程所指向的卡。 */
static int vmm_device(const hipMemAllocationProp *prop)
{
    if (prop != NULL && prop->location.type == hipMemLocationTypeDevice) {
        return prop->location.id;
    }
    return vrocm_current_device();
}

VROCM_EXPORT hipError_t hipMemCreate(hipMemGenericAllocationHandle_t *handle, size_t size,
                                     const hipMemAllocationProp *prop, unsigned long long flags)
{
    VROCM_REAL(hipMemCreate, fn_mem_create);
    VROCM_ENTRY(hipMemCreate);

    struct vmm_ctx ctx = { .handle = handle, .size = size, .prop = prop, .flags = flags,
                           .real = real_hipMemCreate };
    enum vrocm_admit rc;

    VROCM_HIT(hipMemCreate);
    rc = vrocm_ledger_admit(vmm_device(prop), size, call_mem_create, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMemCreate);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 退款的发放取决于其状态，原因在于当前树中的每一个免费项都是：一个处理运行时拒绝释放的句柄，另一个
 * 仍然支持使用的句柄，将记账项重新记账则会让容器再次使用那些字节。 */
VROCM_EXPORT hipError_t hipMemRelease(hipMemGenericAllocationHandle_t handle)
{
    hipError_t status;

    VROCM_REAL(hipMemRelease, fn_mem_release);
    VROCM_ENTRY(hipMemRelease);

    VROCM_HIT(hipMemRelease);
    status = real_hipMemRelease(handle);
    if (status == hipSuccess) {
        vrocm_ledger_release((unsigned long long)(uintptr_t)handle, NULL, NULL);
    }
    return status;
}

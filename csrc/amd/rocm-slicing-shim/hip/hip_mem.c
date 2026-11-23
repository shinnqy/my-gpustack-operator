/* hip_mem.c 实现 HIP 的常规显存分配入口，以及成功释放后归还额度的对应入口。
 *
 * 这些入口都经过实际测量。若只拦截 hipMalloc，在 256 MiB 限额下，hipMallocManaged、
 * hipExtMallocWithFlags 和 hipMallocPitch 仍各能成功申请 512 MiB。它们是独立导出、访问同一设备内存
 * 的入口，因此各自都要接入账本。
 *
 * 运行时 API 和驱动 API 使用不同的导出符号，例如 hipMallocPitch 与 hipMemAllocPitch、
 * hipMallocArray 与 hipArrayCreate、hipFreeArray 与 hipArrayDestroy。每对入口作用于同一类显存。实
 * 测仅拦截运行时入口时，驱动入口仍可在 64 MiB 限额下申请 512 MiB。任一入口创建的分配，都要能从配对
 * 的任何释放入口归还额度。
 *
 * hipHostMalloc 锁定的是系统内存。该调用计入调用次数，但不占显存配额，避免因主机内存用量拒绝正常的
 * 设备分配。 */
#define _GNU_SOURCE

#include <stdint.h>

#include <hip/hip_runtime_api.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_log.h"
#include "hip/hip_resolve.h"
#include "hip/hip_table.h"

VROCM_INTERNAL int vrocm_current_device(void);

typedef hipError_t (*fn_malloc)(void **, size_t);
typedef hipError_t (*fn_managed)(void **, size_t, unsigned int);
typedef hipError_t (*fn_pitch)(void **, size_t *, size_t, size_t);
typedef hipError_t (*fn_drv_pitch)(hipDeviceptr_t *, size_t *, size_t, size_t, unsigned int);
typedef hipError_t (*fn_malloc3d)(struct hipPitchedPtr *, struct hipExtent);
typedef hipError_t (*fn_drv_array)(hipArray_t *, const HIP_ARRAY_DESCRIPTOR *);
typedef hipError_t (*fn_drv_array3d)(hipArray_t *, const HIP_ARRAY3D_DESCRIPTOR *);
typedef hipError_t (*fn_drv_array_destroy)(hipArray_t);
typedef hipError_t (*fn_ext_flags)(void **, size_t, unsigned int);
typedef hipError_t (*fn_array)(hipArray_t *, const struct hipChannelFormatDesc *, size_t, size_t,
                               unsigned int);
typedef hipError_t (*fn_array3d)(hipArray_t *, const struct hipChannelFormatDesc *,
                                 struct hipExtent, unsigned int);
typedef hipError_t (*fn_free)(void *);
typedef hipError_t (*fn_free_array)(hipArray_t);
typedef hipError_t (*fn_host_malloc)(void **, size_t, unsigned int);

/* vrocm_admit_status 将一次配额检查结果转换为 HIP 状态码。
 *
 * 拦截库拒绝分配时返回框架能够处理的 hipErrorOutOfMemory，例如 PyTorch 会显示 HIP out of memory 和
 * 虚拟显存容量。若真正的运行时返回错误，则保留它的原始状态。 */
VROCM_INTERNAL hipError_t vrocm_admit_status(enum vrocm_admit rc, hipError_t runtime_status)
{
    switch (rc) {
    case VROCM_ADMIT_OK:
        return hipSuccess;
    case VROCM_ADMIT_ALLOC_FAILED:
        return runtime_status;
    default:
        return hipErrorOutOfMemory;
    }
}

/* 常规分配入口。 */

struct plain_ctx {
    hipError_t status;
    void **ptr;
    size_t size;
    unsigned int flags;
    fn_malloc plain;
    fn_managed managed;
    fn_ext_flags ext;
};

static bool call_plain(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct plain_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->plain(ctx->ptr, ctx->size);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

static bool call_managed(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct plain_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->managed(ctx->ptr, ctx->size, ctx->flags);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

static bool call_ext(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct plain_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->ext(ctx->ptr, ctx->size, ctx->flags);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

VROCM_EXPORT hipError_t hipMalloc(void **ptr, size_t size)
{
    // IMPORTANT: 1. 用 dlsym(RTLD_NEXT, "hipMalloc") 找到真正 ROCm 库里的原函数指针 real_hipMalloc
    VROCM_REAL(hipMalloc, fn_malloc);
    VROCM_ENTRY(hipMalloc);

    struct plain_ctx ctx = { .ptr = ptr, .size = size, .plain = real_hipMalloc };
    enum vrocm_admit rc;

    VROCM_HIT(hipMalloc);
    // IMPORTANT: 2. 不直接调用 real_hipMalloc！而是把回调函数 call_plain 交给账本管理器 vrocm_ledger_admit
    rc = vrocm_ledger_admit(vrocm_current_device(), size, call_plain, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMalloc);
    }
    // IMPORTANT: 3. 如果被配额拦截，转换成 PyTorch 能听懂的标准错误码 hipErrorOutOfMemory
    return vrocm_admit_status(rc, ctx.status);
}

VROCM_EXPORT hipError_t hipMallocManaged(void **dev_ptr, size_t size, unsigned int flags)
{
    VROCM_REAL(hipMallocManaged, fn_managed);
    VROCM_ENTRY(hipMallocManaged);

    struct plain_ctx ctx = { .ptr = dev_ptr, .size = size, .flags = flags,
                             .managed = real_hipMallocManaged };
    enum vrocm_admit rc;

    VROCM_HIT(hipMallocManaged);
    rc = vrocm_ledger_admit(vrocm_current_device(), size, call_managed, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMallocManaged);
    }
    return vrocm_admit_status(rc, ctx.status);
}

VROCM_EXPORT hipError_t hipExtMallocWithFlags(void **ptr, size_t size_bytes, unsigned int flags)
{
    VROCM_REAL(hipExtMallocWithFlags, fn_ext_flags);
    VROCM_ENTRY(hipExtMallocWithFlags);

    struct plain_ctx ctx = { .ptr = ptr, .size = size_bytes, .flags = flags,
                             .ext = real_hipExtMallocWithFlags };
    enum vrocm_admit rc;

    VROCM_HIT(hipExtMallocWithFlags);
    rc = vrocm_ledger_admit(vrocm_current_device(), size_bytes, call_ext, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipExtMallocWithFlags);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 带行距的分配需要在调用前后传递字节数。调用方指定宽度，驱动返回实际 stride，实际占用为 stride ×
 * height。在同一把卡锁内按宽度准入、按实际行距结算。 */
struct pitch_ctx {
    hipError_t status;
    void **ptr;
    size_t *pitch;
    size_t width;
    size_t height;
    fn_pitch real;
};

static bool call_pitch(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct pitch_ctx *ctx = c;

    ctx->status = ctx->real(ctx->ptr, ctx->pitch, ctx->width, ctx->height);
    if (ctx->status != hipSuccess) {
        return false;
    }
    if (ctx->pitch != NULL && ctx->height != 0 && *ctx->pitch > ctx->width) {
        *bytes = (unsigned long long)*ctx->pitch * ctx->height;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

VROCM_EXPORT hipError_t hipMallocPitch(void **ptr, size_t *pitch, size_t width, size_t height)
{
    VROCM_REAL(hipMallocPitch, fn_pitch);
    VROCM_ENTRY(hipMallocPitch);

    struct pitch_ctx ctx = { .ptr = ptr, .pitch = pitch, .width = width, .height = height,
                             .real = real_hipMallocPitch };
    enum vrocm_admit rc;

    VROCM_HIT(hipMallocPitch);
    rc = vrocm_ledger_admit(vrocm_current_device(), (unsigned long long)width * height, call_pitch,
                            &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMallocPitch);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 驱动 API 的带行距分配拥有独立的导出符号。只拦截 hipMallocPitch 时，驱动入口仍曾在 64 MiB 限额下
 * 分配 512 MiB，所以此处也要核对额度。 */
struct drv_pitch_ctx {
    hipError_t status;
    hipDeviceptr_t *ptr;
    size_t *pitch;
    size_t width;
    size_t height;
    unsigned int element;
    fn_drv_pitch real;
};

static bool call_drv_pitch(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct drv_pitch_ctx *ctx = c;

    ctx->status = ctx->real(ctx->ptr, ctx->pitch, ctx->width, ctx->height, ctx->element);
    if (ctx->status != hipSuccess) {
        return false;
    }
    if (ctx->pitch != NULL && ctx->height != 0 && *ctx->pitch > ctx->width) {
        *bytes = (unsigned long long)*ctx->pitch * ctx->height;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->ptr;
    return true;
}

VROCM_EXPORT hipError_t hipMemAllocPitch(hipDeviceptr_t *dptr, size_t *pitch, size_t width_in_bytes,
                                         size_t height, unsigned int element_size_bytes)
{
    VROCM_REAL(hipMemAllocPitch, fn_drv_pitch);
    VROCM_ENTRY(hipMemAllocPitch);

    struct drv_pitch_ctx ctx = { .ptr = dptr, .pitch = pitch, .width = width_in_bytes,
                                 .height = height, .element = element_size_bytes,
                                 .real = real_hipMemAllocPitch };
    enum vrocm_admit rc;

    VROCM_HIT(hipMemAllocPitch);
    rc = vrocm_ledger_admit(vrocm_current_device(), (unsigned long long)width_in_bytes * height,
                            call_drv_pitch, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMemAllocPitch);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 三维带行距分配相当于在 hipMallocPitch 的宽度和高度之外增加深度。它直接导出到同一线性设备显存。实
 * 测只拦截其他入口时，hipMalloc 正确拒绝超额请求，但这个入口仍能在 64 MiB 额度下分配 512 MiB。
 *
 * 三维线性分配的宽度以字节表示。后面的数组入口则以元素计数，须结合通道格式才能换算为字节。 */
struct malloc3d_ctx {
    hipError_t status;
    struct hipPitchedPtr *ptr;
    struct hipExtent extent;
    fn_malloc3d real;
};

static bool call_malloc3d(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct malloc3d_ctx *ctx = c;

    ctx->status = ctx->real(ctx->ptr, ctx->extent);
    if (ctx->status != hipSuccess) {
        return false;
    }
    if (ctx->ptr->pitch > ctx->extent.width) {
        *bytes = (unsigned long long)ctx->ptr->pitch * (ctx->extent.height ? ctx->extent.height : 1) *
                 (ctx->extent.depth ? ctx->extent.depth : 1);
    }
    *key = (unsigned long long)(uintptr_t)ctx->ptr->ptr;
    return true;
}

VROCM_EXPORT hipError_t hipMalloc3D(struct hipPitchedPtr *pitched_dev_ptr, struct hipExtent extent)
{
    VROCM_REAL(hipMalloc3D, fn_malloc3d);
    VROCM_ENTRY(hipMalloc3D);

    struct malloc3d_ctx ctx = { .ptr = pitched_dev_ptr, .extent = extent,
                                .real = real_hipMalloc3D };
    enum vrocm_admit rc;

    VROCM_HIT(hipMalloc3D);
    rc = vrocm_ledger_admit(vrocm_current_device(),
                            (unsigned long long)extent.width * (extent.height ? extent.height : 1) *
                                (extent.depth ? extent.depth : 1),
                            call_malloc3d, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMalloc3D);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 数组分配的参数没有直接给出字节数，按格式描述和各维度计算。运行时可能额外填充且不报告填充大小，因
 * 此数组按调用方请求量近似记账。 */
static unsigned long long array_bytes(const struct hipChannelFormatDesc *desc, size_t width,
                                      size_t height, size_t depth)
{
    unsigned long long bits;

    if (desc == NULL) {
        return 0;
    }
    bits = (unsigned long long)desc->x + desc->y + desc->z + desc->w;
    return ((bits + 7ULL) / 8ULL) * (width ? width : 1) * (height ? height : 1) *
           (depth ? depth : 1);
}

struct array_ctx {
    hipError_t status;
    hipArray_t *array;
    const struct hipChannelFormatDesc *desc;
    size_t width;
    size_t height;
    struct hipExtent extent;
    unsigned int flags;
    fn_array real2d;
    fn_array3d real3d;
};

static bool call_array(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct array_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->real2d(ctx->array, ctx->desc, ctx->width, ctx->height, ctx->flags);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->array;
    return true;
}

static bool call_array3d(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct array_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->real3d(ctx->array, ctx->desc, ctx->extent, ctx->flags);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->array;
    return true;
}

VROCM_EXPORT hipError_t hipMallocArray(hipArray_t *array, const struct hipChannelFormatDesc *desc, size_t width,
                          size_t height, unsigned int flags)
{
    VROCM_REAL(hipMallocArray, fn_array);
    VROCM_ENTRY(hipMallocArray);

    struct array_ctx ctx = { .array = array, .desc = desc, .width = width, .height = height,
                             .flags = flags, .real2d = real_hipMallocArray };
    enum vrocm_admit rc;

    VROCM_HIT(hipMallocArray);
    rc = vrocm_ledger_admit(vrocm_current_device(), array_bytes(desc, width, height, 1), call_array,
                            &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMallocArray);
    }
    return vrocm_admit_status(rc, ctx.status);
}

VROCM_EXPORT hipError_t hipMalloc3DArray(hipArray_t *array, const struct hipChannelFormatDesc *desc,
                            struct hipExtent extent, unsigned int flags)
{
    VROCM_REAL(hipMalloc3DArray, fn_array3d);
    VROCM_ENTRY(hipMalloc3DArray);

    struct array_ctx ctx = { .array = array, .desc = desc, .extent = extent, .flags = flags,
                             .real3d = real_hipMalloc3DArray };
    enum vrocm_admit rc;

    VROCM_HIT(hipMalloc3DArray);
    rc = vrocm_ledger_admit(vrocm_current_device(),
                            array_bytes(desc, extent.width, extent.height, extent.depth),
                            call_array3d, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipMalloc3DArray);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 驱动 API 的数组入口同样独立导出。实测 hipArrayCreate 能在 64 MiB 限额下分配 512 MiB。这里使用格
 * 式枚举和通道数计算大小，与运行时数组入口共用准入和记账流程。 */
static unsigned long long format_bytes(hipArray_Format format)
{
    switch (format) {
    case HIP_AD_FORMAT_UNSIGNED_INT8:
    case HIP_AD_FORMAT_SIGNED_INT8:
        return 1;
    case HIP_AD_FORMAT_UNSIGNED_INT16:
    case HIP_AD_FORMAT_SIGNED_INT16:
    case HIP_AD_FORMAT_HALF:
        return 2;
    default:
        /* 枚举中的其余格式每个通道占四字节。未知格式也先按四字节计，以免低估新格式的显存占用。 */
        return 4;
    }
}

static unsigned long long drv_array_bytes(hipArray_Format format, unsigned int channels,
                                          size_t width, size_t height, size_t depth)
{
    return format_bytes(format) * (channels ? channels : 1) * (width ? width : 1) *
           (height ? height : 1) * (depth ? depth : 1);
}

struct drv_array_ctx {
    hipError_t status;
    hipArray_t *array;
    const HIP_ARRAY_DESCRIPTOR *desc2d;
    const HIP_ARRAY3D_DESCRIPTOR *desc3d;
    fn_drv_array real2d;
    fn_drv_array3d real3d;
};

static bool call_drv_array(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct drv_array_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->real2d(ctx->array, ctx->desc2d);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->array;
    return true;
}

static bool call_drv_array3d(void *c, unsigned long long *key, unsigned long long *bytes)
{
    struct drv_array_ctx *ctx = c;

    (void)bytes;
    ctx->status = ctx->real3d(ctx->array, ctx->desc3d);
    if (ctx->status != hipSuccess) {
        return false;
    }
    *key = (unsigned long long)(uintptr_t)*ctx->array;
    return true;
}

VROCM_EXPORT hipError_t hipArrayCreate(hipArray_t *array, const HIP_ARRAY_DESCRIPTOR *desc)
{
    VROCM_REAL(hipArrayCreate, fn_drv_array);
    VROCM_ENTRY(hipArrayCreate);

    struct drv_array_ctx ctx = { .array = array, .desc2d = desc, .real2d = real_hipArrayCreate };
    enum vrocm_admit rc;
    unsigned long long bytes = 0;

    VROCM_HIT(hipArrayCreate);
    if (desc != NULL) {
        bytes = drv_array_bytes(desc->Format, desc->NumChannels, desc->Width, desc->Height, 1);
    }
    rc = vrocm_ledger_admit(vrocm_current_device(), bytes, call_drv_array, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipArrayCreate);
    }
    return vrocm_admit_status(rc, ctx.status);
}

VROCM_EXPORT hipError_t hipArray3DCreate(hipArray_t *array, const HIP_ARRAY3D_DESCRIPTOR *desc)
{
    VROCM_REAL(hipArray3DCreate, fn_drv_array3d);
    VROCM_ENTRY(hipArray3DCreate);

    struct drv_array_ctx ctx = { .array = array, .desc3d = desc, .real3d = real_hipArray3DCreate };
    enum vrocm_admit rc;
    unsigned long long bytes = 0;

    VROCM_HIT(hipArray3DCreate);
    if (desc != NULL) {
        bytes = drv_array_bytes(desc->Format, desc->NumChannels, desc->Width, desc->Height,
                                desc->Depth);
    }
    rc = vrocm_ledger_admit(vrocm_current_device(), bytes, call_drv_array3d, &ctx);
    if (rc != VROCM_ADMIT_OK) {
        VROCM_DENIED(hipArray3DCreate);
    }
    return vrocm_admit_status(rc, ctx.status);
}

/* 释放入口。 */

/* 仅在底层运行时确认释放成功后归还显存额度。无效指针或重复释放都不会释放实际显存，此时若给账本加回
 * 额度，同一批字节就会被再次分配。
 *
 * 库未登记的指针不归还任何额度，包括预加载之前分配的指针和无效指针。先检查 NULL 可以省去一次注定不
 * 会命中的表查找。 */
VROCM_EXPORT hipError_t hipFree(void *ptr)
{
    hipError_t status;

    VROCM_REAL(hipFree, fn_free);
    VROCM_ENTRY(hipFree);

    VROCM_HIT(hipFree);
    status = real_hipFree(ptr);
    if (status == hipSuccess && ptr != NULL) {
        // 只有真正释放成功且指针非空，才从账本里退还当时记下的字节数！
        vrocm_ledger_release((unsigned long long)(uintptr_t)ptr, NULL, NULL);
    }
    return status;
}

VROCM_EXPORT hipError_t hipFreeArray(hipArray_t array)
{
    hipError_t status;

    VROCM_REAL(hipFreeArray, fn_free_array);
    VROCM_ENTRY(hipFreeArray);

    VROCM_HIT(hipFreeArray);
    status = real_hipFreeArray(array);
    if (status == hipSuccess && array != NULL) {
        vrocm_ledger_release((unsigned long long)(uintptr_t)array, NULL, NULL);
    }
    return status;
}

/* 驱动 API 的释放入口与 hipFreeArray 接受相同句柄，均能释放任一对应入口创建的数组。因此两处都按同
 * 一个键归还额度，未登记的键无需处理。只拦截其中一个入口会导致另一条路径的账目一直保留到进程退出。 */
VROCM_EXPORT hipError_t hipArrayDestroy(hipArray_t array)
{
    hipError_t status;

    VROCM_REAL(hipArrayDestroy, fn_drv_array_destroy);
    VROCM_ENTRY(hipArrayDestroy);

    VROCM_HIT(hipArrayDestroy);
    status = real_hipArrayDestroy(array);
    if (status == hipSuccess && array != NULL) {
        vrocm_ledger_release((unsigned long long)(uintptr_t)array, NULL, NULL);
    }
    return status;
}

/* 主机内存调用只统计次数，不占显存配额。 */

VROCM_EXPORT hipError_t hipHostMalloc(void **ptr, size_t size, unsigned int flags)
{
    VROCM_REAL(hipHostMalloc, fn_host_malloc);
    VROCM_ENTRY(hipHostMalloc);

    VROCM_HIT(hipHostMalloc);
    return real_hipHostMalloc(ptr, size, flags);
}

VROCM_EXPORT hipError_t hipHostFree(void *ptr)
{
    VROCM_REAL(hipHostFree, fn_free);
    VROCM_ENTRY(hipHostFree);

    VROCM_HIT(hipHostFree);
    return real_hipHostFree(ptr);
}

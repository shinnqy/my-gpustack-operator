/* hip_query.c — 报告容量系列：每种导出的工作负载方式都能用来查询卡子的内存大小。
 *
 * 共有四个函数，其中那个显而易见的函数从未被调用。libamdhip64在三个不同的地址处导出
 * hipGetDeviceProperties@@hip_4.2、hipGetDevicePropertiesR0600@@hip_6.0和
 * hipGetDevicePropertiesR0000@@hip_4.2以及hipDeviceTotalMem。ROCm 6+ 头文件中的宏将普通名称映射到
 * R0600，而追踪器仅记录到R0600的绑定情况。仅拦截普通名称并不能实现任何虚拟化效果。
 *
 * 这两个结构体形状并不相同。在 ROCm 7.2.4 上测试发现：hipDeviceProp_tR0600 大小为 1472 字节，其中
 * totalGlobalMem 为 288；而 hipDeviceProp_tR0000 大小为 792 字节，其中 totalGlobalMem 为 256。如果
 * 将 6.0 字段写入传递给 4.2 结构体的调用者中，会超出调用者预期的范围存储 32 字节——这是内存损坏问题，
 * 而非配额未满足。因此，6.0 之前的条目仍使用 6.0 之前的类型，偏移量则通过 offsetof 获取并固定在下
 * 方。
 *
 * MULTIPROCESSORCOUNT 被故意保留原样不修改。此库并不强制进行计算；平台通过 CU 掩码来实现这一点，且
 * 测量结果显示该掩码也不会改变 multiProcessorCount 的值。若在此处重写它，将会生成一个既不符合硬件
 * 要求也不符合掩码要求的数值。 */
#define _GNU_SOURCE

#include <stddef.h>

#include <hip/hip_runtime_api.h>
/* 6.0 之前的属性结构体仅在此处声明。它是一个 amd_detail/ 头文件，是一个值得命名的依赖项：如果未来
 * 的 ROCm 对该项进行修改或移除，此文件将无法编译，这是合理的失败情况——另一种做法则是硬编码 256 并
 * 需要在运行时处理问题。 */
#include <hip/amd_detail/hip_prof_str.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_log.h"
#include "common/vrocm_quota.h"
#include "hip/hip_resolve.h"
#include "hip/hip_table.h"

/* 在 ROCm 7.2 / gfx1101 和 ROCm 7.2.4 / gfx942 上的回归测试工具——两者结果一致——以及针对 ROCm 6.4
 * 头文件进行的验证也均得到确认，正是这些才使得人们能够构建适用于所有版本的软件。任何更改上述内容的
 * 版本都必须进行重新测试，而非凭猜测确定，这也是防止其默默发布的原因。 */
_Static_assert(sizeof(hipDeviceProp_tR0600) == 1472, "hipDeviceProp_t size changed; re-measure");
_Static_assert(offsetof(hipDeviceProp_tR0600, totalGlobalMem) == 288,
               "totalGlobalMem moved; re-measure");
_Static_assert(offsetof(hipDeviceProp_tR0600, multiProcessorCount) == 388,
               "multiProcessorCount moved; re-measure");
_Static_assert(sizeof(hipDeviceProp_tR0000) == 792, "pre-6.0 prop size changed; re-measure");
_Static_assert(offsetof(hipDeviceProp_tR0000, totalGlobalMem) == 256,
               "pre-6.0 totalGlobalMem moved; re-measure");

typedef hipError_t (*fn_mem_get_info)(size_t *, size_t *);
typedef hipError_t (*fn_total_mem)(size_t *, hipDevice_t);
typedef hipError_t (*fn_props_r0600)(hipDeviceProp_tR0600 *, int);
typedef hipError_t (*fn_props_r0000)(hipDeviceProp_tR0000 *, int);
typedef hipError_t (*fn_get_device)(int *);

/* vrocm_current_device — 当前生效的卡的容器本地索引。
 *
 * 与进行分配的家庭共享资源，它们出于相同原因需要相同的答案：该拦截库中每个元素都是按卡计算的，而索
 * 引则是 ROCR_VISIBLE_DEVICES 对列表进行筛选和重新排序后的下一个索引——相同的索引空间 HSA_CU_MASK
 * 的 GPU_list 就使用这个索引。 */
VROCM_INTERNAL int vrocm_current_device(void)
{
    VROCM_REAL(hipGetDevice, fn_get_device);

    int device = 0;

    if (real_hipGetDevice(&device) != hipSuccess) {
        return -1;
    }
    return device;
}

/* reported_total — 用于回答容量问题的数值：该部分数量，但绝不会超过卡所拥有的数量。
 *
 * 只能向下移动。运行时报告的数值高于实际值，说明配置错误——分配器根据卡的自身容量来生成所有数值，因
 * 此无法生成该数值；响应此问题则会表明无法分配内存。强制限制值实际上就是两者中较小的值，因为无论账
 * 本显示如何，超过卡容量的请求在驱动程序中都会失败；在此降低限制值则使报告数值与强制限制值一致。如
 * 果框架以该值为基准进行大小调整，否则该差异将被视为内存不足而非错误答案。 */
static size_t reported_total(size_t real_total, unsigned long long quota)
{
    return quota != 0 && quota < real_total ? (size_t)quota : real_total;
}

VROCM_EXPORT hipError_t hipMemGetInfo(size_t *free_bytes, size_t *total_bytes)
{
    VROCM_REAL(hipMemGetInfo, fn_mem_get_info);
    VROCM_ENTRY(hipMemGetInfo);

    hipError_t status;
    unsigned long long quota, used;
    int device;

    VROCM_HIT(hipMemGetInfo);
    status = real_hipMemGetInfo(free_bytes, total_bytes);
    if (status != hipSuccess) {
        return status;
    }

    device = vrocm_current_device();
    quota = vrocm_quota_memory_bytes(device);
    if (quota == 0) {
        return status;
    }

    used = vrocm_ledger_used(device);
    if (total_bytes != NULL) {
        *total_bytes = reported_total(*total_bytes, quota);
    }
    if (free_bytes != NULL) {
        size_t remaining = (size_t)(used < quota ? quota - used : 0);

        /* 切片剩余量与卡剩余量中较小的数值。仅报告切片数值，会使得卡无法提供内存，因为其邻居已经占满内存；
         * 以这种方式确定内存大小的框架，在分配过程中会失败，而非在问题本身上失败。 */
        *free_bytes = remaining < *free_bytes ? remaining : *free_bytes;
    }
    return status;
}

VROCM_EXPORT hipError_t hipDeviceTotalMem(size_t *bytes, hipDevice_t device)
{
    VROCM_REAL(hipDeviceTotalMem, fn_total_mem);
    VROCM_ENTRY(hipDeviceTotalMem);

    hipError_t status;
    unsigned long long quota;

    VROCM_HIT(hipDeviceTotalMem);
    status = real_hipDeviceTotalMem(bytes, device);
    if (status != hipSuccess || bytes == NULL) {
        return status;
    }
    quota = vrocm_quota_memory_bytes((int)device);
    *bytes = reported_total(*bytes, quota);
    return status;
}

VROCM_EXPORT hipError_t hipGetDevicePropertiesR0600(hipDeviceProp_tR0600 *prop, int device)
{
    VROCM_REAL(hipGetDevicePropertiesR0600, fn_props_r0600);
    VROCM_ENTRY(hipGetDevicePropertiesR0600);

    hipError_t status;
    unsigned long long quota;

    VROCM_HIT(hipGetDevicePropertiesR0600);
    status = real_hipGetDevicePropertiesR0600(prop, device);
    if (status != hipSuccess || prop == NULL) {
        return status;
    }
    quota = vrocm_quota_memory_bytes(device);
    prop->totalGlobalMem = reported_total(prop->totalGlobalMem, quota);
    return status;
}

VROCM_EXPORT hipError_t hipGetDevicePropertiesR0000(hipDeviceProp_tR0000 *prop, int device)
{
    VROCM_REAL(hipGetDevicePropertiesR0000, fn_props_r0000);
    VROCM_ENTRY(hipGetDevicePropertiesR0000);

    hipError_t status;
    unsigned long long quota;

    VROCM_HIT(hipGetDevicePropertiesR0000);
    status = real_hipGetDevicePropertiesR0000(prop, device);
    if (status != hipSuccess || prop == NULL) {
        return status;
    }
    quota = vrocm_quota_memory_bytes(device);
    prop->totalGlobalMem = reported_total(prop->totalGlobalMem, quota);
    return status;
}

/* 这个简单的名称是一个独立的 THIRD 实现，并非任何版本化的名称的别名，并且它包含 4.2 ABI 定义——因此
 * 需要使用 6.0 之前的结构。头文件中的宏映射将这个名称指向 R0600，因此定义它时首先需要移除该映射。 */
#undef hipGetDeviceProperties

VROCM_EXPORT hipError_t hipGetDeviceProperties(hipDeviceProp_tR0000 *prop, int device)
{
    VROCM_REAL(hipGetDeviceProperties, fn_props_r0000);
    VROCM_ENTRY(hipGetDeviceProperties);

    hipError_t status;
    unsigned long long quota;

    VROCM_HIT(hipGetDeviceProperties);
    status = real_hipGetDeviceProperties(prop, device);
    if (status != hipSuccess || prop == NULL) {
        return status;
    }
    quota = vrocm_quota_memory_bytes(device);
    prop->totalGlobalMem = reported_total(prop->totalGlobalMem, quota);
    return status;
}

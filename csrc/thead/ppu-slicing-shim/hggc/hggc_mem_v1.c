/* hggc_mem_v1.c — 第 v1 版的 ABI 内存条目。
 *
 * libhggc.so同时导出hgMemAlloc和hgMemAlloc_v2，Free、GetInfo、AllocPitch和AllocHost也是如此。每对
 * 函数中只有第二个版本会被源代码级别的调用编译生成，因为hggc.h将普通名称映射到版本化名称——因此仅基
 * 于该头文件编写的拦截库只能覆盖每对中的一半，另一半则保持开放状态。使用较旧SDK构建的工作负载，或
 * 手动解析普通名称的库，会触及那一半内容。
 *
 * V1 的接口形式并非带有不同名称的 V2 接口形式。HGdeviceptr_v1 为 unsigned int 类型，而 HGdeviceptr
 * 为 unsigned long long 类型；其大小也为 unsigned int，而非 size_t。若使用 V2 的原型来定义 V1 的符
 * 号，会导致调用者的参数被错误读取，因此下面的每个签名均为 V1 的版本。
 *
 * 为什么它们放在自己的文件中。定义普通符号意味着首先会执行 #undef 操作取消头文件的映射，而一个同时
 * 保留并取消该映射的文件，其定义顺序也会影响结果。这里有五个 #undef 操作，位于文件顶部，该文件除此
 * 之外没有其他内容。
 *
 * 这些原型函数来自 hggc.h 文件本身，该文件在 __HGGC_API_VERSION_INTERNAL 目录下对其进行了声明。这
 * 个宏被故意没有定义：它还会改变头文件中无关声明和枚举成员，这种改变过于广泛，无法通过仅修改五个签
 * 名来实现。 */
#define _GNU_SOURCE

#include <limits.h>

/* hggc.h 仅包含 <stdlib.h> 和 <stdint.h>，因此在其声明中既没有提供 NULL 也不提供 bool。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>

#include "common/vppu.h"
#include "hggc/hggc_quota.h"

/* 通过取消标题与版本化内容的映射，达成 v1 符号。 */
#undef hgMemAlloc
#undef hgMemAllocHost
#undef hgMemAllocPitch
#undef hgMemFree
#undef hgMemGetInfo

HGresult HGGCAPI hgMemAlloc(HGdeviceptr_v1 *dptr, unsigned int bytesize)
{
    int device = -1;

    vppu_hggc_count(VPPU_MEM_ALLOC_V1);
    if (!vppu_hggc_admit(VPPU_MEM_ALLOC_V1, bytesize, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr_v1 *, unsigned int) = vppu_hggc_next(VPPU_MEM_ALLOC_V1);
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

/* 这里两个维度都是 32 位，因此它们的乘积不会超过账本所运行的 64 位计数范围——v2 版本所需的溢出保护
 * 机制无需进行校验。 */
HGresult HGGCAPI hgMemAllocPitch(HGdeviceptr_v1 *dptr, unsigned int *pPitch,
                                 unsigned int WidthInBytes, unsigned int Height,
                                 unsigned int ElementSizeBytes)
{
    int device = -1;
    unsigned long long request = (unsigned long long)WidthInBytes * Height;

    vppu_hggc_count(VPPU_MEM_ALLOC_PITCH_V1);
    if (!vppu_hggc_admit(VPPU_MEM_ALLOC_PITCH_V1, request, &device)) {
        return HGGC_ERROR_OUT_OF_MEMORY;
    }

    HGresult (*real)(HGdeviceptr_v1 *, unsigned int *, unsigned int, unsigned int,
                     unsigned int) = vppu_hggc_next(VPPU_MEM_ALLOC_PITCH_V1);
    if (real == NULL) {
        vppu_hggc_rollback(device, request);
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr, pPitch, WidthInBytes, Height, ElementSizeBytes);
    if (rc == HGGC_SUCCESS && dptr != NULL) {
        unsigned long long actual = request;

        if (pPitch != NULL) {
            actual = (unsigned long long)*pPitch * Height;
        }
        vppu_hggc_commit_sized(device, (unsigned long long)*dptr, request, actual);
    } else {
        vppu_hggc_rollback(device, request);
    }
    return rc;
}

HGresult HGGCAPI hgMemFree(HGdeviceptr_v1 dptr)
{
    vppu_hggc_count(VPPU_MEM_FREE_V1);

    HGresult (*real)(HGdeviceptr_v1) = vppu_hggc_next(VPPU_MEM_FREE_V1);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(dptr);
    if (rc == HGGC_SUCCESS) {
        vppu_hggc_refund((unsigned long long)dptr);
    }
    return rc;
}

/* v1查询以unsigned int格式报告，因此超过4 GiB的配额无法使用。唯一诚实的回答就是达到上限：如果报告
 * 低32位数据，那么工作负载实际只有几百兆字节，而实际却拥有数十吉字节。 */
HGresult HGGCAPI hgMemGetInfo(unsigned int *free, unsigned int *total)
{
    vppu_hggc_count(VPPU_MEM_GET_INFO_V1);

    HGresult (*real)(unsigned int *, unsigned int *) = vppu_hggc_next(VPPU_MEM_GET_INFO_V1);
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
        *total = (quota > UINT_MAX) ? UINT_MAX : (unsigned int)quota;
    }
    if (free != NULL) {
        *free = (available > UINT_MAX) ? UINT_MAX : (unsigned int)available;
    }
    return rc;
}

/* 主机内存，因此不计入且不会进行记账——详见 hggc_mem.c 了解原因。 */
HGresult HGGCAPI hgMemAllocHost(void **pp, unsigned int bytesize)
{
    vppu_hggc_count(VPPU_MEM_ALLOC_HOST_V1);

    HGresult (*real)(void **, unsigned int) = vppu_hggc_next(VPPU_MEM_ALLOC_HOST_V1);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(pp, bytesize);
}

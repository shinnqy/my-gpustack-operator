/* hgml_nohook.c — 第一道拦截：相同的 HGML 符号，无 dlsym 拦截。
 *
 * 这样做是为了使 Gate 1 的结果具有明确性。在预加载状态下，它导出 hgmlDeviceGetMemoryInfo 和
 * hgmlDeviceGetMemoryInfo_v2 的方式与 hgml_dlsym_hook.c 中的方式完全一致，但它不会介入 dlsym() 操
 * 作——因此如果 ppu-smi 在预加载状态下仍报告物理卡尺寸，那么“仅定义符号即可”的论断就成立，且拦截臂
 * 的成功也确实由拦截机制导致，而非偶然现象。
 *
 * 有两个特性使其成为控制手段，而非出于错误目的的通行方式：它会显示自身的负载情况，因为那些无声失败
 * 加载的库也会显示实际数值；此外，如果其符号实体被输入，会打印出标记，因为这意味着有某种方式是通过
 * 全局作用域实现 HGML 的解析，而整个设计的出发点就是错误的。
 *
 * 仅包含 libc 链接，与拦截库相同。 */
#include <stdio.h>
#include <stdlib.h>

/* hgml.h 没有任何 #include 指令，也没有定义它在声明中使用的 NULL 和 bool。调用方在包含该头文件之前
 * 必须先提供两者。 */
#include <stdbool.h>
#include <stddef.h>

#include <hgml.h>

#define VPPU_ENV_MEMORY_LIMIT_MIB "VPPU_DEVICE_MEMORY_LIMIT_MIB"
#define VPPU_TAG "[vppu] "

/* quota_bytes — 特意从 hgml_dlsym_hook.c 中复制过来：此处每个构建产物都保持为一个独立的编译单元，
 * 因此其链接断言也由该单元自行处理。 */
static unsigned long long quota_bytes(void)
{
    const char *value = getenv(VPPU_ENV_MEMORY_LIMIT_MIB);
    if (value == NULL || *value == '\0') {
        return 0ULL;
    }

    char *end = NULL;
    unsigned long long mib = strtoull(value, &end, 10);
    if (end == value || mib == 0ULL) {
        return 0ULL;
    }
    return mib * 1024ULL * 1024ULL;
}

hgmlReturn_t hgmlDeviceGetMemoryInfo(hgmlDevice_t device, hgmlMemory_t *memory)
{
    (void)device;

    fprintf(stderr, VPPU_TAG "hgml_nohook: hgmlDeviceGetMemoryInfo CALLED unexpectedly\n");
    if (memory == NULL) {
        return HGML_ERROR_INVALID_ARGUMENT;
    }

    unsigned long long quota = quota_bytes();
    memory->total = quota;
    memory->used = 0ULL;
    memory->free = quota;
    return HGML_SUCCESS;
}

hgmlReturn_t hgmlDeviceGetMemoryInfo_v2(hgmlDevice_t device, hgmlMemory_v2_t *memory)
{
    (void)device;

    fprintf(stderr, VPPU_TAG "hgml_nohook: hgmlDeviceGetMemoryInfo_v2 CALLED unexpectedly\n");
    if (memory == NULL) {
        return HGML_ERROR_INVALID_ARGUMENT;
    }

    /* .version 属于调用方的内容，会原封不动地返回； .reserved 是驱动程序中的一项数值。 */
    unsigned long long quota = quota_bytes();
    memory->total = quota;
    memory->used = 0ULL;
    memory->free = quota;
    return HGML_SUCCESS;
}

__attribute__((constructor)) static void announce(void)
{
    fprintf(stderr, VPPU_TAG "hgml_nohook loaded, quota=%llu bytes, dlsym NOT hooked\n",
            quota_bytes());
}

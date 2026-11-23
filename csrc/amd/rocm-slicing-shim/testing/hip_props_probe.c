/* hip_props_probe.c — 每个导出的工作负载方式都能询问卡拥有多少内存，以及它们各自实际绑定到哪个符号
 * 上。
 *
 * 为什么绑定至关重要，正如该图所示。libamdhip64在三个不同的地址上导出
 * hipGetDeviceProperties@@hip_4.2、hipGetDevicePropertiesR0600@@hip_6.0和
 * hipGetDevicePropertiesR0000@@hip_4.2，而ROCm 6+的头文件宏会将普通名称映射为R0600。因此，仅使用普
 * 通名称的包装函数将无法进行编译、链接、加载或虚拟化——经测量，追踪器只记录到了一次绑定为R0600的情
 * 况。打印每个名称对应的对象，才能将这一说法转化为实际观察：当库被预加载后，被拦截的名称会解析为
 * libvrocm.so，而仍解析为libamdhip64.so的部分则是一个空白。
 *
 * 为什么显示四个入口点，而不是一个？hipMemGetInfo、hipDeviceTotalMem 和
 * hipDeviceProp_t.totalGlobalMem 是访问卡容量的三种独立方法，框架可以任选其中一种。PyTorch 的
 * get_device_properties().total_memory 采用第三种方法，因此若不处理会让实际容量被泄露给最有可能根
 * 据它来分配内存空间的代码。
 *
 * 这两个属性STRUCT的结构并不相同，因此此报告了其编译时所需的尺寸：hipDeviceProp_tR0600的大小为1472
 * 字节，且totalGlobalMem为288；而hipDeviceProp_tR0000的大小为792字节，且totalGlobalMem为256。对不
 * 同ROCm版本进行此输出的比较，旨在验证hip/hip_query.c中的回归测试工具是否仍然能正确描述其运行环境。
 *
 * MULTIPROCESSORCOUNT 被报告了，预计不会发生变化。该库不会重新计算该值，测量得到的 CU 掩码也是如此
 * ——因此这个数值属于该卡在任何配额和掩码下的实际值。将其打印出来是因为如果该值发生变化，要么会出现
 * 某个库开始重新计算计算数值，要么会出现运行时改变主意的情况。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include <hip/hip_runtime.h>
#include <hip/hip_runtime_api.h>
/* 6.0之前的性质结构体仅在此处，在amd_detail/头文件中被声明——同样的依赖项hip/hip_query.c也使用它，
 * 原因也是一样的：否则就需要硬编码256。 */
#include <hip/amd_detail/hip_prof_str.h>

#define MIB (1024ULL * 1024ULL)

/* report_binding — 哪个共享对象定义了该进程会调用的符号。
 *
 * dlsym(RTLD_DEFAULT)采用加载器所使用的相同搜索顺序，因此这回答了该案例实际想要解答的问题：不是“
 * libvrocm是否导出此内容”，而是“调用操作是否会在那里执行”。 */
static void report_binding(const char *name)
{
    void *sym = dlsym(RTLD_DEFAULT, name);
    Dl_info info;

    if (sym == NULL) {
        printf("BIND name=%s object=<unresolved>\n", name);
        return;
    }
    if (dladdr(sym, &info) == 0 || info.dli_fname == NULL) {
        printf("BIND name=%s object=<unknown> addr=%p\n", name, sym);
        return;
    }
    printf("BIND name=%s object=%s addr=%p\n", name, info.dli_fname, sym);
}

int main(int argc, char **argv)
{
    hipDeviceProp_tR0600 prop600;
    hipDeviceProp_tR0000 prop000;
    size_t free_bytes = 0, total_bytes = 0, device_total = 0;
    int device = 0;
    hipError_t rc;

    if (argc > 1) {
        device = (int)strtol(argv[1], NULL, 10);
    }
    rc = hipSetDevice(device);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipSetDevice(%d): %s\n", device, hipGetErrorString(rc));
        return 2;
    }

    printf("LAYOUT r0600_size=%zu r0600_total_off=%zu r0600_mpc_off=%zu r0000_size=%zu"
           " r0000_total_off=%zu\n",
           sizeof(hipDeviceProp_tR0600), offsetof(hipDeviceProp_tR0600, totalGlobalMem),
           offsetof(hipDeviceProp_tR0600, multiProcessorCount), sizeof(hipDeviceProp_tR0000),
           offsetof(hipDeviceProp_tR0000, totalGlobalMem));

    report_binding("hipMemGetInfo");
    report_binding("hipDeviceTotalMem");
    report_binding("hipGetDeviceProperties");
    report_binding("hipGetDevicePropertiesR0600");
    report_binding("hipGetDevicePropertiesR0000");

    rc = hipMemGetInfo(&free_bytes, &total_bytes);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipMemGetInfo: %s\n", hipGetErrorString(rc));
        return 2;
    }
    printf("CAP entry=hipMemGetInfo total_mib=%llu free_mib=%llu\n",
           (unsigned long long)total_bytes / MIB, (unsigned long long)free_bytes / MIB);

    rc = hipDeviceTotalMem(&device_total, device);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipDeviceTotalMem: %s\n", hipGetErrorString(rc));
        return 2;
    }
    printf("CAP entry=hipDeviceTotalMem total_mib=%llu\n", (unsigned long long)device_total / MIB);

    /* 通过标题的宏映射，这就是实际工作负载的编译结果。 */
    rc = hipGetDeviceProperties(&prop600, device);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipGetDeviceProperties: %s\n", hipGetErrorString(rc));
        return 2;
    }
    printf("CAP entry=hipGetDeviceProperties(mapped) total_mib=%llu mpc=%d name=%s\n",
           (unsigned long long)prop600.totalGlobalMem / MIB, prop600.multiProcessorCount,
           prop600.gcnArchName);

    rc = hipGetDevicePropertiesR0600(&prop600, device);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipGetDevicePropertiesR0600: %s\n", hipGetErrorString(rc));
        return 2;
    }
    printf("CAP entry=hipGetDevicePropertiesR0600 total_mib=%llu mpc=%d\n",
           (unsigned long long)prop600.totalGlobalMem / MIB, prop600.multiProcessorCount);

    /* 6.0 之前的入口函数，使用 6.0 之前的结构体调用。将 6.0 的结构体传入会超出真实 6.0 之前调用者所期
     * 望的 32 字节范围——这会导致调用者帧中的损坏，而非配额被错过——而本程序的存在部分是为了证明包装器能
     * 够正确处理这些参数。 */
    rc = hipGetDevicePropertiesR0000(&prop000, device);
    if (rc != hipSuccess) {
        fprintf(stderr, "hip_props_probe: hipGetDevicePropertiesR0000: %s\n", hipGetErrorString(rc));
        return 2;
    }
    printf("CAP entry=hipGetDevicePropertiesR0000 total_mib=%llu mpc=%d\n",
           (unsigned long long)prop000.totalGlobalMem / MIB, prop000.multiProcessorCount);

    return 0;
}

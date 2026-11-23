/* hggc_launch.c — 驱动层启动条目。
 *
 * libhggc.so的启动面中每个导出名称对应一个定义，正如SDK的符号清单所列出的那样。其中就有十六个，每
 * 一个都包含在这里——无论是被限制还是仅被统计，原因就是内存面所说明的：覆盖度就是要求，而被遗漏的名
 * 称则是工作负载用于使用计算以达到该限制而看不到的方式。
 *
 * what A WRAPPER 的功能分为三种，而 hggc_quota.h 中的枚举类型按这些功能将各个名称进行了归类：
 * - gate: 将调用者保留到其卡的计算窗口允许启动时，然后调用供应商。该决策在 hggc_compute.c 中。
 * - 以图形式呈现的 gate：相同，会按窗口大小的可配置倍数进行记账，因为一个图式启动会运行被捕获到其
 * 中的任意数量的核心线程；
 * - count：以未改变的方式调用供应商，已统计了跨越量。主机拦截调用在 CPU 上执行，因此延迟它们不会释
 * 放任何卡资源。
 *
 * 拒绝并不等同于停滞。vppu_hggc_gate() 会等待一个仅需等待的启动过程；只有在容器配置完全无法执行时，
 * 它才会返回错误值——即没有可用的配额、没有当前上下文、没有可访问的账本。这就是下方
 * HGGC_ERROR_NOT_PERMITTED 的含义：与 HGGC_ERROR_OUT_OF_MEMORY 一样，内存条目也处于失败关闭状态，
 * 绝不会允许启动过程进行。
 *
 * 与显存入口一样，函数签名直接由厂商头文件提供。内核启动入口没有版本映射，libhggc.so 导出
 * hgLaunchKernel 而非 hgLaunchKernel_v2，所以普通名称直接定义普通符号，编译器会按头文件检查每个参
 * 数。_ptsz 变体使用 __typeof__ 从普通入口取得类型。只有定义 __HGGC_API_PER_THREAD_DEFAULT_STREAM
 * 时，头文件才会把普通名称映射到 _ptsz，本库不能定义该宏。 */
#define _GNU_SOURCE

/* hggc.h 仅包含 <stdlib.h> 和 <stdint.h>，因此在其声明中既没有提供 NULL 也不提供 bool。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>

/* hgLaunchKernelExAD及其_ptsz变体由libhggc.so导出，但声明在AD头文件中而非hggc.h中，因此覆盖它们意
 * 味着包含该头文件——否则就需要为这个文件中的配置结构体编写两个手工编写的原型。 */
#include <hggc_ad.h>

#include "common/vppu.h"
#include "hggc/hggc_quota.h"

/* 那些基于其所映射的普通条目的流变体。 */
extern __typeof__(hgLaunchKernel) hgLaunchKernel_ptsz;
extern __typeof__(hgLaunchKernelEx) hgLaunchKernelEx_ptsz;
extern __typeof__(hgLaunchKernelExAD) hgLaunchKernelExAD_ptsz;
extern __typeof__(hgLaunchCooperativeKernel) hgLaunchCooperativeKernel_ptsz;
extern __typeof__(hgGraphLaunch) hgGraphLaunch_ptsz;
extern __typeof__(hgLaunchHostFunc) hgLaunchHostFunc_ptsz;

/* --------------------------------------------------------------------------------------- 内核启动。
 * 受限制区域：这是工作负载使用卡的地方。
 * --------------------------------------------------------------------------------------- */

static HGresult launch_kernel(enum vppu_entry entry, HGfunction f, unsigned int gridDimX,
                              unsigned int gridDimY, unsigned int gridDimZ,
                              unsigned int blockDimX, unsigned int blockDimY,
                              unsigned int blockDimZ, unsigned int sharedMemBytes,
                              HGstream hStream, void **kernelParams, void **extra)
{
    vppu_hggc_count(entry);
    if (!vppu_hggc_gate(entry, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGfunction, unsigned int, unsigned int, unsigned int, unsigned int,
                     unsigned int, unsigned int, unsigned int, HGstream, void **, void **) =
        vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(f, gridDimX, gridDimY, gridDimZ, blockDimX, blockDimY, blockDimZ, sharedMemBytes,
                hStream, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernel(HGfunction f, unsigned int gridDimX, unsigned int gridDimY,
                                unsigned int gridDimZ, unsigned int blockDimX,
                                unsigned int blockDimY, unsigned int blockDimZ,
                                unsigned int sharedMemBytes, HGstream hStream,
                                void **kernelParams, void **extra)
{
    return launch_kernel(VPPU_LAUNCH_KERNEL, f, gridDimX, gridDimY, gridDimZ, blockDimX,
                         blockDimY, blockDimZ, sharedMemBytes, hStream, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernel_ptsz(HGfunction f, unsigned int gridDimX, unsigned int gridDimY,
                                     unsigned int gridDimZ, unsigned int blockDimX,
                                     unsigned int blockDimY, unsigned int blockDimZ,
                                     unsigned int sharedMemBytes, HGstream hStream,
                                     void **kernelParams, void **extra)
{
    return launch_kernel(VPPU_LAUNCH_KERNEL_PTSZ, f, gridDimX, gridDimY, gridDimZ, blockDimX,
                         blockDimY, blockDimZ, sharedMemBytes, hStream, kernelParams, extra);
}

static HGresult launch_kernel_ex(enum vppu_entry entry, const HGlaunchConfig *config,
                                 HGfunction f, void **kernelParams, void **extra)
{
    vppu_hggc_count(entry);
    if (!vppu_hggc_gate(entry, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(const HGlaunchConfig *, HGfunction, void **, void **) =
        vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(config, f, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernelEx(const HGlaunchConfig *config, HGfunction f,
                                  void **kernelParams, void **extra)
{
    return launch_kernel_ex(VPPU_LAUNCH_KERNEL_EX, config, f, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernelEx_ptsz(const HGlaunchConfig *config, HGfunction f,
                                       void **kernelParams, void **extra)
{
    return launch_kernel_ex(VPPU_LAUNCH_KERNEL_EX_PTSZ, config, f, kernelParams, extra);
}

/* AD表单会使用自己的配置结构体，因此无法共享上述内容，尽管两个读取的内容非常相似：此文件所传递的参
 * 数类型却有所不同。 */
static HGresult launch_kernel_ex_ad(enum vppu_entry entry, const HGlaunchConfigAD *config,
                                    HGfunction f, void **kernelParams, void **extra)
{
    vppu_hggc_count(entry);
    if (!vppu_hggc_gate(entry, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(const HGlaunchConfigAD *, HGfunction, void **, void **) =
        vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(config, f, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernelExAD(const HGlaunchConfigAD *config, HGfunction f,
                                    void **kernelParams, void **extra)
{
    return launch_kernel_ex_ad(VPPU_LAUNCH_KERNEL_EX_AD, config, f, kernelParams, extra);
}

HGresult HGGCAPI hgLaunchKernelExAD_ptsz(const HGlaunchConfigAD *config, HGfunction f,
                                         void **kernelParams, void **extra)
{
    return launch_kernel_ex_ad(VPPU_LAUNCH_KERNEL_EX_AD_PTSZ, config, f, kernelParams, extra);
}

static HGresult launch_cooperative(enum vppu_entry entry, HGfunction f, unsigned int gridDimX,
                                   unsigned int gridDimY, unsigned int gridDimZ,
                                   unsigned int blockDimX, unsigned int blockDimY,
                                   unsigned int blockDimZ, unsigned int sharedMemBytes,
                                   HGstream hStream, void **kernelParams)
{
    vppu_hggc_count(entry);
    if (!vppu_hggc_gate(entry, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGfunction, unsigned int, unsigned int, unsigned int, unsigned int,
                     unsigned int, unsigned int, unsigned int, HGstream, void **) =
        vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(f, gridDimX, gridDimY, gridDimZ, blockDimX, blockDimY, blockDimZ, sharedMemBytes,
                hStream, kernelParams);
}

HGresult HGGCAPI hgLaunchCooperativeKernel(HGfunction f, unsigned int gridDimX,
                                           unsigned int gridDimY, unsigned int gridDimZ,
                                           unsigned int blockDimX, unsigned int blockDimY,
                                           unsigned int blockDimZ, unsigned int sharedMemBytes,
                                           HGstream hStream, void **kernelParams)
{
    return launch_cooperative(VPPU_LAUNCH_COOPERATIVE_KERNEL, f, gridDimX, gridDimY, gridDimZ,
                              blockDimX, blockDimY, blockDimZ, sharedMemBytes, hStream,
                              kernelParams);
}

HGresult HGGCAPI hgLaunchCooperativeKernel_ptsz(HGfunction f, unsigned int gridDimX,
                                                unsigned int gridDimY, unsigned int gridDimZ,
                                                unsigned int blockDimX, unsigned int blockDimY,
                                                unsigned int blockDimZ,
                                                unsigned int sharedMemBytes, HGstream hStream,
                                                void **kernelParams)
{
    return launch_cooperative(VPPU_LAUNCH_COOPERATIVE_KERNEL_PTSZ, f, gridDimX, gridDimY,
                              gridDimZ, blockDimX, blockDimY, blockDimZ, sharedMemBytes, hStream,
                              kernelParams);
}

/* 多设备模式会在其列表中的每个上下文环境中启动，它所受保护的窗口即为调用上下文的卡。将列表解码为单
 * 独保护每个卡会导致同时等待多个窗口，而该拦截库没有理由定义这种锁定顺序；调用上下文的窗口是一种较
 * 为合理的近似方式，且入口会被计数，这样使用它的工作负载就能被显示出来，而不是被假设为不存在。 */
HGresult HGGCAPI hgLaunchCooperativeKernelMultiDevice(HGGC_LAUNCH_PARAMS *launchParamsList,
                                                      unsigned int numDevices, unsigned int flags)
{
    vppu_hggc_count(VPPU_LAUNCH_COOPERATIVE_KERNEL_MULTI_DEVICE);
    if (!vppu_hggc_gate(VPPU_LAUNCH_COOPERATIVE_KERNEL_MULTI_DEVICE, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGGC_LAUNCH_PARAMS *, unsigned int, unsigned int) =
        vppu_hggc_next(VPPU_LAUNCH_COOPERATIVE_KERNEL_MULTI_DEVICE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(launchParamsList, numDevices, flags);
}

/* 三个遗留条目：一个函数以之前调用所设定的网格尺寸启动，而非通过参数传入。像其他拦截库一样进行限制
 * ——这些尺寸属于供应商的权益，而窗口大小并不依赖于它们。 */
HGresult HGGCAPI hgLaunch(HGfunction f)
{
    vppu_hggc_count(VPPU_LAUNCH);
    if (!vppu_hggc_gate(VPPU_LAUNCH, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGfunction) = vppu_hggc_next(VPPU_LAUNCH);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(f);
}

HGresult HGGCAPI hgLaunchGrid(HGfunction f, int grid_width, int grid_height)
{
    vppu_hggc_count(VPPU_LAUNCH_GRID);
    if (!vppu_hggc_gate(VPPU_LAUNCH_GRID, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGfunction, int, int) = vppu_hggc_next(VPPU_LAUNCH_GRID);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(f, grid_width, grid_height);
}

HGresult HGGCAPI hgLaunchGridAsync(HGfunction f, int grid_width, int grid_height,
                                   HGstream hStream)
{
    vppu_hggc_count(VPPU_LAUNCH_GRID_ASYNC);
    if (!vppu_hggc_gate(VPPU_LAUNCH_GRID_ASYNC, false)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGfunction, int, int, HGstream) = vppu_hggc_next(VPPU_LAUNCH_GRID_ASYNC);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(f, grid_width, grid_height, hStream);
}

/* --------------------------------------------------------------------------------------- 图式启动。
 * 以图式形式进行限制，因此它们被允许出现的窗口大小可以通过 HGGC_SM_GRAPH_WEIGHT 进行调整，该值根据
 * 测量结果来判断图式是否超出限制。
 * --------------------------------------------------------------------------------------- */

static HGresult graph_launch(enum vppu_entry entry, HGgraphExec hGraphExec, HGstream hStream)
{
    vppu_hggc_count(entry);
    if (!vppu_hggc_gate(entry, true)) {
        return HGGC_ERROR_NOT_PERMITTED;
    }

    HGresult (*real)(HGgraphExec, HGstream) = vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(hGraphExec, hStream);
}

HGresult HGGCAPI hgGraphLaunch(HGgraphExec hGraphExec, HGstream hStream)
{
    return graph_launch(VPPU_GRAPH_LAUNCH, hGraphExec, hStream);
}

HGresult HGGCAPI hgGraphLaunch_ptsz(HGgraphExec hGraphExec, HGstream hStream)
{
    return graph_launch(VPPU_GRAPH_LAUNCH_PTSZ, hGraphExec, hStream);
}

/* --------------------------------------------------------------------------------------- 主机回调。
 * 被计数且永不延迟：它们运行在 CPU 上，因此推迟其中一个会延迟工作负载，同时不会释放任何卡上的资源。
 * --------------------------------------------------------------------------------------- */

static HGresult launch_host_func(enum vppu_entry entry, HGstream hStream, HGhostFn fn,
                                 void *userData)
{
    vppu_hggc_count(entry);

    HGresult (*real)(HGstream, HGhostFn, void *) = vppu_hggc_next(entry);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }
    return real(hStream, fn, userData);
}

HGresult HGGCAPI hgLaunchHostFunc(HGstream hStream, HGhostFn fn, void *userData)
{
    return launch_host_func(VPPU_LAUNCH_HOST_FUNC, hStream, fn, userData);
}

HGresult HGGCAPI hgLaunchHostFunc_ptsz(HGstream hStream, HGhostFn fn, void *userData)
{
    return launch_host_func(VPPU_LAUNCH_HOST_FUNC_PTSZ, hStream, fn, userData);
}

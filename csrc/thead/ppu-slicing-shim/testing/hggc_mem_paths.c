/* hggc_mem_paths.c — Gate 2的执行器：每个调用一个内存路径，一种尺寸。
 *
 * Gate 2 询问：在驱动层强制执行的 VRAM 配额是否真的能覆盖工作负载使用设备内存的所有方式。此程序就
 * 是该问题的工作负载部分：它驱动一个名为 path 的进程，并报告分配是否成功。因此，thead-case-3.sh 可
 * 以针对每个 path 运行三次——在拦截库 shim 的配额限制下、无 shim 的超出配额情况、以及有 shim 的超出
 * 配额情况——然后进行比较。
 *
 * 有意为每个进程分配一个槽位。这样就能使每个观察结果独立于其他记账结果，因此拒绝行为只能来自待测试
 * 的规模。refund是一个故意设计的异常情况——它需要在同一个进程中完成两次分配及释放操作，因为只有这样
 * 才能观察到记账行为。
 *
 * 普通和异步路径会特意调用 RUNTIME 条目（libhggcrt 中的 hggcMalloc、hggcMallocAsync），因为从运行
 * 时层进入 libhggc.so 的过程正是 Gate 2所监测的内容。池和 VMM 路径没有对应的运行时实现，因此它们直
 * 接调用驱动条目。procaddr 路径则不会通过名称调用，而是向驱动程序请求条目的地址，并通过该指针进行
 * 分配。
 *
 * 与利用探针类似，这是一个用于连接 SDK 的测试二进制文件，并非拦截库：链接器会解析头文件的 _v2/_v4
 * 映射关系，并对每次调用进行类型检查。 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* SDK 头文件并不包含其自身声明所需的内容。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>
#include <hggc_runtime_api.h>

static void report(const char *path, const char *step, int rc)
{
    printf("PATH %s step=%s rc=%d\n", path, step, rc);
}

static int finish(const char *path, bool ok, int rc)
{
    printf("PATH %s result=%s rc=%d\n", path, ok ? "success" : "failed", rc);
    return 0;
}

/* driver_context — hgInit + 一个上下文，用于那些没有运行时对应项的路径。 */
static bool driver_context(const char *path, int index, HGdevice *dev, HGcontext *ctx)
{
    HGresult rc = hgInit(0);
    report(path, "hgInit", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    rc = hgDeviceGet(dev, index);
    report(path, "hgDeviceGet", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    rc = hgCtxCreate(ctx, NULL, 0, *dev);
    report(path, "hgCtxCreate", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    rc = hgCtxSetCurrent(*ctx);
    report(path, "hgCtxSetCurrent", (int)rc);
    return rc == HGGC_SUCCESS;
}

static int path_plain(int index, size_t bytes)
{
    const char *path = "plain";
    hggcError_t rc = hggcSetDevice(index);
    report(path, "hggcSetDevice", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    void *ptr = NULL;
    rc = hggcMalloc(&ptr, bytes);
    report(path, "hggcMalloc", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    hggcFree(ptr);
    return finish(path, true, 0);
}

static int path_async(int index, size_t bytes)
{
    const char *path = "async";
    hggcError_t rc = hggcSetDevice(index);
    report(path, "hggcSetDevice", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    hggcStream_t stream = NULL;
    rc = hggcStreamCreate(&stream);
    report(path, "hggcStreamCreate", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    void *ptr = NULL;
    rc = hggcMallocAsync(&ptr, bytes, stream);
    report(path, "hggcMallocAsync", (int)rc);
    /* 判断前先同步：异步分配可能仅在流上失败。 */
    hggcError_t sync_rc = hggcStreamSynchronize(stream);
    report(path, "hggcStreamSynchronize", (int)sync_rc);
    if (rc != hggcSuccess || sync_rc != hggcSuccess) {
        return finish(path, false, (int)(rc != hggcSuccess ? rc : sync_rc));
    }

    hggcFree(ptr);
    return finish(path, true, 0);
}

/* path_refund — 那个同时持有两次分配资源并随后释放两者的路径。
 *
 * 这里的其他路径只会分配一次然后退出，因此拦截库的退款记账功能永远不会被触发：那些在免费时不会返还
 * 任何内容的账本仍然会被处理，因为进程在第二次分配被拒绝之前就结束了。该路径会两次使用 bytes 参数
 * （调用者将其大小设为配额的一半，因此两者恰好填满配额），然后释放两者，最后请求 bytes * 2 的配额—
 * —即整个配额。最后一个请求只有在两次退款都成功时才会被接受，因此那些在第二次退款中丢失的账本会拒
 * 绝该请求。 */
static int path_refund(int index, size_t bytes)
{
    const char *path = "refund";
    hggcError_t rc = hggcSetDevice(index);
    report(path, "hggcSetDevice", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    void *first = NULL;
    rc = hggcMalloc(&first, bytes);
    report(path, "hggcMalloc.first", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    void *second = NULL;
    rc = hggcMalloc(&second, bytes);
    report(path, "hggcMalloc.second", (int)rc);
    if (rc != hggcSuccess) {
        hggcFree(first);
        return finish(path, false, (int)rc);
    }

    /* 按插入顺序释放，即账本按清空槽位来删除条目的顺序：第一个空闲的会截断探测链，第二个则需要它。 */
    rc = hggcFree(first);
    report(path, "hggcFree.first", (int)rc);
    rc = hggcFree(second);
    report(path, "hggcFree.second", (int)rc);

    void *whole = NULL;
    rc = hggcMalloc(&whole, bytes * 2);
    report(path, "hggcMalloc.whole", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    hggcFree(whole);
    return finish(path, true, 0);
}

/* path_hold — 进行分配并 KEEP 内存，因此同一容器中的其他进程会面临已使用的配额。
 *
 * 其他路径在退出前会释放资源，导致跨进程记账功能无法显示：当第二个进程请求任何内容时，相关记录就已
 * 消失。判断结果会在等待之前被打印出来，因此程序可以立即读取该结果，而无需等待等待状态结束。 */
static int path_hold(int index, size_t bytes, unsigned int seconds)
{
    const char *path = "hold";
    hggcError_t rc = hggcSetDevice(index);
    report(path, "hggcSetDevice", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    void *ptr = NULL;
    rc = hggcMalloc(&ptr, bytes);
    report(path, "hggcMalloc", (int)rc);
    if (rc != hggcSuccess) {
        return finish(path, false, (int)rc);
    }

    finish(path, true, 0);
    fflush(stdout);
    sleep(seconds);
    hggcFree(ptr);
    return 0;
}

/* path_procaddr — 通过 DRIVER 提供的指针获取内存，而非链接器所分配的那种。
 *
 * 其他所有路径都按名称调用分配条目，因此动态链接器会解析它，而预加载的定义会占据优势。而
 * hgGetProcAddress 则能解决这一问题：它返回驱动程序自身针对指定条目的地址，持有该指针的调用者也不
 * 会再查询链接器。libhggcrt 就是以这种方式绑定驱动条目的，因此无法覆盖解析器的配额，运行时层通过它
 * 进行的解析也就毫无意义。
 *
 * 所要求的版本为 SDK 自有的版本（HGGC_VERSION），正是这个版本使得驱动程序返回当前形式的入口，而非
 * 其 v1 祖先版本。 */
static int path_procaddr(int index, size_t bytes)
{
    const char *path = "procaddr";
    HGdevice dev = 0;
    HGcontext ctx = NULL;
    if (!driver_context(path, index, &dev, &ctx)) {
        return finish(path, false, -1);
    }

    void *pfn = NULL;
    HGdriverProcAddressQueryResult status = HG_GET_PROC_ADDRESS_SUCCESS;
    HGresult rc = hgGetProcAddress("hgMemAlloc", &pfn, HGGC_VERSION,
                                   HG_GET_PROC_ADDRESS_DEFAULT, &status);
    printf("PATH %s step=hgGetProcAddress rc=%d status=%d pfn=%s\n", path, (int)rc, (int)status,
           (pfn == NULL) ? "null" : "resolved");
    if (rc != HGGC_SUCCESS || pfn == NULL) {
        return finish(path, false, (int)rc);
    }

    HGresult (*alloc)(HGdeviceptr *, size_t) = (HGresult(*)(HGdeviceptr *, size_t))pfn;
    HGdeviceptr dptr = 0;
    rc = alloc(&dptr, bytes);
    report(path, "hgMemAlloc.resolved", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return finish(path, false, (int)rc);
    }

    hgMemFree(dptr);
    return finish(path, true, 0);
}

static int path_pool(int index, size_t bytes, int target)
{
    const char *path = "pool";
    HGdevice dev = 0;
    HGcontext ctx = NULL;
    if (!driver_context(path, index, &dev, &ctx)) {
        return finish(path, false, -1);
    }

    /* 与 VMM 路径相同，通过另一个携带卡的句柄：即不需要成为上下文的卡的默认池。 */
    HGdevice pool_dev = (target >= 0) ? (HGdevice)target : dev;
    HGmemoryPool pool = NULL;
    HGresult rc = hgDeviceGetDefaultMemPool(&pool, pool_dev);
    printf("PATH %s context=%d target=%d\n", path, (int)dev, (int)pool_dev);
    report(path, "hgDeviceGetDefaultMemPool", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return finish(path, false, (int)rc);
    }

    HGstream stream = NULL;
    rc = hgStreamCreate(&stream, 0);
    report(path, "hgStreamCreate", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return finish(path, false, (int)rc);
    }

    HGdeviceptr dptr = 0;
    rc = hgMemAllocFromPoolAsync(&dptr, bytes, pool, stream);
    report(path, "hgMemAllocFromPoolAsync", (int)rc);
    HGresult sync_rc = hgStreamSynchronize(stream);
    report(path, "hgStreamSynchronize", (int)sync_rc);
    if (rc != HGGC_SUCCESS || sync_rc != HGGC_SUCCESS) {
        return finish(path, false, (int)(rc != HGGC_SUCCESS ? rc : sync_rc));
    }

    hgMemFreeAsync(dptr, stream);
    hgStreamSynchronize(stream);
    /* 修剪是池路径 Gate 2 名称的一部分，拦截库会将其计入统计。 */
    rc = hgMemPoolTrimTo(pool, 0);
    report(path, "hgMemPoolTrimTo", (int)rc);
    hgStreamDestroy(stream);
    return finish(path, true, 0);
}

static int path_vmm(int index, size_t bytes, int target)
{
    const char *path = "vmm";
    HGdevice dev = 0;
    HGcontext ctx = NULL;
    if (!driver_context(path, index, &dev, &ctx)) {
        return finish(path, false, -1);
    }

    HGmemAllocationProp prop;
    memset(&prop, 0, sizeof(prop));
    prop.type = HG_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = HG_MEM_LOCATION_TYPE_DEVICE;
    /* 用于 ALLOCATION 的卡，调用者可能将其目标指向与其调用上下文不同的地方——这就是表明分配给道具卡的配
     * 额与分配给调用线程的配额之间的区别的一种表现形式，而且需要一个能容纳两张卡的容器才能看清。 */
    prop.location.id = (target >= 0) ? target : (int)dev;
    printf("PATH %s context=%d target=%d\n", path, (int)dev, prop.location.id);

    size_t granularity = 0;
    HGresult rc = hgMemGetAllocationGranularity(&granularity, &prop,
                                                HG_MEM_ALLOC_GRANULARITY_RECOMMENDED);
    report(path, "hgMemGetAllocationGranularity", (int)rc);
    if (rc != HGGC_SUCCESS || granularity == 0) {
        return finish(path, false, (int)rc);
    }

    /* VMM路径会拒绝未对齐的大小，因此需要进行四舍五入——并报告四舍五入后的大小，因为配额是根据
     * hgMemCreate实际请求的大小来记账的。 */
    size_t size = ((bytes + granularity - 1) / granularity) * granularity;
    printf("PATH %s granularity=%zu rounded_bytes=%zu\n", path, granularity, size);

    HGmemGenericAllocationHandle handle = 0;
    rc = hgMemCreate(&handle, size, &prop, 0);
    report(path, "hgMemCreate", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return finish(path, false, (int)rc);
    }

    HGdeviceptr ptr = 0;
    rc = hgMemAddressReserve(&ptr, size, granularity, 0, 0);
    report(path, "hgMemAddressReserve", (int)rc);
    if (rc == HGGC_SUCCESS) {
        rc = hgMemMap(ptr, size, 0, handle, 0);
        report(path, "hgMemMap", (int)rc);
        if (rc == HGGC_SUCCESS) {
            hgMemUnmap(ptr, size);
        }
        hgMemAddressFree(ptr, size);
    }

    hgMemRelease(handle);
    return finish(path, rc == HGGC_SUCCESS, (int)rc);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr,
                "usage: %s <device-index> <bytes> "
                "<plain|async|pool|vmm|procaddr|refund|hold> [hold-seconds | target-card]\n",
                argv[0]);
        return 2;
    }

    int index = (int)strtol(argv[1], NULL, 10);
    size_t bytes = (size_t)strtoull(argv[2], NULL, 10);
    const char *path = argv[3];
    /* 第四个参数是 hold 的持有时长，以及 ALLOCATION 的配额，用于两条分别承载各自数据的路径；-1 表示“上
     * 下文的”，即他们之前所采用的方式。 */
    int target = (argc > 4) ? (int)strtol(argv[4], NULL, 10) : -1;
    printf("PATHS device=%d bytes=%zu path=%s\n", index, bytes, path);

    if (strcmp(path, "plain") == 0) {
        return path_plain(index, bytes);
    }
    if (strcmp(path, "async") == 0) {
        return path_async(index, bytes);
    }
    if (strcmp(path, "pool") == 0) {
        return path_pool(index, bytes, target);
    }
    if (strcmp(path, "vmm") == 0) {
        return path_vmm(index, bytes, target);
    }
    if (strcmp(path, "procaddr") == 0) {
        return path_procaddr(index, bytes);
    }
    if (strcmp(path, "refund") == 0) {
        return path_refund(index, bytes);
    }
    if (strcmp(path, "hold") == 0) {
        unsigned int seconds = (argc > 4) ? (unsigned int)strtoul(argv[4], NULL, 10) : 5u;

        return path_hold(index, bytes, seconds);
    }

    fprintf(stderr, "unknown path: %s\n", path);
    return 2;
}

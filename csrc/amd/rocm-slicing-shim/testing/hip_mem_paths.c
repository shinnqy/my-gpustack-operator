/* hip_mem_paths.c — 每次调用对应一个分配家族，因此可以记录哪个路径被跨越了。
 *
 * 为什么每个进程对应一个槽位？每个观察结果必须独立于其他观察结果：如果两个家族在同一个进程中进行运
 * 行，第二个家族的拒绝可能源自第一个家族的记账行为，而非测试中的大小因素，因此无法判断具体是哪种情
 * 况。因此，这个家族就是一个参数，每次运行都是一个新的账本文件。refund是特例——它需要两次实时分配以
 * 及中间的一次释放，这是实现退款可观测性的唯一方式。
 *
 * 为什么是这个列表。这里的每个名称都代表一扇被测量开启的门，并非出于预防措施。仅对 hipMalloc 和池
 * 式家族进行包装后，hipMallocManaged、hipExtMallocWithFlags 以及 hipMallocPitch 均在 256 MiB 限额
 * 下满足了 512 MiB 的请求；而仅对经典家族进行包装后，hipMallocFromPoolAsync 在 RDNA 架构下使用了
 * 10 GiB，在 CDNA 架构下使用了 50 GiB，而该卡的限额仅为 2 GiB；此外，hipMalloc3D 在同样的一次运行
 * 中从 64 MiB 的限额中使用了 512 MiB，且在该运行中 hipMalloc 被拒绝使用。池式家族并非是覆盖
 * hipMalloc 的额外层，3D pitched 项也是如此。
 *
 * 最后四个函数是通过减法而非猜测得出的。列出所有分配名称 libamdhip64 的导出函数，并移除中间插入的
 * 函数后，剩下的只有 hipMemCreate”——这是经过调整的 PyTorch 任务所采用的虚拟内存管理序列——以及
 * DRIVER-API 中的两个函数 hipMemAllocPitch、hipArrayCreate 和 hipArray3DCreate，这些属于其运行时
 * -API中的独立符号。这四个函数共使用了 512 MiB 的内存，而原本的配额是 64 MiB。现在，这种方法才是最
 * 重要的：某人试图尝试的菜单列表并不构成覆盖声明。
 *
 * 为什么这里有 host，并且预计能够成功。固定的主机页面并非设备 VRAM，因此库不会将其计入，也不会进行
 * 记账。运行它就是证明这一点仍然成立的例子——一个在配额限制下开始失效的 host 路径，意味着库开始按设
 * 备实际数值对系统内存进行记账。
 *
 * 它连接了 HIP运行时，与产品不同。这是一项工作负载，而非中间层设备：让链接器处理头文件的 R0600 映
 * 射关系并进行类型检查所有签名，这才是关键所在；而手工编写的 dlsym 表则容易导致 ABI 名称出错。
 *
 * 当程序按要求运行完成时，退出状态为 0；分配是否成功则显示在 result= 行中，因为“refused”表示一半情
 * 况属于通过状态，另一半则属于失败状态，只有具体情况才能知道属于哪种状态。 */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hip/hip_runtime.h>

#define MIB (1024ULL * 1024ULL)

static void step(const char *path, const char *name, hipError_t rc)
{
    printf("PATH %s step=%s rc=%d (%s)\n", path, name, (int)rc, hipGetErrorString(rc));
}

static int finish(const char *path, int ok, hipError_t rc)
{
    printf("PATH %s result=%s rc=%d\n", path, ok ? "success" : "failed", (int)rc);
    return 0;
}

static int path_plain(size_t bytes)
{
    void *ptr = NULL;
    hipError_t rc = hipMalloc(&ptr, bytes);

    step("plain", "hipMalloc", rc);
    if (rc != hipSuccess) {
        return finish("plain", 0, rc);
    }
    (void)hipFree(ptr);
    return finish("plain", 1, hipSuccess);
}

static int path_managed(size_t bytes)
{
    void *ptr = NULL;
    hipError_t rc = hipMallocManaged(&ptr, bytes, hipMemAttachGlobal);

    step("managed", "hipMallocManaged", rc);
    if (rc != hipSuccess) {
        return finish("managed", 0, rc);
    }
    (void)hipFree(ptr);
    return finish("managed", 1, hipSuccess);
}

static int path_ext(size_t bytes)
{
    void *ptr = NULL;
    hipError_t rc = hipExtMallocWithFlags(&ptr, bytes, hipDeviceMallocDefault);

    step("ext", "hipExtMallocWithFlags", rc);
    if (rc != hipSuccess) {
        return finish("ext", 0, rc);
    }
    (void)hipFree(ptr);
    return finish("ext", 1, hipSuccess);
}

/* path_pitch — 那个家族的实际占用空间并非调用者所传递的数字。
 *
 * 运行时系统将每一行对齐到其自身的对齐方式，因此 width x height 所表示的值会低于卡实际输出的数值；
 * 该库根据调用者的设定进行修正，并在同一锁定条件下进行 pitch x height 的校准。打印出对齐方式使得案
 * 例能够自行进行校准，而不是简单地假设。 */
static int path_pitch(size_t bytes)
{
    size_t height = 1024;
    size_t width = bytes / height;
    size_t pitch = 0;
    void *ptr = NULL;
    hipError_t rc;

    if (width == 0) {
        width = 1;
    }
    rc = hipMallocPitch(&ptr, &pitch, width, height);
    step("pitch", "hipMallocPitch", rc);
    printf("PATH pitch width=%zu height=%zu pitch=%zu asked_mib=%llu took_mib=%llu\n", width, height,
           pitch, (unsigned long long)(width * height) / MIB,
           (unsigned long long)(pitch * height) / MIB);
    if (rc != hipSuccess) {
        return finish("pitch", 0, rc);
    }
    (void)hipFree(ptr);
    return finish("pitch", 1, hipSuccess);
}

/* path_malloc3d — 具有深度的倾斜入口，也是上次被测量为打开的入口。
 *
 * 在 64 MiB 配额内，它满足了一个 512 MiB 的请求，同时 hipMalloc 则正确地拒绝了该请求，这也就将其纳
 * 入了拦截库中。其深度被刻意设定为大于 1：当 depth = 1 时，这就是 hipMallocPitch 的另一种名称，此
 * 时记账操作根本不会触发深度乘法操作。 */
static int path_malloc3d(size_t bytes)
{
    size_t height = 256, depth = 4;
    hipPitchedPtr pitched;
    hipExtent extent;
    hipError_t rc;

    extent.width = bytes / (height * depth);
    extent.height = height;
    extent.depth = depth;
    if (extent.width == 0) {
        extent.width = 1;
    }
    memset(&pitched, 0, sizeof(pitched));

    rc = hipMalloc3D(&pitched, extent);
    step("malloc3d", "hipMalloc3D", rc);
    printf("PATH malloc3d width=%zu height=%zu depth=%zu pitch=%zu asked_mib=%llu took_mib=%llu\n",
           extent.width, extent.height, extent.depth, pitched.pitch,
           (unsigned long long)(extent.width * height * depth) / MIB,
           (unsigned long long)(pitched.pitch * height * depth) / MIB);
    if (rc != hipSuccess) {
        return finish("malloc3d", 0, rc);
    }
    (void)hipFree(pitched.ptr);
    return finish("malloc3d", 1, hipSuccess);
}

/* device_limit — 一个纹理维度的上限，若无法读取则采用保守的默认值。
 *
 * 这些数组家族是唯一一类，其请求具有形状和大小；如果硬件无法接受的形状，将会被拒绝，并抛出
 * hipErrorInvalidValue 错误——这一情况不可与配额相关的 hipErrorOutOfMemory 混淆。在存在不同限制条件
 * 的情况下，通过询问设备而非硬编码形状，才能保持这种区分的清晰性。 */
static size_t device_limit(hipDeviceAttribute_t which, int device, size_t fallback)
{
    int value = 0;

    if (hipDeviceGetAttribute(&value, which, device) != hipSuccess || value <= 0) {
        return fallback;
    }
    return (size_t)value;
}

/* shape_2d — 最宽的形状，能够容纳 elems，且适合放入设备的 2D 上限范围内。
 *
 * 将宽度减半，高度翻倍，直到宽度符合要求；这样得到的布局与硬件推荐的布局最为接近，符合测试需求，因
 * 此会进行报告，以便识别拒绝情况。
 *
 * 无法处理的过大请求实际上代表了上限限制，而非配额结果，两者不可混淆：max_w * max_h * 4 在每方向为
 * 16384 的区域内大约相当于 1 GiB，因此必须在低于该值的配额范围内才能使用数组结构。失败现象正是因为
 * 这一原因而引发的。 */
static int shape_2d(size_t elems, int device, size_t *width, size_t *height, size_t *ceiling)
{
    size_t max_w = device_limit(hipDeviceAttributeMaxTexture2DWidth, device, 16384);
    size_t max_h = device_limit(hipDeviceAttributeMaxTexture2DHeight, device, 16384);

    *ceiling = max_w * max_h;
    *height = 1;
    *width = elems;
    while (*width > max_w && *height * 2 <= max_h) {
        *height *= 2;
        *width = elems / *height;
    }
    return *width <= max_w && *width > 0 && *height <= max_h;
}

static int path_array(size_t bytes, int device)
{
    hipChannelFormatDesc desc = hipCreateChannelDesc(32, 0, 0, 0, hipChannelFormatKindFloat);
    hipArray_t array = NULL;
    size_t width = 0, height = 0, ceiling = 0;
    hipError_t rc;

    if (!shape_2d(bytes / 4, device, &width, &height, &ceiling)) {
        printf("PATH array shape=unavailable asked_mib=%llu ceiling_mib=%llu\n",
               (unsigned long long)bytes / MIB, (unsigned long long)(ceiling * 4) / MIB);
        return finish("array", 0, hipErrorInvalidValue);
    }
    printf("PATH array width=%zu height=%zu\n", width, height);
    rc = hipMallocArray(&array, &desc, width, height, hipArrayDefault);
    step("array", "hipMallocArray", rc);
    /* 测量结果：无论形状如何、配额如何，该操作在 gfx942 上都会返回“操作不支持”的提示，因此其有效性只能
     * 通过 RDNA 架构来验证。读取此内容的代码必须根据架构进行分支处理，而不是将拒绝情况视为配额生效。 */
    if (rc != hipSuccess) {
        return finish("array", 0, rc);
    }
    (void)hipFreeArray(array);
    return finish("array", 1, hipSuccess);
}

static int path_array3d(size_t bytes, int device)
{
    hipChannelFormatDesc desc = hipCreateChannelDesc(32, 0, 0, 0, hipChannelFormatKindFloat);
    size_t max_d = device_limit(hipDeviceAttributeMaxTexture3DDepth, device, 2048);
    hipArray_t array = NULL;
    size_t width = 0, height = 0, depth = 64, ceiling = 0;
    hipExtent extent;
    hipError_t rc;

    if (depth > max_d) {
        depth = max_d;
    }
    if (!shape_2d(bytes / (4 * depth), device, &width, &height, &ceiling)) {
        printf("PATH array3d shape=unavailable asked_mib=%llu ceiling_mib=%llu\n",
               (unsigned long long)bytes / MIB, (unsigned long long)(ceiling * depth * 4) / MIB);
        return finish("array3d", 0, hipErrorInvalidValue);
    }
    /* 3D天花板的高度远低于2D天花板，因此适合2D数组的形状可能不适合体积结构；这里采用限制而非失败的方式，
     * 使得路径可以在任何尺寸下使用。 */
    if (width > device_limit(hipDeviceAttributeMaxTexture3DWidth, device, 2048)) {
        width = device_limit(hipDeviceAttributeMaxTexture3DWidth, device, 2048);
    }
    if (height > device_limit(hipDeviceAttributeMaxTexture3DHeight, device, 2048)) {
        height = device_limit(hipDeviceAttributeMaxTexture3DHeight, device, 2048);
    }
    extent.width = width;
    extent.height = height;
    extent.depth = depth;
    printf("PATH array3d width=%zu height=%zu depth=%zu took_mib=%llu\n", width, height, depth,
           (unsigned long long)(width * height * depth * 4) / MIB);

    rc = hipMalloc3DArray(&array, &desc, extent, hipArrayDefault);
    step("array3d", "hipMalloc3DArray", rc);
    if (rc != hipSuccess) {
        return finish("array3d", 0, rc);
    }
    (void)hipFreeArray(array);
    return finish("array3d", 1, hipSuccess);
}

/* path_vmm — 虚拟内存管理序列，即经过优化的 PyTorch 任务进行分配的方式。
 *
 * 有四次调用，其他调用各一次，只有第一次调用会占用内存：reserve 调用会获取地址空间，map 将句柄绑定
 * 到该地址空间中，而 access 调用则设置权限。阅读此内容的程序必须执行整个序列，因为如果在
 * hipMemCreate 时拒绝配额，并允许其余操作继续，就会导致调用者绑定一个它没有的句柄。 */
static int path_vmm(size_t bytes, int device)
{
    hipMemAllocationProp prop;
    hipMemAccessDesc access;
    hipMemGenericAllocationHandle_t handle;
    size_t granularity = 0, size;
    void *ptr = NULL;
    hipError_t rc;

    memset(&prop, 0, sizeof(prop));
    prop.type = hipMemAllocationTypePinned;
    prop.location.type = hipMemLocationTypeDevice;
    prop.location.id = device;

    rc = hipMemGetAllocationGranularity(&granularity, &prop, hipMemAllocationGranularityMinimum);
    step("vmm", "hipMemGetAllocationGranularity", rc);
    if (granularity == 0) {
        granularity = 2 * MIB;
    }
    /* 因为运行时拒绝非倍数的大小，所以进行四舍五入处理；此外，某种情况不应将这种拒绝视为配额。 */
    size = ((bytes + granularity - 1) / granularity) * granularity;
    printf("PATH vmm granularity=%zu asked_mib=%llu rounded_mib=%llu\n", granularity,
           (unsigned long long)bytes / MIB, (unsigned long long)size / MIB);

    rc = hipMemCreate(&handle, size, &prop, 0);
    step("vmm", "hipMemCreate", rc);
    if (rc != hipSuccess) {
        return finish("vmm", 0, rc);
    }
    rc = hipMemAddressReserve(&ptr, size, 0, NULL, 0);
    step("vmm", "hipMemAddressReserve", rc);
    if (rc == hipSuccess) {
        rc = hipMemMap(ptr, size, 0, handle, 0);
        step("vmm", "hipMemMap", rc);
        memset(&access, 0, sizeof(access));
        access.location = prop.location;
        access.flags = hipMemAccessFlagsProtReadWrite;
        (void)hipMemSetAccess(ptr, size, &access, 1);
        (void)hipMemUnmap(ptr, size);
        (void)hipMemAddressFree(ptr, size);
    }
    (void)hipMemRelease(handle);
    return finish("vmm", 1, hipSuccess);
}

/* path_drvpitch / path_drvarray / path_drvarray3d — 驱动-API的两种形式。
 *
 * 它们的存在原因与 pool 相同：即不同的导出符号占据了相同的内存空间。在 64 MiB 配额限制下，每个都能
 * 够满足 512 MiB 的请求需求，而其运行时 API 对应组件则被拒绝了这一请求。 */
static int path_drvpitch(size_t bytes)
{
    hipDeviceptr_t ptr = NULL;
    size_t pitch = 0, height = 1024, width = bytes / 1024;
    hipError_t rc;

    if (width == 0) {
        width = 1;
    }
    rc = hipMemAllocPitch(&ptr, &pitch, width, height, 4);
    step("drvpitch", "hipMemAllocPitch", rc);
    printf("PATH drvpitch width=%zu height=%zu pitch=%zu asked_mib=%llu took_mib=%llu\n", width,
           height, pitch, (unsigned long long)(width * height) / MIB,
           (unsigned long long)(pitch * height) / MIB);
    if (rc != hipSuccess) {
        return finish("drvpitch", 0, rc);
    }
    (void)hipFree(ptr);
    return finish("drvpitch", 1, hipSuccess);
}

static int path_drvarray(size_t bytes, int device)
{
    HIP_ARRAY_DESCRIPTOR desc;
    hipArray_t array = NULL;
    size_t width = 0, height = 0, ceiling = 0;
    hipError_t rc;

    if (!shape_2d(bytes / 4, device, &width, &height, &ceiling)) {
        printf("PATH drvarray shape=unavailable asked_mib=%llu ceiling_mib=%llu\n",
               (unsigned long long)bytes / MIB, (unsigned long long)(ceiling * 4) / MIB);
        return finish("drvarray", 0, hipErrorInvalidValue);
    }
    memset(&desc, 0, sizeof(desc));
    desc.Width = width;
    desc.Height = height;
    desc.Format = HIP_AD_FORMAT_FLOAT;
    desc.NumChannels = 1;
    printf("PATH drvarray width=%zu height=%zu\n", width, height);

    rc = hipArrayCreate(&array, &desc);
    step("drvarray", "hipArrayCreate", rc);
    if (rc != hipSuccess) {
        return finish("drvarray", 0, rc);
    }
    (void)hipArrayDestroy(array);
    return finish("drvarray", 1, hipSuccess);
}

static int path_drvarray3d(size_t bytes, int device)
{
    HIP_ARRAY3D_DESCRIPTOR desc;
    hipArray_t array = NULL;
    size_t width = 0, height = 0, depth = 64, ceiling = 0;
    hipError_t rc;

    if (!shape_2d(bytes / (4 * depth), device, &width, &height, &ceiling)) {
        printf("PATH drvarray3d shape=unavailable asked_mib=%llu\n",
               (unsigned long long)bytes / MIB);
        return finish("drvarray3d", 0, hipErrorInvalidValue);
    }
    if (width > device_limit(hipDeviceAttributeMaxTexture3DWidth, device, 2048)) {
        width = device_limit(hipDeviceAttributeMaxTexture3DWidth, device, 2048);
    }
    if (height > device_limit(hipDeviceAttributeMaxTexture3DHeight, device, 2048)) {
        height = device_limit(hipDeviceAttributeMaxTexture3DHeight, device, 2048);
    }
    memset(&desc, 0, sizeof(desc));
    desc.Width = width;
    desc.Height = height;
    desc.Depth = depth;
    desc.Format = HIP_AD_FORMAT_FLOAT;
    desc.NumChannels = 1;
    printf("PATH drvarray3d width=%zu height=%zu depth=%zu took_mib=%llu\n", width, height, depth,
           (unsigned long long)(width * height * depth * 4) / MIB);

    rc = hipArray3DCreate(&array, &desc);
    step("drvarray3d", "hipArray3DCreate", rc);
    if (rc != hipSuccess) {
        return finish("drvarray3d", 0, rc);
    }
    (void)hipArrayDestroy(array);
    return finish("drvarray3d", 1, hipSuccess);
}

static int path_host(size_t bytes)
{
    void *ptr = NULL;
    hipError_t rc = hipHostMalloc(&ptr, bytes, hipHostMallocDefault);

    step("host", "hipHostMalloc", rc);
    if (rc != hipSuccess) {
        return finish("host", 0, rc);
    }
    (void)hipHostFree(ptr);
    return finish("host", 1, hipSuccess);
}

static int path_async(size_t bytes)
{
    void *ptr = NULL;
    hipStream_t stream = NULL;
    hipError_t rc = hipStreamCreate(&stream);

    step("async", "hipStreamCreate", rc);
    if (rc != hipSuccess) {
        return finish("async", 0, rc);
    }
    rc = hipMallocAsync(&ptr, bytes, stream);
    step("async", "hipMallocAsync", rc);
    if (rc != hipSuccess) {
        return finish("async", 0, rc);
    }
    (void)hipFreeAsync(ptr, stream);
    (void)hipStreamSynchronize(stream);
    (void)hipStreamDestroy(stream);
    return finish("async", 1, hipSuccess);
}

/* path_pool — 所测量的6倍超出量，也是按流排序的家族被拦截的原因。
 *
 * hipDeviceGetDefaultMemPool 分配了一个可使用的内存池，没有特殊权限要求，而 PyTorch 的内存池路径则
 * 直接经过该内存池。 */
static int path_pool(size_t bytes)
{
    void *ptr = NULL;
    hipMemPool_t pool = NULL;
    hipStream_t stream = NULL;
    hipError_t rc = hipDeviceGetDefaultMemPool(&pool, 0);

    step("pool", "hipDeviceGetDefaultMemPool", rc);
    if (rc != hipSuccess) {
        return finish("pool", 0, rc);
    }
    rc = hipStreamCreate(&stream);
    step("pool", "hipStreamCreate", rc);
    if (rc != hipSuccess) {
        return finish("pool", 0, rc);
    }
    rc = hipMallocFromPoolAsync(&ptr, bytes, pool, stream);
    step("pool", "hipMallocFromPoolAsync", rc);
    if (rc != hipSuccess) {
        return finish("pool", 0, rc);
    }
    (void)hipFreeAsync(ptr, stream);
    (void)hipStreamSynchronize(stream);
    (void)hipStreamDestroy(stream);
    return finish("pool", 1, hipSuccess);
}

/* path_refund — 占用配额，归还配额，再次占用。
 *
 * 第二次分配是这样一个假设：只有当免费部分真正被退还时，这一操作才能成功。这是该程序中需要在一个进
 * 程中进行两次分配的唯一情况。以略高于配额一半的大小来调用它，因此这两个操作无法同时完成。 */
static int path_refund(size_t bytes)
{
    void *first = NULL, *second = NULL;
    hipError_t rc = hipMalloc(&first, bytes);

    step("refund", "hipMalloc/1", rc);
    if (rc != hipSuccess) {
        return finish("refund", 0, rc);
    }
    rc = hipFree(first);
    step("refund", "hipFree/1", rc);

    rc = hipMalloc(&second, bytes);
    step("refund", "hipMalloc/2", rc);
    if (rc != hipSuccess) {
        return finish("refund", 0, rc);
    }
    (void)hipFree(second);
    return finish("refund", 1, hipSuccess);
}

static void usage(void)
{
    fprintf(stderr, "usage: hip_mem_paths <path> <mib> [device]\n"
                    "  paths: plain managed ext pitch malloc3d array array3d vmm drvpitch drvarray\n"
                    "         drvarray3d host async pool refund\n");
}

int main(int argc, char **argv)
{
    const char *path;
    unsigned long long mib;
    size_t bytes;
    int device = 0;
    hipError_t rc;

    if (argc < 3) {
        usage();
        return 2;
    }
    path = argv[1];
    mib = strtoull(argv[2], NULL, 10);
    if (argc > 3) {
        device = (int)strtol(argv[3], NULL, 10);
    }
    if (mib == 0) {
        fprintf(stderr, "hip_mem_paths: <mib> must be non-zero\n");
        return 2;
    }
    bytes = (size_t)(mib * MIB);

    rc = hipSetDevice(device);
    step(path, "hipSetDevice", rc);
    if (rc != hipSuccess) {
        return finish(path, 0, rc);
    }
    printf("PATH %s device=%d mib=%llu\n", path, device, mib);

    if (strcmp(path, "plain") == 0) {
        return path_plain(bytes);
    }
    if (strcmp(path, "managed") == 0) {
        return path_managed(bytes);
    }
    if (strcmp(path, "ext") == 0) {
        return path_ext(bytes);
    }
    if (strcmp(path, "pitch") == 0) {
        return path_pitch(bytes);
    }
    if (strcmp(path, "malloc3d") == 0) {
        return path_malloc3d(bytes);
    }
    if (strcmp(path, "array") == 0) {
        return path_array(bytes, device);
    }
    if (strcmp(path, "array3d") == 0) {
        return path_array3d(bytes, device);
    }
    if (strcmp(path, "vmm") == 0) {
        return path_vmm(bytes, device);
    }
    if (strcmp(path, "drvpitch") == 0) {
        return path_drvpitch(bytes);
    }
    if (strcmp(path, "drvarray") == 0) {
        return path_drvarray(bytes, device);
    }
    if (strcmp(path, "drvarray3d") == 0) {
        return path_drvarray3d(bytes, device);
    }
    if (strcmp(path, "host") == 0) {
        return path_host(bytes);
    }
    if (strcmp(path, "async") == 0) {
        return path_async(bytes);
    }
    if (strcmp(path, "pool") == 0) {
        return path_pool(bytes);
    }
    if (strcmp(path, "refund") == 0) {
        return path_refund(bytes);
    }
    usage();
    return 2;
}

/* hgml_dlsym_hook.c — 该拦截库用于处理切片可见性：确定卡的 VRAM 配额以及此容器对其的记账使用量，这
 * 些数据由 ppu-smi 报告该卡的情况。
 *
 * ppu-smi 不会链接 HGML。它使用 dlopen() 加载 libhgml.so，并通过该显式句柄使用 dlsym() 解析每个入
 * 口点，因此仅定义 hgmlDeviceGetMemoryInfo 的预加载库是无效的：在全局作用域中根本不会查找该名称。
 * 使用 dlsym() 本身才是有效的方式——我们可以看到 (句柄, 符号) 对，通过调用者自己的句柄获取真实函数，
 * 然后返回一个包装函数。通过 dladdr 与 2.1.1 版本库进行验证，同时测试了/hgml_nohook.c 文件，后者采
 * 用了不同的方式，没有使用 dlsym 拦截机制，因此无法实现相同的控制效果。
 *
 * 两个 PUBLIC 获取器都分别经过包装处理，这并非冗余：libhgml.so 中的 hgmlDeviceGetMemoryInfo 和
 * hgmlDeviceGetMemoryInfo_v2 共享一个 FUNC LOCAL 辅助函数，因此没有需要插入的单一内部符号。它们的
 * 结构体也不同——_v2 以调用者设置的 .version 开始，所以一个包装器无法同时覆盖两者。
 *
 * 这些数据来自执行配额限制的地方。total 是卡的设定配额，used 则是该卡的已使用总量，即“账本”中的总
 * 数值。因此，ppu-smi 中显示的数据以及“分配”相关数据都是同一个数值，而非两个不同的数值。原用于统计
 * 供应商自身 used 值的做法——该值统计了卡上所有容器的情况——已被替代为新的方式：在共享卡上，当自身配
 * 额未被触及时，会显示出一个忙碌的片段。free 表示剩余量，而 used 则被限制在配额范围内，因此超出配
 * 额的部分（由驱动程序选择的行填充方式，该方式由限制部分报告，而非拒绝）会显示为满状态，而不会让
 * free 部分产生溢出。
 *
 * 未配置图表的卡会保持透明状态，这正是强制处理部分的 hgMemGetInfo 视图所进行的操作：当没有数据可报
 * 告时，两者都会显示供应商的图表信息；相反地，驱动层查询与 ppu-smi 在每种状态下都能达成一致。准入
 * 机制则是拒绝的部分；报告则只是单纯的记录行为。整个容器范围的配置错误会在加载时通过 common/ 在拒
 * 绝级别被报告一次。
 *
 * 按卡计算，而非按容器计算。与分配条目不同，获取器会传入所查询的设备，因此能识别出该卡——只需将其转
 * 换为索引，这就是 hgmlDeviceGetIndex 的作用。该索引通过调用者的 dlopen 句柄进行解析，获取器的处理
 * 方式也相同，因此这个拦截库仍然只连接了 libc。
 *
 * 两个保护机制，因为预加载的 dlsym 钩子绝不是容器中的唯一元素：供应商会提供 libhggc_wrapper.so，而
 * 监控代理也可能自带自己的拦截器。
 * - RE-ENTRANCY, 按线程计算。当该线程已处于拦截状态时，我们直接转发数据，因此在此阶段到达的调用者
 * 会收到供应商的指针，而非我们的指针。让任何人获得我们自己的拦截库指针，正是两个库之间交替进行的调
 * 用链所包含的内容。
 * - 来源，按指针处理。我们从未包装属于该对象的地址。以下的指针是按进程处理的，而上面的保护则按线程
 * 处理，因此第二个线程的解析可能会将我们的包装函数存储为 real_* —— 而其真实函数本身也是包装函数的
 * 包装，这就构成了无限制递归，下一个查询就会陷入这种情况。
 *
 * 测量结果显示，这与模块设计的预期不符：以这种方式进行拦截的两个库根本无法相互连接。下面的查找请求
 * 在明确的 GLIBC 版本下使用 dlsym —— 必须如此，因为此处按名称调用 dlsym会调用该函数 —— 而带版本的
 * 查找与对象中不带版本的定义不匹配，该对象包含版本表，这些库都通过 libc 导入实现了这一点。因此，
 * RTLD_NEXT 会直接跳过任何同级对象，无论预加载顺序如何。由此产生两个后果：
 * - 一个拦截器无法通过这条链子再返回，这比为重入保护机制设计的方案更有效。该保护机制依然有效：它只
 * 是对非热点路径的一个线程局部测试，也是唯一能覆盖通过其他路径到达这里的调用者的机制；
 * 这个拦截库位于另一个 DLSYM 拦截器之后，因为加载器会将符号分配给最先被预加载的模块，而该模块的调
 * 用链永远不会到达我们这里。因此，注入协议具有排序约束——我们的预加载顺序优先——这是该库无法从内部强
 * 制执行的。cases/thead-case-2.sh 同时支持两种情况，因此这一约束是经过验证的事实，而非约定俗成的规
 * 则。
 *
 * 故意只链接 libc —— 工作负载容器自带 SDK，因此 dlsym/dlvsym 在此处保持未定义状态，由该容器所依赖
 * 的 glibc 来决定其含义。
 *
 * 仍需构建：为跳过 HGML 的调用者提供可选的 UKI 备用实现（ppuGetDeviceRuntimeInfo）。以下函数指针存
 * 储在无锁的全局变量中，在唯一的调用者为单线程 SMI 工具时，该变量会保持锁定状态。这些函数指针也是
 * 每个进程共享同一组，而非每个 dlopen 句柄单独分配，因此打开两个不同 libhgml 副本的进程将把两者通
 * 过最后一个已解析的副本进行路由处理——在仅包含一个 SDK 的容器中，这是理论上的情况。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

/* hgml.h 没有任何 #include 指令，也没有定义它在声明中使用的 NULL 和 bool。调用方在包含该头文件之前
 * 必须先提供两者。 */
#include <stdbool.h>
#include <stddef.h>

#include <hgml.h>

#include "common/vppu_ledger.h"
#include "common/vppu_quota.h"

static void *(*real_dlsym)(void *handle, const char *symbol);

/* 通过 handle 所解决的真正拦截库，即调用者传递给 dlsym()的内容。 */
static hgmlReturn_t (*real_get_memory_info)(hgmlDevice_t device, hgmlMemory_t *memory);
static hgmlReturn_t (*real_get_memory_info_v2)(hgmlDevice_t device, hgmlMemory_v2_t *memory);

/* 通过相同的处理程序解决了相关问题：没有它就无法应用每张卡的配额，而把某张卡的数值应用到所有卡上，
 * 反而比完全不拦截更糟糕。 */
static hgmlReturn_t (*real_get_index)(hgmlDevice_t device, unsigned int *index);

/* in_dlsym — 此线程已处于拦截状态中。
 *
 * 初始执行，因为默认的 TLS 模型通过动态链接器进行解析，这会在 DT_NEEDED 中产生第二个条目，从而破坏
 * “只有 libc.so.6”的保证情况。情况1的断言。此处是合理的：这个库通过预加载方式加载，因此属于初始集
 * 合。 */
static __thread bool in_dlsym __attribute__((tls_model("initial-exec")));

/* device_index — 表示此句柄对应的卡编号，若为 -1 则无法确定。 */
static int device_index(hgmlDevice_t device)
{
    if (real_get_index == NULL) {
        return -1;
    }

    unsigned int index = 0;
    if (real_get_index(device, &index) != HGML_SUCCESS) {
        return -1;
    }
    if (index >= (unsigned int)VPPU_MAX_DEVICES) {
        return -1;
    }
    return (int)index;
}

/* apply_slice — 该槽位的数值将替代供应商的数值，适用于 index 的卡。
 *
 * 账本读取过程不需要加锁，根据该模块的设计，总共一个分配状态在报告路径中比一个可能因供应商分配而阻
 * 塞的查询更有价值。无法访问的区域会被记录为零使用量——在拒绝级别时已经说过原因，在此编造一个数字只
 * 会比一个低数值更糟糕。 */
static void apply_slice(int index, unsigned long long quota, unsigned long long *total,
                        unsigned long long *used, unsigned long long *available)
{
    unsigned long long accounted = vppu_ledger_used(index);

    if (accounted > quota) {
        accounted = quota;
    }
    *total = quota;
    *used = accounted;
    *available = quota - accounted;
}

/* slice_of — 本次调用所涉及的卡及其配置的配额，或选择否，让供应商的固有数据保持原样。 */
static bool slice_of(hgmlDevice_t device, int *index_out, unsigned long long *quota_out)
{
    int index = device_index(device);
    if (index < 0) {
        return false;
    }

    unsigned long long quota = vppu_quota_memory_bytes(index);
    if (quota == 0ULL) {
        return false;
    }

    *index_out = index;
    *quota_out = quota;
    return true;
}

static hgmlReturn_t hook_get_memory_info(hgmlDevice_t device, hgmlMemory_t *memory)
{
    if (real_get_memory_info == NULL) {
        return HGML_ERROR_FUNCTION_NOT_FOUND;
    }

    hgmlReturn_t rc = real_get_memory_info(device, memory);
    if (rc == HGML_SUCCESS && memory != NULL) {
        int index = -1;
        unsigned long long quota = 0ULL;

        if (slice_of(device, &index, &quota)) {
            apply_slice(index, quota, &memory->total, &memory->used, &memory->free);
        }
    }
    return rc;
}

static hgmlReturn_t hook_get_memory_info_v2(hgmlDevice_t device, hgmlMemory_v2_t *memory)
{
    if (real_get_memory_info_v2 == NULL) {
        return HGML_ERROR_FUNCTION_NOT_FOUND;
    }

    /* 调用方在调用之前会写入 .version 字段，而 API 契约会将其原样返回，因此包装层不得修改该字段：修改
     * 该字段会破坏调用方的结构体与版本之间的协商机制，而恢复调用方自己的值则会导致供应商所报告的不匹配
     * 情况被掩盖。出于同样的原因，.reserved 字段保持不变——它只是驱动层的数值，并非配额的一部分。 */
    hgmlReturn_t rc = real_get_memory_info_v2(device, memory);
    if (rc == HGML_SUCCESS && memory != NULL) {
        int index = -1;
        unsigned long long quota = 0ULL;

        if (slice_of(device, &index, &quota)) {
            apply_slice(index, quota, &memory->total, &memory->used, &memory->free);
        }
    }
    return rc;
}

/* resolve_real_dlsym — 哪个库负责调用 dlsym 函数，以及在该哪个版本标签下，取决于此 shim 所加载的容
 * 器中的 glibc 版本，而非其原始构建版本：glibc 2.34 将 dlsym 函数移入 libc.so.6 中，标记为
 * GLIBC_2.34；在此之前，它位于 libdl.so.2 中，标记为 GLIBC_2.2.5。请尝试这两种版本。 */
static void resolve_real_dlsym(void)
{
    if (real_dlsym != NULL) {
        return;
    }

    real_dlsym = dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.34");
    if (real_dlsym == NULL) {
        real_dlsym = dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.2.5");
    }
}

/* is_own_address — 这个指针是否属于该代码所属的对象？
 *
 * 针对我们自己的包装器进行了拦截，而不是使用链接器提供的基类，因此它会保留对象被映射时的状态。指针
 * dladdr不能被视为我们的计数方式：拒绝拦截会大声指出错误数据，而包装我们自己的包装器会递归调用，直
 * 到栈结束。 */
static bool is_own_address(void *address)
{
    Dl_info mine;
    Dl_info theirs;

    if (dladdr((void *)hook_get_memory_info, &mine) == 0 || dladdr(address, &theirs) == 0) {
        return true;
    }
    return mine.dli_fbase == theirs.dli_fbase;
}

/* can_intercept — 针对这个句柄是否有可能进行每张卡级别的重写？
 *
 * 仅当索引查找完成之后：一个无法区分不同卡的包装器必须给所有卡应用同一个数值，而在包含多张卡的容器
 * 中，除了一张卡外其他卡的显示数值都会出错。拒绝进行拦截是一种诚实的失败，它只是这样表明，而不是显
 * 示一个合理的错误数值。 */
static bool can_intercept(void *handle, const char *symbol)
{
    if (real_get_index == NULL) {
        real_get_index = real_dlsym(handle, "hgmlDeviceGetIndex");
    }
    if (real_get_index == NULL) {
        vppu_log(VPPU_LOG_DENY,
                 "not intercepting %s: hgmlDeviceGetIndex unavailable, so a per-card quota "
                 "cannot be applied\n",
                 symbol);
        return false;
    }

    vppu_log(VPPU_LOG_DEBUG, "intercepted dlsym(%s)\n", symbol);
    return true;
}

/* 拦截库 — 通过调用者的自身句柄解决一个内存获取操作，然后决定是将包装器归还，还是归还链条返回的结
 * 果。 */
static void *intercept(void *handle, const char *symbol, bool v2)
{
    void *real = real_dlsym(handle, symbol);

    if (real == NULL) {
        return NULL;
    }
    if (is_own_address(real)) {
        /* 已经被这个对象拦截了——可能是通过另一个线程，或者通过某个以另一种方式解析相同符号的对等方。再次拦
         * 截它将会让包装器成为其自身的实际函数。 */
        vppu_log(VPPU_LOG_DEBUG,
                 "dlsym(%s) already resolves into this shim, not wrapping it twice\n", symbol);
        return real;
    }
    if (!can_intercept(handle, symbol)) {
        return real;
    }

    if (v2) {
        real_get_memory_info_v2 = real;
        return (void *)hook_get_memory_info_v2;
    }
    real_get_memory_info = real;
    return (void *)hook_get_memory_info;
}

void *dlsym(void *handle, const char *symbol)
{
    resolve_real_dlsym();
    if (real_dlsym == NULL) {
        return NULL;
    }

    /* 重新进入拦截保护：被转发了，未被拦截，此时该线程已处于拦截状态。请参考顶部的两个保护说明——通过全
     * 局范围解析的对等对象会返回到这里，而拦截在该内部解析操作中的内容则会将指针传递至该对象中。 */
    if (in_dlsym) {
        return real_dlsym(handle, symbol);
    }

    /* symbol 上没有 NULL 检查：glibc 将其声明为 __nonnull，因此该比较属于死代码，编译器会在
     * -Wnonnull-compare 模式下拒绝它。 */
    bool v2 = strcmp(symbol, "hgmlDeviceGetMemoryInfo_v2") == 0;
    if (!v2 && strcmp(symbol, "hgmlDeviceGetMemoryInfo") != 0) {
        return real_dlsym(handle, symbol);
    }

    in_dlsym = true;
    void *wrapper = intercept(handle, symbol, v2);
    in_dlsym = false;
    return wrapper;
}

/* 在 stderr 中公告负载情况：第 1 个门架的臂部由解析输出决定，那些库在加载过程中无声失败臂部，不应
 * 被误认为是已加载却无动作的臂部。 */
__attribute__((constructor)) static void announce(void)
{
    /* 任何卡的账本数据均为拦截库中的误配置情况，并非需要报告实际数值的状态。common/在拒绝级别下负责进
     * 行此类报告，因此关闭拦截库并不会使其停止工作。 */
    vppu_quota_validate();

    vppu_log(VPPU_LOG_DEBUG, "hgml_dlsym_hook loaded, per-card %s<i>\n",
             VPPU_ENV_MEMORY_LIMIT_PREFIX);
}

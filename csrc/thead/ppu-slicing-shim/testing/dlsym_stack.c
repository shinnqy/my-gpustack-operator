/* dlsym_stack.c — 第二个用于插入 DLSYM 的库，因此拦截库的可见性得以保留，能够与同类库共存而不被假
 * 设为不存在。
 *
 * Gate 1的臂部（c）在拦截库之前预加载了供应商的libhggc_wrapper.so文件。这证明了存在一个单一顺序的
 * peer，但这与证明不存在顺序递归无关：供应商的拦截库根本不会在dlsym调用中插入任何内容，因此它永远
 * 不会出现在拦截库自己的链表中。这个peer确实会执行dlsym调用，而且它产生的答案比它所针对的答案更有
 * 价值。
 *
 * 因为假想敌论无法证明任何东西，这就是事实。
 * - 一个合法的拦截器。它调用 dlsym，使用 dlvsym(RTLD_NEXT, ...) 进行下一项解析——这是 shim 库必须遵
 * 循的方式，因为在 dlsym 钩子内部以名称调用 dlsym会触发该钩子——随后用 pass-through 包装函数拦截两
 * 个 HGML 内存获取函数，因此它位于调用链中，而不仅仅是解析链中；
 * - 有一个真实的细节。对于每个符号，它还会通过 GLOBAL 作用域解析该符号，这便是库在查找未链接的内容
 * 时采用的方式，同时会通过 dladdr 报告该指针所指向的对象位于哪个对象中。该指针绝不能存在于
 * hgml_dlsym_hook.so 文件中：将拦截库的指针交给另一个库中的对象，这就是两个库之间交替进行的调用链
 * 的构成方式。
 *
 * 所测量的结果。这两个库无论以何种顺序都不相互链接。有版本的 dlsym 查找与没有版本的定义在包含版本
 * 表的对象中不匹配——从它们的 libc 导入来看，两者都是如此——因此每个库的 RTLD_NEXT 步骤会绕过另一个
 * 库，最终指向 libc。首先被加载的库拥有 dlsym；另一个则被加载、初始化后便不再被使用。
 * cases/thead-case-2.sh 在两种顺序下都严格遵循这一规则，这使得钩子中的顺序约束不再只是一个假设。
 *
 * 全局查找操作为 MEMOISED，该备注在调用之前设置，而非之后：无条件重新进入全局作用域的拦截器会通过
 * 自身进行递归调用，占用自己的账本，且不会提及其他人的保护机制。
 *
 * 只链接 libc，就像这里的其他组件一样——dlvsym 和 dladdr 会根据容器所依赖的 glibc 进行解析。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

/* hgml.h 没有任何 #include 指令，也没有定义它在声明中使用的 NULL 和 bool。调用方在包含该头文件之前
 * 必须先提供两者。 */
#include <stdbool.h>
#include <stddef.h>

#include <hgml.h>

#define STACK_TAG "[stack] "

static void *(*real_dlsym)(void *handle, const char *symbol);

/* GLOBAL作用域中的dlsym函数会指向——无论是否进行了任何预加载操作，首先加载的拦截器。该函数是通过显
 * 式查找而非按名称调用，因此此文件所执行的操作不依赖于该对象自身定义的符号是否通过PLT进行调用。 */
static void *(*global_dlsym)(void *handle, const char *symbol);

static hgmlReturn_t (*real_get_memory_info)(hgmlDevice_t device, hgmlMemory_t *memory);
static hgmlReturn_t (*real_get_memory_info_v2)(hgmlDevice_t device, hgmlMemory_v2_t *memory);

static void resolve_chain(void)
{
    if (real_dlsym == NULL) {
        /* glibc 2.34将dlsym移动到了libc.so.6中；在此之前，它存在于libdl.so.2中。 */
        real_dlsym = dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.34");
        if (real_dlsym == NULL) {
            real_dlsym = dlvsym(RTLD_NEXT, "dlsym", "GLIBC_2.2.5");
        }
    }
    if (global_dlsym == NULL && real_dlsym != NULL) {
        global_dlsym = real_dlsym(RTLD_DEFAULT, "dlsym");
    }
}

static hgmlReturn_t wrap_get_memory_info(hgmlDevice_t device, hgmlMemory_t *memory)
{
    if (real_get_memory_info == NULL) {
        return HGML_ERROR_FUNCTION_NOT_FOUND;
    }
    return real_get_memory_info(device, memory);
}

static hgmlReturn_t wrap_get_memory_info_v2(hgmlDevice_t device, hgmlMemory_v2_t *memory)
{
    if (real_get_memory_info_v2 == NULL) {
        return HGML_ERROR_FUNCTION_NOT_FOUND;
    }
    return real_get_memory_info_v2(device, memory);
}

/* note_chain — 通过全局作用域解析一个符号，仅一次，并报告其最终位置。 */
static void note_chain(void *handle, const char *symbol, bool *noted)
{
    if (*noted || global_dlsym == NULL) {
        return;
    }
    *noted = true;

    void *fn = global_dlsym(handle, symbol);
    Dl_info info;
    const char *origin = "unresolved";

    if (fn != NULL && dladdr(fn, &info) != 0 && info.dli_fname != NULL) {
        origin = info.dli_fname;
    }
    fprintf(stderr, STACK_TAG "chained %s origin=%s\n", symbol, origin);
}

void *dlsym(void *handle, const char *symbol)
{
    static bool noted_v1;
    static bool noted_v2;

    resolve_chain();
    if (real_dlsym == NULL) {
        return NULL;
    }

    bool v2 = strcmp(symbol, "hgmlDeviceGetMemoryInfo_v2") == 0;
    if (!v2 && strcmp(symbol, "hgmlDeviceGetMemoryInfo") != 0) {
        return real_dlsym(handle, symbol);
    }

    note_chain(handle, symbol, v2 ? &noted_v2 : &noted_v1);

    void *real = real_dlsym(handle, symbol);
    if (real == NULL) {
        return NULL;
    }

    fprintf(stderr, STACK_TAG "wrapped %s\n", symbol);
    if (v2) {
        real_get_memory_info_v2 = real;
        return (void *)wrap_get_memory_info_v2;
    }
    real_get_memory_info = real;
    return (void *)wrap_get_memory_info;
}

__attribute__((constructor)) static void announce(void)
{
    fprintf(stderr, STACK_TAG "dlsym_stack loaded, a second dlsym interposer\n");
}

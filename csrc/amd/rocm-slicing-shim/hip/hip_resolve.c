#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/vrocm_log.h"
#include "hip/hip_resolve.h"

/* 以下三项约束共同维持最低 GLIBC 版本。
 *
 * glibc 2.34 将 libdl 合并到了 libc 中，因此在该版本及更高版本的任何构建主机上，这三个符号都绑定在
 * GLIBC_2.34 上——经测量，它们是整个产品在 GLIBC_2.4 之前的版本中唯一的符号。将这些符号固定在合并之
 * 前的版本中，可以确保该组件能够在基于 Ubuntu 20.04 和 RHEL 8 构建的工作负载镜像中被加载使用。
 *
 * dladdr 这个需要被忽略的组件，因为它是在调用者来源诊断阶段出现的，而非解析阶段：在添加该诊断之前，
 * 环境是干净的；而将其加入后，上限又被重新设定为 2.34。build.sh check 则负责处理后续情况。 */
__asm__(".symver dlopen,dlopen@GLIBC_2.2.5");
__asm__(".symver dlsym,dlsym@GLIBC_2.2.5");
__asm__(".symver dladdr,dladdr@GLIBC_2.2.5");

/* 库名而非链接器名称：某些工作负载镜像包含 libamdhip64.so.6 或 .so.7，而仅有的开发镜像包含未版本化
 * 的符号链接，因此要求显示普通名称会忽略该库实际加载的特定镜像。RTLD_NOLOAD 表示我们从不加载运行时
 * 库——如果它尚未映射，则没有需要插入或查找的内容。 */
static const char *const HIP_SONAMES[] = {
    "libamdhip64.so.7",
    "libamdhip64.so.6",
    "libamdhip64.so",
};

/* 答案是在存在之后才被发布的，这种顺序正是下方图形结构的核心意义。首先设置 tried 会留下一个窗口，
 * 在此期间另一个线程会读取“已解决”的内容，同时得到一个仍为空值的句柄；而 vrocm_resolve 则将无法解
 * 析的名称视为致命错误——因此通过此路径第一次调用时发生的竞争会导致工作负载中断。两个线程都可以进行
 * dlopen 操作；而 RTLD_NOLOAD 仅允许对已映射的库进行引用操作，它们会获得相同的句柄，因此失去该竞争
 * 的优势所带来的代价，其实这个进程根本不会减少引用计数。 */
static void *hip_handle(void)
{
    static void *handle;
    static int tried;
    void *found = NULL;
    size_t i;

    if (__atomic_load_n(&tried, __ATOMIC_ACQUIRE)) {
        return __atomic_load_n(&handle, __ATOMIC_RELAXED);
    }
    for (i = 0; i < sizeof(HIP_SONAMES) / sizeof(HIP_SONAMES[0]); i++) {
        found = dlopen(HIP_SONAMES[i], RTLD_NOLOAD | RTLD_LAZY);
        if (found != NULL) {
            vrocm_log(VROCM_LOG_DEBUG, "resolving through %s\n", HIP_SONAMES[i]);
            break;
        }
    }
    /* 两个线程可能同时到达这里，即便写入同一个句柄，普通并发写仍构成数据竞争。这里使用原子写，relaxed
     * 顺序即可；随后对 tried 的 release 操作负责向读取方发布句柄。 */
    __atomic_store_n(&handle, found, __ATOMIC_RELAXED);
    __atomic_store_n(&tried, 1, __ATOMIC_RELEASE);
    return found;
}

VROCM_INTERNAL void *vrocm_resolve(const char *name)
{
    void *sym = dlsym(RTLD_NEXT, name);

    if (sym == NULL) {
        void *handle = hip_handle();

        if (handle != NULL) {
            sym = dlsym(handle, name);
        }
    }
    if (sym == NULL) {
        /* 故意选择那种嘈杂且终端化的方式。每一个替代方案都更糟糕：返回状态会引发运行时错误，而运行时从未产
         * 生过这种错误；返回成功则会让调用者得到一个未初始化的指针。一个无法找到所拦截对象的拦截库，也就没
         * 有正确的答案。
         *
         * 直接编写而非通过 vrocm_log() 进行编写，因为这一行代码必须能在 LIBVROCM_LOG_LEVEL=0 的情况下存活
         * 下来：即将中止的进程必须向其操作器说明原因。 */
        fprintf(stderr, VROCM_TAG "cannot resolve %s; aborting rather than inventing a status\n",
                name);
        abort();
    }
    return sym;
}

VROCM_INTERNAL const char *vrocm_caller_of(void *return_address)
{
    Dl_info info;

    if (return_address == NULL || dladdr(return_address, &info) == 0) {
        return NULL;
    }
    return info.dli_fname;
}

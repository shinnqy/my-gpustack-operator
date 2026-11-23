/* hip_resolve.h — 包装层如何到达真正的入口点，以及它如何说明是谁调用了它。
 *
 * 两次查找操作，且绝不会返回 A 这种构造的返回码。当框架使用 dlopen() 加载 libamdhip64 而非进行链接
 * 时，RTLD_NEXT 会找不到任何内容——PyTorch 就是如此——因此句柄查找并非额外的辅助机制，而是实际工作负
 * 载所遵循的路径。如果解析失败，必须终止程序而非返回结果：原本自然的占位值 1 **实际**为
 * hipErrorInvalidValue，因此返回该值会将“无法找到符号”转化为“运行时拒绝了您的参数”，这是一种错误的
 * 线索，会浪费一整个下午的时间。这个错误只出现过一次，发生在机制比较期间，它证明了预加载会破坏
 * ROCm 的功能。
 *
 * 以下 LOADER 调用在 hip_resolve.c 中已针对特定版本进行适配。在 GLIBC_2.34 版本中，glibc 将 libdl
 * 移动到 libc 中，因此 dlopen、dlsym 和 dladdr 在这些版本下可在任何现代构建平台上生效，成为超过
 * GLIBC_2.4 版本的产品中唯一的符号。三个 .symver 指令维持了这一状态；build.sh check 操作即可证明这
 * 些符号仍然存在。 */
#ifndef VROCM_HIP_HIP_RESOLVE_H
#define VROCM_HIP_HIP_RESOLVE_H

#include "common/vrocm.h"

/* vrocm_resolve — 这是隐藏在某个名称后的真实入口点，否则会中断操作。绝不会返回 NULL。 */
VROCM_INTERNAL void *vrocm_resolve(const char *name);

/* vrocm_caller_of — 发起调用对象，来自返回地址，或 NULL。
 *
 * 预加载机制无法拒绝在 HIP 运行时对自身导出符号进行的调用，因此解决方案是提高可见性而非避免这种情
 * 况。今日测量结果显示暴露量为零——在 PyTorch 运行过程中直至 OOM 为止，所有对这些包装层的调用均来自
 * 框架对象，而非 libamdhip64——正因如此，对这一情况的修改会变成一条日志条目，而非无法解释的重复记账。 */
VROCM_INTERNAL const char *vrocm_caller_of(void *return_address);

/* VROCM_REAL — 每个调用点都会解析一次位于一个条目后的真实函数指针。
 *
 * 并发的第一次调用会引发竞争，每个竞争线程都会将相同的名称映射到相同的地址，因此不需要锁，但这里的
 * 实现会使得任何插入的调用都变得串行化，无法产生任何效果。缓存仍然通过宽松的原子操作进行读取和写入，
 * 而不是作为普通对象处理：无论涉及的值如何，数据竞争都是未定义行为，而此拦截库所扩展的包装层会被加
 * 载到运行数十个线程的框架中。宽松的排序方式正是所需的所有排序方式——指针旁边没有任何需要发布的内容
 * ——而且它在所有基于 ROCm 的架构上都需要进行普通的加载操作。
 *
 * real_##fn 是一个 LOCAL 副本，因此后续的调用点所使用的数值是其他线程无法写入的，且每个包装器都会
 * 保留其已使用的名称。 */
#define VROCM_REAL(fn, type)                                                             \
    static type real_##fn##_cache;                                                       \
    type real_##fn = __atomic_load_n(&real_##fn##_cache, __ATOMIC_RELAXED);              \
    if (real_##fn == NULL) {                                                             \
        real_##fn = (type)vrocm_resolve(#fn);                                            \
        __atomic_store_n(&real_##fn##_cache, real_##fn, __ATOMIC_RELAXED);               \
    }

#endif /* VROCM_HIP_HIP_RESOLVE_H */

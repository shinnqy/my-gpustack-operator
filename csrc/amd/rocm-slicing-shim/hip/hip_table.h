/* hip_table.h — 每个插入的条目共有的内容：一个计数器、调用者来源诊断信息，以及 dump 中的位置。
 *
 * 条目由它们所依赖的编译单元自行注册。这里没有统一的名称列表，这是有意为之的：如果在一个文件中列出
 * 所有条目，那么每个家族的修改就会与所有其他家族的修改产生差异，而家族才是自然的单位——报告容量条目、
 * 经典分配条目以及池式条目彼此之间毫无关联。这个文件所拥有的是机制；每个家族所拥有的则是自己的条目。
 *
 * 这些计数器并非装饰性数据。LIBVROCM_LOG_LEVEL=2 在退出时会输出这些数据，而验证案例通过读取该输出
 * 结果来决定行是否通过——“是否完全达到池族”是一个其他输出无法回答的问题。 */
#ifndef VROCM_HIP_HIP_TABLE_H
#define VROCM_HIP_HIP_TABLE_H

#include "common/vrocm.h"

struct vrocm_entry {
    const char *name;
    unsigned long long calls;
    unsigned long long denials;
    unsigned traced;           /* 此条目已经报告了多少个调用者 */
    struct vrocm_entry *next;  /* 在首次触发时链接，因此 dump 可以执行实际触发的行为 */
};

/* vrocm_entry_hit — 统计一次调用，并在第2层级指出是哪个对象引发了该调用。
 *
 * return_address 属于调用者的地址，由 __builtin_return_address(0) 在包装函数的顶层帧中生成。仅会报
 * 告每次进入时前几次的调用情况：重点在于识别是哪些对象触发了进入，如果是一个在循环中分配内存的工作
 * 负载，它可能会掩盖这一情况。 */
VROCM_INTERNAL void vrocm_entry_hit(struct vrocm_entry *entry, void *return_address);

/* vrocm_entry_denied — 仅统计一次拒绝情况，因此 dump 能够区分从未被访问的条目和被访问后被拒绝的条
 * 目。 */
VROCM_INTERNAL void vrocm_entry_denied(struct vrocm_entry *entry);

/* VROCM_EXPORT — 一个插入的入口点，可在动态符号表中看到。
 *
 * 在每个包装文件中声明，而非继承。该库被编译为 -fvisibility=hidden 格式，因此 common/ 保持内部状态
 * ；在该标志下，只有某些声明赋予默认可见性时，定义才会被导出——对于这些名称而言，这意味着只有 ROCm
 * 自己的头文件声明它时才会被导出。其中两个没有导出：简单的 hipGetDeviceProperties 在头文件声明之前
 * 就被宏映射掉了，而 hipGetDevicePropertiesR0000 则完全没有被声明。两者都经过干净编译，没有导出任
 * 何内容，也不会插入任何内容。 */
#define VROCM_EXPORT __attribute__((visibility("default")))

/* VROCM_ENTRY / VROCM_HIT — 在其包装器旁声明一个条目，并统计对其的调用次数。
 *
 * __builtin_return_address(0) 必须存在于包装器本身中，而不是在辅助函数内部，因此这是一个宏。 */
#define VROCM_ENTRY(fn) static struct vrocm_entry vrocm_entry_##fn = { .name = #fn }
#define VROCM_HIT(fn) vrocm_entry_hit(&vrocm_entry_##fn, __builtin_return_address(0))
#define VROCM_DENIED(fn) vrocm_entry_denied(&vrocm_entry_##fn)

#endif /* VROCM_HIP_HIP_TABLE_H */

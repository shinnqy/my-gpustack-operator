/* hggc_entry.c — 入口点解析器，因此调用者无法向驱动程序请求未拦截的函数指针。
 *
 * 这些为何属于 QUOTA 的一部分？仅对通过动态链接器访问 hgMemAlloc 的调用者进行拦截是可行的。
 * libhggc.so 还导出了 hgGetProcAddress、hgGetProcAddress_v2 和 hgGetExportTable 等函数，当调用者请
 * 求其中某个函数以获取分配入口时，会得到驱动程序自身的地址并直接调用它——这绕过了 hggc_mem.c 中的所
 * 有包装函数。运行时层正是这样的调用者：这就是 libhggcrt 绑定所需驱动函数入口的方式。
 *
 * 如何决定替换方案。hgGetProcAddress 会接收基础名称及版本参数，然后选择带版本的符号：版本为 13000
 * 时，会指向 hgMemAlloc_v2；而在较旧版本中则指向 v1 形式。与其根据名称和版本重新实现这一选择——而每
 * 次 SDK 发布都需要重新推导——不如将驱动程序返回的指针与每个库定义背后的供应商条目进行比较。如果匹
 * 配到其中某个条目，则采用该条目；否则则采用我们的条目。地址比较能够回答调用者真正的问题，无论供应
 * 商的版本管理规则如何。
 *
 * 2.1.1 实际执行的是什么？是测量而非假设：其解析器返回已插入的地址——它通过链接器进行解析，如同其他
 * 调用者一样，因此预加载的定义也会在此处生效，通过它获取的条目无需任何替换操作。这是该驱动程序的特
 * 性，而非接口特性：NVIDIA的对应实现则返回驱动程序内部实现，这就是替换操作存在的原因。保持这一特性
 * 则只需对每个解析的符号进行一次比较，使得未来更改策略的 SDK 不会成为问题，而是不会造成配额中的空
 * 白区域。哪种情况发生可在日志中看到，无需推断——请参见 substitute()。
 *
 * hgGetExportTable 被观察到，但未被重写。它返回了一个不透明的供应商函数指针表，其结构为私有且未记
 * 录于文档中；试图猜测内部指针的交换偏移量会破坏运行时环境，而非帮助分析问题。因此，该请求会以表标
 * 识符的形式进行记录——这样，如果内存路径显示“未被拒绝且计数器未移动”，记录中就会显示是否获取了可能
 * 解释此情况的表。这是覆盖范围声明中公认的缺失点，而计数器则防止了这一情况的沉默状态。 */
#define _GNU_SOURCE

#include <dlfcn.h>

/* hggc.h 仅包含 <stdlib.h> 和 <stdint.h>，因此在其声明中既没有提供 NULL 也不提供 bool。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>

#include "common/vppu.h"
#include "hggc/hggc_quota.h"

/* substitute — 按照此库自己的定义，让指针成为驱动程序用于覆盖项分配的对象。
 *
 * 两种结果都会被记录，这就是目的。与 SDK 2.1.1 相比，此驱动程序的解析器已经返回了被插入的地址——它
 * 像其他调用者一样通过链接器进行解析——因此替换分支不会在那里触发，只有“already”那行会被打印出来。
 * 在那种情况下保持沉默的函数，与那些匹配错误、永远无法识别任何内容的函数毫无区别，而这正是此保护机
 * 制原本要防止的故障。对于被覆盖的符号，总会有一行被打印出来；而对于未被覆盖的符号，则不会打印任何
 * 内容，因此日志会说明其中哪一种情况发生了。
 *
 * 未覆盖的符号保持沉默是正确的：请求该配额范围之外的条目时，调用方必须得到供应商的地址保持不变，而
 * 不是出现失败。 */
static void substitute(const char *symbol, void **pfn)
{
    if (pfn == NULL || *pfn == NULL) {
        return;
    }

    for (int i = 0; i < VPPU_ENTRY_COUNT; i++) {
        enum vppu_entry entry = (enum vppu_entry)i;
        const char *name = vppu_hggc_name(entry);
        void *ours = vppu_hggc_self(entry);

        if (ours != NULL && *pfn == ours) {
            vppu_log(VPPU_LOG_DEBUG, "resolved %s as %s: already the interposed entry\n",
                     symbol, name);
            return;
        }
        if (ours == NULL || *pfn != vppu_hggc_next(entry)) {
            continue;
        }

        vppu_log(VPPU_LOG_DEBUG, "resolved %s as %s: handed out the interposed entry\n",
                 symbol, name);
        *pfn = ours;
        return;
    }
}

HGresult HGGCAPI hgGetProcAddress(const char *symbol, void **pfn, int hggcVersion,
                                  hguint64_t flags,
                                  HGdriverProcAddressQueryResult *symbolStatus)
{
    vppu_hggc_count(VPPU_GET_PROC_ADDRESS);

    HGresult (*real)(const char *, void **, int, hguint64_t,
                     HGdriverProcAddressQueryResult *) = vppu_hggc_next(VPPU_GET_PROC_ADDRESS);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(symbol, pfn, hggcVersion, flags, symbolStatus);
    if (rc == HGGC_SUCCESS) {
        substitute(symbol, pfn);
    }
    return rc;
}

/* v1解析器需要四个参数，而当前版本需要五个参数，因此需要单独定义，而不是直接调用上述函数。#undef标
 * 记则用于指向纯导出名称，在hggc.h中将其映射到hgGetProcAddress_v2函数，上述定义也使用了该函数，因
 * 此这个标记必须出现在该函数之后。 */
#undef hgGetProcAddress

HGresult HGGCAPI hgGetProcAddress(const char *symbol, void **pfn, int hggcVersion,
                                  hguint64_t flags)
{
    vppu_hggc_count(VPPU_GET_PROC_ADDRESS_V1);

    HGresult (*real)(const char *, void **, int, hguint64_t) =
        vppu_hggc_next(VPPU_GET_PROC_ADDRESS_V1);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(symbol, pfn, hggcVersion, flags);
    if (rc == HGGC_SUCCESS) {
        substitute(symbol, pfn);
    }
    return rc;
}

/* report_table — 用十六进制表示该表的名称，因为该标识符是一个没有可打印形式的原始16字节值。 */
static void report_table(const HGuuid *id)
{
    static const char digits[] = "0123456789abcdef";
    char text[2 * sizeof(id->bytes) + 1];
    size_t i;

    if (id == NULL) {
        vppu_log(VPPU_LOG_DEBUG, "hgGetExportTable: an unnamed table was handed out — entries "
                                 "reached through it are not interposed\n");
        return;
    }

    for (i = 0; i < sizeof(id->bytes); i++) {
        unsigned char byte = (unsigned char)id->bytes[i];

        text[2 * i] = digits[byte >> 4];
        text[2 * i + 1] = digits[byte & 0x0fU];
    }
    text[2 * i] = '\0';

    vppu_log(VPPU_LOG_DEBUG, "hgGetExportTable %s: an opaque table was handed out — entries "
                             "reached through it are not interposed\n",
             text);
}

HGresult HGGCAPI hgGetExportTable(const void **ppExportTable, const HGuuid *pExportTableId)
{
    vppu_hggc_count(VPPU_GET_EXPORT_TABLE);

    HGresult (*real)(const void **, const HGuuid *) = vppu_hggc_next(VPPU_GET_EXPORT_TABLE);
    if (real == NULL) {
        return HGGC_ERROR_NOT_FOUND;
    }

    HGresult rc = real(ppExportTable, pExportTableId);
    if (rc == HGGC_SUCCESS) {
        report_table(pExportTableId);
    }
    return rc;
}

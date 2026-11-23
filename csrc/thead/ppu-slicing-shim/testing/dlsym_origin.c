/* dlsym_origin.c — 门1机制探测：哪个对象赢得了符号？
 *
 * 它按照 ppu-smi 的方式解决两个 HGML 内存获取问题——在 libhgml.so 上执行 dlopen，然后在那个显式句柄
 * 上执行 dlsym——并且根据符号报告出返回地址实际指向的对象。dladdr 使得答案具有明确性：它直接指出包
 * 含对象，而不是要求调用者从行为中推断出来，这也是它位于可见性行旁边的原因。拦截库可能看起来像成功
 * 了，因为图形恰好匹配；只有来源会说明调用是进入哪个库。
 *
 * 这个问题不需要 PPU，因此 cases/thead-case-2.sh 可以作为无硬件依赖的版本运行：通过预加载拦截库，
 * 两种获取函数都必须从拦截库中输出；而由于没有 dlsym 控制，它们仍然必须从 libhgml.so 中输出——仅定
 * 义 HGML 符号，对于在指定句柄上解析的调用者来说是毫无意义的。
 *
 * 它连接了 libdl，没有 SDK：它以字符串的形式命名供应商库，从不包含供应商头文件，因此可以在任何包含
 * 该库的运行时镜像中构建。
 *
 * 每个符号对应的输出为一条 MECH <symbol> origin=<object> 行，因此是按案例进行解析，而非从文本中抓
 * 取内容。 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>

static void report(void *handle, const char *symbol)
{
    void *fn = dlsym(handle, symbol);
    Dl_info info;
    const char *origin = "unresolved";

    if (fn != NULL && dladdr(fn, &info) != 0 && info.dli_fname != NULL) {
        origin = info.dli_fname;
    }
    printf("MECH %s origin=%s\n", symbol, origin);
}

int main(void)
{
    void *handle = dlopen("libhgml.so", RTLD_NOW);

    if (handle == NULL) {
        printf("MECH dlopen failed: %s\n", dlerror());
        return 1;
    }
    report(handle, "hgmlDeviceGetMemoryInfo");
    report(handle, "hgmlDeviceGetMemoryInfo_v2");
    return 0;
}

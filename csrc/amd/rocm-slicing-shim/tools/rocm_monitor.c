/* rocm_monitor.c — 打印一个容器的切片：每张卡当前的 VRAM 配额以及对应的记账情况。
 *
 * 为何需要 A 读取器。在 AMD 主机上，没有任何数据报告有内存切片。rocm-smi 和 amd-smi 读取的是 sysfs
 * 和 DRM 节点数据，而非 HIP 数据，因此这些数据根本无法被预加载机制获取——经测量，这些数据显示的是物
 * 理卡上完整的 15.984 GiB 内存，但实际容器中的内存限制仅为 4 GiB。如果没有这个工具，内存限制的唯一
 * 证明就是拦截库初始化日志中的相关记录，或者某个工作负载遇到内存限制的情况。ppu-monitor 和
 * enpu-monitor 作为预加载机制的一部分被集成其中，它们也得出相同的结论并采用相同的处理方式。
 *
 * 为什么它读取的是该区域，而不是 shim？shim 中的每个符号都是故意隐藏的：一个预加载的库如果导出自己
 * 的 seam，就能被它所监管的工作负载所使用。因此没有需要调用的东西，而且这种结构本来就是正确的——它
 * 所读取的同一个文件，就是指标抓取器会读取的文件，而抓取器无法被要求将切片库预加载到自身中。这种布
 * 局正是出于这个原因而制定的。
 *
 * 为什么不能链接 common/vrocm_ledger.c 文件？该文件严格映射了区域，这意味着当该区域不存在时它会自
 * 动创建，其其他条目会获取卡的 fcntl 锁。读取器必须不做这两件事：创建区域会为仅查看容器的内容创建
 * 一个片段，而获取锁则会让监视器在驱动分配过程中阻塞——这是该区域的读取端被设计绝不允许发生的事情。
 * 因此，它从头文件中提取结构定义及其 _Static_assert 语句，并进行自己的只读 mmap 操作。作为同一布局
 * 的第二个解析器，这就是拥有契约的意义所在；如果这些断言失效，则会导致构建失败，因为某个字段会移动
 * 到该契约下方。
 *
 * 它既不需要 ROCm，也不需要特定设备，相关规则也明确规定了这一点：build.sh 在没有任何 ROCm 包含路径
 * 且未链接任何供应商库的情况下编译此程序，因此它运行在没有这些组件的容器中。
 *
 * 这里不显示的内容是计算掩码（COMPUTE CAP），因为它不在该区域，也无法诚实地放在那里。计算机制由平
 * 台通过CU掩码强制实现，而此库既看不到该掩码，也不拥有它。容器所接收到的内容存储在其自己的
 * HSA_CU_MASK 中，至于硬件是否遵守该掩码，只有 rocm-cumask-check 可以通过运行内核并读取其使用的CU
 * 来回答。这里显示的数据将是该库重复的数字，而它根本无法验证这些数字。
 *
 * 它无法显示的是容器所持有的、且从未分配过的卡。该区域会在第一次有输入时记录一张卡，因此此处未被修
 * 改的卡与容器不持有的卡无法区分。
 *
 * EXIT CODES，因为爬虫必须区分这些状态：0 该区域已被解析；1 没有区域可供读取（此容器中的内容尚未被
 * 分割，或路径无法读取）；2 文件存在，但此读取器可能无法解析它——可能是陌生的魔法机制、未知的布局版
 * 本，或者该读取器并非为处理此类情况而构建。拒绝是既定协议；如果读取器对未知版本进行猜测，就会从错
 * 误的偏移量中读取错误数据。 */
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_quota.h"

#define MIB (1024ULL * 1024ULL)

#define TOOL "rocm-monitor"

static void print_card(unsigned index, const struct vrocm_device_usage *usage, unsigned procs)
{
    unsigned long long quota = usage->memory_quota_bytes;
    unsigned long long used = usage->memory_used_bytes;
    unsigned long long freeb = quota > used ? quota - used : 0;
    unsigned slot;

    printf("card=%u mem_quota_mib=%llu mem_used_mib=%llu mem_free_mib=%llu lock_holder_pid=%d\n",
           index, quota / MIB, used / MIB, freeb / MIB, (int)usage->lock_holder_pid);

    for (slot = 0; slot < procs; slot++) {
        const struct vrocm_process_charge *charge = &usage->processes[slot];

        if (charge->pid == 0) {
            continue;
        }
        printf("  proc pid=%d mem_mib=%llu mem_bytes=%llu\n", (int)charge->pid,
               (unsigned long long)charge->memory_bytes / MIB,
               (unsigned long long)charge->memory_bytes);
    }
}

int main(int argc, char **argv)
{
    const char *path;
    /* 整个区域，而不仅仅是其头部：下面的每个检查操作都针对一次性读取的副本进行，因此保护该区域的尺寸测
     * 试，也是之后映射到的每个字节的尺寸测试。按其实际功能命名，因为将文件当作头部处理的读取器会将其视
     * 为与头部大小相当的头部，从而可能报告一个被截断的文件，导致 SIGBUS信号发生。 */
    struct vrocm_region snapshot;
    struct vrocm_region *region;
    struct stat st;
    unsigned charged = 0;
    unsigned index;
    int fd;

    /* argv[1] 为第一个参数，因此操作器可以从主机指向另一个容器的区域；否则，该拦截库会写入当前容器中的
     * 区域以及未设置或为空时的默认值。
     *
     * 此处进行了修复，而非调用 shim 自己的解析器，因为此工具未链接 common/ 下的任何对象——请参见此文件
     * 顶部的注释。两者必须一致，因此名称和默认值均来自共享头文件，且不会重复出现。 */
    path = (argc > 1) ? argv[1] : getenv(VROCM_ENV_LEDGER_PATH);
    if (path == NULL || *path == '\0') {
        path = VROCM_LEDGER_DEFAULT_PATH;
    }

    /* 仅使用 O_RDONLY，其他均不采用。此处使用 O_CREAT 进行打开，完全违背了该工具的设计初衷：它会为每个
     * 仅被查看的容器留下一个空白区域，后续的读取方无法从那些未分配任何资源的片段中识别出这一点。 */
    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, TOOL ": %s: %s\n", path, strerror(errno));
        fprintf(stderr, TOOL ": no usage region — nothing in this container has been sliced, or the"
                        " path is wrong. Nothing was created.\n");
        return 1;
    }

    if (fstat(fd, &st) != 0) {
        fprintf(stderr, TOOL ": %s: %s\n", path, strerror(errno));
        close(fd);
        return 1;
    }
    if ((size_t)st.st_size < sizeof(snapshot)) {
        fprintf(stderr, TOOL ": %s: %llu bytes, too small to be a region — one is %llu bytes and"
                        " every one of them is read before any is mapped\n",
                path, (unsigned long long)st.st_size, (unsigned long long)sizeof(snapshot));
        close(fd);
        return 2;
    }
    /* LOOPED，因为一次 read()操作并不对应一个确定的答案。允许返回比请求量更少的字节数，它返回的内容无
     * 法说明该区域是否较短或调用行为如何——上述 fstat 已经解决了第一个问题，因此后续读取操作才能区分两
     * 者差异。对于格式错误的区域，单次调用会报告账本内容无法解析，而任何扫描工具都会以无法解析为由拒绝
     * 该账本。 */
    {
        size_t got = 0;

        while (got < sizeof(snapshot)) {
            ssize_t n = read(fd, (char *)&snapshot + got, sizeof(snapshot) - got);

            if (n > 0) {
                got += (size_t)n;
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            fprintf(stderr, TOOL ": %s: cannot read the region: %s\n", path,
                    n < 0 ? strerror(errno) : "it ended before the region did");
            close(fd);
            return 2;
        }
    }

    if (memcmp(snapshot.magic, VROCM_REGION_MAGIC, VROCM_REGION_MAGIC_BYTES) != 0) {
        fprintf(stderr, TOOL ": %s: not a usage region, no %s magic at offset 0\n", path,
                VROCM_REGION_MAGIC);
        close(fd);
        return 2;
    }
    if (snapshot.layout_version != VROCM_REGION_VERSION) {
        fprintf(stderr,
                TOOL ": %s: layout version %u, this reader speaks %u. Refusing rather than reading"
                     " figures out of offsets that may have moved.\n",
                path, (unsigned)snapshot.layout_version, VROCM_REGION_VERSION);
        close(fd);
        return 2;
    }
    /* 仅版本本身是不够的：两个版本可以就布局达成一致，但在所包含的槽位数量上可能存在分歧，而这个阅读器
     * 会索引一个固定大小的表。 */
    if (snapshot.device_slots != VROCM_MAX_DEVICES ||
        snapshot.process_slots != VROCM_MAX_PROCESSES_PER_DEVICE) {
        fprintf(stderr, TOOL ": %s: %u cards x %u processes, this reader was built for %u x %u\n",
                path, (unsigned)snapshot.device_slots, (unsigned)snapshot.process_slots,
                VROCM_MAX_DEVICES, VROCM_MAX_PROCESSES_PER_DEVICE);
        close(fd);
        return 2;
    }

    region = mmap(NULL, sizeof(*region), PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (region == MAP_FAILED) {
        fprintf(stderr, TOOL ": %s: cannot map: %s\n", path, strerror(errno));
        return 2;
    }

    printf("region path=%s version=%u cards=%u procs=%u\n", path, VROCM_REGION_VERSION,
           (unsigned)snapshot.device_slots, (unsigned)snapshot.process_slots);

    /* 无需锁定卡即可读取，请留意标题说明。一次分配失效的账本价值，远超过那些能在无效配额后强行插入的读
     * 取器。 */
    for (index = 0; index < VROCM_MAX_DEVICES; index++) {
        const struct vrocm_device_usage *usage = &region->devices[index];

        if (usage->memory_quota_bytes == 0 && usage->memory_used_bytes == 0) {
            continue;
        }
        print_card(index, usage, VROCM_MAX_PROCESSES_PER_DEVICE);
        charged++;
    }
    if (charged == 0) {
        printf("# no card charged yet: a card appears here the first time an allocation on it is"
               " admitted\n");
    }

    munmap(region, sizeof(*region));
    return 0;
}

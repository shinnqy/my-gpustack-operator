/* ppu_monitor.c — 打印一个容器的切片：每张卡对应的配额和使用情况。
 *
 * 为何需要 A 读取器？一个切片的内存配额至少可以从 ppu-smi 中推断出来，因为可见性拦截库会重写
 * ppu-smi 所报告的数据。但 ppu-smi 的 COMPUTE 限制则无法推断：ppu-smi 没有最大 SM 列，就像
 * nvidia-smi 没有一样。因此，没有这个工具的话，关于计算限制的唯一证据，就是拦截库的初始化日志中的
 * 一行记录，或者与之相关的压力测试。Ascend 后端也得出了相同的结论，并以相同的方式回应——即使用
 * enpu-monitor 来监控，该工具与预加载程序一起构建并挂载在容器中。
 *
 * 为什么读取的是该区域而不是 shim？shim 中的每个符号都是故意隐藏的：一个预加载的库如果导出自己的
 * seam，就能被它所监管的工作负载所使用。因此没有需要调用的东西，而且这种结构本来就是正确的——当前正
 * 在读取的文件，正是指标抓取器会读取的文件，而抓取器无法被要求将切片库预加载到自身中。这种布局正是
 * 出于这个原因而制定的；它已记录在
 * .claude/skills/gpustack-operator-xbuild-and-verify/references/thead-usage-region.md 文件中。
 *
 * 为什么无法链接 common/vppu_ledger.c 文件？该文件的读取辅助函数会严格匹配区域定义，这意味着当不存
 * 在该区域时它就会自动创建，其其他条目会获取卡的 fcntl 锁。读取器必须避免这两件事：创建区域会为仅
 * 查看容器的任何内容生成一个片段，而获取锁则会让监视器在供应商分配后的阻塞状态中阻止操作——这正是区
 * 域读取端被设计绝不会执行的动作。因此，该文件从头文件中获取结构定义及其 _Static_assert 语句，然后
 * 执行自己的只读 mmap 操作。作为同一布局的第二个解析器，这就是拥有契约的意义所在，如果某个字段移动
 * 到契约下方，头文件中的断言就会导致构建失败。
 *
 * 它既不需要 SDK，也不需要设备，而相关规则也明确规定了这一点：build.sh 在没有任何 SDK 包含路径且未
 * 链接任何供应商库的情况下编译此文件，因此运行在没有这些元素的容器中。案例 1 会检查生成的
 * DT_NEEDED 值，而非依赖该规则来判断。
 *
 * 它刻意不显示的是比例形式的限流值。控制器的允许范围在相应区间内，但其所占比例范围则不是：该区间即
 * 为容器自身的配置（HGGC_SM_CONTROL_PERIOD_MS），任何无法理解该环境的读取器都会输出错误的比例数值。
 * 因此，allow_us 以原始形式被报告，只有配置该值的人员才能根据该区间进行解读。
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

#include "common/vppu_ledger.h"

#define MIB (1024ULL * 1024ULL)

/* 按照编写者的处理方式完全解决，包括空字符串：一个对拦截库关于应打开的文件选择有不同意见的读取器，
 * 会报告另一个容器的切片，或者根本不报告。 */
static const char *region_path(void)
{
    const char *path = getenv(VPPU_ENV_LEDGER_PATH);

    return (path != NULL && *path != '\0') ? path : VPPU_LEDGER_DEFAULT_PATH;
}

/* 如果该区域曾经记录过任何关于该卡的信息，那么它就是一个卡。仅配额本身并不构成标准：在存在记账行为
 * 的同时，其配额被取消的卡，正是值得关注的状态。 */
static bool card_is_held(const struct vppu_device_usage *card)
{
    return card->memory_quota_bytes != 0ULL || card->memory_used_bytes != 0ULL;
}

/* 每张卡一行，每个记账进程有一行缩进行，整个内容都包含 key=value格式——相同的输出既适合人类使用，也
 * 适合替换 od 配方的解析器。单位位于 KEY 中，因此值无需被剥离，精确的字节数字也位于同一行中，因为
 * 小于 MiB 的记账是真实存在的，且四舍五入后为零 MiB。 */
static void print_card(unsigned int index, const struct vppu_device_usage *card,
                       unsigned int process_slots)
{
    unsigned long long quota = (unsigned long long)card->memory_quota_bytes;
    unsigned long long used = (unsigned long long)card->memory_used_bytes;
    unsigned long long unused = (used < quota) ? quota - used : 0ULL;

    printf("card=%u mem_quota_mib=%llu mem_used_mib=%llu mem_free_mib=%llu sm_limit_pct=%u "
           "sm_util_pct=%u allow_us=%llu lock_pid=%d mem_quota_bytes=%llu mem_used_bytes=%llu\n",
           index, quota / MIB, used / MIB, unused / MIB, card->sm_limit_percent,
           card->sm_util_percent, (unsigned long long)(card->control.allow_ns / 1000ULL),
           card->lock_holder_pid, quota, used);

    for (unsigned int slot = 0; slot < process_slots; slot++) {
        const struct vppu_process_charge *charge = &card->processes[slot];

        if (charge->pid == 0) {
            continue;
        }
        printf("  proc pid=%d mem_mib=%llu mem_bytes=%llu\n", charge->pid,
               (unsigned long long)charge->memory_bytes / MIB,
               (unsigned long long)charge->memory_bytes);
    }
}

int main(void)
{
    const char *path = region_path();
    int fd = open(path, O_RDONLY | O_CLOEXEC);

    if (fd < 0) {
        fprintf(stderr, "ppu-monitor: %s: %s\n", path, strerror(errno));
        fprintf(stderr, "ppu-monitor: no usage region — nothing in this container has been sliced,"
                        " or %s names another path\n", VPPU_ENV_LEDGER_PATH);
        return 1;
    }

    /* 在文件被调整大小或映射之前会读取头部信息，因此未知的版本会被当作正常版本处理，而非表示大小不匹配
     * ：未来的布局可能具有不同的大小，若告诉读取方“太小了”，会让他们去错误的地方寻找。 */
    unsigned char head[VPPU_REGION_MAGIC_BYTES + sizeof(uint32_t)];
    ssize_t got = pread(fd, head, sizeof(head), 0);
    if (got < 0) {
        /* 标记为 FAILED 的读取操作会退出代码为 1，而非 2：上述合约在文件名为“the path is unreadable”的文件
         * 中出现“没有区域”的提示，而将 I/O 错误报告为“太小”则会让抓取器寻找被截断的文件，而那并非问题所在。 */
        fprintf(stderr, "ppu-monitor: %s: cannot read the region header: %s\n", path,
                strerror(errno));
        close(fd);
        return 1;
    }
    if (got != (ssize_t)sizeof(head)) {
        fprintf(stderr, "ppu-monitor: %s: too small to carry a region header\n", path);
        close(fd);
        return 2;
    }
    if (memcmp(head, VPPU_REGION_MAGIC, VPPU_REGION_MAGIC_BYTES) != 0) {
        fprintf(stderr, "ppu-monitor: %s: not a usage region, no %s magic at offset 0\n", path,
                VPPU_REGION_MAGIC);
        close(fd);
        return 2;
    }

    uint32_t version = 0;
    memcpy(&version, head + VPPU_REGION_MAGIC_BYTES, sizeof(version));
    if (version != VPPU_REGION_VERSION) {
        fprintf(stderr,
                "ppu-monitor: %s: layout version %u, this reader knows %u — refusing rather than"
                " guessing at the offsets\n",
                path, version, (unsigned int)VPPU_REGION_VERSION);
        close(fd);
        return 2;
    }

    /* 在映射前进行尺寸调整，因为在文件末尾之后的映射在访问时会出错，而不是在此处失败。 */
    struct stat st;
    if (fstat(fd, &st) != 0 || (size_t)st.st_size < sizeof(struct vppu_region)) {
        fprintf(stderr, "ppu-monitor: %s: %llu bytes, a version %u region is %zu\n", path,
                (unsigned long long)st.st_size, version, sizeof(struct vppu_region));
        close(fd);
        return 2;
    }

    void *mapped = mmap(NULL, sizeof(struct vppu_region), PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        fprintf(stderr, "ppu-monitor: %s: cannot map: %s\n", path, strerror(errno));
        return 1;
    }
    const struct vppu_region *region = mapped;

    /* 这些计数被检查而非信任，出现不匹配则会被拒绝：它们设定了每个下方字段的偏移量，而卡锁区域的字节则
     * 按卡索引进行分配，因此与这些计数不符的写入者不会锁定该账本认为的内容。 */
    if (region->header_bytes != (uint32_t)offsetof(struct vppu_region, devices)
        || region->device_slots != VPPU_MAX_DEVICES
        || region->process_slots != VPPU_MAX_PROCESSES_PER_DEVICE) {
        fprintf(stderr,
                "ppu-monitor: %s: header %u bytes, %u cards, %u processes per card — this reader"
                " was built for %zu/%u/%u, refusing\n",
                path, region->header_bytes, region->device_slots, region->process_slots,
                offsetof(struct vppu_region, devices), (unsigned int)VPPU_MAX_DEVICES,
                (unsigned int)VPPU_MAX_PROCESSES_PER_DEVICE);
        return 2;
    }

    printf("region path=%s version=%u cards=%u procs=%u\n", path, version, region->device_slots,
           region->process_slots);

    unsigned int printed = 0;
    for (unsigned int index = 0; index < region->device_slots; index++) {
        if (!card_is_held(&region->devices[index])) {
            continue;
        }
        print_card(index, &region->devices[index], region->process_slots);
        printed++;
    }
    if (printed == 0) {
        printf("# no card charged yet: a card appears here the first time an allocation on it is"
               " admitted\n");
    }
    return 0;
}

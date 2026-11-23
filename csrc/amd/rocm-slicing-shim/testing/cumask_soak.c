/* cumask_soak.c — 这个被屏蔽的容器实际获得的 것은：吞吐量，以及它所依赖的物理单位。
 *
 * 这里有三件事更偏向负载负担，而非性能优化，且每一项都存在错误的测量结果。
 *
 * 障碍。仅在相近时间开始的租户在整次运行期间不会重叠，且每个租户都使用该卡自身来测量其窗口的一部分。
 * 如果没有集中点，N 个租户的总量会超过该卡的峰值——这个看似结果却实际上不可能实现的数字。因此，每个
 * 租户都记录在一个文件中，等待群体形成后，在释放时立即报告；如果发现这些时间点相距甚远，就会放弃该
 * 运行而不予相信。
 *
 * 饱和内核。受延迟限制的内核无法填充小分区，因此分配给租户的卡部分资源时，其报告结果优于预期，且所
 * 有重叠读取值都被夸大。该内核每个线程运行八条独立的 FMA 链，这使得 ALU 在指令级并行性上保持忙碌，
 * 而非基于占用率，因此无论是小掩码还是大掩码都能被充分利用。这些链将结果输入给编译器无法证明不可达
 * 的源点——没有它，整个循环就变成死代码；而立即返回的内核则反映了执行路径。
 *
 * 占用率读数。仅通过吞吐量无法区分已生效的掩码与在某些 XCC 中被丢弃的掩码：在 304-CU 部件中，一位
 * 掩码所代表的卡比例约为 3.7%，而容器温度达到 267 CUs，因为运行时间由限制最严格的 XCC 决定，并未提
 * 及其他七种情况。因此，每个温度记录其运行位置，每个租户会报告其占用的不同单元数量以及其 GFLOP/s
 * 值。解码逻辑位于 device/vrocm_hwid.h 中，该文件与 tools/rocm-cumask-check 共享，因此两者在测量结
 * 果上不会产生分歧。
 *
 * 它要求硬件具备无拓扑结构特性，且链接环节无需 HSA。它所报告的关于该卡的所有信息均来自其自身数据。
 * 它输出的 XCC 数量，即其数据在哪些 XCC 上被观测到，正是此处重要的数据——容器始终未能达到的 XCC 就
 * 是被遮罩排除的 XCC，这就是观察结果，而非其中的空白。
 *
 * --self-check 是程序对自身状态的验证：要么只有一名租户，要么有两名租户属于同一掩码，且两者数量之
 * 和必须等于一。如果数量超过一，屏障就无法有效约束它们；如果数量远小于一，内核也不会达到饱和状态。
 * 无论哪种情况，该程序最终生成的任何重叠数据都是无用的。 */
/* 这里没有 _GNU_SOURCE 声明，与此树中的 C 文件不同：hipcc 已经定义了它，再次定义只会引发警告——而且
 * build.sh 在成功执行时必须保持静默。 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <hip/hip_runtime.h>

#include "device/vrocm_hwid.h"

#define TOOL "cumask_soak"

/* 八条链路能够确保每个ALU操作槽位不被占用：下面的循环每条链路携带一个活跃的浮点值，而更宽的集合则
 * 会导致寄存器占用问题，这超出了所需的容量。 */
#define CHAINS 8
#define BLOCKS 2048u
#define THREADS 256u
#define ITERS 1000u

/* 在每次同步之前启动已排队的任务，这些任务分布在多个流中。这两个数字都不是调节旋钮；它们都能纠正某
 * 个进程对卡的低估问题。
 *
 * 每次启动后同步操作会耗尽队列资源，导致卡在等待期间处于闲置状态，而一个进程会依次填补那个空白区域，
 * 使两个进程能够相互补充。以一批为一个单位进行测量：整个卡以 21636 GFLOP/s 的速率单独处理数据，而
 * 使用它的两个进程总处理量达到了 25015——这一总量高于单独处理的数值，呈现出一种无法逾越的障碍，需要
 * 通过其他途径来突破。
 *
 * 仅靠批量处理并不能使其关闭，因为在一个流上进行的启动操作会相互串行化：当某个操作结束时，下一个操
 * 作无法开始。两个进程通过两个队列与这些操作重叠，而单独运行的操作则需要多个流才能实现相同的效果。 */
#define BATCH 16u
#define STREAMS 4u

#define SLOT_SINK VROCM_HWID_SLOTS

/* 每个融合乘加操作涉及2个浮点运算，每条链、每个线程、每次迭代各进行一次。 */
#define FLOPS_PER_LAUNCH \
    ((double)BLOCKS * (double)THREADS * (double)CHAINS * (double)ITERS * 2.0)

__global__ void vrocm_saturate(float *sink, unsigned int *slots, unsigned int iters)
{
    float chain[CHAINS];
    float mul = 1.0000001f, add = 0.0000001f;
    unsigned int i;
    int k;

    for (k = 0; k < CHAINS; k++) {
        chain[k] = (float)(threadIdx.x + blockIdx.x + (unsigned int)k + 1u);
    }
    for (i = 0; i < iters; i++) {
        for (k = 0; k < CHAINS; k++) {
            chain[k] = fmaf(chain[k], mul, add);
        }
    }
    slots[vrocm_hwid_slot()] = 1u;

    {
        float total = 0.0f;

        for (k = 0; k < CHAINS; k++) {
            total += chain[k];
        }
        /* 这些链表都从正值开始并持续增长，因此无法被占用——但编译器无法证明这一点，而这正是让上述循环继续运
         * 行的原因。 */
        if (total < 0.0f) {
            sink[0] = total;
        }
    }
}

/* ---- 时钟 ------------------------------------------------------------------------------- */

static double monotonic_seconds(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* wall_seconds — CLOCK_REALTIME，这是有意为之的，因为发布时刻是跨进程比较的，而单调时钟没有可比较
 * 的共享起点。 */
static double wall_seconds(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ---- 障碍 -------------------------------------------------------------------------- */

/* barrier_wait — 在文件中注册后，等待 tenants 的注册完成。返回该进程被释放的瞬间。
 *
 * 每个租户对应一个追加的字节，文件大小即为该数量。O_APPEND操作进行单字节写入时具有原子性，无需锁定，
 * 若某个租户在写入完成前崩溃，也无需进行清理——等待过程仅会超时，这是期望看到的结果，而非死锁。 */
static double barrier_wait(const char *path, unsigned int tenants, double timeout)
{
    double deadline;
    int fd;

    fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (fd < 0) {
        fprintf(stderr, TOOL ": barrier %s: %s\n", path, strerror(errno));
        return -1.0;
    }
    if (write(fd, "1", 1) != 1) {
        fprintf(stderr, TOOL ": barrier %s: cannot register: %s\n", path, strerror(errno));
        close(fd);
        return -1.0;
    }
    close(fd);

    deadline = monotonic_seconds() + timeout;
    for (;;) {
        struct stat st;

        if (stat(path, &st) == 0 && (unsigned long long)st.st_size >= tenants) {
            return wall_seconds();
        }
        if (monotonic_seconds() > deadline) {
            fprintf(stderr, TOOL ": barrier %s: only %lld of %u tenants arrived in %.0fs\n", path,
                    (long long)(stat(path, &st) == 0 ? st.st_size : 0), tenants, timeout);
            return -1.0;
        }
        usleep(2000);
    }
}

/* ---- 测量 ---------------------------------------------------------------------- */

struct result {
    double gflops;
    double seconds;
    double released;
    unsigned int units;
    unsigned int xccs;
};

/* 拦截库会保留条件、否定、数字和反引号内的符号。分配。
 *
 * 整体进行计时而非按每次启动单独计时：该循环保持队列的充实状态，因此按每次启动进行计时会导致每次启
 * 动的记账都基于之前启动的消耗。槽位表在每次启动中累积数据，因此最终呈现的是所有单元的数据汇总，即
 * 每次启动中所有单元所产生的结果。 */
static int soak(int device, double seconds, struct result *out)
{
    unsigned int host[VROCM_HWID_SLOTS + 1];
    unsigned int *slots = NULL;
    unsigned int seen_xcc[VROCM_HWID_MAX_XCC];
    hipStream_t stream[STREAMS];
    float *sink = NULL;
    double started, elapsed;
    unsigned long long launches = 0;
    unsigned int i;
    hipError_t rc;

    rc = hipSetDevice(device);
    if (rc != hipSuccess) {
        fprintf(stderr, TOOL ": hipSetDevice(%d): %s\n", device, hipGetErrorString(rc));
        return 0;
    }
    if (hipMalloc((void **)&slots, sizeof(host)) != hipSuccess ||
        hipMalloc((void **)&sink, sizeof(float) * 4) != hipSuccess ||
        hipMemset(slots, 0, sizeof(host)) != hipSuccess) {
        fprintf(stderr, TOOL ": cannot set up the device buffers\n");
        return 0;
    }
    for (i = 0; i < STREAMS; i++) {
        if (hipStreamCreate(&stream[i]) != hipSuccess) {
            fprintf(stderr, TOOL ": cannot create stream %u\n", i);
            return 0;
        }
    }

    /* 超出窗口时间的单次启动，因为第一次启动会产生模块加载和代码对象重新定位的费用，否则这些费用将计入
     * 卡。 */
    vrocm_saturate<<<BLOCKS, THREADS, 0, stream[0]>>>(sink, slots, ITERS);
    rc = hipDeviceSynchronize();
    if (rc != hipSuccess) {
        fprintf(stderr, TOOL ": warm-up launch failed: %s\n", hipGetErrorString(rc));
        return 0;
    }

    started = monotonic_seconds();
    do {
        for (i = 0; i < BATCH; i++) {
            vrocm_saturate<<<BLOCKS, THREADS, 0, stream[i % STREAMS]>>>(sink, slots, ITERS);
        }
        rc = hipDeviceSynchronize();
        if (rc != hipSuccess) {
            fprintf(stderr, TOOL ": launch failed: %s\n", hipGetErrorString(rc));
            return 0;
        }
        launches += BATCH;
        elapsed = monotonic_seconds() - started;
    } while (elapsed < seconds);

    for (i = 0; i < STREAMS; i++) {
        (void)hipStreamDestroy(stream[i]);
    }

    if (hipMemcpy(host, slots, sizeof(host), hipMemcpyDeviceToHost) != hipSuccess) {
        fprintf(stderr, TOOL ": cannot read the slot table back\n");
        return 0;
    }
    (void)hipFree(slots);
    (void)hipFree(sink);

    memset(seen_xcc, 0, sizeof(seen_xcc));
    out->units = 0;
    for (i = 0; i < VROCM_HWID_SLOTS; i++) {
        if (host[i]) {
            out->units++;
            seen_xcc[VROCM_HWID_SLOT_XCC(i)] = 1;
        }
    }
    out->xccs = 0;
    for (i = 0; i < VROCM_HWID_MAX_XCC; i++) {
        out->xccs += seen_xcc[i];
    }
    out->seconds = elapsed;
    out->gflops = (double)launches * FLOPS_PER_LAUNCH / elapsed / 1e9;
    return 1;
}

static void print_result(const char *label, int device, const struct result *r)
{
    printf("TENANT label=%s device=%d seconds=%.3f gflops=%.1f units=%u xccs=%u released=%.6f\n",
           label, device, r->seconds, r->gflops, r->units, r->xccs, r->released);
    fflush(stdout);
}

/* result_write / result_read — 分拆的租户如何将其数据返回。
 *
 * 每个租户一个文件，而不是管道，因为租户是执行（exec）而非分叉（fork），而管道必须通过一个继承的描
 * 述符编号在执行过程中存活下来。文件只是一个路径参数，大写形式也能被读取。 */
static void result_write(const char *path, const struct result *r)
{
    FILE *f = fopen(path, "w");

    if (f == NULL) {
        fprintf(stderr, TOOL ": cannot write %s: %s\n", path, strerror(errno));
        return;
    }
    fprintf(f, "%.6f %.6f %.6f %u %u\n", r->gflops, r->seconds, r->released, r->units, r->xccs);
    fclose(f);
}

static int result_read(const char *path, struct result *r)
{
    FILE *f = fopen(path, "r");
    int fields;

    if (f == NULL) {
        return 0;
    }
    fields = fscanf(f, "%lf %lf %lf %u %u", &r->gflops, &r->seconds, &r->released, &r->units,
                    &r->xccs);
    fclose(f);
    return fields == 5;
}

/* ---- 自检查 ------------------------------------------------------------------------ */

static int fails;

static void check_that(int ok, const char *name, const char *detail)
{
    printf("%s | %s | %s\n", ok ? "PASS" : "FAIL", name, detail);
    if (!ok) {
        fails++;
    }
}

/* spawn_tenant — 一个租户，在其自己的进程中运行，并拥有自己的 ROCr 初始化。
 *
 * 选择 exec 而不是仅使用 fork：故意如此，因为当 ROCr 初始化时就会读取该掩码，而当前进程已经完成了
 * 初始化。通过 fork 产生的子进程会继承该状态，无论父进程被赋予何种环境，它都会测量父进程的环境。 */
static pid_t spawn_tenant(const char *exe, int device, double seconds, const char *barrier,
                          unsigned int tenants, const char *label, const char *result_path)
{
    char s_device[32], s_seconds[32], s_tenants[32];
    pid_t pid = fork();

    if (pid != 0) {
        return pid;
    }
    snprintf(s_device, sizeof(s_device), "%d", device);
    snprintf(s_seconds, sizeof(s_seconds), "%.3f", seconds);
    snprintf(s_tenants, sizeof(s_tenants), "%u", tenants);

    execl(exe, exe, "--device", s_device, "--seconds", s_seconds, "--barrier", barrier, "--tenants",
          s_tenants, "--label", label, "--result", result_path, (char *)NULL);
    fprintf(stderr, TOOL ": cannot exec %s: %s\n", exe, strerror(errno));
    _exit(127);
}

static int self_check(int device, double seconds)
{
    struct result solo, a, b;
    char barrier[] = "/tmp/vrocm-soak-barrier-XXXXXX";
    char path_a[128], path_b[128];
    double sum, ratio, spread;
    char detail[256];
    pid_t pid_a, pid_b;
    int fd, status;

    fd = mkstemp(barrier);
    if (fd < 0) {
        fprintf(stderr, TOOL ": cannot make a barrier file: %s\n", strerror(errno));
        return 2;
    }
    close(fd);
    unlink(barrier); /* barrier_wait 函数会创建它；mkstemp仅保留了名称。 */
    snprintf(path_a, sizeof(path_a), "%s.a", barrier);
    snprintf(path_b, sizeof(path_b), "%s.b", barrier);

    memset(&solo, 0, sizeof(solo));
    solo.released = wall_seconds();
    if (!soak(device, seconds, &solo)) {
        return 2;
    }
    print_result("solo", device, &solo);

    pid_a = spawn_tenant("/proc/self/exe", device, seconds, barrier, 2, "a", path_a);
    pid_b = spawn_tenant("/proc/self/exe", device, seconds, barrier, 2, "b", path_b);
    if (pid_a < 0 || pid_b < 0) {
        fprintf(stderr, TOOL ": cannot start the tenants\n");
        return 2;
    }
    waitpid(pid_a, &status, 0);
    waitpid(pid_b, &status, 0);

    if (!result_read(path_a, &a) || !result_read(path_b, &b)) {
        fprintf(stderr, TOOL ": a tenant produced no result\n");
        return 2;
    }
    /* 租户在继承的 stdout 上打印了他们自己的行内容；在这里重新打印这些行会导致每个案例需要解析的数字数
     * 量翻倍。 */
    unlink(barrier);
    unlink(path_a);
    unlink(path_b);

    /* 饱和性检查。同一掩码下的两个租户会相互干扰，因此它们的总和等于一个租户的数值。如果总和远高于该值，
     * 说明它们没有产生重叠——屏障没有起到应有的作用。如果总和低于该值，则意味着内核让分区处于空闲状态，
     * 这会低估该程序所测量的每个切片的实际值。 */
    sum = a.gflops + b.gflops;
    ratio = solo.gflops > 0.0 ? sum / solo.gflops : 0.0;
    snprintf(detail, sizeof(detail), "solo %.1f, a+b %.1f, ratio %.3f (want 0.85..1.15)",
             solo.gflops, sum, ratio);
    check_that(ratio >= 0.85 && ratio <= 1.15, "soak/two_tenants_sum_to_one", detail);

    /* 以及那个障碍本身，以数字形式报告，而非假设。 */
    spread = a.released > b.released ? a.released - b.released : b.released - a.released;
    snprintf(detail, sizeof(detail), "released %.0f ms apart (want < 100)", spread * 1000.0);
    check_that(spread < 0.100, "soak/barrier_released_together", detail);

    snprintf(detail, sizeof(detail), "solo %u, a %u, b %u", solo.units, a.units, b.units);
    check_that(a.units == solo.units && b.units == solo.units, "soak/tenants_see_the_same_units",
               detail);

    printf("FAILS=%d\n", fails);
    return fails == 0 ? 0 : 1;
}

/* ---- 入口点 -------------------------------------------------------------------------- */

static void usage(void)
{
    fprintf(stderr,
            "usage: " TOOL " [--device N] [--seconds S] [--label L]\n"
            "               [--barrier PATH --tenants N] [--result PATH]\n"
            "       " TOOL " --self-check [--device N] [--seconds S]\n"
            "  With --barrier, waits for N tenants to register before measuring.\n"
            "  With --self-check, measures one tenant then two and asserts they add up.\n");
}

int main(int argc, char **argv)
{
    const char *barrier = NULL, *result_path = NULL, *label = "solo";
    unsigned int tenants = 1;
    double seconds = 3.0;
    struct result r;
    int device = 0, check = 0, i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--device") == 0 && i + 1 < argc) {
            device = (int)strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            seconds = strtod(argv[++i], NULL);
        } else if (strcmp(argv[i], "--label") == 0 && i + 1 < argc) {
            label = argv[++i];
        } else if (strcmp(argv[i], "--barrier") == 0 && i + 1 < argc) {
            barrier = argv[++i];
        } else if (strcmp(argv[i], "--tenants") == 0 && i + 1 < argc) {
            tenants = (unsigned int)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--result") == 0 && i + 1 < argc) {
            result_path = argv[++i];
        } else if (strcmp(argv[i], "--self-check") == 0) {
            check = 1;
        } else {
            usage();
            return 2;
        }
    }
    if (seconds <= 0.0) {
        fprintf(stderr, TOOL ": --seconds must be positive\n");
        return 2;
    }
    if (check) {
        return self_check(device, seconds);
    }

    memset(&r, 0, sizeof(r));
    if (barrier != NULL) {
        /* 对于那些在注册前必须先在 ROCm 运行时中页面化的租户来说，这个配额相当慷慨；同时该配额又足够短，以
         * 至于当一个租户从未进行此类操作时，情况只会卡住几秒钟而不是永远如此。 */
        r.released = barrier_wait(barrier, tenants, 60.0);
        if (r.released < 0.0) {
            return 2;
        }
    } else {
        r.released = wall_seconds();
    }
    if (!soak(device, seconds, &r)) {
        return 2;
    }
    print_result(label, device, &r);
    if (result_path != NULL) {
        result_write(result_path, &r);
    }
    return 0;
}

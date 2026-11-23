/* hggc_launch_load.cu — 第四道工作负载的半部分：通过内核启动操作使某张显卡达到饱和状态，并报告该进
 * 程所测量的使用率。
 *
 * 计算限制是此处没有执行器能够达到的一个维度：其他测试/构建产物会分配内存或读取数据，但都不会使用
 * 计算资源。限制行为只能以原本会占用整个卡体的工作负载作为参考标准，因此这里会连续启动一个设计上较
 * 长的内核，以固定时间作为结束标志。
 *
 * 它故意测量自身。cases/thead-case-7.sh 根据该进程自身的 hgmlDeviceGetProcessUtilization 数据，而
 * 非整体卡性能来判断拦截库的效果，原因与控制器采用该方式处理的原因相同：整体卡性能无法区分一个容器
 * 所占比例与相邻容器的比例，因此整体卡性能无法判断限制是否生效。该数据会显示在启动次数旁边，因此由
 * 限制导致的低利用率与因未启动任何任务导致的低利用率可以区分开来。
 *
 * 这是此处唯一的文件，采用供应商设备方言编写，并使用 hgcc 构建，因为内核是核心：用纯 C 语言无法占
 * 用 PPU。它像其他测试代码一样自由链接 SDK —— 只会在 gpustack/thead-ppu-devel 内部运行 —— 且从未被
 * 发布。
 *
 * 每个事实对应一条 LOAD <key>=<value> 行，因此系统会解析这些内容而非仅抓取文本。无论运行结果如何，
 * 退出状态均为 0，包括当结果为负面时：结果信息就体现在输出中。 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* SDK 头文件并不包含其自身声明所需的内容。 */
#include <stdbool.h>
#include <stddef.h>

#include <hgml.h>

#define LOAD_MAX_SAMPLES 128u
#define LOAD_BLOCKS 32
#define LOAD_THREADS 64

/* 足够长，以至于一次启动会占据卡在控制窗口中的一段可测量时间，因此当窗口关闭时，卡实际上会处于空闲
 * 状态，而不仅仅是重新排序启动过程。 */
#define LOAD_ITERS 2000000ULL

/* spin — 存在数据依赖关系，因此编译器无法将循环展开，该进程在运行期间确实处于忙碌状态。 */
__global__ void spin(unsigned long long iters, int seed, int *out)
{
    int acc = seed;

    for (unsigned long long i = 0; i < iters; i++) {
        acc = acc * 31 + (int)(i & 7);
    }
    if (out != NULL) {
        *out = acc;
    }
}

/* now_seconds — 单调时钟，若无法读取则使用 NEGATIVE 哨兵。
 *
 * 不是零而是负数，因为整个运行过程仅受墙钟时间的限制：零只是一个普通的数值，将失败的情况纳入 now -
 * started < seconds 中会导致循环继续而非结束。调用者会测试其符号。 */
static double now_seconds(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1.0;
    }
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* sample_self — 这是驱动程序为该进程保存的最新使用样本。
 *
 * 与历史总和不同，这是基于拦截库自身的采样器所依据的原因：查询是针对所有历史数据进行的，因此某个进
 * 程会反复出现，将其相加则会显示出其使用量的倍数。 */
static bool sample_self(hgmlDevice_t device, unsigned int *sm_util)
{
    hgmlProcessUtilizationSample_t samples[LOAD_MAX_SAMPLES];
    unsigned int count = LOAD_MAX_SAMPLES;

    if (hgmlDeviceGetProcessUtilization(device, samples, &count, 0ULL) != HGML_SUCCESS) {
        return false;
    }
    if (count > LOAD_MAX_SAMPLES) {
        count = LOAD_MAX_SAMPLES;
    }

    bool found = false;
    unsigned long long newest = 0ULL;
    for (unsigned int i = 0; i < count; i++) {
        if ((int)samples[i].pid != (int)getpid()) {
            continue;
        }
        if (!found || samples[i].timeStamp >= newest) {
            newest = samples[i].timeStamp;
            *sm_util = samples[i].smUtil;
            found = true;
        }
    }
    return found;
}

int main(int argc, char **argv)
{
    unsigned int index = (argc > 1) ? (unsigned int)strtoul(argv[1], NULL, 10) : 0u;
    double seconds = (argc > 2) ? strtod(argv[2], NULL) : 10.0;

    printf("LOAD device=%u seconds=%.1f pid=%d\n", index, seconds, (int)getpid());
    fflush(stdout);

    int *out = NULL;
    hggcError_t crc = hggcSetDevice((int)index);
    printf("LOAD set_device rc=%d\n", (int)crc);
    if (crc != hggcSuccess) {
        printf("LOAD result=failed reason=set_device\n");
        return 0;
    }
    crc = hggcMalloc((void **)&out, sizeof(int));
    printf("LOAD malloc rc=%d\n", (int)crc);
    if (crc != hggcSuccess) {
        printf("LOAD result=failed reason=malloc\n");
        return 0;
    }

    /* HGML 的处理与负载处理是分开进行的：无法读取使用情况不应被误认为是无法生成该数据。 */
    hgmlDevice_t device;
    bool measurable = (hgmlInit_v2() == HGML_SUCCESS)
                      && (hgmlDeviceGetHandleByIndex_v2(index, &device) == HGML_SUCCESS);
    printf("LOAD measurable=%s\n", measurable ? "yes" : "no");

    unsigned long long launches = 0ULL;
    unsigned long long failed = 0ULL;
    unsigned int sm_util = 0u;
    unsigned int samples = 0u;
    unsigned long long sm_util_sum = 0ULL;
    double started = now_seconds();
    double last_sample = started;
    double now = started;

    /* 在首次启动之前，无法读取的时钟会被拒绝，而不是被限制运行：这次运行没有其他停止条件，否则它就会占
     * 用设备资源，直到该案例的超时机制将其终止，而被终止的运行过程根本不会输出任何结果。 */
    if (started < 0.0) {
        printf("LOAD result=failed reason=clock\n");
        hggcFree(out);
        return 0;
    }

    for (;;) {
        now = now_seconds();
        if (now < 0.0) {
            printf("LOAD result=failed reason=clock\n");
            hggcFree(out);
            return 0;
        }
        if (now - started >= seconds) {
            break;
        }

        /* 每次迭代进行一次启动，随后每隔一段时间进行一次同步，这样在拦截库保持窗口关闭时队列就不会无限增长。 */
        spin<<<LOAD_BLOCKS, LOAD_THREADS>>>(LOAD_ITERS, (int)launches, out);
        if (hggcGetLastError() != hggcSuccess) {
            failed++;
        }
        launches++;

        if (launches % 4ULL == 0ULL) {
            hggcDeviceSynchronize();
        }

        /* 在第一个秒后采样，之后每秒采样一次：第一个窗口是前馈模式，对其进行平均会反映循环的冷启动状态，而
         * 非最终稳定状态。 */
        if (measurable && now - started > 1.0 && now - last_sample >= 1.0) {
            last_sample = now;
            if (sample_self(device, &sm_util)) {
                sm_util_sum += sm_util;
                samples++;
            }
        }
    }
    hggcDeviceSynchronize();

    /* 按照之前的测量方式进行，但最终会回到结束循环时的读数，因此在这个最后的调用中出现问题的时钟无法将
     * 完成的运行时间变成负数。 */
    double finished = now_seconds();
    double elapsed = ((finished < 0.0) ? now : finished) - started;
    unsigned int mean = (samples > 0u) ? (unsigned int)(sm_util_sum / samples) : 0u;

    printf("LOAD launches=%llu failed=%llu elapsed_ms=%.0f rate_per_s=%.0f\n", launches, failed,
           elapsed * 1000.0, (elapsed > 0.0) ? (double)launches / elapsed : 0.0);
    printf("LOAD sm_util_mean=%u samples=%u last=%u\n", mean, samples, sm_util);
    printf("LOAD result=%s\n", (launches > 0ULL && failed == 0ULL) ? "success" : "failed");

    hggcFree(out);
    if (measurable) {
        hgmlShutdown();
    }
    return 0;
}

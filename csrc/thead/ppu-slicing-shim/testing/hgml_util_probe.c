/* hgml_util_probe.c — Gate 3：是否可以将 hgmlDeviceGetProcessUtilization 用作计算控制器的反馈输入，
 * 以及它会报告什么 PID？
 *
 * 这是设计输入，而非拦截库。计算配额是一个 PID 循环，其决定因素是该循环是否可以根据进程使用率（本
 * 容器自身的份额）进行分配，还是必须回退到总卡量。总卡量会将每个容器的控制器绑定到同一张卡上，并随
 * 之波动，因此答案决定了设计方案。
 *
 * 与这里的拦截库不同，这是一个仅能在 SDK 容器内部运行的测试二进制文件，因此它直接调用 libhgml 和
 * libhggc，而不进行任何中间处理。此外，这还能让编译器检查每一个签名，让链接器正确解析头文件的
 * _v2/_v4 宏映射关系，而不是依赖手工编写的 dlsym 表来猜测 ABI 名称。
 *
 * 它解答了两个问题，并拒绝将两者混为一谈：
 * 1. 运行时支持 — 调用返回 HGML_SUCCESS 且至少有一个样本。如果成功但没有样本，则被报告为 empty，绝
 * 不会被视为支持：这就是错误处理在这种情况下会呈现的确切形式。
 * 2. 哪个 PID 命名空间——通过将报告中的 pid 与 getpid() 进行匹配来确定。该操作在具有自身 PID 命名空
 * 间的容器中执行，因此拦截库的自身 pid 较小，而主机 pid 则不是；还会打印出原始的 NSpid 行，以便重
 * 新检查比较结果，而非直接依赖该结果。
 *
 * 有东西可供采样就会产生自身的控制负载：一个上下文、一个设备缓冲区，以及样本之间的主机到设备的复制
 * 操作。如果驱动程序仅负责计算工作，那么这些复制操作将不会产生任何样本，问题1的答案为empty——这是一
 * 个发现结果，而hgmlDeviceGetComputeRunningProcesses仍然能解答问题2。
 *
 * 每个事实对应一条 PROBE <key>=<value> 行，因此 cases/thead-case-4.sh 会直接解析这些内容，而非仅抓
 * 取文本。当探测操作完全完成时，退出状态为 0，包括当结果为负的情况——结果信息就体现在输出中。 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* SDK 头文件并不包含其自身声明所需的内容。 */
#include <stdbool.h>
#include <stddef.h>

#include <hggc.h>
#include <hgml.h>

#define PROBE_MAX_SAMPLES 64u
#define PROBE_MAX_PROCS 64u
#define PROBE_LOAD_BYTES (32u * 1024u * 1024u)
#define PROBE_COPIES_PER_ROUND 32

static void print_nspid(void)
{
    FILE *status = fopen("/proc/self/status", "re");
    if (status == NULL) {
        printf("PROBE self_nspid=unavailable\n");
        return;
    }

    char line[256];
    while (fgets(line, sizeof(line), status) != NULL) {
        if (strncmp(line, "NSpid:", 6) == 0) {
            line[strcspn(line, "\n")] = '\0';
            printf("PROBE self_nspid=%s\n", line + 7);
            fclose(status);
            return;
        }
    }
    fclose(status);
    printf("PROBE self_nspid=absent\n");
}

/* start_load — 上下文 + 设备缓冲区，因此进程会进行真正的分配，并能产生复制流量。在首次失败时返回
 * false，并打印原因。 */
static bool start_load(int index, HGcontext *ctx, HGdeviceptr *dptr)
{
    HGresult rc = hgInit(0);
    printf("PROBE driver_init rc=%d\n", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    HGdevice dev = 0;
    rc = hgDeviceGet(&dev, index);
    printf("PROBE driver_device_get rc=%d index=%d\n", (int)rc, index);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    rc = hgCtxCreate(ctx, NULL, 0, dev);
    printf("PROBE driver_ctx_create rc=%d\n", (int)rc);
    if (rc != HGGC_SUCCESS) {
        return false;
    }

    rc = hgMemAlloc(dptr, PROBE_LOAD_BYTES);
    printf("PROBE driver_mem_alloc rc=%d bytes=%u\n", (int)rc, PROBE_LOAD_BYTES);
    if (rc != HGGC_SUCCESS) {
        /* 此处会销毁该上下文，而不是留待 main()的清理操作，后者仅在负载真正开始时才会执行：一个孤立的上下
         * 文会保存卡状态，此后续探测过程会针对该状态进行测量。 */
        hgCtxDestroy(*ctx);
        *ctx = NULL;
        return false;
    }
    return true;
}

static void run_copies(HGdeviceptr dptr, const void *host)
{
    for (int i = 0; i < PROBE_COPIES_PER_ROUND; i++) {
        hgMemcpyHtoD(dptr, host, PROBE_LOAD_BYTES);
    }
    hgCtxSynchronize();
}

int main(int argc, char **argv)
{
    unsigned int index = (argc > 1) ? (unsigned int)strtoul(argv[1], NULL, 10) : 0u;
    int rounds = (argc > 2) ? atoi(argv[2]) : 3;

    printf("PROBE self_pid=%d\n", (int)getpid());
    print_nspid();
    printf("PROBE device_index=%u rounds=%d\n", index, rounds);

    hgmlReturn_t hrc = hgmlInit_v2();
    printf("PROBE hgml_init rc=%d msg=%s\n", (int)hrc, hgmlErrorString(hrc));
    if (hrc != HGML_SUCCESS) {
        printf("PROBE VERDICT util=unavailable\nPROBE VERDICT pidns=unknown\n");
        return 0;
    }

    unsigned int device_count = 0;
    hrc = hgmlDeviceGetCount_v2(&device_count);
    printf("PROBE device_count rc=%d n=%u\n", (int)hrc, device_count);

    hgmlDevice_t device;
    hrc = hgmlDeviceGetHandleByIndex_v2(index, &device);
    printf("PROBE device_handle rc=%d msg=%s\n", (int)hrc, hgmlErrorString(hrc));
    if (hrc != HGML_SUCCESS) {
        printf("PROBE VERDICT util=unavailable\nPROBE VERDICT pidns=unknown\n");
        hgmlShutdown();
        return 0;
    }

    void *host = malloc(PROBE_LOAD_BYTES);
    HGcontext ctx = NULL;
    HGdeviceptr dptr = 0;
    bool loaded = (host != NULL) && start_load((int)index, &ctx, &dptr);
    printf("PROBE load_started=%s\n", loaded ? "yes" : "no");

    /* 一个样本的 PID 与我们的匹配，就是整个 PID 命名空间问题的答案，因此要分别追踪两部分的状况：是否有
     * 任何样本到达，以及是否有属于我们的样本。 */
    unsigned int total_samples = 0;
    bool saw_self = false;
    bool saw_other = false;
    /* 与 saw_self 分离，该状态也会被进程列表记录：下面的查询设置 lastSeenTimeStamp=0，因此会返回所有历
     * 史数据；因此，相邻容器中的过时样本就足以使 total_samples 不为零，即使当前进程从未出现过。设计目
     * 标在于判断循环是否能读取其自身的使用情况，因此只有携带 pid 的样本能够满足这一要求。 */
    bool util_saw_self = false;
    hgmlReturn_t util_rc = HGML_ERROR_UNKNOWN;

    for (int round = 0; round < rounds; round++) {
        if (loaded) {
            run_copies(dptr, host);
        }

        hgmlProcessUtilizationSample_t samples[PROBE_MAX_SAMPLES];
        unsigned int count = PROBE_MAX_SAMPLES;
        util_rc = hgmlDeviceGetProcessUtilization(device, samples, &count, 0ULL);
        printf("PROBE util_call round=%d rc=%d count=%u msg=%s\n",
               round, (int)util_rc, count, hgmlErrorString(util_rc));
        if (util_rc != HGML_SUCCESS) {
            continue;
        }

        if (count > PROBE_MAX_SAMPLES) {
            count = PROBE_MAX_SAMPLES;
        }
        for (unsigned int i = 0; i < count; i++) {
            printf("PROBE sample round=%d pid=%u smUtil=%u memUtil=%u timeStamp=%llu\n",
                   round, samples[i].pid, samples[i].smUtil, samples[i].memUtil,
                   samples[i].timeStamp);
            if ((int)samples[i].pid == (int)getpid()) {
                saw_self = true;
                util_saw_self = true;
            } else {
                saw_other = true;
            }
            total_samples++;
        }
    }

    /* 进程列表能够回答 PID-命名空间相关的问题，即使使用率统计报告中没有任何信息，因为仅仅持有上下文就
     * 足以被识别。 */
    hgmlProcessInfo_t procs[PROBE_MAX_PROCS];
    unsigned int proc_count = PROBE_MAX_PROCS;
    hgmlReturn_t proc_rc = hgmlDeviceGetComputeRunningProcesses_v3(device, &proc_count, procs);
    printf("PROBE proc_call rc=%d count=%u msg=%s\n",
           (int)proc_rc, proc_count, hgmlErrorString(proc_rc));
    if (proc_rc == HGML_SUCCESS) {
        if (proc_count > PROBE_MAX_PROCS) {
            proc_count = PROBE_MAX_PROCS;
        }
        for (unsigned int i = 0; i < proc_count; i++) {
            printf("PROBE proc pid=%u usedGpuMemory=%llu\n", procs[i].pid, procs[i].usedGpuMemory);
            if ((int)procs[i].pid == (int)getpid()) {
                saw_self = true;
            } else {
                saw_other = true;
            }
        }
    }

    if (util_rc != HGML_SUCCESS) {
        printf("PROBE VERDICT util=unsupported\n");
    } else if (total_samples == 0) {
        printf("PROBE VERDICT util=empty\n");
    } else if (!util_saw_self) {
        printf("PROBE VERDICT util=others-only samples=%u\n", total_samples);
    } else {
        printf("PROBE VERDICT util=supported samples=%u\n", total_samples);
    }

    if (saw_self) {
        printf("PROBE VERDICT pidns=container\n");
    } else if (saw_other) {
        printf("PROBE VERDICT pidns=host\n");
    } else {
        printf("PROBE VERDICT pidns=unknown\n");
    }

    if (loaded) {
        hgMemFree(dptr);
        hgCtxDestroy(ctx);
    }
    free(host);
    hgmlShutdown();
    return 0;
}

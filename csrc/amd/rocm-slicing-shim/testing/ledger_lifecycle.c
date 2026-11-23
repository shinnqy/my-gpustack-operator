/* ledger_lifecycle.c — 进行记账，死亡而不释放，并证明下一个进程可以将其取回。
 *
 * 为什么这不是一个单元测试？common/vrocm_test.c已经涵盖了扫描操作，该测试来自一个被测试程序自己杀
 * 死的子进程。而fork操作无法复现的情况是：扫描操作确实存在一种情况——即某个记录残留在区域文件中，该
 * 文件会持续存在，不受任何知晓此情况的进程影响，随后某个进程启动后不会记得这一情况。这需要两个真实
 * 进程以及外部的信号，因此需要一个测试脚本，也需要一个能够运行该测试的程序。
 *
 * 它所保护的失败并非假设性的。在扫描之前就存在的情况：一个进程被 SIGKILL 终止，同时持有 4 GiB 的 6
 * GiB 配额，下一个进程只能获得 2 GiB，且这一缺口会一直存在，直到区域文件结束——对于 Pod 来说，直到
 * 它被删除。
 *
 * 它直接驱动账本，且不链接 ROCm。问题出在 common/ 的 reclaim 机制上，而非拦截器的问题：通过
 * hipMalloc 进行分配会导致运行时、预加载和配额变量在测试与被测试对象之间产生冲突，该链条中任何地方
 * 的失败都会被判定为 reclaim 失败。用虚假分配来驱动账本可以将其隔离——这意味着该程序可以在没有 GPU
 * 的主机上运行，这正是可以廉价运行该程序的场景。
 *
 * 它需要在环境中具备 VROCM_LEDGER_PATH 和 VROCM_DEVICE_MEMORY_LIMIT 参数，这与拦截库的要求一致；如
 * 果没有这些参数，配置将被拒绝。 */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common/vrocm_ledger.h"
#include "common/vrocm_quota.h"

#define MIB (1024ULL * 1024ULL)

/* fake_alloc — 代表运行时，并成功执行。
 *
 * 在过程中，每个记账必须使用唯一的密钥，而一个计数器就足够了：账本只进行密钥的相等性比较。使用任何
 * 真实地址会更糟糕，因为免费后两次记账可能会指向同一个地址。 */
static bool fake_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    unsigned long long *next = (unsigned long long *)ctx;

    (void)bytes;
    *key = ++(*next);
    return true;
}

static const char *admit_name(enum vrocm_admit rc)
{
    switch (rc) {
    case VROCM_ADMIT_OK:
        return "ok";
    case VROCM_ADMIT_DENIED_QUOTA:
        return "denied-quota";
    case VROCM_ADMIT_DENIED_CONFIG:
        return "denied-config";
    case VROCM_ADMIT_DENIED_TRACKING:
        return "denied-tracking";
    case VROCM_ADMIT_ALLOC_FAILED:
        return "alloc-failed";
    }
    return "unknown";
}

static void report(int device)
{
    printf("LEDGER device=%d quota_mib=%llu used_mib=%llu pid=%d\n", device,
           vrocm_ledger_quota(device) / MIB, vrocm_ledger_used(device) / MIB, (int)getpid());
    fflush(stdout);
}

/* do_admit — 一次记账，无论被接受还是被拒绝，报告方式均相同。 */
static enum vrocm_admit do_admit(int device, unsigned long long mib, unsigned long long *keys)
{
    enum vrocm_admit rc = vrocm_ledger_admit(device, mib * MIB, fake_alloc, keys);

    printf("ADMIT device=%d mib=%llu result=%s\n", device, mib, admit_name(rc));
    return rc;
}

static void usage(void)
{
    fprintf(stderr,
            "usage: ledger_lifecycle <command> [args]\n"
            "  hold <device> <mib>      take the charge, report, then wait to be killed\n"
            "  acquire <device> <mib>   take the charge and release it before exiting\n"
            "  probe <device>           report the card's quota and accounted usage\n"
            "  reclaim <device> <mib>   admit at a size that forces the sweep, and report\n");
}

int main(int argc, char **argv)
{
    unsigned long long keys = 0;
    unsigned long long mib = 0;
    const char *command;
    int device = 0;

    if (argc < 3) {
        usage();
        return 2;
    }
    command = argv[1];
    device = (int)strtol(argv[2], NULL, 10);
    if (argc > 3) {
        mib = strtoull(argv[3], NULL, 10);
    }

    /* 该拦截库通过自身的构造函数实现此功能；直接链接 common/ 的程序没有构造函数，必须手动实现。跳过它
     * 并非简单的性能下降——可用性锁定状态仍然为开启，每一次配置请求都会被拒绝，这完全表明环境存在问题。 */
    vrocm_quota_validate();
    if (!vrocm_quota_usable()) {
        fprintf(stderr, "ledger_lifecycle: unusable configuration; set %s and %s\n",
                VROCM_ENV_LEDGER_PATH, VROCM_ENV_MEMORY_LIMIT);
        return 2;
    }

    if (strcmp(command, "probe") == 0) {
        report(device);
        return 0;
    }

    if (strcmp(command, "hold") == 0) {
        if (do_admit(device, mib, &keys) != VROCM_ADMIT_OK) {
            return 1;
        }
        report(device);
        /* 故意从未释放。该案例发送 SIGKILL 信号，这正是其目的：进程无法捕获或清理的信号会导致记账工作陷入
         * 僵局，而任何更温和的处理方式则会让 atexit 路径能够正常清理，从而无法证明任何事情。 */
        printf("HOLD ready pid=%d\n", (int)getpid());
        fflush(stdout);
        for (;;) {
            pause();
        }
    }

    if (strcmp(command, "acquire") == 0) {
        enum vrocm_admit rc = do_admit(device, mib, &keys);

        if (rc != VROCM_ADMIT_OK) {
            report(device);
            return 1;
        }
        report(device);
        if (!vrocm_ledger_release(keys, NULL, NULL)) {
            printf("RELEASE device=%d result=not-recorded\n", device);
            return 1;
        }
        printf("RELEASE device=%d result=ok\n", device);
        report(device);
        return 0;
    }

    if (strcmp(command, "reclaim") == 0) {
        /* 扫描操作在接纳机制内部进行，只有当记账操作无法以其他方式完成时才会执行——因此实现这一操作的方法就
         * 是要求一个与滞留的记账量相冲突的大小。如果拒绝这一要求，就意味着扫描操作没有发生或恢复程度不足。 */
        enum vrocm_admit rc = do_admit(device, mib, &keys);

        report(device);
        return rc == VROCM_ADMIT_OK ? 0 : 1;
    }

    usage();
    return 2;
}

/* vrocm_test.c — 针对 common/的单元测试，也是此代码库中唯一不需要硬件的测试。
 *
 * 它们存在是因为没有 hip* 或 hsa* 类型的相关名称：因此，配额计算、键映射和区域功能可以在没有 ROCm、
 * 没有 HIP 运行时以及没有设备的情况下实现，而这一点在其它方面则不成立。此树中的其他内容只能基于
 * AMD 主机进行评估。
 *
 * 由 build.sh unit 构建，由 amd-case-6.sh 运行。它会打印与每个案例脚本相同的三列表格，以 FAILS=<n>
 * 结尾，因此案例程序会直接使用它，而不进行重新解析。
 *
 * 所有涉及该区域的测试都会在 A FORKED CHILD 环境中运行，而父进程永远不会进行映射操作。这并非无谓的
 * 复杂化：每次进程都会独立记录映射行为，因此出现“账本损坏”的情况时，只需进行一次 open() 操作即可，
 * 而非每次分配都需执行一次。因此，只进行了一次映射的父进程无法再测试另一个区域——而且跨进程测试本质
 * 上需要独立的进程。 */
#define _GNU_SOURCE

#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "vrocm_ledger.h"
#include "vrocm_log.h"
#include "vrocm_quota.h"

#define MIB VROCM_MEMORY_LIMIT_UNIT
#define GIB (1024ULL * MIB)

static int fails;
static char tmpdir[] = "/tmp/vrocm-test-XXXXXX";

/* 在自己的语句中调用任何会写入输出参数的函数，切勿放在需要格式化该输出参数的 check_that() 条件内：
 * 调用函数的参数评估顺序未指定，因此可以在调用前根据值来格式化细节，从而生成一个看似正确的结果，但
 * 实际上并未存在该数字。 */
static void check_that(bool ok, const char *name, const char *fmt, ...)
{
    va_list ap;

    printf("%s | %s | ", ok ? "PASS" : "FAIL", name);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    if (!ok) {
        fails++;
    }
}

/* region_path — 每次测试使用一个新的账本路径，因此没有测试会受其他测试遗留问题的影响。 */
static const char *region_path(int which)
{
    static char path[256];

    snprintf(path, sizeof(path), "%s/ledger-%d", tmpdir, which);
    return path;
}

/* run_child — 在自己的进程中运行一个主体，针对自己的账本进行操作，并报告其选择的退出代码。0表示该
 * 主体完成任务；其他数值则属于其自身的诊断信息。 */
static int run_child(int (*body)(void), const char *path)
{
    pid_t pid = fork();
    int status = 0;

    if (pid == 0) {
        if (path != NULL) {
            setenv(VROCM_ENV_LEDGER_PATH, path, 1);
        }
        _exit(body());
    }
    if (pid < 0) {
        return -1;
    }
    if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
        return -1;
    }
    return WEXITSTATUS(status);
}

/* 这是运行时实际分配的替代实现。每次都会返回一个独特的密钥，并统计自身调用次数，这就是失败封闭测试
 * 证明从未到达运行时机制的原因。 */
static unsigned long long fake_next_key = 0x1000;
static int fake_calls;

static bool fake_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    (void)ctx;
    (void)bytes;
    fake_calls++;
    *key = fake_next_key++;
    return true;
}

static bool refusing_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    (void)ctx;
    (void)key;
    (void)bytes;
    fake_calls++;
    return false;
}

/* 一种等效的分配方式：其所需量 adalah 被允许量的两倍，就像运行时对宽度的向上取整过程一样。 */
static bool overrunning_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    (void)ctx;
    fake_calls++;
    *key = fake_next_key++;
    *bytes *= 2;
    return true;
}

/* asserts_lock_held — 一个分配回调函数，如果卡的锁未被持有，则会导致失败。这就是“单一锁获取”属性所
 * 涉及的拦截函数：在检查与记账之间释放了锁的构建过程将不会触发此回调。 */
static bool asserts_lock_held(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    int device = -1;

    (void)bytes;
    if (!vrocm_ledger_holding(&device) || device != *(int *)ctx) {
        return false;
    }
    *key = fake_next_key++;
    return true;
}

/* ---- 配额 ---------------------------------------------------------------------------- */

static void test_quota_parse(void)
{
    check_that(vrocm_quota_parse("4096", MIB) == 4096ULL * MIB, "quota_parse/plain",
               "4096 MiB -> %llu bytes", vrocm_quota_parse("4096", MIB));
    check_that(vrocm_quota_parse("0", MIB) == 0, "quota_parse/zero",
               "a quota of zero is not a quota");
    check_that(vrocm_quota_parse("-1", MIB) == 0, "quota_parse/negative",
               "a leading minus must not wrap into a huge positive figure");
    check_that(vrocm_quota_parse("4096MiB", MIB) == 0, "quota_parse/trailing",
               "a trailing unit is not silently ignored");
    check_that(vrocm_quota_parse("", MIB) == 0, "quota_parse/empty", "an empty value is unusable");
    check_that(vrocm_quota_parse(NULL, MIB) == 0, "quota_parse/unset", "an unset value is 0");
    check_that(vrocm_quota_parse("18446744073709551615", MIB) == 0, "quota_parse/overflow",
               "a figure that would wrap the scale is refused, not multiplied");
    /* strtoull会跳过开头的空白字符，然后读取一个符号，因此仅通过第一个字符的测试无法检测到这些字符——这
     * 就是为什么解析器需要在该位置使用DIGIT标记。正的情况才是会被识别的：“ 10”在之前曾被接受为10 MiB。 */
    check_that(vrocm_quota_parse(" 10", MIB) == 0, "quota_parse/leading_space",
               "leading whitespace is junk in a figure, the same as trailing junk is");
    check_that(vrocm_quota_parse(" -10", MIB) == 0, "quota_parse/space_then_minus",
               "whitespace hides a sign from a first-character test; the digit rule sees it");
    check_that(vrocm_quota_parse("+10", MIB) == 0, "quota_parse/leading_plus",
               "a sign is not a digit, whichever sign it is");
}

static void test_quota_precedence(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "1024", 1);
    setenv(VROCM_ENV_MEMORY_LIMIT_PREFIX "1", "2048", 1);

    check_that(vrocm_quota_memory_bytes(1) == 2048ULL * MIB, "quota_precedence/indexed_wins",
               "card 1 takes its own 2048 MiB over the container's 1024");
    check_that(vrocm_quota_memory_bytes(0) == 1024ULL * MIB, "quota_precedence/unindexed_covers",
               "card 0 carries no figure of its own and takes the container's");

    /* 必须拒绝那张无法使用的卡，绝不要让它掉到上面那个层级：这种掉落现象就是因输入错误导致的每卡金额，
     * 悄悄用于购买其他人的津贴。 */
    setenv(VROCM_ENV_MEMORY_LIMIT_PREFIX "2", "not-a-number", 1);
    check_that(vrocm_quota_memory_bytes(2) == 0, "quota_precedence/malformed_does_not_fall_through",
               "a malformed indexed figure denies its card rather than inheriting 1024 MiB");

    check_that(vrocm_quota_memory_bytes(-1) == 0 &&
                   vrocm_quota_memory_bytes(VROCM_MAX_DEVICES) == 0,
               "quota_precedence/bounds", "an out-of-range index answers 0 rather than reading on");

    unsetenv(VROCM_ENV_MEMORY_LIMIT_PREFIX "1");
    unsetenv(VROCM_ENV_MEMORY_LIMIT_PREFIX "2");
    unsetenv(VROCM_ENV_MEMORY_LIMIT);
}

static void test_quota_usable_latch(void)
{
    setenv(VROCM_ENV_LEDGER_PATH, region_path(90), 1);
    setenv(VROCM_ENV_MEMORY_LIMIT, "1024", 1);
    vrocm_quota_validate();
    check_that(vrocm_quota_usable(), "quota_latch/unindexed_only_is_complete",
               "the un-indexed figure alone is a complete configuration");

    unsetenv(VROCM_ENV_MEMORY_LIMIT);
    vrocm_quota_validate();
    check_that(!vrocm_quota_usable(), "quota_latch/no_figure_is_unusable",
               "a container with no figure in either form refuses everything");

    /* 路径选择的是“回退”而非“拒绝”，因此手动运行的程序只需要一个变量，而不是两个。图形才是决定可使用的
     * 关键——区域是答案被书写的地方，而不是让某件事成为可能的因素。 */
    setenv(VROCM_ENV_MEMORY_LIMIT, "1024", 1);
    unsetenv(VROCM_ENV_LEDGER_PATH);
    vrocm_quota_validate();
    check_that(vrocm_quota_usable(), "quota_latch/ledger_path_falls_back",
               "%s unset falls back to %s rather than refusing", VROCM_ENV_LEDGER_PATH,
               VROCM_LEDGER_DEFAULT_PATH);
    check_that(strcmp(vrocm_quota_ledger_path(), VROCM_LEDGER_DEFAULT_PATH) == 0,
               "quota_latch/ledger_path_default", "%s", vrocm_quota_ledger_path());
    /* 当替换中没有内容可填入时，变量就呈现为空的状态，它会回退而不是被打开为无字符的路径。 */
    setenv(VROCM_ENV_LEDGER_PATH, "", 1);
    check_that(strcmp(vrocm_quota_ledger_path(), VROCM_LEDGER_DEFAULT_PATH) == 0,
               "quota_latch/ledger_path_empty_falls_back", "%s", vrocm_quota_ledger_path());
    unsetenv(VROCM_ENV_LEDGER_PATH);

    unsetenv(VROCM_ENV_MEMORY_LIMIT);
}

/* 日志 */

/* 该级别在首次使用时被锁定，这符合产品的设计需求——它位于每个分配的拒绝路径上，也是导致分裂测试出现
 * 问题的陷阱。任何高于此级别的测试，只要记录任何内容，就会在 PARENT 中锁定该级别；而分裂出的子进程
 * 会继承被锁定的值，而不是读取自己的环境；之后每个级别都会以默认设置的方式运行。因此这里的子进程是
 * 此二进制文件的全新 EXEC 实例，而非其分裂结果，这使得结果与之前运行的程序无关。两种实现方式都已被
 * 尝试过，只有分裂版本在首先运行时通过了测试。 */
static const char *self;

static int exec_log_level(const char *value)
{
    pid_t pid = fork();
    int status = 0;

    if (pid == 0) {
        if (value != NULL) {
            setenv(VROCM_ENV_LOG_LEVEL, value, 1);
        } else {
            unsetenv(VROCM_ENV_LOG_LEVEL);
        }
        execl(self, self, "--log-level", (char *)NULL);
        _exit(127);
    }
    if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
        return -1;
    }
    return WEXITSTATUS(status);
}

static void test_log_levels(void)
{
    check_that(exec_log_level("0") == VROCM_LOG_QUIET, "log/quiet", "0 is silent");
    check_that(exec_log_level("2") == VROCM_LOG_DEBUG, "log/debug",
               "2 carries the load marker and the counter dump the cases grep");
    check_that(exec_log_level("9") == VROCM_LOG_DENY, "log/out_of_range",
               "an out-of-range level falls back to the default rather than refusing anything");
    check_that(exec_log_level(NULL) == VROCM_LOG_DENY, "log/default",
               "the default is 1: one line per denial, which is the question a user asks");
}

/* 共享区域 */

static int child_creates_region(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    return vrocm_ledger_admit(0, MIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 1;
}

static void test_region_layout(void)
{
    const char *path = region_path(1);
    struct vrocm_region header;
    int fd, got;

    check_that(run_child(child_creates_region, path) == 0, "region/created",
               "the first admission creates and initialises the region");

    fd = open(path, O_RDONLY);
    got = (fd >= 0 && read(fd, &header, sizeof(header)) == (ssize_t)sizeof(header)) ? 0 : -1;
    if (fd >= 0) {
        close(fd);
    }
    check_that(got == 0 && memcmp(header.magic, VROCM_REGION_MAGIC, VROCM_REGION_MAGIC_BYTES) == 0,
               "region/magic", "a reader identifies the file by its magic alone");
    check_that(got == 0 && header.layout_version == VROCM_REGION_VERSION &&
                   header.header_bytes == offsetof(struct vrocm_region, devices) &&
                   header.device_slots == VROCM_MAX_DEVICES &&
                   header.process_slots == VROCM_MAX_PROCESSES_PER_DEVICE,
               "region/header", "version %u, devices at +%u, %u cards, %u processes",
               (unsigned)header.layout_version, (unsigned)header.header_bytes,
               (unsigned)header.device_slots, (unsigned)header.process_slots);
    check_that(offsetof(struct vrocm_region, lock_arena) == 32, "region/lock_arena_frozen",
               "two builds must lock the same byte for the same card, so this offset may not move");
}

static int child_foreign_region(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    /* 拒绝是必经之路：这条路径由操作器提供，因此如果出现错误，不能覆盖到其他人的账本。 */
    return vrocm_ledger_admit(0, MIB, fake_alloc, NULL) == VROCM_ADMIT_DENIED_CONFIG ? 0 : 1;
}

static void test_region_foreign(void)
{
    const char *path = region_path(2);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);

    if (fd >= 0) {
        (void)!write(fd, "NOTALEDGERnotaledger", 20);
        close(fd);
    }
    check_that(run_child(child_foreign_region, path) == 0, "region/foreign_refused",
               "a file that is not a ledger is refused rather than overwritten");
}

static int child_wrong_version(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    return vrocm_ledger_admit(0, MIB, fake_alloc, NULL) == VROCM_ADMIT_DENIED_CONFIG ? 0 : 1;
}

static void test_region_version(void)
{
    const char *path = region_path(3);
    struct vrocm_region header;
    int fd;

    memset(&header, 0, sizeof(header));
    memcpy(header.magic, VROCM_REGION_MAGIC, VROCM_REGION_MAGIC_BYTES);
    header.layout_version = VROCM_REGION_VERSION + 1;

    fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd >= 0) {
        (void)!write(fd, &header, sizeof(header));
        close(fd);
    }
    check_that(run_child(child_wrong_version, path) == 0, "region/version_refused",
               "a layout version this build does not speak is refused, not misparsed");
}

/* ---- 四种行为属性 ---------------------------------------------------- */

/* 1. 配额在附加时会被重新读取，绝不会被创建该区域的进程冻结。 */
static int child_creates_at_4gib(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    return vrocm_ledger_admit(0, 3ULL * GIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 1;
}

static int child_attaches_at_2gib(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "2048", 1);
    vrocm_quota_validate();
    /* 该区域是由一个 4 GiB 的进程创建的，现在已消失。如果一次构建操作导致创建者的形象受损，那就可以接
     * 受；清理过程会消除那些无效的记账，然后 2 GiB 的设定就会决定最终结果，因此 3 GiB 的分配必须被拒绝。 */
    if (vrocm_ledger_admit(0, 3ULL * GIB, fake_alloc, NULL) != VROCM_ADMIT_DENIED_QUOTA) {
        return 1;
    }
    return vrocm_ledger_admit(0, 2ULL * GIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 2;
}

static void test_quota_reread_on_attach(void)
{
    const char *path = region_path(4);

    check_that(run_child(child_creates_at_4gib, path) == 0, "ledger/creator_admitted",
               "a 4 GiB container takes 3 GiB");
    check_that(run_child(child_attaches_at_2gib, path) == 0,
               "ledger/quota_reread_on_attach",
               "a later 2 GiB container is decided against 2 GiB, not the creator's 4");
}

/* 2. 检查、分配和记账操作在卡锁定的同一时刻完成。 */
static int child_one_lock_epoch(void)
{
    unsigned long long before, after;
    int device = 0;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();

    /* 第一次分配也映射了该区域，因此要测量第二次分配。 */
    if (vrocm_ledger_admit(0, MIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    before = vrocm_ledger_lock_epochs();
    if (vrocm_ledger_admit(0, MIB, asserts_lock_held, &device) != VROCM_ADMIT_OK) {
        return 2; /* 拦截函数在锁释放后执行 */
    }
    after = vrocm_ledger_lock_epochs();
    return (after - before) == 1 ? 0 : 3;
}

static void test_one_lock_acquisition(void)
{
    int rc = run_child(child_one_lock_epoch, region_path(5));

    check_that(rc == 0, "ledger/check_allocate_charge_under_one_lock",
               "%s", rc == 2 ? "the allocation ran with the card unlocked"
                             : rc == 3 ? "the admission took the lock more than once"
                                       : "one acquisition covers check, allocate and charge");
}

/* 3. 无法记录的记账会被拒绝，且永远无法达到运行时状态。 */
static int child_tracking_fail_closed(void)
{
    int i;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4194304", 1); /* 4 TiB：配额不得是拒绝项 */
    vrocm_quota_validate();

    for (i = 0; i < 200000; i++) {
        enum vrocm_admit rc = vrocm_ledger_admit(0, 1, fake_alloc, NULL);

        if (rc == VROCM_ADMIT_DENIED_TRACKING) {
            int calls_before = fake_calls;

            /* 拒绝操作必须没有到达运行时阶段，且必须确保不会到达该阶段。 */
            if (vrocm_ledger_admit(0, 1, fake_alloc, NULL) != VROCM_ADMIT_DENIED_TRACKING) {
                return 2;
            }
            if (fake_calls != calls_before) {
                return 3;
            }
            return vrocm_ledger_tracking_refusals() >= 2 ? 0 : 4;
        }
        if (rc != VROCM_ADMIT_OK) {
            return 1;
        }
    }
    return 5; /* 账本上从未有记录，因此没有任何证据可证明 */
}

static void test_tracking_fail_closed(void)
{
    int rc = run_child(child_tracking_fail_closed, region_path(6));

    check_that(rc == 0, "ledger/tracking_insert_is_fail_closed",
               "%s", rc == 3 ? "the runtime was called for a charge that could not be recorded"
                             : rc == 5 ? "the key map never filled; the property was not exercised"
                                       : "a charge with nowhere to be recorded is refused");
}

/* 4. 已死亡的进程的记账会被清除，并重新计算卡的总配额。 */
static int child_dies_holding(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    if (vrocm_ledger_admit(0, 3ULL * GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    /* 不进行退款处理，就像 SIGKILL 工作负载一样。 */
    _exit(0);
}

static int child_after_death(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    /* 没有进行清理的构建会发现仍有 3 GiB 的记账项，因此会拒绝此操作。 */
    return vrocm_ledger_admit(0, 4ULL * GIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 1;
}

static void test_dead_charge_swept(void)
{
    const char *path = region_path(7);

    check_that(run_child(child_dies_holding, path) == 0, "ledger/charge_survives_its_process",
               "a process takes 3 GiB of a 4 GiB card and leaves without refunding");
    check_that(run_child(child_after_death, path) == 0, "ledger/dead_charge_swept",
               "the next container reclaims it and takes the whole 4 GiB");
}

/* ---- 会计和锁定 ------------------------------------------------------------- */

static int child_charge_refund(void)
{
    unsigned long long key = 0;
    int device = -1;
    unsigned long long bytes = 0;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();

    key = fake_next_key;
    if (vrocm_ledger_admit(0, GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    if (vrocm_ledger_used(0) != GIB) {
        return 2;
    }
    if (!vrocm_ledger_release(key, &device, &bytes) || device != 0 || bytes != GIB) {
        return 3;
    }
    if (vrocm_ledger_used(0) != 0) {
        return 4;
    }
    /* 同一密钥的第二次释放“无需退款”，绝不会进行第二次退款：双重释放绝不能让容器拥有无限制的配额。 */
    if (vrocm_ledger_release(key, NULL, NULL)) {
        return 5;
    }
    return vrocm_ledger_used(0) == 0 ? 0 : 6;
}

static void test_charge_refund(void)
{
    int rc = run_child(child_charge_refund, region_path(8));

    check_that(rc == 0, "ledger/charge_refund_roundtrip",
               "%s", rc == 5 ? "a double free was refunded twice" : "a charge round-trips exactly once");
}

static int child_alloc_refused(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    if (vrocm_ledger_admit(0, GIB, refusing_alloc, NULL) != VROCM_ADMIT_ALLOC_FAILED) {
        return 1;
    }
    /* 没有进行任何记账，因此卡保持原状，预留的槽位也恢复了。 */
    return vrocm_ledger_used(0) == 0 ? 0 : 2;
}

static void test_alloc_refused_charges_nothing(void)
{
    check_that(run_child(child_alloc_refused, region_path(9)) == 0,
               "ledger/runtime_refusal_charges_nothing",
               "an allocation the runtime refused leaves the card's total unchanged");
}

static int child_quota_denies(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "1024", 1);
    vrocm_quota_validate();
    if (vrocm_ledger_admit(0, 512ULL * MIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    if (vrocm_ledger_admit(0, 1024ULL * MIB, fake_alloc, NULL) != VROCM_ADMIT_DENIED_QUOTA) {
        return 2;
    }
    /* 边界处的精度很重要：剩余的 512 MiB 是允许的，完全符合要求。 */
    return vrocm_ledger_admit(0, 512ULL * MIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 3;
}

static void test_quota_boundary(void)
{
    check_that(run_child(child_quota_denies, region_path(10)) == 0, "ledger/quota_boundary",
               "the card admits exactly up to its figure and refuses the byte past it");
}

static int child_quota_wraps(void)
{
    setenv(VROCM_ENV_MEMORY_LIMIT, "1024", 1);
    vrocm_quota_validate();
    if (vrocm_ledger_admit(0, 512ULL * MIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    /* 那个使得 used + bytes 结果重新归零的尺寸。按照那个总和来看，该卡被判定为空，这一点被承认了；但作
     * 为减法运算来看，它却因自身性质而被拒绝。这个尺寸是荒谬的，对其进行的任何分配都无法成功——但到达运
     * 行环境的请求，就是这个库认为能够处理的请求，而这一决定正是需要验证的内容。 */
    if (vrocm_ledger_admit(0, ~0ULL - (512ULL * MIB) + 1ULL, fake_alloc, NULL) !=
        VROCM_ADMIT_DENIED_QUOTA) {
        return 2;
    }
    /* 并且这种拒绝使得卡保持原样。 */
    return vrocm_ledger_admit(0, 512ULL * MIB, fake_alloc, NULL) == VROCM_ADMIT_OK ? 0 : 3;
}

static void test_quota_wraps(void)
{
    check_that(run_child(child_quota_wraps, region_path(17)) == 0, "ledger/quota_wraps",
               "a request that wraps the running total is refused, not read as an empty card");
}

static int child_reentrant_lock(void)
{
    int device = -1;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    if (vrocm_ledger_admit(0, MIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    /* 在入口处之外，没有任何东西被保留；任何随意的解锁或泄露的深度信息都会在这里显现出来。 */
    if (vrocm_ledger_holding(&device)) {
        return 2;
    }
    return 0;
}

static void test_lock_released(void)
{
    check_that(run_child(child_reentrant_lock, region_path(11)) == 0, "ledger/lock_balanced",
               "an admission leaves nothing held, so the depth counter cannot leak");
}

static int child_keymap_is_local(void)
{
    unsigned long long key;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    key = fake_next_key;
    if (vrocm_ledger_admit(0, GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    /* 兄弟节点对同一卡进行了相同的键值记账。如果映射关系被共享，两个释放操作中的其中一个会退还另一个的
     * 分配配额。 */
    if (!vrocm_ledger_release(key, NULL, NULL)) {
        return 2;
    }
    return vrocm_ledger_release(key, NULL, NULL) ? 3 : 0;
}

/* 分配比例是根据调用者的宽度来确定的，并在运行时步长上得到落实，因此记账必须遵循步长——无论是在同一
 * 锁下，还是在其他进程中，对已经过时的数据总量进行操作都是不允许的。 */
static int child_overrun(void)
{
    unsigned long long key;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    key = fake_next_key;
    if (vrocm_ledger_admit(0, GIB, overrunning_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }
    if (vrocm_ledger_used(0) != 2 * GIB) {
        return 2; /* 进行记账的是该请求，而非被取走的内容 */
    }
    /* 并且退款必须退还已收取的款项，而不是所要求的金额。 */
    if (!vrocm_ledger_release(key, NULL, NULL)) {
        return 3;
    }
    return vrocm_ledger_used(0) == 0 ? 0 : 4;
}

static void test_overrun_is_charged(void)
{
    int rc = run_child(child_overrun, region_path(13));

    check_that(rc == 0, "ledger/allocation_charged_for_what_it_took",
               "%s", rc == 2 ? "an over-running allocation was charged its request, not its size"
                             : rc == 4 ? "the refund gave back the request rather than the charge"
                                       : "a stride wider than the width is charged and refunded whole");
}

static void test_keymap_process_local(void)
{
    const char *path = region_path(12);

    check_that(run_child(child_keymap_is_local, path) == 0 &&
                   run_child(child_keymap_is_local, path) == 0,
               "ledger/keymap_is_process_local",
               "two processes holding the same key value refund only their own charge");
}

/* ---- 零尺寸分配 ---------------------------------------------------------------- */

/* zero_key_alloc — 运行时对零尺寸请求响应的内容：成功，且没有指针。 */
static bool zero_key_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    (void)ctx;
    (void)bytes;
    fake_calls++;
    *key = 0;
    return true;
}

static int child_zero_size_keeps_no_slot(void)
{
    int i;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();

    /* 比关键映射表所包含的数量更多。每一个都是成功的分配，没有任何免费资源可以与之匹配——释放空指针被定
     * 义为无操作——因此为其中任何一个保留的槽位会持续存在整个进程期间，而映射表则会被那些根本不需要内存
     * 的分配项填满。框架正是通过空批次来实现这些操作的。 */
    for (i = 0; i < 20000; i++) {
        if (vrocm_ledger_admit(0, 0, zero_key_alloc, NULL) != VROCM_ADMIT_OK) {
            return 1;
        }
    }
    if (vrocm_ledger_tracking_refusals() != 0) {
        return 2;
    }
    /* 该卡从未输出任何内容，因此真正的请求仍必须得到满足。 */
    if (vrocm_ledger_admit(0, GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 3;
    }
    return vrocm_ledger_used(0) == GIB ? 0 : 4;
}

static void test_zero_size_keeps_no_slot(void)
{
    int rc = run_child(child_zero_size_keeps_no_slot, region_path(14));

    check_that(rc == 0, "ledger/zero_size_keeps_no_slot", "%s",
               rc == 1   ? "zero-size allocations filled the key map and were then refused"
               : rc == 3 ? "1 GiB was refused on an empty card after 20000 zero-size allocations"
                         : "a zero-size allocation is served, charged nothing and keeps no slot");
}

/* fork 后的状态
 *
 * 一个分叉子进程继承了关键映射、重新进入计数器和自旋锁，而每一个副本在子进程中都是错误的。下面的两
 * 个测试通过公共条目而非重置操作本身来验证这些后果，因为这些后果正是工作负载所面临的状况：一个能够
 * 退还父进程记账记录的子进程，以及一个在锁上挂起无法释放的子进程。 */

/* fork 子进程与创建它的父进程之间的检查机制。在父进程仍持有该卡时，子进程无法获取它——fcntl 的记录
 * 锁定属于进程本身，因此当父进程等待子进程时，子进程会阻塞在父进程上。因此，父进程完成分配后释放资
 * 源，之后才会允许子进程使用。 */
static int fork_gate[2] = { -1, -1 };

/* forking_alloc — 一个分配回调，在卡锁被持有时进行分叉操作。
 *
 * 这就是值得测试的时刻，且并非人为设计的：锁在整个运行时系统的实际分配过程中被保持，而这就是数据加
 * 载器工作进程被创建时的窗口。子进程通过其退出代码报告所发现的内容。 */
static bool forking_alloc(void *ctx, unsigned long long *key, unsigned long long *bytes)
{
    pid_t *child = ctx;

    (void)bytes;
    *child = fork();
    if (*child == 0) {
        int device = -1;
        char go = 0;

        /* 下面的循环没有自己的超时机制，因此如果没有这个，回归会导致测试套件挂起，而不是失败。 */
        alarm(10);
        /* 继承的重新进入计数器使得这一情况成立，而相信持有该卡的子进程则会在没有任何锁定的情况下继续进行记
         * 账操作。 */
        if (vrocm_ledger_holding(&device)) {
            _exit(1);
        }
        if (read(fork_gate[0], &go, 1) != 1) {
            _exit(2);
        }
        /* 父进程已经释放了该卡，因此现在必须完成此操作。从fork操作继承来的device_lock条目仍由fork操作设置，
         * 在本进程中没有拥有者，也永远无法被释放：这就是那个会一直运行直到警报结束的调用。 */
        if (vrocm_ledger_admit(0, GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
            _exit(3);
        }
        _exit(0);
    }
    if (*child < 0) {
        return false;
    }
    *key = fake_next_key++;
    return true;
}

static int child_fork_under_lock(void)
{
    pid_t child = -1;
    int status = 0;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    if (pipe(fork_gate) != 0) {
        return 1;
    }
    if (vrocm_ledger_admit(0, GIB, forking_alloc, &child) != VROCM_ADMIT_OK) {
        return 2;
    }
    if (write(fork_gate[1], "g", 1) != 1) {
        return 3;
    }
    if (waitpid(child, &status, 0) != child) {
        return 4;
    }
    if (!WIFEXITED(status)) {
        return 5;
    }
    return WEXITSTATUS(status) == 0 ? 0 : 5 + WEXITSTATUS(status);
}

static void test_fork_under_lock(void)
{
    int rc = run_child(child_fork_under_lock, region_path(15));

    check_that(rc == 0, "ledger/fork_under_lock", "%s",
               rc == 5   ? "a child forked under the card's lock spun on it until the alarm"
               : rc == 6 ? "a child forked under the card's lock believed it held it"
               : rc == 8 ? "a child forked under the card's lock could not go on to take it"
                         : "a child forked while a card was locked takes it cleanly afterwards");
}

static int child_fork_refunds_nothing(void)
{
    unsigned long long key;
    pid_t child;
    int status = 0;

    setenv(VROCM_ENV_MEMORY_LIMIT, "4096", 1);
    vrocm_quota_validate();
    key = fake_next_key;
    if (vrocm_ledger_admit(0, GIB, fake_alloc, NULL) != VROCM_ADMIT_OK) {
        return 1;
    }

    child = fork();
    if (child == 0) {
        /* 子进程继承了键映射、键所代表的指针以及映射区域。释放一个被继承的键时必须确保没有问题：这笔记账属
         * 于某个进程，而当前进程并不属于该进程，在此进行退款会从仍保留这些数据的卡中扣除字节数——而下一次扫
         * 描会基于槽位重新计算总数，从而固定该数据。 */
        _exit(vrocm_ledger_release(key, NULL, NULL) ? 1 : 0);
    }
    if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status)) {
        return 2;
    }
    if (WEXITSTATUS(status) != 0) {
        return 3;
    }
    return vrocm_ledger_used(0) == GIB ? 0 : 4;
}

static void test_fork_refunds_nothing(void)
{
    int rc = run_child(child_fork_refunds_nothing, region_path(16));

    check_that(rc == 0, "ledger/fork_refunds_nothing", "%s",
               rc == 3   ? "a forked child refunded a charge its parent still holds"
               : rc == 4 ? "the parent's charge did not survive its child"
                         : "an inherited key is not the child's to give back");
}

int main(int argc, char **argv)
{
    self = argv[0];

    /* 拦截组件：仅报告此环境生成的日志级别，其他内容不显示。 */
    if (argc == 2 && strcmp(argv[1], "--log-level") == 0) {
        return vrocm_log_level();
    }

    if (mkdtemp(tmpdir) == NULL) {
        printf("FAIL | setup | cannot create a temporary directory\n");
        printf("FAILS=1\n");
        return 1;
    }

    test_quota_parse();
    test_quota_precedence();
    test_quota_usable_latch();
    test_log_levels();

    test_region_layout();
    test_region_foreign();
    test_region_version();

    test_quota_reread_on_attach();
    test_one_lock_acquisition();
    test_tracking_fail_closed();
    test_dead_charge_swept();

    test_charge_refund();
    test_alloc_refused_charges_nothing();
    test_quota_boundary();
    test_quota_wraps();
    test_lock_released();
    test_keymap_process_local();
    test_overrun_is_charged();
    test_zero_size_keeps_no_slot();
    test_fork_under_lock();
    test_fork_refunds_nothing();

    printf("FAILS=%d\n", fails);
    return fails == 0 ? 0 : 1;
}

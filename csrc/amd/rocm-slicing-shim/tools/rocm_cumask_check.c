/* rocm_cumask_check.c — 通过在其下运行并读取硬件数据，来决定 CU 掩码是否生效。
 *
 * 为什么会有这种情况？CU 掩码会以静默方式失效：拒绝该掩码的运行时环境不会返回错误，不会记录日志，
 * 也不会改变返回代码。容器失去的并非部分隔离功能，而是全部隔离功能。对四种此类结构进行了测量，每一
 * 种都被记录得十分合理：在 RDNA 上拆分 WGP 对的 CU 设置会丢弃整个掩码；所有位位于 WGP 计数之上或以
 * 上的 ROC_GLOBAL_CU_MASK 会被完全忽略；以 UUID 而非索引形式编写的 GPU_list 会丢失整个段；在多 XCC
 * 部分中，不将任何位放置于某个 XCC 的掩码会让该 XCC 保持未屏蔽状态。平台上没有任何相关报告，因此这
 * 里进行了报告。
 *
 * 它依据占用率而非吞吐量进行评判。吞吐量比较能够避免那些最糟糕的缺陷。在 304 个 CU 的 gfx942 部件
 * 中，一位掩码 0:0 占到了芯片吞吐量的 3.7%，正好符合其位数的指示范围；而容器中的波动达到了芯片 304
 * 个 CU 的 267，因为八个 XCC 中有七个根本没有收到任何掩码。完成时间由受限最严重的 XCC 决定，并未提
 * 及其他七个。因此，该组件会启动自己的内核，让每个波动从硬件中读取其自身的物理身份，并将实际运行的
 * 单元与掩码所要求的单元进行比较。
 *
 * 为什么它再次执行自身。ROCr 只读取了一次 HSA_CU_MASK，而此时它正处于初始化阶段，远在此处任何代码
 * 能够设置该变量之前。一个从环境中获取该掩码并在同一进程内启动内核的探针，会测量其启动时的环境状态。
 * 因此，该变量被写入后通过 execv 执行，第二次运行时看到环境中存在该掩码，从而进行验证。这也构成了
 * 该工具的完整外部结构：环境中已存在的掩码会被如实验证，这正是上述一种失败修复方案的体现。
 *
 * 该单元基于架构差异进行比较——CDNA 中的 CU，RDNA 中的 WGP——且每个单元都符合该架构的分配原子规则。
 * device/vrocm_hwid.h 包含寄存器及相关逻辑；它与正在测试的 soak 库共享资源，因此两者在测量结果上不
 * 会产生分歧。
 *
 * TOPOLOGY 来自 HSA AGENT-INFO API，并非来自 KFD sysfs。这两种机制在两种架构上均保持一致，因此这关
 * 乎契约的稳定性而非正确性——但他们所报告的数据中的一个特性却是一个陷阱：着色器引擎数量是以整个设备
 * 范围为单位的，且已包含 XCC 乘数，因此在八个 XCC 部分的系统中，NUM_SHADER_ENGINES 为 32 意味着每
 * 个 XCC 有 4 个引擎，而非 32 个。以下没有任何内容会除以 NUM_XCC 来纠正这一情况，这才是重点而非疏
 * 漏：唯一使用引擎数量的规则是 RDNA 的对齐机制，该机制中数量已经是按每个 XCC 计算的，因为只有一个
 * XCC；而 CDNA 的规则则体现在 XCC 和 CUs 中，根本不需要按每个 XCC 的引擎数量来计算。
 *
 * 它被编译为 HIP 格式，而非 C 格式，因为它包含设备代码。build.sh 明确指定了 -x hip 选项：hipcc 将
 * .c 文件编译为 C 格式——实际上，它甚至不会将 HIP 头文件添加到包含路径中——下面的内核将无法正常运行。
 *
 * EXIT CODES，因为其预期调用者是检测器，该检测器不会为失败卡宣传可使用的功能，也不会宣传节点不会遵
 * 守的条款：0 掩码如所要求那样生效；1 并未生效，这就是此工具存在的目的；2 探针无法运行——没有代理，
 * 或者无法满足的请求，又或者参数格式错误。 */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <hip/hip_runtime.h>
#include <hsa/hsa.h>
#include <hsa/hsa_ext_amd.h>

#include "device/vrocm_hwid.h"

#define TOOL "rocm-cumask-check"

/* 足够容纳当今任何该平台可使用的面具（最大的部分为 304 CUs），且还有剩余空间；同时又足够小，以便将
 * 解析结果保持为平面数组而非位图，从而简化运算。 */
#define MAX_MASK_BITS 1024u

#define MAX_XCC VROCM_HWID_MAX_XCC
#define SLOT_COUNT VROCM_HWID_SLOTS
#define SLOT_SINK SLOT_COUNT

#define MAX_AGENTS 64u

/* ---- 内核 ---------------------------------------------------------------------------- */

/* vrocm_occupy — 旋转，然后记录旋转发生的位置。
 *
 * 该自旋机制确保了输出结果的准确性，而非近似性：立即返回的内核在调度器完成其工作组的分配之前就退出
 * 了，因此它仅报告了已启用的部分单元，而生效的掩码则被误判为未生效的。 sink 存储机制则维持了自旋的
 * 活性——若不使用累加器，循环部分将变成死代码，编译器会将其删除；此外，由于种子值是一个线程索引，该
 * 条件永远无法被证明为错误。 */
__global__ void vrocm_occupy(unsigned int *slots, unsigned int spin)
{
    unsigned int acc = threadIdx.x + blockIdx.x * blockDim.x + 1u;
    unsigned int i;

    for (i = 0; i < spin; i++) {
        acc = acc * 1664525u + 1013904223u;
    }
    slots[vrocm_hwid_slot()] = 1u;
    if (acc == 0u) {
        slots[SLOT_SINK] = acc;
    }
}

/* ---- 报告 ---------------------------------------------------------------------------- */

static int fails;

static void check_that(int ok, const char *name, const char *fmt, ...)
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

/* 注释 — 一种虽然会消耗容器吞吐量但不会带来隔离效果的属性。它被记录但不被计算，因为浪费资源的遮罩
 * 仍然会限制工作负载；如果无法满足该条件，探针将拒绝平台所认可的分配。 */
static void note(const char *name, const char *fmt, ...)
{
    va_list ap;

    printf("WARN | %s | ", name);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

/* ---- 拓扑结构 ----------------------------------------------------------------------------- */

struct topology {
    char name[64];
    unsigned int cu;
    unsigned int se;        /* 设备级范围，已通过 XCC 计数进行相乘 */
    unsigned int sa_per_se; /* 每个着色器引擎的数量，尚未乘以引擎数。 */
    unsigned int xcc;
};

struct agent_list {
    hsa_agent_t agents[MAX_AGENTS];
    unsigned int count;
};

/* collect_gpu — 按枚举顺序收集 GPU 代理，跳过那些无法说明自身身份的代理，否则会中断遍历过程：一个
 * 无法读取的代理不应成为其背后卡的代价。 */
static hsa_status_t collect_gpu(hsa_agent_t agent, void *data)
{
    struct agent_list *list = (struct agent_list *)data;
    hsa_device_type_t type;

    if (hsa_agent_get_info(agent, HSA_AGENT_INFO_DEVICE, &type) != HSA_STATUS_SUCCESS) {
        return HSA_STATUS_SUCCESS;
    }
    if (type == HSA_DEVICE_TYPE_GPU && list->count < MAX_AGENTS) {
        list->agents[list->count++] = agent;
    }
    return HSA_STATUS_SUCCESS;
}

/* topology_read — 用于一张卡的、所需的四项代理信息字段。
 *
 * 该索引是 GPU-代理枚举中的一个位置，即 HSA_CU_MASK 的 GPU_list 所使用的索引空间。在代理被迭代之前，
 * ROCR 已经应用了 ROCR_VISIBLE_DEVICES，因此过滤和重新排序的操作就体现在这里，无需再读取该变量。 */
static int topology_read(unsigned int device, struct topology *out)
{
    struct agent_list list;
    hsa_agent_t agent;

    list.count = 0;
    if (hsa_init() != HSA_STATUS_SUCCESS) {
        fprintf(stderr, TOOL ": cannot initialise ROCr\n");
        return 0;
    }
    if (hsa_iterate_agents(collect_gpu, &list) != HSA_STATUS_SUCCESS) {
        fprintf(stderr, TOOL ": cannot iterate agents\n");
        return 0;
    }
    if (device >= list.count) {
        fprintf(stderr, TOOL ": device %u of %u GPU agents\n", device, list.count);
        return 0;
    }
    agent = list.agents[device];

    memset(out, 0, sizeof(*out));
    if (hsa_agent_get_info(agent, HSA_AGENT_INFO_NAME, out->name) != HSA_STATUS_SUCCESS ||
        hsa_agent_get_info(agent, (hsa_agent_info_t)HSA_AMD_AGENT_INFO_COMPUTE_UNIT_COUNT,
                           &out->cu) != HSA_STATUS_SUCCESS ||
        hsa_agent_get_info(agent, (hsa_agent_info_t)HSA_AMD_AGENT_INFO_NUM_SHADER_ENGINES,
                           &out->se) != HSA_STATUS_SUCCESS ||
        hsa_agent_get_info(agent, (hsa_agent_info_t)HSA_AMD_AGENT_INFO_NUM_SHADER_ARRAYS_PER_SE,
                           &out->sa_per_se) != HSA_STATUS_SUCCESS) {
        fprintf(stderr, TOOL ": device %u does not report its topology\n", device);
        return 0;
    }
    /* older ROCr可能未携带的字段。一个XCC是默认的诚实状态，而非故障：这是所有在属性出现之前的部分所具
     * 备的。 */
    if (hsa_agent_get_info(agent, (hsa_agent_info_t)HSA_AMD_AGENT_INFO_NUM_XCC, &out->xcc) !=
            HSA_STATUS_SUCCESS ||
        out->xcc == 0u) {
        out->xcc = 1u;
    }
    if (out->cu == 0u || out->se == 0u || out->xcc > MAX_XCC) {
        fprintf(stderr, TOOL ": device %u reports an unusable topology: cu=%u se=%u xcc=%u\n",
                device, out->cu, out->se, out->xcc);
        return 0;
    }
    return 1;
}

/* ---- 掩码 ----------------------------------------------------------------------------- */

struct cu_mask {
    unsigned char bit[MAX_MASK_BITS];
    unsigned int highest; /* 最高索引集的下一个值；未设置时则为0 */
    unsigned int count;
    int applies;     /* 名为此设备的某些部分 */
    int non_numeric; /* GPU_list列表中的项并非十进制索引 */
    int overflow;    /* 一个超过此解析器所能容纳范围的索引 */
    int malformed;
};

static const char *read_uint(const char *s, const char *end, unsigned int *out, int *ok)
{
    unsigned int value = 0;
    int digits = 0;

    while (s < end && *s >= '0' && *s <= '9') {
        value = value * 10u + (unsigned int)(*s - '0');
        s++;
        digits++;
    }
    *ok = digits > 0;
    *out = value;
    return s;
}

static const char *skip_item(const char *s, const char *end)
{
    while (s < end && *s != ',') {
        s++;
    }
    return (s < end) ? s + 1 : s;
}

/* gpu_list_names — 这个用逗号分隔的列表名称 device 是什么？
 *
 * 非十进制项会被记录下来，而不仅仅是被跳过，因为这本身也是一种失败模式：ROCr的解析器只接受索引，无
 * 法解析的GPU-<uuid>段会被完全丢弃，导致卡未被标记，没有任何说明。 */
static int gpu_list_names(const char *s, const char *end, unsigned int device, int *non_numeric)
{
    int found = 0;

    while (s < end) {
        unsigned int lo, hi;
        int ok;

        s = read_uint(s, end, &lo, &ok);
        if (!ok) {
            *non_numeric = 1;
            s = skip_item(s, end);
            continue;
        }
        hi = lo;
        if (s < end && *s == '-') {
            s = read_uint(s + 1, end, &hi, &ok);
            if (!ok) {
                *non_numeric = 1;
            }
        }
        if (device >= lo && device <= hi) {
            found = 1;
        }
        s = skip_item(s, end);
    }
    return found;
}

static void mask_set(struct cu_mask *m, unsigned int index)
{
    if (index >= MAX_MASK_BITS) {
        m->overflow = 1;
        return;
    }
    if (!m->bit[index]) {
        m->bit[index] = 1;
        m->count++;
    }
    if (index + 1u > m->highest) {
        m->highest = index + 1u;
    }
}

/* cu_list_set — 设置由逗号分隔的 CU 列表名称中的各项，每个项对应 n 或 a-b。无法读取的该项会中断遍
 * 历过程，而不是跳到下一个项：列表的其余部分不再可靠，而整体解读半个掩码则是一种错误的预期，而非缺
 * 失的情况。 */
static void cu_list_set(const char *s, const char *end, struct cu_mask *m)
{
    while (s < end) {
        unsigned int lo, hi, i;
        int ok;

        s = read_uint(s, end, &lo, &ok);
        if (!ok) {
            m->malformed = 1;
            return;
        }
        hi = lo;
        if (s < end && *s == '-') {
            s = read_uint(s + 1, end, &hi, &ok);
            if (!ok) {
                m->malformed = 1;
                return;
            }
        }
        for (i = lo; i <= hi && i < MAX_MASK_BITS; i++) {
            mask_set(m, i);
        }
        if (hi >= MAX_MASK_BITS) {
            m->overflow = 1;
        }
        s = skip_item(s, end);
    }
}

/* parse_hsa_cu_mask — GPU_list:CU_list[;GPU_list:CU_list...，其中 CU_list 的第 i 位为 CU i。 */
static void parse_hsa_cu_mask(const char *spec, unsigned int device, struct cu_mask *m)
{
    const char *p = spec;

    while (*p != '\0') {
        const char *seg_end = strchr(p, ';');
        const char *colon;

        if (seg_end == NULL) {
            seg_end = p + strlen(p);
        }
        colon = (const char *)memchr(p, ':', (size_t)(seg_end - p));
        if (colon == NULL) {
            m->malformed = 1;
        } else if (gpu_list_names(p, colon, device, &m->non_numeric)) {
            m->applies = 1;
            cu_list_set(colon + 1, seg_end, m);
        }
        p = (*seg_end == ';') ? seg_end + 1 : seg_end;
    }
}

/* parse_global_cu_mask — 一个普通的十六进制位掩码，其中第 i 位在 RDNA 架构下为 WGP，在 CDNA 架构下
 * 为 CU。这种差异性正是使得 multiProcessorCount 在两种架构下具有不同含义的原因，也是为什么这个解析
 * 器将处理单位交由调用者处理。 */
static void parse_global_cu_mask(const char *spec, struct cu_mask *m)
{
    size_t len, i;

    if (spec[0] == '0' && (spec[1] == 'x' || spec[1] == 'X')) {
        spec += 2;
    }
    len = strlen(spec);
    if (len == 0) {
        m->malformed = 1;
        return;
    }
    for (i = 0; i < len; i++) {
        char c = spec[len - 1 - i];
        unsigned int nibble, b;

        if (c >= '0' && c <= '9') {
            nibble = (unsigned int)(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            nibble = (unsigned int)(c - 'a') + 10u;
        } else if (c >= 'A' && c <= 'F') {
            nibble = (unsigned int)(c - 'A') + 10u;
        } else {
            m->malformed = 1;
            return;
        }
        for (b = 0; b < 4u; b++) {
            if ((nibble & (1u << b)) != 0u) {
                mask_set(m, (unsigned int)i * 4u + b);
            }
        }
    }
    m->applies = 1;
}

/* expand_wgps_to_cus — 将 WGP 索引掩码重新表述为 CU 索引，以便一组规则同时处理这两个变量。该逻辑被
 * 重构为新的掩码，而非直接在原地重复操作：计数器的值必须与位值一致，而直接扩展操作会覆盖尚未读取的
 * 索引。 */
static void expand_wgps_to_cus(struct cu_mask *m, unsigned int wgps)
{
    struct cu_mask out;
    unsigned int w;

    memset(&out, 0, sizeof(out));
    out.applies = m->applies;
    out.non_numeric = m->non_numeric;
    out.overflow = m->overflow;
    out.malformed = m->malformed;
    for (w = 0; w < wgps && w < MAX_MASK_BITS; w++) {
        if (m->bit[w]) {
            mask_set(&out, 2u * w);
            mask_set(&out, 2u * w + 1u);
        }
    }
    *m = out;
}

/* ---- 符合性规则 ---------------------------------------------------------------- */

struct expectation {
    unsigned int units;
    unsigned int per_xcc[MAX_XCC];
};

/* check_bits_in_range — 超过 CU 数量的某一位超出了运行时环境的接受范围，且不确定是丢弃该位还是整个
 * 掩码从未被测量过；无论是哪种情况，该掩码都无法准确反映调用者所认为的约束范围。 */
static void check_bits_in_range(const struct topology *t, const struct cu_mask *m)
{
    if (m->overflow) {
        /* 通过报告而不是留待下游比较，这样就能发现不匹配情况，并因该解析器的限制而指责遮罩。 */
        check_that(0, "mask/bits_in_range", "an index beyond the %u this reader holds", MAX_MASK_BITS);
        return;
    }
    if (m->count == 0u) {
        check_that(1, "mask/bits_in_range", "no bits set");
        return;
    }
    check_that(m->highest <= t->cu, "mask/bits_in_range", "highest index %u against %u CUs",
               m->highest - 1u, t->cu);
}

/* check_rdna — 针对单个 XCC 部分的掩码规则，其中原子为 WGP 对。 */
static void check_rdna(const struct topology *t, const struct cu_mask *m, struct expectation *e)
{
    unsigned int wgps = t->cu / 2u;
    unsigned int split = 0, whole = 0, w;

    check_bits_in_range(t, m);

    for (w = 0; w < wgps; w++) {
        unsigned int a = m->bit[2u * w], b = m->bit[2u * w + 1u];

        if (a != b) {
            split++;
        } else if (a) {
            whole++;
        }
    }
    /* 一个孤立的 CU 不会使掩码减少一个 CU——运行时会判定整个集合无效，从而将整个卡返回。测量结果显示：
     * 0:0-14 占用了所有 60 个 CU，而 0:0-13（少一个 CU）则正确地将容器限制为 7 个 WGPs。 */
    check_that(split == 0, "mask/wgp_pairs_whole",
               "%u whole WGP pairs, %u split", whole, split);

    /* 内核以轮转方式将掩码位传递给着色器引擎，因此并非引擎数量公倍数的WGP计数会留下无法产生吞吐量的剩
     * 余值。在18个样本点中测量发现：3和4个WGPs的数值相同，6和8个、28和29也相同。 */
    if (whole % t->se != 0u) {
        note("mask/shader_engine_aligned",
             "a %u-WGP set is not a multiple of %u shader engines; the remainder yields nothing",
             whole, t->se);
    }

    memset(e, 0, sizeof(*e));
    e->units = whole;
}

/* check_global_mask — 仅属于 ROC_GLOBAL_CU_MASK 的规则，以及允许按架构分别读取的重新表述。
 *
 * 其位元 i 在 RDNA 架构下属于 WGP，在 CDNA 架构下属于 CU——这种不对称性导致
 * hipDeviceProp_t.multiProcessorCount 在某一架构下显示一半的 CU 数量，在另一架构下则显示全部数量。
 * 无论如何，位于或高于宽度的位元都会被忽略，因此所有位元都位于该位置的掩码会使卡完全未被屏蔽。 */
static void check_global_mask(const struct topology *t, struct cu_mask *m)
{
    unsigned int width = (t->xcc > 1u) ? t->cu : t->cu / 2u;
    unsigned int within = 0, b;

    for (b = 0; b < width && b < MAX_MASK_BITS; b++) {
        within += m->bit[b];
    }
    check_that(within > 0, "mask/bits_within_width", "%u of %u bits below the %u-%s width", within,
               m->count, width, t->xcc > 1u ? "CU" : "WGP");
    if (m->highest > width) {
        note("mask/bits_above_width", "bits at or above %u are ignored", width);
    }
    if (t->xcc == 1u) {
        expand_wgps_to_cus(m, width);
    }
}

/* check_cdna — 多XCC部分的掩码规则，其中每个XCC中原子对应一个CU。
 *
 * 位映射是从硬件中直接读取出来的，而非推断得出的：第 i 位落在 XCC 上（i mod X），因此连续八位分别
 * 对应八种不同的 XCC，每个对应一个 CU”——绝不会出现八个 CU 都属于同一个 XCC。以下所有内容均基于这一
 * 事实得出。 */
static void check_cdna(const struct topology *t, const struct cu_mask *m, struct expectation *e)
{
    unsigned int covered = 0, i;

    check_bits_in_range(t, m);

    memset(e, 0, sizeof(*e));
    for (i = 0; i < m->highest && i < t->cu; i++) {
        if (m->bit[i]) {
            e->per_xcc[i % t->xcc]++;
            e->units++;
        }
    }
    for (i = 0; i < t->xcc; i++) {
        if (e->per_xcc[i] != 0u) {
            covered++;
        }
    }
    /* 没有 RDNA 对应组件且没有单一 XCC 见证者的失败情况：未接收任何位的 XCC 不会获得少量份额，因此被保
     * 留为未遮罩状态。经测量，一位掩码 0:0 占据了 304 个 CUs 中的 267 个，而 0:0-3 则占据了 156 个，这
     * 两种情况都是因为该掩码从未被提及的 XCC 以全量规模运行容器中的操作。 */
    check_that(covered == t->xcc, "mask/every_xcc_covered", "%u of %u XCCs carry a bit", covered,
               t->xcc);

    if (e->units % t->xcc != 0u) {
        note("mask/xcc_atom_aligned",
             "a %u-CU set is not a multiple of %u XCCs; the remainder is occupied but unusable",
             e->units, t->xcc);
    }
}

/* ---- 占用 ---------------------------------------------------------------------------- */

/* occupancy — 该进程自身波次所使用的物理单元集合。
 *
 * 超量分配为8比1，并重复进行，因为下游比较是精确的：此保护机制所涉及的配额是拦截库启用的一个单位，
 * 没有工作组恰好触及该配额，那样就会将正常的工作掩模报告为损坏的掩模。重复操作会累积到同一表中，因
 * 此结果就是多个进程之间的并集。 */
static int occupancy(unsigned int device, const struct topology *t, unsigned int *found,
                     unsigned int *per_xcc)
{
    unsigned int host[SLOT_COUNT + 1];
    unsigned int *slots = NULL;
    unsigned int blocks = 8u * t->cu;
    unsigned int round, i;
    hipError_t rc;

    /* 内核必须落在已读取拓扑结构的卡上，除非有特别说明，HIP的当前设备为0——因此如果没有这个拦截库，运行
     * 在设备1上的程序会报告设备1的拓扑结构以及设备0的占用情况，并将这种差异称为掩码失败。 */
    rc = hipSetDevice((int)device);
    if (rc != hipSuccess) {
        fprintf(stderr, TOOL ": cannot select HIP device %u: %s\n", device, hipGetErrorString(rc));
        return 0;
    }
    rc = hipMalloc((void **)&slots, sizeof(host));
    if (rc != hipSuccess) {
        fprintf(stderr, TOOL ": cannot allocate the slot table: %s\n", hipGetErrorString(rc));
        return 0;
    }
    rc = hipMemset(slots, 0, sizeof(host));
    for (round = 0; rc == hipSuccess && round < 3u; round++) {
        vrocm_occupy<<<blocks, 64u>>>(slots, 200000u);
        rc = hipDeviceSynchronize();
    }
    if (rc == hipSuccess) {
        rc = hipMemcpy(host, slots, sizeof(host), hipMemcpyDeviceToHost);
    }
    (void)hipFree(slots);
    if (rc != hipSuccess) {
        fprintf(stderr, TOOL ": the occupancy probe failed: %s\n", hipGetErrorString(rc));
        return 0;
    }

    *found = 0;
    for (i = 0; i < MAX_XCC; i++) {
        per_xcc[i] = 0;
    }
    for (i = 0; i < SLOT_COUNT; i++) {
        if (host[i]) {
            (*found)++;
            per_xcc[VROCM_HWID_SLOT_XCC(i)]++;
        }
    }
    return 1;
}

/* ---- 推导 --------------------------------------------------------------------------- */

/* derive_and_reexec — 将百分比转换为掩码，将其放入环境中，然后重新开始。
 *
 * 这两个分支刻意没有进行任何运算。两者看起来都正确，但每个在另一种架构下都是错误的：将 RDNA 的配对
 * 规则应用到 CDNA 上，会使每个切片数量翻倍而不会出现故障；而将 CDNA 的原子结构应用到 RDNA 上，则会
 * 导致配对失败并完全丢失掩码。 */
static int derive_and_reexec(char **argv, unsigned int device, unsigned int percent,
                             const struct topology *t)
{
    char spec[64];
    unsigned int last;

    if (t->xcc > 1u) {
        unsigned int n = (t->cu * percent + 50u) / 100u;

        n = (n / t->xcc) * t->xcc;
        if (n == 0u) {
            /* 不是限制。亚原子级掩码不会将容器限制在一小部分，它会让大部分区域保持开放状态，因此低于一个原子的
             * 请求根本无法得到满足——在8个XCC 304个CU的模块中，最小的满足槽位为8个CU，占2.63%。 */
            fprintf(stderr,
                    TOOL ": %u%% of %u CUs is below one %u-CU atom; this request cannot be"
                         " honoured, and rounding it up or down would not confine the container\n",
                    percent, t->cu, t->xcc);
            return 2;
        }
        last = n - 1u;
    } else {
        unsigned int wgps = t->cu / 2u;
        unsigned int n = (wgps * percent + 50u) / 100u;

        n = (n / t->se) * t->se;
        if (n < t->se) {
            n = t->se;
        }
        if (n > wgps) {
            n = wgps;
        }
        last = 2u * n - 1u;
    }
    snprintf(spec, sizeof(spec), "%u:0-%u", device, last);
    printf("derive percent=%u mask=%s\n", percent, spec);
    fflush(stdout);

    if (setenv("HSA_CU_MASK", spec, 1) != 0) {
        fprintf(stderr, TOOL ": cannot set HSA_CU_MASK\n");
        return 2;
    }
    /* /proc/self/exe 而不是 argv[0]，当工具被找到时，argv[0]并非可用的路径。子进程在其环境中看到一个掩
     * 码，便采用该验证路径，因此该进程在恰好一次跳转后就会终止。 */
    execv("/proc/self/exe", argv);
    fprintf(stderr, TOOL ": cannot re-exec to apply the mask: ROCr reads HSA_CU_MASK when it"
                    " initialises, which has already happened here\n");
    return 2;
}

/* ---- 入口点 -------------------------------------------------------------------------- */

static void usage(void)
{
    fprintf(stderr,
            "usage: " TOOL " [--device N] [--percent P]\n"
            "  With no mask in the environment, derives a P%% mask (default 50) for device N and\n"
            "  re-execs under it. With HSA_CU_MASK or ROC_GLOBAL_CU_MASK already set, verifies\n"
            "  that mask as it stands.\n");
}

int main(int argc, char **argv)
{
    unsigned int device = 0, percent = 50;
    unsigned int occupied = 0, occupied_per_xcc[MAX_XCC];
    const char *hsa_spec, *global_spec;
    struct expectation expect;
    struct topology topo;
    struct cu_mask mask;
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--device") == 0 && i + 1 < argc) {
            device = (unsigned int)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--percent") == 0 && i + 1 < argc) {
            percent = (unsigned int)strtoul(argv[++i], NULL, 10);
        } else {
            usage();
            return 2;
        }
    }
    if (percent == 0u || percent > 100u) {
        fprintf(stderr, TOOL ": --percent %u is outside 1..100\n", percent);
        return 2;
    }

    if (!topology_read(device, &topo)) {
        return 2;
    }
    printf("topology device=%u name=%s cu=%u se=%u sa_per_se=%u xcc=%u unit=%s units=%u\n", device,
           topo.name, topo.cu, topo.se, topo.sa_per_se, topo.xcc, topo.xcc > 1u ? "cu" : "wgp",
           topo.xcc > 1u ? topo.cu : topo.cu / 2u);

    hsa_spec = getenv("HSA_CU_MASK");
    global_spec = getenv("ROC_GLOBAL_CU_MASK");
    if (hsa_spec == NULL && global_spec == NULL) {
        return derive_and_reexec(argv, device, percent, &topo);
    }

    /* HIP在已经进行过的过滤操作的基础上应用HIP_VISIBLE_DEVICES，这会使内核运行在与刚刚读取的拓扑结构不
     * 同的卡上。 */
    if (getenv("HIP_VISIBLE_DEVICES") != NULL || getenv("CUDA_VISIBLE_DEVICES") != NULL) {
        note("env/hip_visible_devices",
             "set, so the HIP device index may not be the HSA agent index this masked");
    }

    memset(&mask, 0, sizeof(mask));
    if (hsa_spec != NULL) {
        printf("mask source=HSA_CU_MASK value=%s\n", hsa_spec);
        if (global_spec != NULL) {
            note("env/both_masks_set",
                 "ROC_GLOBAL_CU_MASK is set too; their interaction is not measured, so only"
                 " HSA_CU_MASK is verified here");
        }
        parse_hsa_cu_mask(hsa_spec, device, &mask);
        check_that(!mask.malformed, "mask/parses", "syntax is GPU_list:CU_list[;...]");
        /* 该段代码被完全删除，因此卡能够以未伪装的状态运行。一个 UUID GPU_list 是偶然到达这里的测量方式：
         * 它看起来像运行时会接受的标识符。 */
        check_that(mask.applies, "mask/applies_to_device",
                   "%s", mask.applies ? "a segment names this device"
                                      : (mask.non_numeric
                                             ? "no segment names device by index; a GPU_list that"
                                               " is not a decimal index is discarded in full"
                                             : "no segment names this device"));
    } else {
        printf("mask source=ROC_GLOBAL_CU_MASK value=%s\n", global_spec);
        parse_global_cu_mask(global_spec, &mask);
        check_that(!mask.malformed, "mask/parses", "expected a hexadecimal bitmask");
        check_global_mask(&topo, &mask);
    }

    if (topo.xcc > 1u) {
        check_cdna(&topo, &mask, &expect);
    } else {
        check_rdna(&topo, &mask, &expect);
    }
    if (!occupancy(device, &topo, &occupied, occupied_per_xcc)) {
        return 2;
    }
    /* 与实际情况相比，包括当掩码没有要求时：被丢弃的掩码会使期望值归零，而输出结果则反映整个卡的情况，
     * 这就使得失败情况以两个数字的形式呈现，而不是表现为没有证据。 */
    check_that(occupied == expect.units, "occupancy/units_match", "%s: masked %u, occupied %u",
               topo.xcc > 1u ? "CU" : "WGP", expect.units, occupied);

    if (topo.xcc > 1u) {
        unsigned int x, mismatched = 0;

        for (x = 0; x < topo.xcc; x++) {
            if (occupied_per_xcc[x] != expect.per_xcc[x]) {
                mismatched++;
            }
            printf("xcc %u expected=%u occupied=%u\n", x, expect.per_xcc[x], occupied_per_xcc[x]);
        }
        /* 每XCC的细分数据是吞吐量比较无法看到的：未被遮罩的XCC以最大幅度运行，而由最受限的XCC设定的完成时
         * 间则看起来很合理。 */
        check_that(mismatched == 0, "occupancy/per_xcc_match", "%u of %u XCCs differ", mismatched,
                   topo.xcc);
    }

    printf("FAILS=%d\n", fails);
    return fails == 0 ? 0 : 1;
}

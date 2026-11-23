/* vppu_quota.h — 公共配置中的一半部分：其中列出了分配给容器的内容，以及这些内容是否完全可用。
 *
 * 在这里比在调用者中避免了两个来自参考实现的陷阱：
 * - 每次调用时，配额值从环境中读取，绝不会被创建该文件的进程写入账本中。HAMi-core则执行后一种操作，
 * 因此过期的缓存文件会导致旧限制失效，更改配额意味着删除该文件；
 * - 缺失或无法解析的数据是错误，绝不会出现“无限制”的情况。flexai将缺失的配置视为“不在容器中”，且完
 * 全不设定任何限制，而这正是该设计明确禁止的结果。 */
#ifndef VPPU_COMMON_VPPU_QUOTA_H
#define VPPU_COMMON_VPPU_QUOTA_H

#include <stdbool.h>

#include "vppu.h"
#include "vppu_pid.h"

/* SDK 自有的环境命名空间——HGGC_INJECTION64_PATH 作为 CUDA_INJECTION64_PATH 的对应项已经存在——因此，
 * 一个拦截容器仅包含单个单词成分，而非像 GPUStack 所发明的那样包含多个成分。
 *
 * 这两种维度以两种形式存在，而这一对特性使得它们成为一种契约，而非两种约定：
 * - 带有 LOCAL 设备索引后缀的 INDEXED 形式，因为 SDK 会重新编号容器内的设备：无论其主机序号如何，
 * 将一张卡节点通过后，其索引即为 0。
 * - 那种UN-INDEXED形式，它决定了每一张没有自身索引数值的卡。HAMi-core按
 * CUDA_DEVICE_{MEMORY,SM}_LIMIT{,_<i>}的顺序进行读取，而GPUStack的NVIDIA分配器则仅输出未索引的计算
 * 数值——因此，仅由只知道一个数值的分配器进行分片的容器仍能正常运行。
 *
 * 这些前缀是从普通名称中衍生出来的，而不是重复书写两次，因此一个维度的两种形式不会偏离。 */
#define VPPU_ENV_MEMORY_LIMIT "HGGC_DEVICE_MEMORY_LIMIT"
#define VPPU_ENV_MEMORY_LIMIT_PREFIX VPPU_ENV_MEMORY_LIMIT "_"
#define VPPU_ENV_SM_LIMIT "HGGC_DEVICE_SM_LIMIT"
#define VPPU_ENV_SM_LIMIT_PREFIX VPPU_ENV_SM_LIMIT "_"

/* 计算控制器的自定义旋钮。它们用于调整参数而非限制配额，因此当未设置或超出范围时，每个旋钮都会回退
 * 到默认状态，从而不会使容器无法使用：即使输入错误，也会留下可正常运行的循环，而若输入错误的上限值，
 * 则不会留下任何上限限制。
 *
 * 它们存在的原因在于，这些收益并非刻意从参考实现中继承——参见 vppu_pid.h——因此它们只能被应用于它们
 * 运行的硬件上。 */
#define VPPU_ENV_SM_PERIOD_MS "HGGC_SM_CONTROL_PERIOD_MS"
#define VPPU_ENV_SM_STEP_MS "HGGC_SM_CONTROL_STEP_MS"
#define VPPU_ENV_SM_KP "HGGC_SM_CONTROL_KP"
#define VPPU_ENV_SM_KI "HGGC_SM_CONTROL_KI"
#define VPPU_ENV_SM_KD "HGGC_SM_CONTROL_KD"
#define VPPU_ENV_SM_GRAPH_WEIGHT "HGGC_SM_GRAPH_WEIGHT"

/* 门限窗口：100 毫秒足够短，因此被限制的工作负载会以小段形式等待，而不会出现长时间的停滞。这些限制
 * 措施能避免输入错误导致账本出现工作负载似乎卡住的状况。 */
#define VPPU_SM_PERIOD_MS_DEFAULT 100U
#define VPPU_SM_PERIOD_MS_MIN 1U
#define VPPU_SM_PERIOD_MS_MAX 10000U

/* 循环步长的频率，是 SENSOR 的特性而非窗口的特性。此驱动程序的进程级使用率数据每百毫秒变化约10个百
 * 分点，但负载变化的幅度可能很大，因此整个波动过程需要一秒——这是实际测量所得，而非假设。步长超过此
 * 速度意味着处理的数据尚未同步，且循环会在整个范围内波动而非稳定。 */
#define VPPU_SM_STEP_MS_DEFAULT 1000U
#define VPPU_SM_STEP_MS_MIN 10U
#define VPPU_SM_STEP_MS_MAX 60000U

/* 增益以百分之一为单位，见 VPPU_PID_SCALE。默认值依据 common/ 中的单元测试设置，没有在真实 PPU 上
 * 拟合。利用率反馈来自采样，求导往往会放大噪声，因此默认关闭微分项，保留在硬件上调试的参数。 */
#define VPPU_SM_KP_DEFAULT 25
#define VPPU_SM_KI_DEFAULT 8
#define VPPU_SM_KD_DEFAULT 0
#define VPPU_SM_GAIN_MAX 10000U

/* 账本启动可以运行被捕获到其中的任意数量的内核，因此可能消耗比单次启动更多的显卡资源。权重是指该账
 * 本启动所涉及的窗口数量，默认值为1——关闭——因为目前尚未有任何人对这种硬件情况下账本能否突破限制进
 * 行测量。 */
#define VPPU_SM_GRAPH_WEIGHT_DEFAULT 1U
#define VPPU_SM_GRAPH_WEIGHT_MAX 1024U

/* vppu_quota_parse — 一个按 unit 缩放的正数值，或对于“未设置或不可使用”则为 0。
 *
 * 纯净且刻意保持沉默，因此调用者可以自行决定如何大声地报告每个变量，而单元测试可以在没有任何环境的
 * 情况下进行算术运算。溢出限制并非装饰性设计：被回绕的乘积结果会默默地变成 0（没有任何约束）或一个
 * 微小的数值，这种数值会否定一切，这两种情况都被视为产品缺陷而非不良配置。 */
VPPU_INTERNAL unsigned long long vppu_quota_parse(const char *value, unsigned long long unit);

/* vppu_quota_knob — 一个有限的整数设置值；若未设置、格式错误或超出[low, high]范围，则采用fallback
 * 值。
 *
 * 与 vppu_quota_parse() 函数分离，因为零是增益的合法值，而零的配额则不属于配额范畴，因此两者不能采
 * 用“0表示不可用”的约定。出于同样的原因，完全保持沉默：调用者可以自行决定如何大声报告其拒绝的值。 */
VPPU_INTERNAL unsigned int vppu_quota_knob(const char *value, unsigned int low, unsigned int high,
                                           unsigned int fallback);

/* 遵循以下优先级规则，必须明确说明，因为下面的两个获取器都实现了该规则：
 * HGGC_DEVICE_<dimension>_LIMIT_<i>，当该值被设定时，则采用无索引形式。
 *
 * 被设置的状态决定了其有效性，而非有效性本身。格式错误的数值会在设定的层级上被置为0——“这张卡无法使
 * 用”——并且不会传递到更高层级，因此输入错误的单卡数值会拒绝该卡的使用，而不会将其归为整体容器范围
 * 内的使用。HAMi-core确实会传递至该层级，然后计算方面会默认设为100，这就会让错误输入变成整张卡对应
 * 的计算资源。
 *
 * 超出范围的索引会返回 0，而不是跳过表格进行读取。 */

/* vppu_quota_memory_bytes — 一张显卡的 VRAM 容量以字节为单位，若无则记为0。 */
VPPU_INTERNAL unsigned long long vppu_quota_memory_bytes(int device);

/* vppu_quota_sm_percent — 一张卡的计算容量占百分比，若没有则为0。超过100的数值不是限制，也对应为0。 */
VPPU_INTERNAL unsigned int vppu_quota_sm_percent(int device);

/* vppu_quota_sm_period_ms — 计算控制器的门限窗口，以毫秒为单位。 */
VPPU_INTERNAL unsigned int vppu_quota_sm_period_ms(void);

/* vppu_quota_sm_step_ms — 循环可以迭代的次数频率，以毫秒为单位。 */
VPPU_INTERNAL unsigned int vppu_quota_sm_step_ms(void);

/* vppu_quota_sm_gains — 控制器的收益，每个项默认独立设置，因此调整其中一个无需重新设定其他项。 */
VPPU_INTERNAL void vppu_quota_sm_gains(struct vppu_pid_gains *gains);

/* vppu_quota_graph_weight — 该账本对应的窗口在多少次启动中会被进行记账；1 则让账本启动像其他情况一
 * 样被限制使用。 */
VPPU_INTERNAL unsigned int vppu_quota_graph_weight(void);

/* vppu_quota_validate — 在负载状态下报告一次配置情况，并确认其是否可用。
 *
 * 调用的是发行版库自身的构造函数，而非此处的构造函数，因此顺序清晰明确，不依赖于链接顺序。
 *
 * 它不会终止进程，这明显偏离了“加载失败”的设定：该拦截库通过 /etc/ld.so.preload 加载，因此退出将会
 * 杀死容器中的所有进程——也就是人类用来诊断配置错误的 shell。在加载时报告错误并拒绝后续的所有分配，
 * 结果与不关闭容器是一样的，都是导致系统失败的结局。 */
VPPU_INTERNAL void vppu_quota_validate(void);

/* vppu_quota_usable — 当 vppu_quota_validate() 发现容器配置不可用时为 false：以任何形式都没有内存
 * 数据，或者设置了但无法解析，又或者某张卡有内存数据，但其计算数据缺失或无法解析。在这种情况下，执
 * 行约束路径必须拒绝。
 *
 * 仅使用未索引内存数据设置时，每张卡都是容器所拥有的对象——环境中没有任何信息表明它包含多少数据——因
 * 此未索引计算数据也必须能够正常使用。因此，将未索引数据形式进行组合就是整个配置；将索引化内存数据
 * 形式与未索引计算数据形式进行组合，按每张卡而言，运作方式相同。
 *
 * 计算量仅在控制器强制执行该规则时才会计入。在此之前，只报告了缺失的情况，没有更多内容，因为如果拒
 * 绝该维度上的库实现，系统就会因错误操作而失败。分配器必须即使在 100% 的情况下也明确注入该量：其自
 * 带的辅助函数会将该请求设置为 100，因此被省略的变量与完整卡中的计算量并无区别。 */
VPPU_INTERNAL bool vppu_quota_usable(void);

#endif /* VPPU_COMMON_VPPU_QUOTA_H */

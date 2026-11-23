/* vrocm_hwid.h — 该文件可以显示程序实际运行的详细情况。
 *
 * 这是其自身的模块。有两个组件需要此解码功能，且两者均不拥有此功能：位于 tools/下的掩模探针，以及
 * 位于 testing/下的 soak 功能。两者都会报告占用情况，且必须报告相同的占用情况，否则这对功能就毫无
 * 价值——如果 soak 功能与掩模探针所反映的运行单元不符，就会像掩模故障一样。如果将其集中放在一个头文
 * 件中，对位数的修正会同时生效或均无效。
 *
 * 为什么不是 common/ 这个目录？它的规则是完全不包含任何 GPU 相关的内容，正因如此，其单元测试才能在
 * 没有 ROCm 安装且没有设备的情况下构建和运行。按定义，这个文件就是设备代码。它既不属于账本，也不属
 * 于拦截库，因此拥有自己的目录——这也清楚地表明了其边界：libvrocm.so 中不包含任何来自此目录的内容，
 * 也不应包含，因为该产品没有内核。
 *
 * 寄存器和单位都因架构不同而有所区别，这就是此文件的全部内容。在 gfx9 中，wave 报告 HW_ID，对于具
 * 有多个 XCC 的组件则报告 XCC_ID；而表示任何内容的单位就是 CU。在 gfx10 及更新的版本中，寄存器为
 * HW_ID1，而单位则是 WGP——其测量值中，SIMD_ID 始终只报告 0 或 1，因此一个 WGP 中的两个 CU 在 wave
 * 内部无法被区分开来。这并非一种差异：WGP 实际上就是 RDNA 的分配原子，因此每个架构都以其分配的单元
 * 来读取。
 *
 * 字段位置是通过硬件读取得到的，而非从文档中获取；支撑这些读取的资料位于
 * references/amd-cumask-conformance.md 文件中：在 RDNA 架构下，WGP 计数完全符合掩模要求（未掩模的
 * 为 30，0:0-29 为 15，0:0-13 为 7，0:0-1 为 1），而在 CDNA 架构下，每 XCC CU 的计数与符合性表格中
 * 的每一行数据均一致。 */
#ifndef VROCM_HWID_H
#define VROCM_HWID_H

/* 槽位表是四个坐标的完全叉积结果，因此解码过程无需知道自己运行在哪个部分：XCC_ID为4位，SE_ID为3位，
 * SA/SH为1位，而CU或WGP索引为4位。4096个无符号整数占16 KiB的设备内存，这与没有按架构划分的表大小设
 * 定并无矛盾。 */
#define VROCM_HWID_MAX_XCC 16u
#define VROCM_HWID_MAX_SE 8u
#define VROCM_HWID_MAX_SH 2u
#define VROCM_HWID_MAX_UNIT 16u
#define VROCM_HWID_SLOTS \
    (VROCM_HWID_MAX_XCC * VROCM_HWID_MAX_SE * VROCM_HWID_MAX_SH * VROCM_HWID_MAX_UNIT)

/* vrocm_hwid_slot_xcc — 这是哪个 XCC 的槽位索引，用于主机端的每个 XCC 统计。 */
#define VROCM_HWID_SLOT_XCC(slot) \
    ((slot) / (VROCM_HWID_MAX_SE * VROCM_HWID_MAX_SH * VROCM_HWID_MAX_UNIT))

/* s_getreg 将寄存器 ID、位偏移量和宽度合并为单个立即数，因此整个描述符必须是一个常量表达式，而非运
 * 行时值。 */
#define VROCM_GETREG(id) __builtin_amdgcn_s_getreg(((id) & 0x3f) | (0 << 6) | ((32 - 1) << 11))

#define VROCM_HWREG_HW_ID 4   /* gfx9 的位域为 CU_ID[11:8]、SH_ID[12]、SE_ID[15:13]。 */
#define VROCM_HWREG_XCC_ID 20 /* 仅限 gfx94x/gfx95x 型号 */
#define VROCM_HWREG_HW_ID1 23 /* gfx10 及以上的位域为 SIMD_ID[9:8]、WGP_ID[13:10]、SA_ID[16]、SE_ID[20:18]。 */

#define VROCM_HWID_SLOT(xcc, se, sh, unit)                                          \
    (((((xcc) * VROCM_HWID_MAX_SE) + (se)) * VROCM_HWID_MAX_SH + (sh)) * VROCM_HWID_MAX_UNIT + \
     (unit))

/* vrocm_hwid_slot — 当此波实际运行时，它作为槽位表中的单个索引存在。
 *
 * 该架构由编译器为所构建的目标定义的宏决定，而非由主机所决定： __GFX9__ 涵盖 CDNA，而此处其他所有
 * 版本均为 gfx10 或更高版本。胖二进制文件每个架构包含一个代码对象，加载器会进行选择，因此单次构建
 * 能够读取到可能被指向的每一块芯片上正确的寄存器。 */
__device__ static inline unsigned int vrocm_hwid_slot(void)
{
#if defined(__GFX9__)
    unsigned int hw = VROCM_GETREG(VROCM_HWREG_HW_ID);
    unsigned int unit = (hw >> 8) & 0xfu;  /* CU_ID — 单位 CDNA 的分配量 */
    unsigned int sh = (hw >> 12) & 0x1u;   /* SH_ID */
    unsigned int se = (hw >> 13) & 0x7u;   /* SE_ID, 依据 XCC */
#if defined(__gfx940__) || defined(__gfx941__) || defined(__gfx942__) || defined(__gfx950__)
    unsigned int xcc = VROCM_GETREG(VROCM_HWREG_XCC_ID) & 0xfu;
#else
    /* Single-XCC gfx9 没有 XCC_ID 寄存器，读取不存在的寄存器并非可诊断的错误——它会返回编码所选择的任何
     * 内容。 */
    unsigned int xcc = 0u;
#endif
#else
    unsigned int hw = VROCM_GETREG(VROCM_HWREG_HW_ID1);
    unsigned int unit = (hw >> 10) & 0xfu; /* WGP_ID — 单位 RDNA 所分配的数量 */
    unsigned int sh = (hw >> 16) & 0x1u;   /* SA_ID */
    unsigned int se = (hw >> 18) & 0x7u;   /* SE_ID */
    unsigned int xcc = 0u;
#endif
    return VROCM_HWID_SLOT(xcc, se, sh, unit);
}

#endif /* VROCM_HWID_H */

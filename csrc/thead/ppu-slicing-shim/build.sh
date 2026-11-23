#!/usr/bin/env bash
# build.sh 是这棵源码树的统一构建入口。
#
# build.sh lib   [<name>...]  构建预加载的共享库，默认构建全部库。
# build.sh tool  [<name>...]  构建 tools/ 下的观察工具，默认构建全部工具。
# build.sh test  [<name>...]  构建 testing/ 下的验收程序，默认构建全部程序。
# build.sh unit  编译 common/ 单元测试，无需厂商 SDK 和设备。
# build.sh check v1  使用 hggc.h 对 v1 ABI 函数原型做类型检查。
# build.sh list  <name>  逐行列出指定产物使用的 C 编译单元。
#
# 编译清单、头文件路径和各目标的 SDK 依赖随源码维护。此前六个用例各自保存构建步骤，hggc_mem_paths
# 被编译三遍，其中两处的源码路径已经不同。集中构建后，用例只保留本身的断言。 build.sh 集中保存这些
# 步骤。
#
# 四个共享库、一个观察工具和五个验收程序均使用固定的源文件，SDK 只提供 x86_64-linux。没有生成源码
# 和安装步骤，十次编译器调用即可完成。gpustack/thead-ppu-devel:2.1.1 提供 make、gcc 和 hgcc，但没
# 有 cmake。固定构建使用 Shell，也符合仓库 hack/ 和 pack/ 的做法。
#
# 预加载库需要 PPU SDK 头文件。调用方可以在 gpustack/thead-ppu-devel 内或已安装 SDK 的宿主机上运行
# 脚本。unit 和 tool 不依赖 SDK。
#
# 成功时脚本保持安静，因为验收用例 1 通过空输出来判断编译器没有诊断。设置 V=1 可以跟踪命令。
#
# OUT 指定输出目录，默认是脚本目录。PPU_HOME 默认为 /usr/local/PPU_SDK，CC 默认为 gcc，厂商设备编
# 译器 HGCC 默认为 hgcc；V=1 打印命令。
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-${HERE}}"
PPU_HOME="${PPU_HOME:-/usr/local/PPU_SDK}"
CC="${CC:-gcc}"
HGCC="${HGCC:-hgcc}"

SDK_INC="${PPU_HOME}/targets/x86_64-linux/include"
SDK_LIB="${PPU_HOME}/targets/x86_64-linux/lib"

# -I"${HERE}" 让各模块的源文件均可从本树根目录包含 common/... 与 hggc/... 的头文件。
WARN="-Wall -Wextra"
INCLUDES="-I${HERE} -I${SDK_INC}"

# 这些库预加载到自带 SDK 的容器中，通过 dlsym 在运行时寻找厂商符号。保持 DT_NEEDED 为空或仅依赖
# libc.so.6，也不添加 -ldl。三个源码列表分别链接必要的模块。vppu_quota.c 只引用增益结构体，因此只
# 需包含其头文件。
COMMON_QUOTA="common/vppu_log.c common/vppu_quota.c"
COMMON_LEDGER="${COMMON_QUOTA} common/vppu_ledger.c"
COMMON_ALL="${COMMON_LEDGER} common/vppu_pid.c"

# srcs <artifact> 按编译顺序列出指定产物的源文件。
srcs() {
    case "$1" in
        # 执行配额限制的共享库在驱动层同时处理计算和显存两项预算。
        hggc_quota) echo "hggc/hggc_quota.c hggc/hggc_mem.c hggc/hggc_mem_v1.c hggc/hggc_entry.c \
hggc/hggc_compute.c hggc/hggc_launch.c ${COMMON_ALL}" ;;
        # 负责展示配额的共享库读取与实际限制相同的账本，使 ppu-smi 显示的显存与配额一致。它不参与算力控制。
        hgml_dlsym_hook) echo "hgml/hgml_dlsym_hook.c ${COMMON_LEDGER}" ;;
        # 验收用例 1 的对照库导出相同的 HGML 符号，但不拦截 dlsym，也不链接 common/。
        hgml_nohook) echo "testing/hgml_nohook.c" ;;
        # 第二个 dlsym 拦截库用来验证多个拦截器共存时两项保护是否生效，也不链接 common/。
        dlsym_stack) echo "testing/dlsym_stack.c" ;;

        # 观察工具自行只读映射账本。它从头文件取得布局，不调用可能创建区域或获取卡锁的 common/ 函数。
        ppu_monitor) echo "tools/ppu_monitor.c" ;;

        hgml_util_probe) echo "testing/hgml_util_probe.c" ;;
        hggc_mem_paths) echo "testing/hggc_mem_paths.c" ;;
        dlsym_origin) echo "testing/dlsym_origin.c" ;;
        hggc_launch_load) echo "testing/hggc_launch_load.cu" ;;
        vppu_test) echo "common/vppu_test.c ${COMMON_ALL}" ;;
        *) return 1 ;;
    esac
}

LIBS="hggc_quota hgml_dlsym_hook hgml_nohook dlsym_stack"
TOOLS="ppu_monitor"
TESTS="hgml_util_probe hggc_mem_paths dlsym_origin hggc_launch_load"

run() {
    [ -n "${V:-}" ] && printf '+ %s\n' "$*" >&2
    "$@"
}

# build_lib 在源码树根目录编译一个预加载共享库，使各模块引用头文件的方式一致。
build_lib() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown library '${name}'" >&2; return 2; }
    # shellcheck disable=SC2086  # 源文件和选项列表需要按词拆分
    run "${CC}" -shared -fPIC -O2 ${WARN} ${INCLUDES} -o "${OUT}/${name}.so" ${list}
}

# build_tool 与 build_unit 都不带厂商 SDK 路径和厂商动态库。这让监控工具可以在没有厂商库或设备的容
# 器里运行。命令名 ppu-monitor 使用连字符，与一同挂载的 Ascend 工具 enpu-monitor 一致。
build_tool() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown tool '${name}'" >&2; return 2; }
    # shellcheck disable=SC2086
    run "${CC}" -std=gnu11 -O2 ${WARN} "-I${HERE}" -o "${OUT}/${name//_/-}" ${list}
}

# build_test 构建指定验收程序。它们在厂商镜像里运行，可以链接 SDK。
build_test() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown test '${name}'" >&2; return 2; }

    case "${name}" in
        # 用真实内核才能让 PPU 忙起来，此程序使用厂商设备编译器。
        hggc_launch_load)
            # shellcheck disable=SC2086
            run "${HGCC}" -O2 "-I${SDK_INC}" -o "${OUT}/${name}" ${list} "-L${SDK_LIB}" -lhgml
            ;;
        # dlsym_origin 通过动态加载器查询实际提供符号的共享库，需要 libdl，无需 SDK。
        dlsym_origin)
            # shellcheck disable=SC2086
            run "${CC}" -O2 ${WARN} -o "${OUT}/${name}" ${list} -ldl
            ;;
        hgml_util_probe)
            # shellcheck disable=SC2086
            run "${CC}" -O2 ${WARN} "-I${SDK_INC}" -o "${OUT}/${name}" ${list} \
                "-L${SDK_LIB}" -lhgml -lhggc -ldl
            ;;
        hggc_mem_paths)
            # shellcheck disable=SC2086
            run "${CC}" -O2 ${WARN} "-I${SDK_INC}" -o "${OUT}/${name}" ${list} \
                "-L${SDK_LIB}" -lhggc -lhggcrt1
            ;;
        *) echo "build.sh: no test recipe for '${name}'" >&2; return 2 ;;
    esac
}

# build_unit 不提供任何 SDK 头文件路径。这样编译也能通过，才说明 common/ 可独立于设备测试。
build_unit() {
    local list
    list="$(srcs vppu_test)"
    # shellcheck disable=SC2086
    run "${CC}" -std=gnu11 -O2 ${WARN} "-I${HERE}" -o "${OUT}/vppu_test" ${list}
}

# 正常构建会取消头文件对带版本符号的映射，因此无法顺带验证原始 v1 ABI 原型。check_v1 在仅做语法检
# 查时同时定义下面两个宏，让 hggc.h 提供原型；产品构建不能使用它们，因为它们还会影响其它声明。缺少
# 第二个宏时声明仍不可见，检查会无效。
check_v1() {
    # shellcheck disable=SC2086
    run "${CC}" -fsyntax-only ${WARN} \
        -D__HGGC_API_VERSION_INTERNAL -D__HGGC_API_VERSION_UMD \
        ${INCLUDES} "${HERE}/hggc/hggc_mem_v1.c"
}

cd "${HERE}" || exit 1
verb="${1:-}"
shift 2>/dev/null || true

case "${verb}" in
    lib)
        names="$*"
        [ -n "${names}" ] || names="${LIBS}"
        for name in ${names}; do build_lib "${name}" || exit $?; done
        ;;
    tool)
        names="$*"
        [ -n "${names}" ] || names="${TOOLS}"
        for name in ${names}; do build_tool "${name}" || exit $?; done
        ;;
    test)
        names="$*"
        [ -n "${names}" ] || names="${TESTS}"
        for name in ${names}; do build_test "${name}" || exit $?; done
        ;;
    unit)
        build_unit || exit $?
        ;;
    check)
        case "${1:-}" in
            v1) check_v1 || exit $? ;;
            *) echo "build.sh: unknown check '${1:-}' (expect v1)" >&2; exit 2 ;;
        esac
        ;;
    list)
        # 先捕获结果再打印。管道只返回最后一个命令的状态，直接接 tr 会把未知产物的错误掩盖。
        list="$(srcs "${1:-}")" || { echo "build.sh: unknown artifact '${1:-}'" >&2; exit 2; }
        # shellcheck disable=SC2086  # 每行一个编译单元，需要按词拆分
        printf '%s\n' ${list}
        ;;
    *)
        sed -n '3,10p' "$0" >&2
        exit 2
        ;;
esac

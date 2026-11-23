#!/usr/bin/env bash
# build.sh 是这棵源码树的统一构建入口。
#
# build.sh lib   [<name>...]  构建预加载的共享库，默认构建全部库。
# build.sh tool  [<name>...]  构建 tools/ 下的观察工具，默认构建全部工具。
# build.sh test  [<name>...]  构建 testing/ 下的验收程序，默认构建全部程序。
# build.sh unit  [mutants]  编译 common/ 单元测试，无需 ROCm 和设备。可选 mutants 用来验证测试能检
# 出缺陷。
# build.sh check [<name>]  检查指定产物的四项链接约束，验收用例 1 也执行这些检查。
# build.sh list  <name>  逐行列出指定产物使用的 C 编译单元。
#
# 编译单元清单、头文件根目录、各目标能否包含 ROCm 头文件，都应该随源码维护。此前 THead 的七个脚本
# 各自复制构建步骤，后来指向了不同源文件。这里统一构建，验收脚本只负责断言。
#
# 本树的一个共享库、两个工具和四个验收程序均由固定的 C 文件构成，没有生成源码和安装步骤，ROCm 用户
# 态也只针对 x86_64。七条编译命令即可完成构建，无需另建 CMake 配置阶段。仓库在 hack/ 和 pack/ 下也
# 使用 Shell 描述构建。
#
# build.sh 在各部分源码到位前统一声明所有产物，使五项并行的开发工作共享构建清单。unit 和 list 当时
# 就能运行；lib、tool、test 则要等对应源文件就绪。
#
# hip/ 和 testing/ 的相关产物需要 HIP 头文件。scripts/build.sh xbuild-amd-rocm 在 ROCm 开发镜像内
# 构建；安装了 ROCm 的宿主机也可直接执行。unit 无需 SDK 或设备，各工具的具体依赖见 build_tool。
#
# 成功时脚本保持安静，因为验收用例 1 通过空输出来判断编译器没有诊断。设置 V=1 可以跟踪命令。
#
# OUT 指定输出目录，默认为脚本目录。ROCM_PATH 默认为 /opt/rocm，CC 默认为 gcc，OFFLOAD_ARCH 指定设
# 备代码架构，V=1 打印命令。
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-${HERE}}"
ROCM_PATH="${ROCM_PATH:-/opt/rocm}"
CC="${CC:-gcc}"

# 读取硬件寄存器的设备代码需要为每种目标架构提供对应的代码对象，交由加载器选择。显式列出已经有掩码
# 实测数据的架构及相近型号。若依赖 hipcc 自动探测，有卡构建机会只面向其本地型号，无卡构建机则可能
# 静默选错默认架构。
OFFLOAD_ARCH="${OFFLOAD_ARCH:-gfx90a gfx942 gfx1100 gfx1101 gfx1102}"

ROCM_INC="${ROCM_PATH}/include"

# -I"${HERE}" 让各目录的源文件都能从本树根目录包含 common/... 或 hip/... 的头文件。
WARN="-Wall -Wextra"
STD="-std=gnu11"

# HIP 头文件要求显式选定平台。缺少该宏时，第一个 #error 之后往往出现大量无法识别类型的诊断。
HIP_PLATFORM="-D__HIP_PLATFORM_AMD__"

# 预加载库用 initial-exec 模型访问线程局部状态，避免引入 __tls_get_addr。通过 /etc/ld.so.preload
# 加载的库从一开始就在链接映射中，所以可以使用 initial-exec。实测 general-dynamic 会让 DT_NEEDED
# 同时包含 libc.so.6 和 ld-linux-x86-64.so.2，initial-exec 则只需要 libc.so.6；添加 -ldl 不会解决
# 线程局部访问造成的依赖。
TLS_MODEL="-ftls-model=initial-exec"

# 共享库只链接 libc。工作负载自带 ROCm，所有运行时符号在容器内解析。不要添加 -ldl，现代 glibc 中它
# 是空存根，旧版上却会多出一条 DT_NEEDED 依赖。
COMMON_ALL="common/vrocm_log.c common/vrocm_quota.c common/vrocm_ledger.c"

# srcs <artifact> 按编译顺序列出指定产物的源文件。
srcs() {
    case "$1" in
        # 预加载库涵盖符号解析、拦截表、三个容量查询入口、常规分配以及流和内存池分配。各类入口在自己的编译
        # 单元注册。
        libvrocm) echo "hip/hip_resolve.c hip/hip_table.c hip/hip_query.c hip/hip_mem.c \
hip/hip_pool.c hip/hip_vmm.c ${COMMON_ALL}" ;;

        # 用量观察工具只读映射已存在的共享区域。common/ 的辅助函数在文件缺失时会创建它，其他入口还会获取卡
        # 锁，所以工具只引用头文件里的布局。
        rocm_monitor) echo "tools/rocm_monitor.c" ;;
        # CU 掩码自检工具使用 HSA 拓扑 API 和自己的内核，既不记账，也无需链接 common/。
        rocm_cumask_check) echo "tools/rocm_cumask_check.c" ;;

        hip_mem_paths) echo "testing/hip_mem_paths.c" ;;
        hip_props_probe) echo "testing/hip_props_probe.c" ;;
        cumask_soak) echo "testing/cumask_soak.c" ;;
        ledger_lifecycle) echo "testing/ledger_lifecycle.c" ;;

        vrocm_test) echo "common/vrocm_test.c ${COMMON_ALL}" ;;
        *) return 1 ;;
    esac
}

LIBS="libvrocm"
TOOLS="rocm_monitor rocm_cumask_check"
TESTS="hip_mem_paths hip_props_probe cumask_soak ledger_lifecycle"

run() {
    [ -n "${V:-}" ] && printf '+ %s\n' "$*" >&2
    "$@"
}

# offload_flags 将 OFFLOAD_ARCH 转成 hipcc 所需的架构选项。
offload_flags() {
    local arch
    for arch in ${OFFLOAD_ARCH}; do printf -- '--offload-arch=%s ' "${arch}"; done
}

# build_lib 从根目录编译预加载库，保持模块头文件路径一致。HIP 头文件只用于核对包装函数签名和
# offsetof() 等编译期信息，不能因此增加 DT_NEEDED；check 会核验实际链接结果。
build_lib() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown library '${name}'" >&2; return 2; }
    # shellcheck disable=SC2086  # 源文件和选项列表需要按词拆分
    run "${CC}" -shared -fPIC -O2 ${STD} ${WARN} -fvisibility=hidden ${TLS_MODEL} ${HIP_PLATFORM} \
        "-I${HERE}" "-I${ROCM_INC}" -o "${OUT}/${name}.so" ${list}
}

# build_tool 编译指定工具。rocm-monitor 无需 ROCm 头文件；掩码探针使用 HSA 拓扑 API。命令名使用连
# 字符，与同类工具 ppu-monitor 和 enpu-monitor 保持一致。
build_tool() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown tool '${name}'" >&2; return 2; }

    case "${name}" in
        rocm_monitor)
            # shellcheck disable=SC2086
            run "${CC}" ${STD} -O2 ${WARN} "-I${HERE}" -o "${OUT}/${name//_/-}" ${list}
            ;;
        # 掩码探针含设备内核，使用 hipcc -x hip 编译。否则 hipcc 会将 .c 文件按 C 处理，实测连 HIP 头文件
        # 目录都不会加入。除了 hipcc 链接的 HIP 运行时，还要链接 libhsa-runtime64 以读取拓扑。
        rocm_cumask_check)
            # shellcheck disable=SC2086,SC2046
            run "${ROCM_PATH}/bin/hipcc" -O2 ${WARN} -x hip $(offload_flags) "-I${HERE}" \
                -o "${OUT}/${name//_/-}" ${list} \
                "-L${ROCM_PATH}/lib" -lhsa-runtime64 "-Wl,-rpath,${ROCM_PATH}/lib"
            ;;
        *) echo "build.sh: no tool recipe for '${name}'" >&2; return 2 ;;
    esac
}

# build_test 每次构建一个验收程序。它们运行在 AMD 主机上，可以链接 HIP，在编译和链接时检查头文件的
# R0600 符号映射和函数签名。
build_test() {
    local name="$1" list
    list="$(srcs "${name}")" || { echo "build.sh: unknown test '${name}'" >&2; return 2; }

    case "${name}" in
        # ledger_lifecycle 让一个实际进程占用额度后接收 SIGKILL，用来验证 common/ 能回收该进程的记账。因此
        # 它直接链接 common/，无需 ROCm、预加载库或设备，在无卡机器也能运行。
        ledger_lifecycle)
            # shellcheck disable=SC2086
            run "${CC}" ${STD} -O2 ${WARN} "-I${HERE}" -o "${OUT}/${name}" ${list} ${COMMON_ALL}
            ;;
        # cumask_soak 需要真正的内核占用 CU，和掩码探针一样通过 hipcc -x hip 编译。
        cumask_soak)
            # shellcheck disable=SC2086,SC2046
            run "${ROCM_PATH}/bin/hipcc" -O3 ${WARN} -x hip $(offload_flags) "-I${HERE}" \
                -o "${OUT}/${name}" ${list}
            ;;
        *)
            # shellcheck disable=SC2086
            run "${CC}" ${STD} -O2 ${WARN} ${HIP_PLATFORM} "-I${HERE}" "-I${ROCM_INC}" \
                -o "${OUT}/${name}" ${list} "-L${ROCM_PATH}/lib" -lamdhip64 "-Wl,-rpath,${ROCM_PATH}/lib"
            ;;
    esac
}

# build_unit 编译 common/ 的单元测试，不引入 ROCm 头文件。这也检查了配额和账本逻辑可以脱离驱动独立
# 测试。
build_unit() {
    local list out="${1:-${OUT}/vrocm_test}" srcdir="${2:-${HERE}}"
    list="$(srcs vrocm_test)"
    # shellcheck disable=SC2086
    run "${CC}" ${STD} -O2 ${WARN} "-I${srcdir}" -o "${out}" $(
        for f in ${list}; do printf '%s ' "${srcdir}/${f}"; done
    )
}

# 产物的四项链接检查。
#
# 构建和验收用例 1 都检查这些约束，使依赖或符号导出错误能在构建时就暴露。

# max_glibc 输出目标文件要求的最高 GLIBC_ 版本。使用版本排序而非字符串排序，确保 GLIBC_2.4 与
# GLIBC_2.34 按数值大小比较。
max_glibc() {
    objdump -T "$1" 2>/dev/null | grep -oE 'GLIBC_[0-9.]+' | sort -uV | tail -1
}

check_artifact() {
    local so="$1" bad=0 max needed undef exported tool

    if [ ! -f "${so}" ]; then
        echo "build.sh: ${so} has not been built" >&2
        return 2
    fi

    # 检查依赖这些读取 ELF 的工具。它们的标准错误会被重定向，先确认命令存在，避免把缺少工具的空输出误
    # 判为检查成功。
    for tool in readelf objdump nm; do
        if ! command -v "${tool}" > /dev/null 2>&1; then
            echo "build.sh: ${tool} is not installed; the artifact checks would pass vacuously" >&2
            return 2
        fi
    done

    # 1. 只导出被拦截的 HIP 入口。用 readelf --dyn-syms 筛选已定义的全局符号，排除编译器内部符号及导入
    # 项。
    exported="$(readelf -W --dyn-syms "${so}" 2>/dev/null |
        awk '$4 == "FUNC" && $5 == "GLOBAL" && $6 == "DEFAULT" && $7 != "UND" { print $8 }' |
        sed 's/@.*//' | sort -u | grep -v '^hip' )"
    if [ -n "${exported}" ]; then
        echo "build.sh: ${so} exports symbols that are not interposed HIP entry points:" >&2
        printf '  %s\n' ${exported} >&2
        bad=1
    fi

    # 2. DT_NEEDED 只允许 libc。额外依赖库可能不存在于工作负载镜像。
    needed="$(readelf -d "${so}" 2>/dev/null | grep NEEDED | grep -oE '\[[^]]+\]' | tr -d '[]')"
    if [ "${needed}" != "libc.so.6" ]; then
        echo "build.sh: ${so} needs '${needed}', expected exactly libc.so.6" >&2
        bad=1
    fi

    # 3. 核对最低 glibc 版本。dlopen、dlsym 和 dladdr 在 GLIBC_2.34 移入 libc，缺少 .symver 固定版本会
    # 抬高此库的运行要求。
    max="$(max_glibc "${so}")"
    if [ -n "${max}" ] && [ "${max}" != "GLIBC_2.4" ] &&
       [ "$(printf '%s\nGLIBC_2.4\n' "${max}" | sort -V | tail -1)" != "GLIBC_2.4" ]; then
        echo "build.sh: ${so} requires ${max}, above the GLIBC_2.4 floor" >&2
        bad=1
    fi

    # 4. 检查没有链接任何 ROCm 共享库，同一构建产物才能适配容器自己提供的 ROCm。
    undef="$(nm -D -u "${so}" 2>/dev/null | grep -cE ' (hip|hsa)[A-Za-z_]' )"
    if [ "${undef}" != "0" ]; then
        echo "build.sh: ${so} has ${undef} undefined hip*/hsa* symbols; it must resolve them at run time" >&2
        bad=1
    fi

    return "${bad}"
}

# 变异测试。
#
# 分别破坏 common/ 的四项约束，重跑测试并要求对应测试项报告 FAIL。若错误实现仍然通过，说明该测试没
# 有检出它要验证的问题。
mutants() {
    local tmp rc=0
    tmp="$(mktemp -d)" || return 2
    trap 'rm -rf "${tmp}"' RETURN

    # <标识> <应当失败的测试项> <应用于 common/vrocm_ledger.c 的 sed 替换式>
    while IFS='|' read -r label row program; do
        [ -n "${label}" ] || continue
        local dir="${tmp}/${label}"

        mkdir -p "${dir}/common"
        cp "${HERE}"/common/*.h "${dir}/common/"
        for f in "${HERE}"/common/*.c; do cp "${f}" "${dir}/common/"; done
        sed -e "${program}" < "${HERE}/common/vrocm_ledger.c" > "${dir}/common/vrocm_ledger.c"

        if ! build_unit "${dir}/vrocm_test" "${dir}" 2>"${dir}/build.log"; then
            echo "build.sh: mutant '${label}' does not compile:" >&2
            cat "${dir}/build.log" >&2
            rc=1
            continue
        fi
        "${dir}/vrocm_test" > "${dir}/run.log" 2>&1
        if ! grep -q "^FAIL | ${row} |" "${dir}/run.log"; then
            echo "build.sh: mutant '${label}' did not fail '${row}' -- that test proves nothing" >&2
            rc=1
        fi
    # 替换式依赖所匹配的源码文本。源码改名或改签名让替换失效时，测试会报告预期的失败未出现。曾有一次回
    # 调签名变化使拆锁变异失效，就是由此发现的。
    #
    # 每条 sed 程序只匹配一行，并在必要时限定函数范围。跨行匹配可能静默失败。替换文本中的反斜线 n 在
    # GNU sed 和 BSD sed 上的行为也不同，前者写入换行，后者可能只留下字母 n。
    done <<'MUTANTS'
quota-frozen|ledger/quota_reread_on_attach|s#usage->memory_quota_bytes = quota;#if (usage->memory_quota_bytes != 0) { quota = usage->memory_quota_bytes; } else { usage->memory_quota_bytes = quota; }#
lock-split|ledger/check_allocate_charge_under_one_lock|s#if (!alloc(ctx, &key, &charged)) {#ledger_unlock(device); if (!ledger_lock(device)) { slot_free(index); return VROCM_ADMIT_DENIED_CONFIG; } if (!alloc(ctx, \&key, \&charged)) {#
tracking-silent|ledger/tracking_insert_is_fail_closed|/^static int slot_reserve/,/^}/ s#return -1;#return 0;#
no-sweep|ledger/dead_charge_swept|s#if (kill((pid_t)slot->pid, 0) < 0 \&\& errno == ESRCH) {#if (0) {#
MUTANTS

    return "${rc}"
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
        case "${1:-}" in
            "") build_unit || exit $? ;;
            mutants) mutants || exit $? ;;
            *) echo "build.sh: unknown unit argument '${1}' (expect nothing or 'mutants')" >&2
               exit 2 ;;
        esac
        ;;
    check)
        names="$*"
        [ -n "${names}" ] || names="${LIBS}"
        for name in ${names}; do check_artifact "${OUT}/${name}.so" || exit $?; done
        ;;
    list)
        # 先捕获结果再打印，避免管道最后的 tr 掩盖未知产物的错误。rocm-monitor 等产物以连字符命名，源文件
        # 则用下划线，list 接受两者。先保存参数再替换，缺少参数时可正常报错，避免 set -u 因直接展开
        # ${1//-/_} 而终止。
        want="${1:-}"
        list="$(srcs "${want}")" || list="$(srcs "${want//-/_}")" ||
            { echo "build.sh: unknown artifact '${want}'" >&2; exit 2; }
        # shellcheck disable=SC2086  # 每行一个编译单元，需要按词拆分
        printf '%s\n' ${list}
        ;;
    *)
        # 使用 "${HERE}/build.sh"。此前已经切换工作目录，脚本以相对路径启动时，原来的 $0 可能已无法定位脚
        # 本。
        sed -n '3,10p' "${HERE}/build.sh" >&2
        exit 2
        ;;
esac

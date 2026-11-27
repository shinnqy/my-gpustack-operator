#!/usr/bin/env bash
#
# 参数依次为 ROCm 切片源码目录、产物目录。
#
# 使用 ROCm 开发镜像编译 csrc/amd/rocm-slicing-shim 下的源码，产物写到指定输出目录。
#
# libvrocm.so 在 HIP 层执行配额，rocm-monitor 查询用量，rocm-cumask-check 验证计算单元掩码。
#
# 由源码目录自己的 build.sh 决定编译单元、头文件和链接依赖，这里只负责调用与交付。
#
# 此脚本只在构建镜像时使用。
#
set -exo pipefail

src="${1:?usage: build-libvrocm.sh <src-dir> <out-dir>}"
out="${2:?usage: build-libvrocm.sh <src-dir> <out-dir>}"

mkdir -p "${out}"

# 源码以只读方式挂载，lib 和 tool 两次构建都将产物写入 OUT。
OUT="${out}" bash "${src}/build.sh" lib
OUT="${out}" bash "${src}/build.sh" tool

# 调用源码自己的 check 目标验证导出符号、动态依赖、glibc 兼容性和切片库的链接约束。
OUT="${out}" bash "${src}/build.sh" check

# 对两个可执行文件记录其动态依赖及 glibc 要求。监控和自检工具的启动代码及所需运行库与预加载库不同。
record_linkage() {
    local path="$1" needed max_glibc

    needed="$(readelf -d "${path}" | awk '/NEEDED/ {gsub(/[][]/, "", $NF); print $NF}' | sort -u | tr '\n' ' ')"
    echo "${path}: NEEDED = ${needed:-none}"

    # 匹配完整的 glibc 版本号，包括补丁位。没有版本化符号时仍可继续输出检查结果。
    max_glibc="$(readelf -W --dyn-syms "${path}" | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' | sort -uV | tail -1 || true)"
    echo "${path}: max glibc requirement = ${max_glibc:-none}"
}

[[ -f "${out}/libvrocm.so" ]]

for artifact in "${out}/rocm-monitor" "${out}/rocm-cumask-check"; do
    # 两个工具都要有执行权限。
    [[ -x "${artifact}" ]]
    record_linkage "${artifact}"
done

ls -la "${out}"

#!/usr/bin/env bash
#
# 参数依次为平头哥切片源码目录、产物目录。
#
# 在 PPU SDK 开发镜像中编译 csrc/thead/ppu-slicing-shim 下的源码。
#
# hggc_quota.so 执行配额，hgml_dlsym_hook.so 让 ppu-smi 显示受限后的资源视图，ppu-monitor 提供用量读数。
#
# 调用源码自带的构建入口，沿用编译单元、头文件与链接约束。
#
# 此脚本只在构建镜像时使用。
#
set -exo pipefail

src="${1:?usage: build-libvppu.sh <src-dir> <out-dir>}"
out="${2:?usage: build-libvppu.sh <src-dir> <out-dir>}"

mkdir -p "${out}"

# 源码可只读挂载，所有构建产物写到 OUT。
OUT="${out}" bash "${src}/build.sh" lib hggc_quota hgml_dlsym_hook
OUT="${out}" bash "${src}/build.sh" tool ppu_monitor

# 检查依赖库和 glibc 版本上限，使产物可装入使用较旧系统库的工作负载。
#
# ppu-monitor 也会进入工作负载容器，因此必须满足相同的兼容要求。
assert_linkage() {
    local path="$1" needed max_glibc highest

    needed="$(readelf -d "${path}" | awk '/NEEDED/ {gsub(/[][]/, "", $NF); print $NF}' | sort -u | tr '\n' ' ')"
    echo "${path}: NEEDED = ${needed:-none}"
    # 只允许依赖 libc。HGGC 和 HGML 的厂商符号在工作负载 SDK 中通过 dlsym 动态解析。
    [[ -z "${needed}" || "${needed}" == "libc.so.6 " ]]

    # 用完整版本号比较 glibc 兼容上限，保留补丁位。没有版本化符号时也允许完成检查。
    max_glibc="$(readelf -W --dyn-syms "${path}" | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' | sort -uV | tail -1 || true)"
    echo "${path}: max glibc requirement = ${max_glibc:-none}"
    highest="$(printf '%s\nGLIBC_2.17\n' "${max_glibc:-GLIBC_2.2.5}" | sort -uV | tail -1)"
    [[ "${highest}" == "GLIBC_2.17" ]]
}

for artifact in "${out}/hggc_quota.so" "${out}/hgml_dlsym_hook.so" "${out}/ppu-monitor"; do
    [[ -f "${artifact}" ]]
    assert_linkage "${artifact}"
done

# 给需要从命令行运行的 ppu-monitor 验证执行权限。
[[ -x "${out}/ppu-monitor" ]]

ls -la "${out}"

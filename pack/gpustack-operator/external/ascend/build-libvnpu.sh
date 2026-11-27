#!/usr/bin/env bash
#
# 参数依次为 vcann-rt 源码目录、产物目录。
#
# 使用对应芯片的 CANN 镜像编译 libvruntime.so 和 enpu-monitor，构建前对固定版本的 vcann-rt 源树应用随项目交付的补丁。
# 库安装在输出目录的 lib/libvruntime.so，工具在 tools/enpu-monitor。
#
# 此脚本只在构建镜像时使用。
#
set -exo pipefail

src="${1:?usage: build-libvnpu.sh <src-dir> <out-dir>}"
out="${2:?usage: build-libvnpu.sh <src-dir> <out-dir>}"

# 优先使用 CANN 镜像提供的 ASCEND_HOME_PATH，未设置时取默认工具链目录。
export ASCEND_HOME_PATH="${ASCEND_HOME_PATH:-/usr/local/Ascend/ascend-toolkit/latest}"

# 复制到可写工作目录，再打补丁和编译。
work="/tmp/vcann-rt"
rm -rf "${work}"
mkdir -p "${work}"
cp -r "${src}/." "${work}/"
cd "${work}"

# 从只读挂载读取所有 vcann-rt 补丁。缺少补丁或应用失败就中止，避免产出缺失分片显存视图的运行库。
patches_dir="/vcann-rt-patches"
patches=("${patches_dir}"/*.patch)
if [ ! -d "${patches_dir}" ] || [ ! -f "${patches[0]}" ]; then
    echo "[ERROR] no vendored patches under ${patches_dir}: the build stage must bind-mount pack/gpustack-operator/external/ascend/vcann-rt there." >&2
    exit 1
fi
for p in "${patches[@]}"; do
    patch -p1 --batch --forward <"${p}"
done

# 构建机没有主机 HDK 驱动时，用源码提供的 dcmi 链接桩解析弱符号。桩仅用于链接，最终只交付实际运行库和监控程序，运行时使用节点驱动。
fake_driver="/opt/enpu-fakedriver"
mkdir -p "${fake_driver}/driver/include" "${fake_driver}/driver/lib64/driver"
cp test/stub/dcmi_interface_api.h "${fake_driver}/driver/include/"
gcc -shared -fPIC -I"${fake_driver}/driver/include" -Wl,-soname,libdcmi.so \
    test/stub/dcmi_stub.c -o "${fake_driver}/driver/lib64/driver/libdcmi.so"
export ENPU_ASCEND_DRIVER_PATH="${fake_driver}"

# 允许 SDK 共享库中需要由主机驱动提供的间接符号在运行时解析。enpu-monitor 自身直接依赖仍由链接器检查。
export LDFLAGS="-Wl,--allow-shlib-undefined ${LDFLAGS:-}"

bash make_build.sh

mkdir -p "${out}/lib" "${out}/tools"
install -m 0644 build/libvruntime.so "${out}/lib/libvruntime.so"
install -m 0755 build/enpu-monitor "${out}/tools/enpu-monitor"
ls -la "${out}/lib/libvruntime.so" "${out}/tools/enpu-monitor"

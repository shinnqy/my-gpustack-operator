#!/usr/bin/env bash
#
# 参数依次为 HAMi-core 源码目录、产物目录。
#
# 用 CUDA 开发镜像编译固定源码归档中的 HAMi-core，将 libvgpu.so 安装到指定输出目录。
#
# 编译使用 CUDA_HOME 内的头文件。运行时挂接 CUDA 驱动与 NVML 接口，无需把 CUDA 库静态链接进预加载库。
#
# 此脚本只在构建镜像时使用。
#
set -exo pipefail

src="${1:?usage: build-libvgpu.sh <src-dir> <out-dir>}"
out="${2:?usage: build-libvgpu.sh <src-dir> <out-dir>}"

export CUDA_HOME="${CUDA_HOME:-/usr/local/cuda}"

# 复制到可写工作目录，再打补丁和编译。
work="/tmp/hami-core"
rm -rf "${work}"
mkdir -p "${work}"
cp -r "${src}/." "${work}/"
cd "${work}"

bash build.sh

mkdir -p "${out}"
install -m 0644 build/libvgpu.so "${out}/libvgpu.so"
ls -la "${out}/libvgpu.so"

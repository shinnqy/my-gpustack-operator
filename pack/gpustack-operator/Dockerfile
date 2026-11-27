ARG GO_VERSION="1.27.3"
ARG HELM_VERSION="3.21.4"
ARG VALS_VERSION="0.50.0"
ARG KUSTOMIZE_VERSION="5.8.3"
ARG YTT_VERSION="0.55.4"
ARG KUBECTL_VERSION="1.36.5"
ARG GPUSTACK_DATA_DIR="/var/lib/gpustack"
ARG GPUSTACK_CONF_DIR="/etc/gpustack"
ARG GPUSTACK_CHARTS_DIR="${GPUSTACK_CONF_DIR}/charts"
ARG GPUSTACK_LIB_DIR="${GPUSTACK_CONF_DIR}/lib"
ARG UBUNTU_IMAGE="ubuntu:24.04@sha256:c4a8d5503dfb2a3eb8ab5f807da5bc69a85730fb49b5cfca2330194ebcc41c7b"

# 三份固定的源码归档供实际编译使用。BuildKit 在解包前校验 SHA-256。
FROM scratch AS skopeo-source
ADD --checksum=sha256:6a3f342a327ec7684198eb0e15114940b6101506db48d9a96590f5942cb3b335 https://codeload.github.com/podman-container-tools/skopeo/tar.gz/refs/tags/v1.24.1 /source.tar.gz

FROM scratch AS ascend-source
ADD --checksum=sha256:ab905be34948f88f5911b76d39c37e6b47efbd2b70a8e1026ed9f05d0b21689e https://codeload.github.com/openeuler-mirror/ubs-virt/tar.gz/19a817e58e4c68bbfef6f4e2326e4877db9fc5f3 /source.tar.gz

FROM scratch AS hami-source
ADD --checksum=sha256:f578852727560f4f3877b02a345354f56507420f630d1b8e7d60382052528741 https://codeload.github.com/Project-HAMi/HAMi-core/tar.gz/ec5d85a3d709e5ed138a1668ebfefd366c05ca1e /source.tar.gz

# 安装最终镜像运行时需要的命令行工具。
FROM ${UBUNTU_IMAGE} AS tools
SHELL ["/bin/bash", "-c"]

ARG TARGETOS
ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
    set -exo pipefail
    apt-get update -y
    apt-get install -y --no-install-recommends ca-certificates aria2
    rm -rf /var/lib/apt/lists/*
    mkdir -p /out/bin
EOF

ARG HELM_VERSION
RUN <<EOF
    set -exo pipefail
    aria2c -x $(nproc) -s $(nproc) -c -o /tmp/helm.tar.gz "https://get.helm.sh/helm-v${HELM_VERSION}-${TARGETOS}-${TARGETARCH}.tar.gz"
    tar -xzf /tmp/helm.tar.gz --directory /out/bin/ ${TARGETOS}-${TARGETARCH}/helm --no-same-owner --strip-components=1
    chmod a+x /out/bin/helm
    rm -f /tmp/helm.tar.gz
EOF

ARG VALS_VERSION
RUN <<EOF
    set -exo pipefail
    aria2c -x $(nproc) -s $(nproc) -c -o /tmp/vals.tar.gz "https://github.com/helmfile/vals/releases/download/v${VALS_VERSION}/vals_${VALS_VERSION}_${TARGETOS}_${TARGETARCH}.tar.gz"
    tar -xzf /tmp/vals.tar.gz --directory /out/bin/ vals --no-same-owner
    chmod a+x /out/bin/vals
    rm -f /tmp/vals.tar.gz
EOF

ARG KUSTOMIZE_VERSION
RUN <<EOF
    set -exo pipefail
    aria2c -x $(nproc) -s $(nproc) -c -o /tmp/kustomize.tar.gz "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${KUSTOMIZE_VERSION}/kustomize_v${KUSTOMIZE_VERSION}_${TARGETOS}_${TARGETARCH}.tar.gz"
    tar -xzf /tmp/kustomize.tar.gz --directory /out/bin/ kustomize --no-same-owner
    chmod a+x /out/bin/kustomize
    rm -f /tmp/kustomize.tar.gz
EOF

ARG YTT_VERSION
RUN <<EOF
    set -exo pipefail
    aria2c -x $(nproc) -s $(nproc) -c -o /out/bin/ytt "https://github.com/carvel-dev/ytt/releases/download/v${YTT_VERSION}/ytt-${TARGETOS}-${TARGETARCH}"
    chmod a+x /out/bin/ytt
EOF

ARG KUBECTL_VERSION
RUN <<EOF
    set -exo pipefail
    aria2c -x $(nproc) -s $(nproc) -c -o /out/bin/kubectl "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/${TARGETOS}/${TARGETARCH}/kubectl"
    chmod a+x /out/bin/kubectl
EOF

# 使用 Debian 12 的 Go 工具链，保证编译出的程序可在最终的 Ubuntu 24.04 镜像加载。
FROM golang:${GO_VERSION}-bookworm AS build-env
SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive GOPROXY=https://proxy.golang.org GOVCS=*:off GOFLAGS=-buildvcs=false
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libgpgme-dev libassuan-dev libbtrfs-dev libdrm-dev \
    && rm -rf /var/lib/apt/lists/*

FROM build-env AS skopeo
RUN --mount=type=bind,from=skopeo-source,source=/source.tar.gz,target=/tmp/skopeo.tar.gz <<EOF
    set -euo pipefail
    mkdir -p /tmp/skopeo
    tar -xzf /tmp/skopeo.tar.gz --strip-components=1 -C /tmp/skopeo
    cd /tmp/skopeo
    GOFLAGS="-buildvcs=false -mod=vendor" DISABLE_DOCS=1 PREFIX=/out make install-binary
    /out/bin/skopeo --version
EOF

# 这里挂载学习目录本身。二进制的版本和随镜像交付的 chart 使用相同的版本号。
FROM build-env AS operator
ARG GPUSTACK_GIT_VERSION=v0.0.0-guide
ARG GPUSTACK_GIT_COMMIT
ARG BUILDPLATFORM
ARG TARGETPLATFORM
WORKDIR /src
RUN --mount=type=bind,target=/src \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked <<EOF
    set -euo pipefail
    mkdir -p /out/bin
    if [[ "${BUILDPLATFORM}" == "${TARGETPLATFORM}" ]]; then
        make test
    fi
    source_id="${GPUSTACK_GIT_COMMIT:-}"
    if [[ -z "${source_id}" ]]; then
        source_id="sha256-$(find cmd api pkg binding staging go.mod go.sum -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -d' ' -f1)"
    fi
    make build OUT=/out/bin/gpustack-operator VERSION="${GPUSTACK_GIT_VERSION}" COMMIT="${source_id}"
    /out/bin/gpustack-operator --version
EOF

# 用对应 CANN 的工具链编译 vcann-rt，并应用项目内的预加载补丁。
FROM quay.io/ascend/cann:8.5.0-910b-ubuntu22.04-py3.11 AS xbuild-ascend-cann-8-910b
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

FROM quay.io/ascend/cann:8.5.0-a3-ubuntu22.04-py3.11 AS xbuild-ascend-cann-8-910c
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

FROM quay.io/ascend/cann:9.1.0-910b-ubuntu22.04-py3.12 AS xbuild-ascend-cann-9-910b
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

FROM quay.io/ascend/cann:9.1.0-a3-ubuntu22.04-py3.12 AS xbuild-ascend-cann-9-910c
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

FROM quay.io/ascend/cann:9.1.0-950-ubuntu22.04-py3.12 AS xbuild-ascend-cann-9-950
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

FROM quay.io/ascend/cann:9.1.0-310p-ubuntu22.04-py3.12 AS xbuild-ascend-cann-9-310p
RUN --mount=type=bind,from=ascend-source,source=/source.tar.gz,target=/tmp/ubs.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/build-libvnpu.sh,target=/build-libvnpu.sh \
    --mount=type=bind,src=pack/gpustack-operator/external/ascend/vcann-rt,target=/vcann-rt-patches <<EOF
    set -exo pipefail
    command -v patch >/dev/null || { apt-get update && apt-get install -y --no-install-recommends patch; }
    mkdir -p /tmp/ubs-virt
    tar -xzf /tmp/ubs.tar.gz --strip-components=1 -C /tmp/ubs-virt
    bash /build-libvnpu.sh /tmp/ubs-virt/ubs-virt-enpu/vcann-rt /out
EOF

# =============================================================================
# 分别在 CUDA 12 和 13 的开发镜像中编译 HAMi-core 预加载库。运行时从对应 CUDA 主版本目录加载 libvgpu.so。
# =============================================================================
FROM nvidia/cuda:12.9.2-cudnn-devel-ubi8 AS xbuild-nvidia-cuda-12
RUN dnf install -y cmake && dnf clean all
RUN --mount=type=bind,from=hami-source,source=/source.tar.gz,target=/tmp/hami.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/nvidia/build-libvgpu.sh,target=/build-libvgpu.sh <<EOF
    set -exo pipefail
    mkdir -p /tmp/hami-core-src
    tar -xzf /tmp/hami.tar.gz --strip-components=1 -C /tmp/hami-core-src
    export CI_COMMIT_SHA=ec5d85a3d709e5ed138a1668ebfefd366c05ca1e
    export CMAKE_OPTIONS=-DCMAKE_DISABLE_FIND_PACKAGE_Git=TRUE
    bash /build-libvgpu.sh /tmp/hami-core-src /out
EOF

FROM nvidia/cuda:13.0.3-cudnn-devel-ubi8 AS xbuild-nvidia-cuda-13
RUN dnf install -y cmake && dnf clean all
RUN --mount=type=bind,from=hami-source,source=/source.tar.gz,target=/tmp/hami.tar.gz \
    --mount=type=bind,src=pack/gpustack-operator/external/nvidia/build-libvgpu.sh,target=/build-libvgpu.sh <<EOF
    set -exo pipefail
    mkdir -p /tmp/hami-core-src
    tar -xzf /tmp/hami.tar.gz --strip-components=1 -C /tmp/hami-core-src
    export CI_COMMIT_SHA=ec5d85a3d709e5ed138a1668ebfefd366c05ca1e
    export CMAKE_OPTIONS=-DCMAKE_DISABLE_FIND_PACKAGE_Git=TRUE
    bash /build-libvgpu.sh /tmp/hami-core-src /out
EOF

# =============================================================================
# 使用 THead PPU SDK 和本项目的 csrc 源码编译两种预加载库以及用量读取工具。
#
# 厂商 SDK 仅支持 Linux amd64。按目标架构选择阶段，arm64 只提供空目录，避免把 x86 库装入 ARM 镜像。
# =============================================================================
FROM --platform=linux/amd64 gpustack/thead-ppu-devel:2.1.1 AS xbuild-thead-ppu-amd64
RUN --mount=type=bind,src=pack/gpustack-operator/external/thead/build-libvppu.sh,target=/build-libvppu.sh \
    --mount=type=bind,src=csrc/thead/ppu-slicing-shim,target=/src <<EOF
    set -exo pipefail
    bash /build-libvppu.sh /src /out
EOF

# WORKDIR 会创建最终 COPY 需要的目录，无需执行其他命令。
FROM ${UBUNTU_IMAGE} AS xbuild-thead-ppu-arm64
WORKDIR /out

FROM xbuild-thead-ppu-${TARGETARCH} AS xbuild-thead-ppu

# =============================================================================
# 在 ROCm 开发镜像中编译本项目的 HIP 配额库、用量读取工具和 CU 掩码自检工具。
#
# 配额库在运行时解析 ROCm 符号，单份库即可服务多个 ROCm 版本。开发镜像的版本决定编译环境和工具可使用的 glibc 下限。
#
# ROCm 目标使用 Linux amd64。arm64 分支只提供空目录，使最终 COPY 保持相同布局。
# =============================================================================
FROM --platform=linux/amd64 rocm/dev-ubuntu-22.04:7.2.4 AS xbuild-amd-rocm-amd64
# 此镜像的 /bin/sh 为 dash，不支持 pipefail。将 heredoc 的执行 shell 明确设为 Bash。
SHELL ["/bin/bash", "-c"]
RUN --mount=type=bind,src=pack/gpustack-operator/external/amd/build-libvrocm.sh,target=/build-libvrocm.sh \
    --mount=type=bind,src=csrc/amd/rocm-slicing-shim,target=/src <<EOF
    set -exo pipefail
    bash /build-libvrocm.sh /src /out
EOF

# WORKDIR 会创建最终 COPY 需要的目录，无需执行其他命令。
FROM ${UBUNTU_IMAGE} AS xbuild-amd-rocm-arm64
WORKDIR /out

FROM xbuild-amd-rocm-${TARGETARCH} AS xbuild-amd-rocm

# =============================================================================
# 最终 Ubuntu 24.04 镜像安装运行依赖，并从构建阶段复制二进制与 CLI。
# =============================================================================
FROM ${UBUNTU_IMAGE} AS gpustack
SHELL ["/bin/bash", "-c"]

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

## 安装运行时软件包。

ENV DEBIAN_FRONTEND=noninteractive \
    LANG='en_US.UTF-8' \
    LANGUAGE='en_US:en' \
    LC_ALL='en_US.UTF-8'

RUN <<EOF
    # 准备运行时环境。
    set -exo pipefail

    # 更新软件包索引。
    apt-get update -y && apt-get install -y --no-install-recommends \
        software-properties-common apt-transport-https \
        ca-certificates gnupg2 lsb-release gnupg-agent \
      && apt-get update -y

    # 安装运维工具、Skopeo 依赖的共享库以及 AMD 设备绑定动态加载的 libdrm。
    apt-get install -y --no-install-recommends \
        bash openssl curl binutils \
        tzdata locales \
        iproute2 iputils-ping ifstat net-tools dnsutils pciutils ipmitool \
        procps sysstat glances htop numactl \
        vim jq bc tree gosu tini \
        logrotate cron netcat-openbsd \
        libgpgme11t64 libassuan0 libbtrfs0 libdevmapper1.02.1 \
        libdrm2 libdrm-amdgpu1

    # 生成镜像使用的区域设置。
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

    # 设置容器时区。
    rm -f /etc/localtime \
        && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
        && echo "Asia/Shanghai" > /etc/timezone \
        && dpkg-reconfigure --frontend noninteractive tzdata

    # 清理构建临时文件和软件包索引。
    rm -rf /var/tmp/* \
        && rm -rf /tmp/* \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*
EOF

## 将各 CANN 主版本和芯片系列编译出的 vcann-rt 复制到对应目录。各阶段使用目标平台的产物。

ARG GPUSTACK_LIB_DIR

COPY --from=xbuild-ascend-cann-8-910b /out ${GPUSTACK_LIB_DIR}/ascend/cann-8-910b
COPY --from=xbuild-ascend-cann-8-910c /out ${GPUSTACK_LIB_DIR}/ascend/cann-8-910c
COPY --from=xbuild-ascend-cann-9-910b /out ${GPUSTACK_LIB_DIR}/ascend/cann-9-910b
COPY --from=xbuild-ascend-cann-9-910c /out ${GPUSTACK_LIB_DIR}/ascend/cann-9-910c
COPY --from=xbuild-ascend-cann-9-950  /out ${GPUSTACK_LIB_DIR}/ascend/cann-9-950
COPY --from=xbuild-ascend-cann-9-310p /out ${GPUSTACK_LIB_DIR}/ascend/cann-9-310p

## 按 CUDA 主版本复制 NVIDIA 的 HAMi-core 运行库。
COPY --from=xbuild-nvidia-cuda-12 /out ${GPUSTACK_LIB_DIR}/nvidia/cuda-12
COPY --from=xbuild-nvidia-cuda-13 /out ${GPUSTACK_LIB_DIR}/nvidia/cuda-13

## 天数智芯的 corex 兼容 CUDA 接口，复用 CUDA 12 阶段的 HAMi-core 产物，放在 iluvatar 目录。
COPY --from=xbuild-nvidia-cuda-12 /out ${GPUSTACK_LIB_DIR}/iluvatar

## 安装 THead 的两个预加载库和用量工具，工作负载提供自身 SDK，产物使用单一目录。arm64 分支目录为空。
COPY --from=xbuild-thead-ppu /out ${GPUSTACK_LIB_DIR}/thead

## 安装 AMD 配额库、用量工具和 CU 掩码自检工具，使用单一运行目录。arm64 分支目录为空。
COPY --from=xbuild-amd-rocm /out ${GPUSTACK_LIB_DIR}/amd

## 从各构建阶段复制命令行工具及 Operator 程序。

COPY --from=tools     /out/bin/helm        /usr/bin/helm
COPY --from=tools     /out/bin/vals        /usr/bin/vals
COPY --from=tools     /out/bin/kustomize   /usr/bin/kustomize
COPY --from=tools     /out/bin/ytt         /usr/bin/ytt
COPY --from=tools     /out/bin/kubectl     /usr/bin/kubectl
COPY --from=skopeo    /out/bin/skopeo      /usr/bin/skopeo
COPY --from=operator  /out/bin/gpustack-operator /usr/bin/gpustack-operator

## 写入 Skopeo 签名策略并检查可执行文件。

RUN <<EOF
    set -exo pipefail

    # 配置 Skopeo 的默认签名接受策略。
    mkdir -p /etc/containers
    cat<<EOT > /etc/containers/policy.json
{
  "default": [
    {
      "type": "insecureAcceptAnything"
    }
  ]
}
EOT

    # 运行各个随镜像交付的命令，检查版本和动态库依赖。
    skopeo --version
    helm version
    vals version
    kustomize version
    ytt version
    kubectl version --client
    gpustack-operator --version
EOF

# 为镜像内的组件安装流程准备 Helm chart。

ARG GPUSTACK_CHARTS_DIR

## 将包含 Kueue、NFD、NFS/S3 CSI 和可选 Topograph 的父 chart 一起打包，Worker 可以直接在镜像内找到这些依赖。
RUN --mount=type=bind,src=deploy/gpustack-operator/chart,target=/tmp/gpustack-operator-chart <<EOF
    set -exo pipefail

    # 从刚构建的二进制读取版本；无法解析开发版字符串时使用 0.0.0。
    VERSION="$(gpustack-operator --version | awk '{for (i = 1; i < NF; i++) if ($i == "version") print $(i + 1)}')"
    CHART_VERSION="${VERSION#v}"
    if [[ ! "${CHART_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
        CHART_VERSION="0.0.0"
    fi

    # chart 的 version 与 appVersion 同步，保存时去掉 v，避免拼接默认镜像标签时产生两个 v。
    helm package /tmp/gpustack-operator-chart \
        --version "${CHART_VERSION}" --app-version "${CHART_VERSION}" \
        --destination "${GPUSTACK_CHARTS_DIR}"
EOF

# 配置厂商运行库。

RUN --mount=type=bind,src=pack/gpustack-operator/rootfs,target=/rootfs <<EOF
    # 配置厂商运行库。
    set -exo pipefail

    # 安装对应架构的共享库并刷新动态链接器缓存。
    cp -rf /rootfs/lib/$(uname -m)-linux-gnu/gpustack /lib/$(uname -m)-linux-gnu/
    cat <<EOT >/etc/ld.so.conf.d/zz-zzzz-9999999999-gpustack.conf
/lib/$(uname -m)-linux-gnu/gpustack
EOT
    ldconfig -v

    # 安装设备管理器初始化容器用于同步主机目录的工具。
    install -m 0755 /rootfs/usr/bin/copy-dir.sh /usr/bin/copy-dir.sh

    # 安装逻辑切片工作负载使用的 ld.so.preload 文件，随后与对应厂商预加载库一起同步到节点。
    install -D -m 0644 /rootfs/etc/gpustack/lib/ascend/ld.so.preload \
        "${GPUSTACK_LIB_DIR}"/ascend/ld.so.preload
    install -D -m 0644 /rootfs/etc/gpustack/lib/nvidia/ld.so.preload \
        "${GPUSTACK_LIB_DIR}"/nvidia/ld.so.preload
    install -D -m 0644 /rootfs/etc/gpustack/lib/iluvatar/ld.so.preload \
        "${GPUSTACK_LIB_DIR}"/iluvatar/ld.so.preload
    install -D -m 0644 /rootfs/etc/gpustack/lib/thead/ld.so.preload \
        "${GPUSTACK_LIB_DIR}"/thead/ld.so.preload
    install -D -m 0644 /rootfs/etc/gpustack/lib/amd/ld.so.preload \
        "${GPUSTACK_LIB_DIR}"/amd/ld.so.preload

    # 尝试更新 PCI 设备 ID 数据库。
    if ! update-pciids; then
        curl -o /usr/share/misc/pci.ids https://pci-ids.ucw.cz/v2.2/pci.ids || true
    fi

    # 清理构建临时文件和软件包索引。
    rm -rf /var/tmp/* \
        && rm -rf /tmp/*
EOF

## AMD 设备的运行时配置。
## -- 通知容器运行时注入全部可见设备。
ENV AMD_VISIBLE_DEVICES="all"
## 昇腾驱动的运行时配置。
## -- 设置运行库搜索路径。
ARG LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH="/usr/local/dcmi:/usr/local/Ascend/driver/lib64/common:/usr/local/Ascend/driver/lib64/driver:/usr/local/Ascend/ascend-toolkit/latest/runtime/lib64:${LD_LIBRARY_PATH}"
## 寒武纪设备的运行时配置。
## -- 注入可见设备并配置厂商工具链与运行库路径。
ENV CAMBRICON_VISIBLE_DEVICES="all" \
    NEUWARE_HOME="/usr/local/neuware" \
    LD_LIBRARY_PATH="/usr/local/neuware/lib64:${LD_LIBRARY_PATH}"
## 海光设备使用镜像中的默认配置。
## -- 天数智芯的设备和运行库配置。
## -- 注入可见设备并配置厂商工具链与运行库路径。
ENV IX_VISIBLE_DEVICES="all" \
    COREX_HOME="/usr/local/corex" \
    LD_LIBRARY_PATH="/usr/local/corex/lib64:${LD_LIBRARY_PATH}"
## 沐曦驱动库路径。
## -- 设置运行库搜索路径。
ENV LD_LIBRARY_PATH="/opt/maca/lib:/opt/mxdriver/lib:${LD_LIBRARY_PATH}"
## 摩尔线程的可见设备与驱动能力。
## -- 通知容器运行时注入全部可见设备。
ENV MTHREADS_VISIBLE_DEVICES="all" \
    MTHREADS_DRIVER_CAPABILITIES="compute,utility"
## NVIDIA 的可见设备与驱动能力。
## -- 通知容器运行时注入全部可见设备。
ENV NVIDIA_DISABLE_REQUIRE="true" \
    NVIDIA_VISIBLE_DEVICES="all" \
    NVIDIA_DRIVER_CAPABILITIES="compute,utility"
## 平头哥的 PPU SDK 与库路径。
## -- 设置运行库搜索路径。
ENV PPU_HOME="/usr/local/PPU_SDK" \
    LD_LIBRARY_PATH="/usr/local/PPU_SDK/CUDA_SDK/lib64:/usr/local/PPU_SDK/lib:${LD_LIBRARY_PATH}"

# 容器的数据目录及进程入口。

ARG GPUSTACK_DATA_DIR

ENV _RUNNING_INSIDE_CONTAINER_="true" \
    KUBE_FEATURE_WatchListClient="false"

VOLUME ${GPUSTACK_DATA_DIR}
WORKDIR /
EXPOSE 80 443
ENTRYPOINT [ "tini", "--" ]

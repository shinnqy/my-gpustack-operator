#!/usr/bin/env bash
#
# 安装或升级前处理 Helm 的所有权迁移不能独立解决的问题。步骤可重复执行，对正常集群为空操作。
#
# 先移除已失联的 Kueue webhook，再清理阻塞旧 CRD 的终结器，否则准入调用可能连清理补丁也拒绝。
#
# 升级时使用服务端 apply 更新随子 chart 交付的 CRD。首次安装时 Helm 已经从 crds/ 安装它们。
#
# 升级接管前先删除仍使用旧 release 选择器的工作负载，因为 Kubernetes 禁止修改这些选择器。
#
# Job 通过环境变量传入命名空间、release 名、本次是安装还是升级，以及目标 chart 版本。
#
# 从 Worker 自安装也使用的 GPUSTACK_CONF_DIR/charts 目录读取已打包的 chart。
set -uo pipefail

NS="${GPUSTACK_NAMESPACE:-gpustack-system}"
RELEASE="${GPUSTACK_RELEASE:-gpustack-operator}"
PHASE="${GPUSTACK_PHASE:-upgrade}"
CHART_VERSION="${GPUSTACK_CHART_VERSION:-}"
CHARTS_DIR="${GPUSTACK_CONF_DIR:-/etc/gpustack}/charts"

# 与卸载脚本使用同一组可能拥有 Kueue 的 release 名，只处理已核实归属的对象。
KUEUE_RELEASES="${RELEASE},gpustack-operator-device-manager,gpustack-kueue"

# 只从旧版独立组件的 release 接管，与升级后的记录清理脚本保持一致。
LEGACY_RELEASES=(
  gpustack-kueue
  gpustack-node-feature-discovery
  gpustack-csi-driver-nfs
  gpustack-csi-driver-s3
)

# 一个选择器涵盖这些旧 release，额外要求 managed-by=Helm。
LEGACY_SELECTOR="app.kubernetes.io/instance in ($(
  IFS=,
  echo "${LEGACY_RELEASES[*]}"
)),app.kubernetes.io/managed-by=Helm"

# 只列出 spec.selector 不可变、可能发生接管冲突的工作负载类型。
SELECTOR_KINDS=(deployments daemonsets statefulsets)

# 等待解除阻塞的 CRD 删除的时间上限。
DRAIN_TIMEOUT=90
DRAIN_INTERVAL=3

log() { echo "[migrate-pre] $*"; }
die() {
  echo "[migrate-pre] FATAL: $*" >&2
  exit 1
}

log "namespace=${NS} release=${RELEASE} phase=${PHASE}"

# --- 1. 清理卡在终止状态的 Kueue -------------------------------------------------------------

# 列出正在终止的 Kueue CRD 的名称、复数资源名及存储版本。用存储版本查询，避免经过已失联的转换 webhook。
stuck_kueue_crds() {
  kubectl get crd -o json |
    jq -r '.items[]
           | select(.spec.group == "kueue.x-k8s.io")
           | select(.metadata.deletionTimestamp != null)
           | [ .metadata.name,
               .spec.names.plural,
               (([.spec.versions[] | select(.storage == true) | .name] | first) // .spec.versions[0].name)
             ] | @tsv'
}

# 仅为正在终止的实例清除终结器。保留正常队列的记账保护，并按名称和命名空间区分集群级与命名空间级对象。
strip_finalizers() {
  local resource="$1"
  local namespace name scope=()

  while IFS=$'\t' read -r name namespace; do
    [[ -n "${name}" ]] || continue
    # 命名空间为空表示集群级资源，如 ClusterQueue。
    scope=()
    [[ -z "${namespace}" ]] || scope=(-n "${namespace}")
    log "clearing finalizers on ${resource} ${namespace:-<cluster>}/${name}"
    kubectl patch "${resource}" "${scope[@]}" "${name}" \
      --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null ||
      log "WARNING: could not clear the finalizers of ${resource} ${name}"
  done < <(
    kubectl get "${resource}" -A -o json |
      jq -r '.items[]
             | select(.metadata.deletionTimestamp != null)
             | select(((.metadata.finalizers // []) | length) > 0)
             | [ .metadata.name, (.metadata.namespace // "") ] | @tsv'
  )
}

# 逐个查询目标 CRD 的名称，直到全部消失，避免每轮重复下载全部 CRD 的大体积 schema。
wait_crds_deleted() {
  local waited=0 name remaining

  while :; do
    remaining=0
    for name in "$@"; do
      kubectl get crd "${name}" >/dev/null 2>&1 && remaining=$((remaining + 1))
    done
    [[ ${remaining} -eq 0 ]] && return 0

    [[ ${waited} -lt ${DRAIN_TIMEOUT} ]] ||
      die "${remaining} Kueue CRD(s) are still terminating after ${waited}s"
    sleep "${DRAIN_INTERVAL}"
    waited=$((waited + DRAIN_INTERVAL))
  done
}

reap_stranded_kueue() {
  local stuck names=() name plural version

  stuck="$(stuck_kueue_crds)" || die "list the Kueue CRDs"
  if [[ -z "${stuck}" ]]; then
    log "no Kueue CRD is terminating, nothing to reap"
    return 0
  fi

  # 先删除失联的 webhook，再处理被它阻塞的对象。
  log "deleting the Kueue webhook configurations of releases: ${KUEUE_RELEASES}"
  kubectl delete validatingwebhookconfigurations,mutatingwebhookconfigurations \
    -l "app.kubernetes.io/instance in (${KUEUE_RELEASES})" --ignore-not-found ||
    die "delete the Kueue webhook configurations"

  while IFS=$'\t' read -r name plural version; do
    [[ -n "${name}" ]] || continue
    names+=("${name}")
    strip_finalizers "${plural}.${version}.kueue.x-k8s.io"
  done <<<"${stuck}"

  log "waiting for ${#names[@]} freed Kueue CRD(s) to drain"
  wait_crds_deleted "${names[@]}"
  log "the stranded Kueue is reaped"
}

reap_stranded_kueue

# --- 2. 应用随附子 chart 的 CRD ------------------------------------------------

apply_subchart_crds() {
  local chart="${CHARTS_DIR}/gpustack-operator-${CHART_VERSION}.tgz"
  local dir crds=()

  if [[ ! -f "${chart}" ]]; then
    # hook 镜像可能与正在升级的 chart 版本不同，因此指定版本不存在时查找镜像内已有的包。
    chart="$(find "${CHARTS_DIR}" -maxdepth 1 -name 'gpustack-operator-*.tgz' 2>/dev/null | sort | tail -1)"
  fi
  [[ -n "${chart}" && -f "${chart}" ]] ||
    die "no packaged gpustack-operator chart under ${CHARTS_DIR}; the hook image must be an operator image"

  dir="$(mktemp -d)" || die "create a temporary directory"
  # shellcheck disable=SC2064
  trap "rm -rf '${dir}'" EXIT
  tar -xzf "${chart}" -C "${dir}" || die "unpack ${chart}"

  shopt -s nullglob
  crds=("${dir}"/*/charts/*/crds/*.yaml "${dir}"/*/charts/*/crds/*.yml)
  shopt -u nullglob
  if [[ ${#crds[@]} -eq 0 ]]; then
    log "no subchart ships a crds/ directory, nothing to apply"
    return 0
  fi

  local args=() file
  for file in "${crds[@]}"; do
    args+=(-f "${file}")
  done

  log "applying ${#crds[@]} subchart CRD file(s) from $(basename "${chart}")"
  # 使用服务端 apply 接管旧版由客户端 apply 管理的 CRD 字段。
  kubectl apply --server-side --force-conflicts "${args[@]}" ||
    die "apply the subchart CRDs"
}

# --- 3. 清理选择器无法就地更改的旧工作负载 -------------------------

# 删除选择器仍指向旧 release 的工作负载，使新 release 可按新标签重建。
#
# Kueue 和 NFD 的工作负载选择器包含实例标签，直接修改会使升级失败并触发回滚。提前删除这些工作负载，其余资源仍可就地接管。
#
# 仅处理选择器实际含旧 release 的工作负载。CSI 的选择器与 release 无关，可以就地接管，用户自定义资源和数据卷继续保留。
free_immutable_selectors() {
  local kind names

  for kind in "${SELECTOR_KINDS[@]}"; do
    names="$(
      kubectl get "${kind}" -n "${NS}" -l "${LEGACY_SELECTOR}" -o json |
        jq -r --arg adopting "${RELEASE}" '
          .items[]
          | select((.spec.selector.matchLabels["app.kubernetes.io/instance"] // "")
                   | . != "" and . != $adopting)
          | .metadata.name'
    )" || die "list the ${kind} of the legacy releases"
    [[ -n "${names}" ]] || continue

    log "deleting ${kind} whose selector adoption cannot rewrite: ${names//$'\n'/ }"
    # shellcheck disable=SC2086 # 每行一个不含空格的资源名，这里需要拆成多个参数
    kubectl delete "${kind}" -n "${NS}" ${names} --ignore-not-found ||
      die "delete the ${kind} a legacy release still selects"
  done
}

if [[ "${PHASE}" == "upgrade" ]]; then
  apply_subchart_crds
  free_immutable_selectors
else
  log "install: Helm applies the subchart CRDs itself and nothing is adopted, skipping"
fi

log "done"

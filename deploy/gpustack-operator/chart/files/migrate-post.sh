#!/usr/bin/env bash
#
# 升级成功并完成归属迁移之后，清理已被子 chart 取代的旧 release 记录及其独有对象。
#
# 只删除旧 release 的记录。调用 helm uninstall 会把新父 release 已经接管的对象一并删除。
#
# 按旧 release 的实例标签清理新版未接管的对象。命名空间资源限定在本次 release 所在的命名空间。
#
# 迁移可以重试。第一次完成后，不再有符合旧标签的对象。
#
# Job 用 GPUSTACK_NAMESPACE 传入安装所在的命名空间。
set -uo pipefail

NS="${GPUSTACK_NAMESPACE:-gpustack-system}"

# 旧版 Worker 分别安装的组件 release 名。
LEGACY_RELEASES=(
  gpustack-kueue
  gpustack-node-feature-discovery
  gpustack-csi-driver-nfs
  gpustack-csi-driver-s3
)

# 逐种查询旧 release 创建的资源，避免某一种类型不存在导致整次查询失败。用户数据所在的 PV 和 PVC 不在清理范围内。
NAMESPACED_KINDS=(
  deployments daemonsets statefulsets services serviceaccounts configmaps secrets
  roles rolebindings poddisruptionbudgets jobs networkpolicies
)
CLUSTER_KINDS=(
  clusterroles clusterrolebindings mutatingwebhookconfigurations
  validatingwebhookconfigurations csidrivers storageclasses apiservices
)

# 一个集合选择器覆盖四个旧 release，同时要求 managed-by=Helm，保护用户手工添加同名标签的对象。
PRUNE_SELECTOR="app.kubernetes.io/instance in ($(
  IFS=,
  echo "${LEGACY_RELEASES[*]}"
)),app.kubernetes.io/managed-by=Helm"

log() { echo "[migrate-post] $*"; }

log "namespace=${NS}"

# --- 1. 移除旧 release 记录 -------------------------------------------------

for release in "${LEGACY_RELEASES[@]}"; do
  records="$(kubectl -n "${NS}" get secret -l "owner=helm,name=${release}" -o name 2>/dev/null)"
  [[ -n "${records}" ]] || continue
  log "retiring the release record of ${release}"
  kubectl -n "${NS}" delete secret -l "owner=helm,name=${release}" --ignore-not-found ||
    log "WARNING: could not delete the release record of ${release}"
done

# --- 2. 回收新清单未接管的资源 ---------------------------------

# prune 清理一种仍带旧实例标签的对象，额外参数用于限定命名空间等范围。
prune() {
  local kind="$1"
  shift
  local found

  if ! found="$(kubectl get "${kind}" "$@" -l "${PRUNE_SELECTOR}" -o name --ignore-not-found 2>&1)"; then
    log "WARNING: could not list ${kind}: ${found}"
    return 0
  fi
  [[ -n "${found}" ]] || return 0

  log "pruning $(echo "${found}" | wc -l | tr -d ' ') orphaned ${kind}:"
  echo "${found}" | sed 's/^/  /'
  kubectl delete "${kind}" "$@" -l "${PRUNE_SELECTOR}" --ignore-not-found ||
    log "WARNING: could not prune every orphaned ${kind}"
}

for kind in "${NAMESPACED_KINDS[@]}"; do
  prune "${kind}" -n "${NS}"
done
for kind in "${CLUSTER_KINDS[@]}"; do
  prune "${kind}"
done

log "done"

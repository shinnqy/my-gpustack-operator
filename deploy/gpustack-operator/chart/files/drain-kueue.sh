#!/usr/bin/env bash
#
# 在 Kueue 控制器仍运行时，删除由 Worker 创建的 Kueue 资源，并等待其终结器完成。
#
# 调用参数依次为命名空间、release、Worker Deployment 和可选的等待秒数。
#
# 只在当前 release 安装了 Kueue 时运行，顺序在 drain.sh 之后。
#
# Kueue 用 resource-in-use 终结器保护在用的队列、规格、拓扑与准入检查，控制器必须继续运行才能释放它们。
#
# 删除 InstanceType 会使 Worker 删除相应 ClusterQueue，其余由 Worker 创建的 Kueue 对象需要显式回收。
#
# 排空业务对象后先把 Worker 缩到零，否则它会重新创建刚删除的 ResourceFlavor 等资源。
#
# 按 Worker 写入的归属标签选择对象，保护同一套 Kueue 中用户自建的队列和规格。
#
# 本步骤失败会使 hook 返回错误，及时报告 Kueue 资源阻塞，避免在删除 CRD 时无提示地等到 Helm 超时。
set -uo pipefail

NS="${1:?usage: drain-kueue.sh <NS> <RELEASE> <WORKER_DEPLOYMENT> [BUDGET_SECONDS]}"
RELEASE="${2:?usage: drain-kueue.sh <NS> <RELEASE> <WORKER_DEPLOYMENT> [BUDGET_SECONDS]}"
WORKER="${3:?usage: drain-kueue.sh <NS> <RELEASE> <WORKER_DEPLOYMENT> [BUDGET_SECONDS]}"
BUDGET="${4:-120}"
DEADLINE=$(( $(date +%s) + BUDGET ))

echo "[drain-kueue] namespace=${NS} release=${RELEASE} worker=${WORKER} budget=${BUDGET}s"

# 先确认 API 可访问，防止把连接错误解释为集群里已无对象。
healthz=(kubectl get --raw=/healthz)
if command -v timeout >/dev/null 2>&1; then
  healthz=(timeout 10 "${healthz[@]}")
fi
if ! "${healthz[@]}" >/dev/null 2>&1; then
  echo "[drain-kueue] FAILED: cannot reach the API server" >&2
  exit 1
fi

# 除了检查 kueue.enabled，还要核实当前 Kueue 确实属于本次安装。
owner="$(kubectl get crd clusterqueues.kueue.x-k8s.io \
  -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-namespace}/{.metadata.annotations.meta\.helm\.sh/release-name}' \
  2>/dev/null)"
if [ "${owner}" != "${NS}/${RELEASE}" ]; then
  echo "[drain-kueue] skipped: Kueue is not owned by this release (owner=${owner:-none})"
  exit 0
fi

remaining_left() { # 返回剩余等待秒数，最低为零。
  local left=$(( DEADLINE - $(date +%s) ))
  [ "${left}" -gt 0 ] && echo "${left}" || echo 0
}

# 先停止 Worker。以 worker.enabled=false 安装时没有需要缩容的 Deployment。
if kubectl -n "${NS}" get deployment "${WORKER}" >/dev/null 2>&1; then
  selector="$(kubectl -n "${NS}" get deployment "${WORKER}" \
    -o go-template='{{range $k, $v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>/dev/null)"
  selector="${selector%,}"
  if [ -z "${selector}" ]; then
    echo "[drain-kueue] FAILED: could not read the selector of deployment/${WORKER}" >&2
    exit 1
  fi
  if ! kubectl -n "${NS}" scale deployment "${WORKER}" --replicas=0 >/dev/null; then
    echo "[drain-kueue] FAILED: could not scale deployment/${WORKER} to zero" >&2
    exit 1
  fi
  # 等待 Worker Pod 完全退出。Deployment 的期望副本变成零时，终止中的 Worker 仍可能触发调谐。
  until pods="$(kubectl -n "${NS}" get pods -l "${selector}" -o name 2>/dev/null)" && [ -z "${pods}" ]; do
    if [ "$(remaining_left)" -eq 0 ]; then
      echo "[drain-kueue] FAILED: the worker Pods (${selector}) did not stop within ${BUDGET}s" >&2
      exit 1
    fi
    sleep 3
  done
  echo "[drain-kueue] worker stopped"
fi

# 按每行给出的资源类型与标签选择器执行删除，然后等待这些对象消失。
OBJECTS="
clusterqueues.kueue.x-k8s.io resource.gpustack.ai/type=instancetypes
resourceflavors.kueue.x-k8s.io resource.gpustack.ai/type=nodes
topologies.kueue.x-k8s.io resource.gpustack.ai/type=topology
admissionchecks.kueue.x-k8s.io app.kubernetes.io/part-of=gpustack-operator
"

# 先发出全部删除请求再等待，让有依赖关系的终结器按实际引用顺序释放。
while read -r res sel; do
  [ -n "${res}" ] || continue
  if ! kubectl delete "${res}" -l "${sel}" --ignore-not-found --wait=false >/dev/null; then
    echo "[drain-kueue] FAILED: the delete of ${res} (${sel}) was refused" >&2
    exit 1
  fi
done <<EOF
${OBJECTS}
EOF

# 列表查询失败时继续等待，直到查询成功或预算耗尽。
left_over() {
  local res sel names
  while read -r res sel; do
    [ -n "${res}" ] || continue
    if ! names="$(kubectl get "${res}" -l "${sel}" -o name 2>/dev/null)"; then
      echo "${res} (could not be listed)"
      continue
    fi
    [ -z "${names}" ] || printf '%s\n' "${names}"
  done <<EOF
${OBJECTS}
EOF
}

while :; do
  remaining="$(left_over)"
  if [ -z "${remaining}" ]; then
    echo "[drain-kueue] done"
    exit 0
  fi
  [ "$(remaining_left)" -gt 0 ] || break
  sleep 3
done

echo "[drain-kueue] FAILED: Kueue did not finalize these within ${BUDGET}s:" >&2
printf '%s\n' "${remaining}" | sed 's|^|[drain-kueue]   |' >&2
exit 1

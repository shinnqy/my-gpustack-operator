#!/usr/bin/env bash
#
# 按消费依赖顺序删除 Operator 管理的自定义资源，在相应控制器还活着时等待终结器完成。
#
# 调用参数为命名空间和可选的整轮时间预算。
#
# 这些操作必须在控制器退出前完成，例如清理后端节点数据和归还租户配额。post-delete 阶段已经无法触发这类收尾工作。
#
# 启用卸载清理的 pre-delete Job 与主机端验收共用本脚本，操作顺序早于 Helm 删除工作负载。
#
# 以报告记录未完成项并返回零，避免某个资源超时阻断整个 Helm 卸载。后续清理继续处理残留，调用方可重新查询集群确认结果。
#
# 可重复执行。命名空间与 CRD 留到各自的生命周期处理，这里只排空资源实例。
set -uo pipefail

NS="${1:-${GPUSTACK_NAMESPACE:-gpustack-system}}"

# 全部类型共享同一时间预算。快速退出的资源可为等待节点数据清理的后端留下更多时间。
#
# 主机调用采用默认预算，chart 的 hook 会显式传入更短的等待时间，留出生成报告和正常退出的时间。
BUDGET="${2:-600}"
DEADLINE=$(( $(date +%s) + BUDGET ))

echo "[drain] namespace=${NS} budget=${BUDGET}s"

# 首先确认 API Server 可以访问，防止吞掉连接错误后误报排空完成。
healthz=(kubectl get --raw=/healthz)
if command -v timeout >/dev/null 2>&1; then
  healthz=(timeout 10 "${healthz[@]}")
fi
if ! "${healthz[@]}" >/dev/null 2>&1; then
  echo "[drain] SKIPPED: cannot reach the API server of the current kubectl context" \
    "($(kubectl config current-context 2>/dev/null || echo 'none set')); nothing was drained" >&2
  exit 0
fi
echo "[drain] context=$(kubectl config current-context 2>/dev/null || echo unknown)"

# 按依赖顺序列出类型，先删除消费者，再删除它使用的资源，否则终结器可能相互等待。
#
# 后续还会扫描本项目组中剩余的类型。显式列表决定依赖顺序，新增类型应放入合适位置。
DRAIN_ORDER="
instances.worker.gpustack.ai
modeldeployments.worker.gpustack.ai
kvcachepoolbindings.worker.gpustack.ai
kvcachepools.worker.gpustack.ai
kvcachebackends.worker.gpustack.ai
instancetypes.worker.gpustack.ai
nodemodelstores.worker.gpustack.ai
"

# 使用 CRD 中的存储版本构造完整的 plural.VERSION.group。
#
# 不指定版本的名称可能解析到 Worker 的聚合 API。显式使用存储版本才能操作底层 CRD 实例。
resource_of() { # 输入 CRD 名，返回带版本的资源名；CRD 缺失时返回空值。
  kubectl get crd "$1" \
    -o jsonpath='{.spec.names.plural}.{.spec.versions[?(@.storage==true)].name}.{.spec.group}' \
    2>/dev/null
}

# 将该类型的每个对象输出为一行“命名空间/名称”。
#
# 用斜杠保留集群级对象的空命名空间。即使预检已通过，后续读取也可能失败，必须检查返回状态。
list_objects() { # 按资源和可选选择器列出对象，查询失败返回非零。
  # shellcheck disable=SC2086
  kubectl get "$1" -A ${2:+-l "$2"} \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null
}

remaining_left() { # 返回剩余等待秒数，最低为零。
  local left=$(( DEADLINE - $(date +%s) ))
  [ "${left}" -gt 0 ] && echo "${left}" || echo 0
}

# 删除一种资源的全部实例，再等待其退出。
#
# 先对所有实例发送非阻塞删除，使各控制器可以并行执行终结器，再统一等待结果。
#
# 可选的选择器只缩小等待范围。对会被运行中的 Worker 重建的类型，可以只发删除而跳过等待。
drain_kind() { # 参数为资源、可选的等待选择器和只删除一次的开关。
  local res="$1" wait_sel="${2:-}" delete_once="${3:-}" objects listing left refused=0 ns name
  if ! objects="$(list_objects "${res}")"; then
    echo "[drain] INCOMPLETE: ${res} could not be listed, so whether it drained is unknown" >&2
    return 1
  fi
  [ -n "$(printf '%s' "${objects}" | tr -d '[:space:]')" ] || return 0

  # 单靠等待看不出删除请求是被拒绝还是尚未完成。分别统计并记录错误，使报告能说明原因。
  #
  # 用 here-document 为循环供给输入，使计数器保留在当前 shell 中。
  echo "[drain] deleting $(printf '%s\n' "${objects}" | grep -c . ) ${res}"
  while IFS='/' read -r ns name; do
    [ -n "${name}" ] || continue
    # shellcheck disable=SC2086
    if ! kubectl delete "${res}" "${name}" ${ns:+-n "${ns}"} \
        --ignore-not-found --wait=false >/dev/null 2>&1; then
      refused=$(( refused + 1 ))
      echo "[drain] the delete of ${ns:+${ns}/}${name} was refused; it will not drain" >&2
    fi
  done <<EOF
${objects}
EOF

  if [ -n "${delete_once}" ] && [ "${refused}" -eq 0 ]; then
    echo "[drain] ${res} delete requested once; the worker may recreate it"
    return 0
  fi

  while :; do
    left="$(remaining_left)"
    [ "${left}" -gt 0 ] || break
    # 读取失败时继续轮询。空输出只有在查询成功后才能作为已排空的证据。
    #
    # 先用临时变量接收查询，保留最后一次成功列出的对象，超时时可报告仍在等待的名称。
    if listing="$(list_objects "${res}" "${wait_sel}")"; then
      objects="${listing}"
      [ -n "$(printf '%s' "${objects}" | tr -d '[:space:]')" ] || return 0
    fi
    sleep 3
  done

  if [ "${refused}" -gt 0 ]; then
    echo "[drain] INCOMPLETE: ${res} had ${refused} delete(s) refused, so it was never going to" \
      "drain; the objects still present are:" >&2
  else
    echo "[drain] INCOMPLETE: ${res} still has objects when the budget ran out:" >&2
  fi
  printf '%s\n' "${objects}" | sed 's|^/||; s|^|[drain]   |' >&2
  return 1
}

incomplete=""
for crd in ${DRAIN_ORDER}; do
  res="$(resource_of "${crd}")"
  # 未使用某功能的集群可能没有对应 CRD。存储版本读不到时也不能猜测资源名。
  case "${res}" in
    ""|.*|*..*) continue ;;
  esac
  # 派生的 InstanceType 删除一次即可，Worker 在运行中可能重新创建它。停止 Worker 后再清理队列和终结器。
  wait_sel=""
  if [ "${crd}" = "instancetypes.worker.gpustack.ai" ]; then
    wait_sel="schedule.gpustack.ai/derived-from-node!=true"
  fi
  # NodeModelStore 会被 Worker 从 CSINode 重建，且没有终结器。这里不等待，随后由清理脚本删除 CRD。
  delete_once=""
  if [ "${crd}" = "nodemodelstores.worker.gpustack.ai" ]; then
    delete_once=yes
  fi
  drain_kind "${res}" "${wait_sel}" "${delete_once}" || incomplete=yes
done

# 最后遍历 GPUStack API 组中其余资源。
#
# Kueue 和 NFD 使用各自的卸载流程。本脚本只负责 GPUStack 资源，以保护外部组件及其用户数据。
#
# 先读取完整列表，再在当前 shell 中循环。否则子 shell 会丢失未完成标记，甚至可能把列表读取失败当作成功。
seen=""
for crd in ${DRAIN_ORDER}; do seen="${seen} ${crd}"; done
remaining_crds="$(kubectl get crd -o name 2>/dev/null \
  | sed 's|^customresourcedefinition\(s\)\{0,1\}\.apiextensions\.k8s\.io/||' \
  | grep -E '\.(worker\.)?gpustack\.ai$')"
while read -r crd; do
  [ -n "${crd}" ] || continue
  case " ${seen} " in *" ${crd} "*) continue ;; esac
  res="$(resource_of "${crd}")"
  case "${res}" in
    ""|.*|*..*) continue ;;
  esac
  drain_kind "${res}" || incomplete=yes
done <<EOF
${remaining_crds}
EOF

if [ -n "${incomplete}" ]; then
  echo "[drain] INCOMPLETE after ${BUDGET}s: the objects above were not drained, so the teardown" >&2
  echo "[drain] their finalizers were holding for did not run. The uninstall continues and" >&2
  echo "[drain] cleanup.sh will strip and delete them; anything they would have released on a node" >&2
  echo "[drain] or in a cache master's ledger stays there." >&2
  exit 0
fi

echo "[drain] done"

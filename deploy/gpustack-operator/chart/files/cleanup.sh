#!/usr/bin/env bash
#
# 清理 Helm 卸载父 chart 后可能留下的对象。
# 包括 Worker 另行安装的 release、旧版独立组件、本安装拥有的 CRD、已失去控制器的聚合 API 与准入 webhook，以及迁移失败残留的高权限绑定。
#
# 集群内的 post-delete Job 和主机上的卸载验收共用此脚本。Job 从 Operator 镜像调用 kubectl 和 Helm。
#
# 可重复执行。保留命名空间、用户自行安装的 Kueue 及 NFD 的 CRD。只有能确认归属的资源才进入清理流程。
set -uo pipefail

NS="${1:-${GPUSTACK_NAMESPACE:-gpustack-system}}"
# 当前安装的 Helm release 名用于区分随父 chart 部署和独立部署的组件。主机上直接调用时可采用约定的默认名称。
RELEASE="${3:-gpustack-operator}"
# 同时考虑父 chart、Worker 从镜像安装的 release 及旧版独立 Kueue。卸载和迁移必须认领同一组 release。
KUEUE_RELEASES="${RELEASE},gpustack-operator-device-manager,gpustack-kueue"
echo "[cleanup] namespace=${NS} release=${RELEASE}"

# 先确认目标 API 能连通。后续清理会容忍对象缺失，跳过预检会把连错集群误报成清理完成。可用时用 timeout 限制探测时间。
healthz=(kubectl get --raw=/healthz)
if command -v timeout >/dev/null 2>&1; then
  healthz=(timeout 10 "${healthz[@]}")
fi
if ! "${healthz[@]}" >/dev/null 2>&1; then
  echo "[cleanup] FATAL: cannot reach the API server of the current kubectl context" \
    "($(kubectl config current-context 2>/dev/null || echo 'none set')) — fix KUBECONFIG/context and re-run" >&2
  exit 1
fi
echo "[cleanup] context=$(kubectl config current-context 2>/dev/null || echo unknown)"

# 先卸载父 chart 之外由 Worker 创建的 release，包括镜像安装模式和旧版独立组件。已不存在的 release 直接跳过。
if command -v helm >/dev/null 2>&1; then
  for r in gpustack-operator-device-manager gpustack-csi-driver-nfs gpustack-csi-driver-s3 gpustack-node-feature-discovery gpustack-kueue; do
    if helm status "${r}" -n "${NS}" >/dev/null 2>&1; then
      echo "[cleanup] helm uninstall ${r}"
      helm uninstall "${r}" -n "${NS}" --wait --timeout 120s 2>/dev/null \
        || helm uninstall "${r}" -n "${NS}" 2>/dev/null || true
    fi
  done
fi

# 在剥离终结器、删除 CRD 前，先移除这些 release 注册的聚合 API 和准入 webhook。失联的后端会阻塞查询或写入。
#
# 资源名只用于筛选候选，独立安装的 Kueue 或 NFD 也可能同名。进一步核对 Service 的命名空间或实际属主。
for r in mutatingwebhookconfigurations validatingwebhookconfigurations; do
  kubectl get "${r}" -o name 2>/dev/null \
    | grep -Ei 'gpustack|kueue|nfd' \
    | while read -r obj; do
        kubectl get "${obj}" -o jsonpath='{.webhooks[*].clientConfig.service.namespace}' 2>/dev/null \
          | grep -qw "${NS}" || continue
        kubectl delete "${obj}" --ignore-not-found 2>/dev/null || true
      done
done
# 通过目标 Service 的命名空间识别聚合 API 及 webhook。kubectl 的名称输出要拆成独立参数后再删除。
kubectl get apiservices \
  -o jsonpath='{.items[?(@.spec.service.namespace=="'"${NS}"'")].metadata.name}' 2>/dev/null \
  | xargs -r kubectl delete apiservice --ignore-not-found 2>/dev/null || true

# 控制器退出后，先去除阻塞删除的终结器。开始删除 CRD 后继续检查，处理刚进入 Terminating 的实例。
#
# 从本安装拥有的 CRD 动态发现资源种类，避免遗漏后来增加的类型。
#
# 使用 CRD 指示的存储版本构造 plural.VERSION.group。Worker 停止后，仍可直接访问存储 API，绕开不可达的聚合服务。
#
# 剥离终结器和删除 CRD 都以 owned_crds 的判断为准。
#
# API 组相同不足以确认归属。GPUStack 的类型由本项目维护，Kueue 等共享类型必须进一步核对 Helm 所有权。
#
# 同时核对 release 名称和命名空间。单独一个字段都有可能匹配另一份安装。
#
# 允许的 release 名包含镜像模式和旧版安装。控制器已卸载时，其 CRD 仍可能等待终结器释放。
#
# 保留 NFD 的 CRD。Helm 从 crds/ 原样安装它们，无法通过 release 注解区分本次安装和独立安装。
owned_crds() {
  kubectl get crd \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.names.plural}.{.spec.versions[?(@.storage==true)].name}.{.spec.group}{" "}{.metadata.annotations.meta\.helm\.sh/release-namespace}{" "}{.metadata.annotations.meta\.helm\.sh/release-name}{"\n"}{end}' \
    2>/dev/null \
    | while read -r crd res owner_ns owner_name; do
        case "${crd}" in
          *.gpustack.ai) ;;
          *.kueue.x-k8s.io)
            [ "${owner_ns}" = "${NS}" ] && [ -n "${owner_name}" ] || continue
            case ",${KUEUE_RELEASES}," in
              *",${owner_name},"*) ;;
              *) continue ;;
            esac ;;
          *) continue ;;
        esac
        echo "${crd} ${res}"
      done
}

strip_gpustack_finalizers() {
  owned_crds | while read -r _ res; do
    # 同时列出资源名称和命名空间，并显式传给 patch。只使用 kubectl 的名称输出会漏掉其他命名空间的实例。
    #
    # 用斜杠分隔命名空间和名称。集群级对象的命名空间为空，空格分隔会让 shell 丢失这一列。
    kubectl get "${res}" -A \
      -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null \
      | while IFS='/' read -r ns name; do
          [ -n "${name}" ] || continue
          kubectl patch "${res}" "${name}" ${ns:+-n "${ns}"} \
            --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
        done
    done
}

# 对本安装拥有的 CRD 发起非阻塞删除，并持续清除阻塞它们退出的终结器。
strip_gpustack_finalizers
owned_crds | while read -r crd _; do
  kubectl delete crd "${crd}" --ignore-not-found --wait=false 2>/dev/null || true
done
for _ in $(seq 1 20); do
  [ -z "$(owned_crds)" ] && break
  strip_gpustack_finalizers
  sleep 3
done

# 按确定的名称清理 Worker 在运行时创建的证书与设置 Secret，避免误删同一命名空间内其他应用的数据。
worker_cert_secret="${2:-gpustack-operator-worker-cert}"
for s in "${worker_cert_secret}" gpustack-settings \
  kueue-webhook-server-cert kueue-metrics-server-cert kueue-visibility-server-cert; do
  kubectl -n "${NS}" delete secret "${s}" --ignore-not-found 2>/dev/null || true
done

# 删除序列化组件安装与控制平面选主使用的 Lease。它们由 Worker 在运行时创建。
for l in applications.worker.gpustack.ai worker.gpustack.ai; do
  kubectl -n "${NS}" delete lease "${l}" --ignore-not-found 2>/dev/null || true
done

# 清理迁移失败留下的 hook 对象，包括高权限绑定。正在执行清理的 ServiceAccount 和它的绑定必须保留到 Job 完成。
for r in jobs configmaps serviceaccounts; do
  kubectl -n "${NS}" get "${r}" -l app.kubernetes.io/part-of=gpustack-operator -o name 2>/dev/null \
    | grep -E 'migrate' \
    | xargs -r -I{} kubectl -n "${NS}" delete {} --ignore-not-found 2>/dev/null || true
done
kubectl get clusterrolebindings -l app.kubernetes.io/part-of=gpustack-operator -o name 2>/dev/null \
  | grep -E 'migrate' \
  | xargs -r -I{} kubectl delete {} --ignore-not-found 2>/dev/null || true

# release 记录可能已随命名空间删除。清理遗留的集群级对象前，检查其 Helm 所有权是否指向本命名空间。
#
# 缺少所有权注解的旧对象也可能阻挡重装。对它们单独核实归属。
#
# 只有所有权注解缺失时才允许用 Helm 管理标签和实例标签补充判断，已指向其他 release 的注解优先。
#
# 实例标签只含 release 名称，无法区分不同命名空间内的同名 release。
#
# 清理前查询一次 Helm，确认其他命名空间中是否存在仍在运行的同名 release。有则关闭标签回退。
#
# 比较时限定其他命名空间里的活跃 release，排除卸载历史和当前命名空间里的旧记录。
#
# Helm 不可用或查询失败时关闭标签回退。先判断命令状态，再解释输出，避免管道掩盖错误。
foreign_release_holds_this_name="" # 非空时禁用标签回退。
if ! command -v helm >/dev/null 2>&1; then
  foreign_release_holds_this_name="yes"
else
  # --filter 接受正则表达式。release 名中的点等字符必须按字面转义。
  release_re="$(printf '%s' "${RELEASE}" | sed 's/[][\\.*^$(){}?+|/]/\\&/g')"
  if ! helm_releases="$(helm list --all-namespaces --deployed --failed --pending \
      --filter "^${release_re}\$" 2>/dev/null)"; then
    foreign_release_holds_this_name="yes"
    echo "[cleanup] helm could not be queried; annotation-less leftovers left alone"
  # 需要从表格同时读取名称和命名空间，跳过标题行；只列名称无法判断跨命名空间的冲突。
  elif printf '%s\n' "${helm_releases}" \
      | awk -v ns="${NS}" 'NR > 1 && $2 != ns { found = 1 } END { exit !found }'; then
    foreign_release_holds_this_name="yes"
    echo "[cleanup] a live release named ${RELEASE} exists in another namespace;" \
      "annotation-less leftovers left alone"
  fi
fi

orphan_sweep() {
  local kind="$1"
  kubectl get "${kind}" -o name 2>/dev/null \
    | grep -Ei 'gpustack|kueue|nfd|node-feature|csi-nfs|csi-s3' \
    | grep -vE -- '-cleanup$' \
    | while read -r obj; do
        # 一次读取三个所有权字段，用竖线分开，减少遍历候选对象时的 API 请求。
        meta="$(kubectl get "${obj}" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-namespace}{"|"}{.metadata.labels.app\.kubernetes\.io/managed-by}{"|"}{.metadata.labels.app\.kubernetes\.io/instance}' 2>/dev/null)"
        owner_ns="${meta%%|*}"
        instance="${meta##*|}"
        managed_by="${meta#*|}"
        managed_by="${managed_by%%|*}"
        if [ -n "${owner_ns}" ]; then
          [ "${owner_ns}" = "${NS}" ] || continue
        else
          [ -z "${foreign_release_holds_this_name}" ] || continue
          [ "${managed_by}" = "Helm" ] && [ "${instance}" = "${RELEASE}" ] || continue
        fi
        echo "[cleanup] delete orphaned ${obj}"
        kubectl delete "${obj}" --ignore-not-found 2>/dev/null || true
      done
}
for r in clusterrole clusterrolebinding csidriver; do
  orphan_sweep "${r}"
done
# kube-system 中 Kueue 的可见性 RoleBinding 使用同样的归属判断。
if [ "$(kubectl -n kube-system get rolebinding kueue-visibility-server-auth-reader \
      -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-namespace}' 2>/dev/null)" = "${NS}" ]; then
  kubectl -n kube-system delete rolebinding kueue-visibility-server-auth-reader --ignore-not-found 2>/dev/null || true
fi

# 删除 Worker 启动时写入的 gpustack-cpu-info NodeFeatureRule，它不属于 Helm 的资源清单。
#
# 固定名称用于发现候选，另用 part-of 标签确认归属，保护管理员自行创建的同名规则。
if [ "$(kubectl get nodefeaturerules.nfd.k8s-sigs.io gpustack-cpu-info \
      -o jsonpath='{.metadata.labels.app\.kubernetes\.io/part-of}' 2>/dev/null)" = "gpustack-operator" ]; then
  kubectl delete nodefeaturerules.nfd.k8s-sigs.io gpustack-cpu-info --ignore-not-found 2>/dev/null || true
fi

# 分别清除经 NFD 传播和由 Worker 直接写入的节点标签。
#
# 删除本安装发布的 NodeFeature，交给 NFD 回收由这些对象聚合出的标签。
for sel in app.kubernetes.io/part-of=gpustack-operator-worker \
  app.kubernetes.io/part-of=gpustack-operator-device-manager topology.gpustack.ai/source-uid; do
  kubectl -n "${NS}" delete nodefeatures.nfd.k8s-sigs.io -l "${sel}" --ignore-not-found 2>/dev/null || true
done
# 逐个节点移除本项目直接管理的拓扑 profile 和设备 fit 标签，保留管理员设置的其他标签。
kubectl get nodes \
  -o go-template='{{range .items}}{{$n := .metadata.name}}{{range $k, $v := .metadata.labels}}{{$n}} {{$k}}{{"\n"}}{{end}}{{end}}' \
  2>/dev/null \
  | awk '$2 == "topology.gpustack.ai/profile" || $2 == "model.csi.gpustack.ai/registered" || $2 ~ /^(sliced-max-free-units|shared-free-cards)\.fit\.gpustack\.ai\// { keys[$1] = keys[$1] " " $2 "-" }
         END { for (n in keys) print n keys[n] }' \
  | while read -r node keys; do
      echo "[cleanup] node ${node}: kubectl label ${keys}"
      # shellcheck disable=SC2086 # 将以空格分隔的各个 <key>- 作为独立的标签移除参数
      kubectl label node "${node}" ${keys} >/dev/null 2>&1 || true
    done

echo "[cleanup] done"

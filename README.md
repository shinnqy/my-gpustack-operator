# GPUStack Operator 学习项目 · 第 26 章

本章把前 25 章写出的程序、节点驱动库和 Helm chart 组成最终交付物。从项目根目录运行下列命令。源码核对和本地编译可以在 Mac 上完成，完整镜像构建及业务验收使用实际实验集群对应的 Linux 平台。

## 核对自己写的代码

```bash
export GUIDE="$HOME/Documents/opensource/gpustack-operator/guide"
export EVIDENCE="$(mktemp -d "${TMPDIR:-/tmp}/operator-26.XXXXXX")"
set -o pipefail
make audit GUIDE="$GUIDE" | tee "$EVIDENCE/source.txt"
make test | tee "$EVIDENCE/go-test.txt"
make build OUT="$EVIDENCE/gpustack-operator"
"$EVIDENCE/gpustack-operator" --help
```

`make audit` 读取讲义的 `steps.toml`，核对各章代码段的数量、顺序和来源行，再到学习项目的同路径文件查找这段实现。Go 的注释、缩进和跨行参数列表末尾的逗号不影响比较，字符串和实际执行的语句参加比较。命令使用本地参考源码，不要求仓库元数据。[核对程序](hack/check/chapter-source/main.go)

本次核对的讲义标记版本为 `a4b463899a64d9e79c3b3c599f470f79f6701a09`。138 个增量实现单元共 10,329 行。学习项目有 137 段实现匹配，另外一段保留了第 15 章 Watch 的断连修复。HTTP 写入中断时，`defer pr.Close()` 唤醒还在管道另一端写入的编码任务。核对程序单独验证这条语句的位置，回归用例验证其效果。[Gateway 回归用例](pkg/workergateway/service/service_test.go)

学习项目按运行入口裁剪过。若要核对配套源码或自己的修改，可另存全文件清单。输出包含中文注释、已裁剪的方法和测试、生成文件以及真正的行为差异；按文件名定位后再核对具体调用者。

```bash
UPSTREAM="$(cd "$GUIDE/.." && pwd -P)"
(
  for dir in api binding cmd csrc pkg staging; do
    rc=0
    diff -qr -x .DS_Store -x docs "$UPSTREAM/$dir" "$dir" || rc=$?
    if [ "$rc" -gt 1 ]; then exit "$rc"; fi
  done
) > "$EVIDENCE/tree-differences.txt"
```

在独立目录删除生成输出、重新运行原生生成器后，得到与本项目相同的 1,023 个生成文件路径。217 个文件因源注释译为中文而出现文字差异，其中包括 CRD 和 OpenAPI 的 description。核对结果中，这些文件的字段、约束和客户端逻辑一致。日后修改手写 API 时，用已有的 `make generate` 更新其输出。

## 编译完整镜像和 chart

在 Linux 构建机进入本项目根目录。准备 Docker Buildx、Helm 3、Go 1.27.3、可访问的组件源码和镜像仓库，并选择与节点一致的 `linux/amd64` 或 `linux/arm64`。完整构建使用多套 CANN、CUDA 和相应目标平台的厂商 SDK，需给构建器预留下载和解压空间。

```bash
export VERSION=v0.0.0-guide
export LAB_IMAGE_TAG=guide-final
: "${LAB_PLATFORM:?填写目标节点的 Linux 平台}"
: "${LAB_IMAGE_REPOSITORY:?填写集群可拉取的 Operator 镜像仓库}"
: "${EVIDENCE:?指定本次验收的输出目录}"
mkdir -p "$EVIDENCE"
set -o pipefail
IMAGE="$LAB_IMAGE_REPOSITORY:$LAB_IMAGE_TAG"
file pack/gpustack-operator/rootfs/lib/x86_64-linux-gnu/gpustack/*.so | tee "$EVIDENCE/driver-libraries.txt"
sha256sum pack/gpustack-operator/rootfs/lib/x86_64-linux-gnu/gpustack/*.so > "$EVIDENCE/driver-sha256.txt"
make chart VERSION="$VERSION" DIST="$EVIDENCE"
make image VERSION="$VERSION" PLATFORM="$LAB_PLATFORM" IMAGE="$IMAGE" 2>&1 | tee "$EVIDENCE/image-build.log"
docker run --rm --platform "$LAB_PLATFORM" "$IMAGE" gpustack-operator --version | tee "$EVIDENCE/image-version.txt"
docker push "$IMAGE" | tee "$EVIDENCE/image-push.txt"
docker buildx imagetools inspect "$IMAGE" | tee "$EVIDENCE/image-digest.txt"
```

`file` 应将两份厂商库都识别为 x86-64 ELF 共享对象。`make image` 从当前学习目录编译 Operator。镜像默认对 `cmd、api、pkg、binding、staging` 中的文件以及 `go.mod、go.sum` 计算 SHA-256，并把组合摘要写入程序版本，也可通过 `COMMIT` 传入自己记录的源码版本。Skopeo、HAMi-core 与 vcann-rt 使用固定内容的源码归档，下载时校验 SHA-256。各厂商阶段和最终镜像之间的库、工具和配置路径与参考项目一致。CUDA 主版本决定 NVIDIA 预加载库的目录，昇腾同时按 CANN 和芯片系列选择。AMD 和平头哥的厂商构建阶段使用 Linux amd64。[镜像构建入口](pack/gpustack-operator/Dockerfile)

父 chart 在本项目内可直接编辑。五个子 chart 使用参考项目已经打过补丁的 Helm 输入，按固定版本打包在 `charts/`。Helm 打父包时会展开依赖，因此 Worker 在镜像内能找到同一份包，升级 hook 也能从中读取 NFD 的 CRD。[依赖版本与启用条件](deploy/gpustack-operator/chart/Chart.yaml) · [部署参数](deploy/gpustack-operator/chart/values.yaml)

## 将最终镜像部署到实验集群

确认当前 shell 有 `LAB_CONTEXT`、`LAB_NS`、`LAB_NODE` 和上述镜像变量。保留前面章节已经验证过的集群配置，例如驱动和实际注册的 CRI handler、启用的厂商、存储和 cert-manager 设置。将这份 values 保存为 `LAB_VALUES`；使用默认配置时可留空。在 Bash 中，同一组参数用于渲染和安装。

```bash
: "${LAB_CONTEXT:?指定真实实验集群的 context}"
: "${LAB_NS:?指定前面章节使用的业务命名空间}"
: "${LAB_NODE:?指定真实加速卡节点}"
K=(kubectl --context "$LAB_CONTEXT")
CHART="$EVIDENCE/gpustack-operator-${VERSION#v}.tgz"
VALUES=(--set-string "image.repository=$LAB_IMAGE_REPOSITORY" --set-string "image.tag=$LAB_IMAGE_TAG")
if [[ -n "${LAB_VALUES:-}" ]]; then VALUES=(-f "$LAB_VALUES" "${VALUES[@]}"); fi
"${K[@]}" get node "$LAB_NODE" -o wide
helm template gpustack-operator "$CHART" --namespace gpustack-system --include-crds "${VALUES[@]}" > "$EVIDENCE/rendered.yaml"
helm upgrade --install gpustack-operator "$CHART" --kube-context "$LAB_CONTEXT" \
  --namespace gpustack-system --create-namespace --wait --timeout 15m "${VALUES[@]}"
"${K[@]}" -n gpustack-system get deploy,ds,pod,svc -o wide | tee "$EVIDENCE/components.txt"
"${K[@]}" -n gpustack-system get pod -o json > "$EVIDENCE/system-pods.json"
"${K[@]}" get apiservice -o wide > "$EVIDENCE/apiservices.txt"
"${K[@]}" get devices.worker.gpustack.ai "$LAB_NODE" -o yaml > "$EVIDENCE/node-devices.yaml"
```

用 `system-pods.json` 中运行容器的 `status.containerStatuses[].imageID` 核对发布记录里的平台镜像 digest。记录节点的驱动、固件、CRI handler、推理镜像和引擎版本。在业务 Pod 里执行真实算子，才能确认驱动和设备交付成功。

## 重放业务请求并留证

沿用前 25 章生成的 `LAB_OUT` 清单、镜像、模型和相同测试输入。每次只重放一种场景，使用本次的最终 release；章节开头用于替换阶段镜像的 `lab_install` 属于分章练习。业务对象仍从 `kubectl apply`、Gateway 或实际推理服务入口进入。将请求、响应、时间、Pod UID、相关日志与指标存到本轮证据目录。

| 章节和请求 | 在哪里看结果 | 通过条件 |
| --- | --- | --- |
| 4–6，发现节点并生成可用规格 | `Devices.spec.groups`、NodeFeature、Node 标签、Topology、ResourceFlavor、ClusterQueue、LocalQueue、InstanceType | 对照驱动列出的卡身份，从该节点一路找到可用的规格与队列入口 |
| 7–8，用 Instance 分别申请 CPU 和一张整卡 | Instance、Pod、Workload admission、容器内 CUDA/NPU 算子、Devices.Status | 记录所获物理卡与实际计算结果，删除实例后同一卡的配额和账本恢复 |
| 9–10，提交两个共享请求，再提交逻辑切片 | 两个 Pod 的卡 UUID、运行时挂载和设备配额、显存分配及并行负载 | 能证明共享的是哪张卡，软切分的显存边界和算力限制有运行数据 |
| 11–12，申请硬件分区并重启管理器 | 支持节点的分区 profile、GI/CI 或厂商实例 ID、Pod UID、marker 和余额 | 同一次分配重试复用预期分区，退出后可重新申请。昇腾 vcann-rt 使用逻辑切片证据 |
| 13–14，提交拓扑工作负载并连接两个 RDMA 端点 | Workload 的拓扑分配、Pod 所在 hostname、容器内设备、对端收发统计 | 角色落位符合要求，两端有实际传输结果和所用网卡身份 |
| 15，从 Worker Gateway 列表并持续 Watch | 第 15 章的 Unix socket、订阅事件、来源集群、取消后的连接 | 创建或停止实际 Instance，能对应初始列表、事件和取消。跨集群用两个独立后端 |
| 16–18，发布权重、提交 Prefetch，再由工作负载挂载 | 固定 artifact 版本及 manifest、NodeModelStore、下载来源、peer 请求、CSI 只读挂载和引用 | 内容摘要、预热命中节点和容器所读内容一致，区分首次远程下载与节点间复制 |
| 19–21，从 router 发起单角色、PD 和 EPD 推理 | Service/EndpointSlice、各角色 Pod UID、联合准入、同一请求的引擎日志、KV 或编码数据传输 | 响应可追溯到实际处理它的角色，角色间有对应的交接证据 |
| 22，跨副本重复同一前缀 | KVCacheBackend/Pool/Binding、租户额度、引擎连接器与共享后端指标 | 在不同实例上核实相同输入确实从共享后端复用，记录命中前后差值 |
| 23，对同一负载打开推测解码 | Pod argv、编译缓存身份、引擎启用日志、draft/accepted token 计数及吞吐和时延 | 正确的引擎收到配置并产生实际草稿与验证数据；保留同负载的基线 |
| 24–25，施压触发 Scaler，再将受管 Elastic EP 宽度从 2 增至 4 | Scaler 建议、目标 generation、准入及 drain，Elastic 操作记录、rank 到 Pod UID 的映射与逐 rank forward | 区分建议、实际副本和已提供服务的宽度。重启 Worker 后根据持久记录恢复。公开接口只接受保持或增大宽度 |

例如，在第 25 章已建立宽度为 2 的真实服务时，先保存初始状态。在另一个终端经该服务已公布的模型名和地址持续发送真实请求，然后编辑同一个对象的 width。

```bash
: "${LAB_ELASTIC_NAME:?填写第 25 章的部署名}"
: "${LAB_BASE_URL:?填写已连通的引擎服务根地址}"
: "${LAB_SERVED_MODEL:?填写该引擎实际公布的模型名}"
"${K[@]}" -n "$LAB_NS" get modeldeployment "$LAB_ELASTIC_NAME" -o yaml > "$EVIDENCE/25-before.yaml"
curl -fsS "$LAB_BASE_URL/v1/models" > "$EVIDENCE/25-models.json"
jq -n --arg model "$LAB_SERVED_MODEL" '{model:$model,prompt:"Count to four:",max_tokens:8}' > "$EVIDENCE/25-request.json"
curl -fsS -H 'Content-Type: application/json' --data-binary @"$EVIDENCE/25-request.json" \
  "$LAB_BASE_URL/v1/completions" > "$EVIDENCE/25-response-before.json"
deployment_uid=$("${K[@]}" -n "$LAB_NS" get modeldeployment "$LAB_ELASTIC_NAME" -o jsonpath='{.metadata.uid}')
record="elastic-resize-$deployment_uid"
rv=$("${K[@]}" -n "$LAB_NS" get configmaps --field-selector "metadata.name=$record" -o jsonpath='{.metadata.resourceVersion}')
"${K[@]}" -n "$LAB_NS" patch modeldeployment "$LAB_ELASTIC_NAME" --type=json \
  -p '[{"op":"replace","path":"/spec/roles/0/elasticEp/width","value":4}]'
"${K[@]}" get --raw "/api/v1/namespaces/$LAB_NS/configmaps?watch=true&resourceVersion=$rv&fieldSelector=metadata.name%3D$record" \
  | tee "$EVIDENCE/25-operation-events.jsonl"
```

这里从 patch 前读到的资源版本开始 Watch，以便记录短暂存在的操作 ConfigMap。完成后控制器可能已将它删除。以下操作可在第二个 Bash 终端运行，沿用相同的变量和 `K` 数组。等待实际宽度收敛，再保存最终状态并结束 Watch。服务地址使用这一部署的 master 端点，确保逐 rank 请求头送达引擎。

```bash
deployment_uid=$("${K[@]}" -n "$LAB_NS" get modeldeployment "$LAB_ELASTIC_NAME" -o jsonpath='{.metadata.uid}')
"${K[@]}" -n "$LAB_NS" get configmap "elastic-observation-$deployment_uid" \
  -o jsonpath='{.data.observation\.json}' | tee "$EVIDENCE/25-observation.json"
"${K[@]}" -n "$LAB_NS" get pod,workload -o yaml > "$EVIDENCE/25-members.yaml"
curl -fsS -H 'Content-Type: application/json' --data-binary @"$EVIDENCE/25-request.json" \
  "$LAB_BASE_URL/v1/completions" > "$EVIDENCE/25-response-after.json"
jq '.max_tokens = 1' "$EVIDENCE/25-request.json" > "$EVIDENCE/25-rank-request.json"
head_selector="app.kubernetes.io/instance=$LAB_ELASTIC_NAME,app.kubernetes.io/component=${LAB_ELASTIC_NAME}-elastic-head"
head_pod=$("${K[@]}" -n "$LAB_NS" get pod -l "$head_selector" -o json |
  jq -er '[.items[] | select(.metadata.deletionTimestamp == null and .status.phase == "Running")] |
          if length == 1 then .[0].metadata.name else error("expected one running Ray head") end')
capture_ray() {
  "${K[@]}" -n "$LAB_NS" exec -i "$head_pod" -c main -- python3 - \
    --gcs 127.0.0.1:6379 --deadline-seconds 20 --max-output-bytes 6291456 \
    < pkg/worker/controllers/worker/model_deployment_elastic_observer.py > "$1"
}
capture_ray "$EVIDENCE/25-ray-before-forwards.json"
for rank in 0 1 2 3; do
  curl -fsS -H 'Content-Type: application/json' -H "X-data-parallel-rank: $rank" \
    --data-binary @"$EVIDENCE/25-rank-request.json" "$LAB_BASE_URL/v1/completions" > "$EVIDENCE/25-rank-$rank.json"
  jq -e '.object == "text_completion" and .usage.completion_tokens == 1' "$EVIDENCE/25-rank-$rank.json"
done
capture_ray "$EVIDENCE/25-ray-after-forwards.json"
```

两个 Ray 快照中，沿 `dp_rank_N` 的 placement group 找到存活的引擎 actor，再以其 `node_id_hex` 找到节点上的 `gpustack-pod-uid` 标签。逐个核对四个 rank 对应的 Pod UID 在前向推理期间是否稳定。每个里程碑分别记录“通过 / 失败 / 未验收 / 不适用”、设备型号和证据路径。“不适用”要写明硬件或引擎限制。[Ray 成员采集脚本](pkg/worker/controllers/worker/model_deployment_elastic_observer.py)

2026-11-27 的本地验收通过了 macOS 和 Linux/arm64 构建、Go 测试、静态检查及两个 Linux 架构的 Dockerfile 检查。Linux/arm64 的 Operator、Skopeo 和命令行工具构建阶段已分别运行。Helm 3.21.4 的六组配置与参考 chart 渲染出等价对象，默认配置为 159 个。Docker 初始可用空间约 15 GiB，完整 arm64 构建所需基础镜像仅压缩层去重后已约 34 GiB。本轮完整多厂商镜像及表中真实硬件、挂载、通信和推理里程碑的状态为“未验收”。

本轮产物位于本地构建目录 `.dist/`，包含从 Operator 构建阶段导出的 Linux/arm64 程序、版本为 `0.0.0-guide` 的 Helm 包和本批次的 `SHA256SUMS`。转移到实验机后，在产物目录运行 `sha256sum -c SHA256SUMS` 即可核对文件。

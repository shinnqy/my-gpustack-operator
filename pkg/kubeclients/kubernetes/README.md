# Kubernetes 类型化客户端

本目录提供 Operator 访问 Kubernetes 的 Go 客户端。`clientset.go` 是各 API 组的统一入口，`scheme/` 提供编解码所需的类型注册，`typed/` 提供各组、各版本的资源操作。**除下表列出的五个扩展外，本目录的 Go 文件都由 Kubernetes 的 `client-gen` 生成。** [Clientset 入口](clientset.go) · [编解码注册](scheme/register.go)

## 一个资源怎样得到客户端

以 `Instance` 为例。项目在 Go 类型上写 `// +genclient`，并将该类型所在的包列入生成清单。修改类型或生成标记后，在仓库根目录重新生成客户端。

```sh
make generate api
```

`gen/api` 将 API 组、版本和资源类型传给 `client-gen`，同时传入生成好的 `applyconfiguration` 包，让各资源客户端的 `Apply` 方法能接收相应的请求类型。生成脚本使用 `go list -deps` 选出 Operator 实际依赖的客户端包，再把产物写回仓库。[Instance 的生成标记](../../../api/worker/v1alpha1/instance.go) · [生成器调用](../../../gen/api/builder/generate.go) · [Apply 请求类型](../applyconfiguration/README.md) · [写回规则](../../../hack/generate_api.py)

例如，`clientset.WorkerV1alpha1().Instances("default").Get(ctx, "demo", opts)` 先选 API 组和版本，再选命名空间中的 `instances` 资源，最后向 API server 读取名为 `demo` 的实例。组客户端和资源客户端分别在两个文件里生成。[组客户端](typed/worker/v1alpha1/worker_client.go) · [Instance 资源客户端](typed/worker/v1alpha1/instance.go)

## 五个保留的扩展文件

以下五个文件来自 Kubernetes `client-go` 的核心资源扩展，生成时作为输入保留。与项目使用的 `client-go v0.36.5` 对照，Event、Node、Pod、Service 四份与上游一致，Namespace 的本地改动是显式返回 `result, err`。

| 文件 | 提供的操作 |
| --- | --- |
| [typed/core/v1/event_expansion.go](typed/core/v1/event_expansion.go) | 按事件对象自身的命名空间创建、更新、修补事件，以及按关联对象查找事件。 |
| [typed/core/v1/namespace_expansion.go](typed/core/v1/namespace_expansion.go) | `Finalize` 更新 Namespace 的 `finalize` 子资源。 |
| [typed/core/v1/node_expansion.go](typed/core/v1/node_expansion.go) | `PatchStatus` 修补 Node 的状态。 |
| [typed/core/v1/pod_expansion.go](typed/core/v1/pod_expansion.go) | `GetLogs` 构造读取 Pod 日志的请求，另提供绑定、驱逐和代理请求。 |
| [typed/core/v1/service_expansion.go](typed/core/v1/service_expansion.go) | `ProxyGet` 通过 Kubernetes 代理向 Service 发起 GET 请求。 |

`client-gen` 会检查每个资源是否已有同名的 `<资源名>_expansion.go`。有这个文件时，生成的资源接口嵌入其中定义的扩展接口，例如 `PodInterface` 嵌入 `PodExpansion`。其余资源的空扩展接口由工具统一写入 `generated_expansion.go`，这个文件本身也是生成产物。[生成器的判断](../../../staging/k8s.io/code-generator/cmd/client-gen/generators/generator_for_expansion.go) · [Pod 接口](typed/core/v1/pod.go) · [生成的空扩展接口](typed/core/v1/generated_expansion.go)

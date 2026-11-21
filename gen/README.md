# 项目的代码生成器

`gen/` 按生成目标组织工具和输入。Kubernetes API 的生成规则归入 `gen/api/`。厂商 C 库的头文件与转换配置归入 `gen/binding/<runtime>/`，生成的 Go 绑定放在仓库根的 `binding/`。`gen/priors/` 用于维护扩缩容所参考的实测容量数据。当前 Makefile 接入了 API 生成目标。[项目命令](../Makefile)

同一个 Kubernetes 资源需要注册类型、编解码对象，还要让客户端能读写它。`gen/api/` 从同一份 Go 类型和注解生成相应代码，结果写回 `api/` 和 `pkg/`。新增资源或字段时，先改 `api/` 中的定义，再从仓库根目录运行 `make generate api`。[命令入口与工具准备](../hack/README.md)

## 谁决定生成什么

`api/main.go` 列出 CRD 类型包、对外提供的聚合 API 类型包、客户端还需要访问的 Kubernetes、NFD、Kueue 类型，以及 webhook 处理器所在的包。`api/builder/generate.go` 把这些输入交给各个生成器。Kubernetes 提供了 DeepCopy、注册和客户端等通用生成器，项目另外维护 CRD、APIService 和 webhook 配置的生成规则。[输入包清单](api/main.go) · [生成步骤](api/builder/generate.go) · [第三方生成器来源](../staging/README.md)

| 输入 | 生成工具 | 产物与用途 |
| --- | --- | --- |
| API 包的 Go 类型、字段标签和包注解 | DeepCopy、注册、转换、Protobuf 和 OpenAPI 生成器 | 在 `api/` 下生成对象深拷贝、类型注册、版本转换、序列化代码和 OpenAPI 定义。 |
| 存储类型上的 `+k8s:crd-gen:resource` 注解及字段定义 | 项目维护的 [crd-gen](api/generator/cmd/crd-gen/generators/crd.go) | 生成 `GetCustomResourceDefinitions`，Worker 用它安装 CRD，让集群能保存这些对象。[CRD 定义](../api/worker/v1alpha1/zz_generated.crds.go) |
| 聚合 API 包的 `+k8s:apireg-gen:service` 注解和公开类型的资源注解 | 项目维护的 [apireg-gen](api/generator/cmd/apireg-gen/generators/apireg.go) | 生成 `GetAPIService` 等代码。Worker 安装 APIService 后，Kubernetes 聚合层将对应组和版本的请求转发给 Worker。[APIService 定义](../api/worker/v1/zz_generated.apiservice.go) · [安装入口](../pkg/worker/apis/setup.go) |
| 资源类型上的 `+genclient` 标记和相关 Go 类型 | Kubernetes 客户端生成器 | 在 `pkg/kubeclients/` 下生成按资源类型访问 API 的客户端，以及构造声明式更新请求的 `applyconfiguration` 包。生成流程也运行 `lister-gen` 和 `informer-gen`。 |
| webhook 处理器上的 `+k8s:webhook-gen` 注解 | 项目维护的 [webhook-gen](api/generator/cmd/webhook-gen/generators/webhook.go) | 生成变更与校验 webhook 的注册配置。请求到达后的默认值和校验逻辑由处理器实现。[注册配置](../pkg/worker/webhooks/worker/zz_generated.webhooks.go) · [Instance 处理器](../pkg/worker/webhooks/worker/instance.go) |

例如，`InstanceSpec.Type` 是项目定义的规格名字段。生成器据此写出 `WithType(value string)`，用于构造设置该字段的请求。字段在 `InstanceSpec` 中维护，生成方法随字段一起更新。[字段定义](../api/worker/v1alpha1/instance.go) · [生成的 WithType 方法](../pkg/kubeclients/applyconfiguration/worker/v1alpha1/instancespec.go)

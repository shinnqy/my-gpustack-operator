# 用于 Kubernetes Apply 请求的类型

**本目录的所有 Go 文件都由 Kubernetes 的 `applyconfiguration-gen` 生成。** 这些类型让调用者用 Go 方法组装 Server-Side Apply 请求，明确写出自己要设置的字段。目录按 API 组和版本组织，例如 `worker/v1alpha1/` 对应 `worker.gpustack.ai/v1alpha1`。

## 从一个字段到一个方法

项目在 `api/worker/v1alpha1/instance.go` 中把规格名定义为 `Type string`。生成器读取这个 Go 字段，写出 `InstanceSpecApplyConfiguration.Type *string` 和 `WithType(value string)`。这里使用指针，是为了让请求能区分“省略这个字段”和“显式传入一个字符串”。[字段定义](../../../api/worker/v1alpha1/instance.go) · [生成模板](../../../staging/k8s.io/code-generator/cmd/applyconfiguration-gen/generators/applyconfiguration.go) · [生成的 WithType](worker/v1alpha1/instancespec.go)

下面用 `applyv1alpha1` 作为本目录下 `worker/v1alpha1` 包的导入别名，构造一个名为 `demo` 的实例请求。

```go
spec := applyv1alpha1.InstanceSpec().WithType("one-gpu").WithImage("nginx")
request := applyv1alpha1.Instance("demo", "default").WithSpec(spec)
```

此时 `request` 在内存里，包含实例名 `demo`、命名空间 `default` 和填好的规格。调用类型化客户端的 `Apply` 方法时，客户端才会把请求发给 Kubernetes。[请求构造函数](worker/v1alpha1/instance.go) · [接收该请求的客户端](../kubernetes/typed/worker/v1alpha1/instance.go)

## 重新生成

修改项目的 API 类型后，在仓库根目录重新生成。

```sh
make generate api
```

`gen/api/main.go` 列出项目 API 和要访问的 Kubernetes、Kueue、NFD 等外部 API 包。`gen/api/builder/generate.go` 把这些类型交给 `applyconfiguration-gen`，生成各组、各版本的请求类型。生成脚本再按 Operator 的 Go 包依赖写回用到的客户端包。要改变某个自有资源的请求字段，从 `api/` 中的源类型改起。[输入包清单](../../../gen/api/main.go) · [生成步骤](../../../gen/api/builder/generate.go) · [写回规则](../../../hack/generate_api.py)

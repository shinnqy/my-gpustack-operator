# 第三方依赖源码

`staging/` 保存从上游 Git 仓库按固定版本克隆的第三方源码。**从零开发 Operator 时，这些库直接引入即可，无需手写。** 根目录的 `go.mod` 通过 `replace` 让 Go 使用本地副本。生成 API 时，`protoc` 也从这里查找依赖的 `.proto` 文件。[Go 模块配置](../go.mod) · [Protobuf 导入路径](../gen/api/main.go)

## 依赖从哪里来

首次引入一个模块时，按下表的 tag 克隆上游仓库，把构建和生成 API 所需的文件连同许可证保存在对应目录，并在模块根的 `_VERSION_` 中写入 tag。其中三个模块还需要应用本项目的补丁。

| 本目录下的位置 | 上游仓库与固定 tag | 本地补丁 |
| --- | --- | --- |
| `k8s.io/api/` | [kubernetes/api · v0.36.5](https://github.com/kubernetes/api/tree/v0.36.5) | 无 |
| `k8s.io/apimachinery/` | [kubernetes/apimachinery · v0.36.5](https://github.com/kubernetes/apimachinery/tree/v0.36.5) | 无 |
| `k8s.io/apiextensions-apiserver/` | [kubernetes/apiextensions-apiserver · v0.36.5](https://github.com/kubernetes/apiextensions-apiserver/tree/v0.36.5) | 无 |
| `k8s.io/kube-aggregator/` | [kubernetes/kube-aggregator · v0.36.5](https://github.com/kubernetes/kube-aggregator/tree/v0.36.5) | 无 |
| `github.com/gogo/protobuf/` | [gogo/protobuf · v1.3.2](https://github.com/gogo/protobuf/tree/v1.3.2) | 无 |
| `k8s.io/code-generator/` | [kubernetes/code-generator · v0.36.5](https://github.com/kubernetes/code-generator/tree/v0.36.5) | API 注册代码模板 |
| `github.com/go-logr/logr/` | [go-logr/logr · v1.4.3](https://github.com/go-logr/logr/tree/v1.4.3) | 日志写入接口和格式化方法 |
| `k8s.io/klog/` | [kubernetes/klog · v2.140.0](https://github.com/kubernetes/klog/tree/v2.140.0) | 按行输出日志，对接本地 `logr` |

例如，项目首次使用 Kubernetes 的 `Pod` 类型时，可以先取 `k8s.io/api` 的 `core/v1` 包。在已有 `go.mod` 的项目根目录执行以下命令。以后用到同一模块的 `apps/v1` 等包，再从同一 tag 取相应目录。

```sh
api_src=$(mktemp -d)
git clone --depth 1 --branch v0.36.5 https://github.com/kubernetes/api.git "$api_src"
mkdir -p staging/k8s.io/api
git -C "$api_src" archive HEAD LICENSE go.mod go.sum core/v1 | tar -xf - -C staging/k8s.io/api
printf '%s' v0.36.5 > staging/k8s.io/api/_VERSION_
go mod edit -require=k8s.io/api@v0.36.5 -replace=k8s.io/api=./staging/k8s.io/api
```

## 哪些文件带有补丁

在本目录收录的上游文件中，以下 **6 个文件**的内容与对应 tag 有差异。链接指向应用补丁后的文件。各模块根目录的 `_VERSION_` 是引入时另写的版本记录。

| 文件（相对本目录） | 项目改了什么 |
| --- | --- |
| [k8s.io/code-generator/cmd/register-gen/generators/register_external.go](k8s.io/code-generator/cmd/register-gen/generators/register_external.go) | 调整注册模板，生成 `SchemeGroupVersionResource`、`Kind`、`SchemeGroupVersionKind`，并让已有的 `Resource` 复用新方法。Worker 注册 `devices` 时据此取得组、版本和资源名。[Devices 注册代码](../pkg/worker/extensionapis/worker/devices.go) |
| [github.com/go-logr/logr/logr.go](github.com/go-logr/logr/logr.go) | 给 `Logger` 添加 `Write`，使它可作为 `io.Writer` 接收子进程输出。另加 `Infof`、`Errorf` 等格式化、按行和指定调用深度的日志方法。[子进程日志接线](../pkg/internalprocesses/kubernetes/embed.go) |
| [k8s.io/klog/klogr.go](k8s.io/klog/klogr.go) | 给日志后端 `klogger` 添加 `Write`，把收到的字节按行拆开，每行带着已有的键值字段交给 `PrintSDepth`。 |
| [k8s.io/klog/klog.go](k8s.io/klog/klog.go) | 增加 `PrintSDepth` 和内部输出方法，写入上述日志时跳过 klog 默认的行头。 |
| [k8s.io/klog/go.mod](k8s.io/klog/go.mod) | 增加 `replace`，单独构建 `k8s.io/klog/v2` 这个模块时也使用本地的 `logr`。 |
| [k8s.io/klog/go.sum](k8s.io/klog/go.sum) | 移除上游 `logr v1.4.1` 的两条校验和。改用本地路径后，该文件为空。 |

要看具体改了哪几行，可以在项目根目录取一份干净的上游源码，再比较表中对应的文件。以 API 注册模板为例，下面的命令会打印可单独保存的补丁。

```sh
upstream_dir=$(mktemp -d)
git clone --depth 1 --branch v0.36.5 \
  https://github.com/kubernetes/code-generator.git "$upstream_dir/code-generator"
template=cmd/register-gen/generators/register_external.go
cp "staging/k8s.io/code-generator/$template" "$upstream_dir/code-generator/$template"
git -C "$upstream_dir/code-generator" diff -- "$template"
```

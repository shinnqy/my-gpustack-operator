# 项目开发脚本

`hack/` 用于维护依赖准备、代码生成、构建检查等开发操作所需的脚本和模板。根目录的 Makefile 列出可用命令。以 API 变更为例，`make generate api` 调用本目录的脚本更新生成文件，`make build` 则直接使用工作树中的代码编译 Operator。[Makefile](../Makefile)

## 从 API 定义重新生成代码

修改 `api/` 中的类型或 webhook 处理器上的注册注解后，在仓库根目录重新生成 API。

```sh
make generate api
```

1. **准备工具。** `generate.sh` 检查 `protoc`、`protoc-gen-go`、`protoc-gen-gogo` 和 `goimports` 的版本。符合要求的工具直接复用，缺失或版本不符时安装到仓库的 `.sbin/`。Protobuf 工具的版本在 `lib/proto.sh`，`goimports` 的版本在 `lib/style.sh`。[Shell 入口](generate.sh) · [Protobuf 工具](lib/proto.sh) · [goimports](lib/style.sh)
2. **在临时目录生成。** `generate_api.py` 将输入复制到物理路径以 `gpustack.ai/gpustack` 结尾的临时目录。这个路径让 `go-to-protobuf` 能按模块名算出输出位置。脚本在副本中清除已有生成文件，再执行 `go run -mod=readonly ./gen/api`。生成器从 `boilerplate/go.txt` 读取版权头。[工作目录与执行过程](generate_api.py) · [版权头模板](boilerplate/go.txt) · [Go 生成器](../gen/README.md)
3. **写回项目。** 脚本写回 `api/` 下的生成文件和 webhook 注册配置。对于 `pkg/kubeclients/`，它用 `go list -deps ./cmd/gpustack-operator` 选出二进制实际依赖的客户端包。内容变化的产物会被替换，过时的产物会被删除。手写的客户端扩展仍作为输入保留，例如给 Pod 客户端提供 `GetLogs` 的 `pod_expansion.go`。[写回逻辑](generate_api.py) · [Pod 客户端扩展](../pkg/kubeclients/kubernetes/typed/core/v1/pod_expansion.go)

安装脚本通过 `lib/util.sh` 识别系统、架构和已安装的 Go 工具版本，通过 `lib/log.sh` 输出安装和版本检查消息。[平台与版本识别](lib/util.sh) · [脚本日志](lib/log.sh)

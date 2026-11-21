# Worker 准入 webhook

**本包只有 [zz_generated.webhooks.go](zz_generated.webhooks.go) 由工具生成。** 它构造 Kubernetes 的 webhook 注册配置，并提供处理器的 HTTP 路径。其余 Go 文件由项目维护，负责填默认值、校验请求以及这些操作所需的辅助逻辑。

## 从处理器注解到集群注册

以 `InstanceWebhook` 为例。类型上方的 `+k8s:webhook-gen:validating` 注解指定 API 组 `worker.gpustack.ai`、版本 `v1alpha1`、资源 `instances`，以及需要校验的 `CREATE` 和 `UPDATE` 操作。同一文件中的 `ValidateCreate` 和 `ValidateUpdate` 实现具体的校验，`Default` 实现默认值处理。[Instance 的注解和处理方法](instance.go)

修改注册注解后，在仓库根目录重新生成注册配置。

```sh
make generate api
```

1. `gen/api/main.go` 将本包列入 `Webhooks`。生成流程先准备处理器依赖的客户端，再调用项目自己的 `webhook-gen` 解析注解，写出 `GetWebhookConfigurations` 等函数。上例还会得到校验路径 `/validate-worker-gpustack-ai-v1alpha1-instance`。[输入包清单](../../../../gen/api/main.go) · [生成规则](../../../../gen/api/generator/cmd/webhook-gen/generators/webhook.go)
2. Worker 启动时，`Worker.Prepare` 调用 `webhooks.Install`，使用生成的配置将匹配哪些请求、请求发往哪里等信息安装到集群。`Worker.Start` 调用 `webhooks.Setup`，将手写处理器接到 Worker 的 HTTP 路由。新增处理器时，也要将它加入上一级 `setup.go` 的 `setups` 列表。[Worker 启动入口](../../worker.go) · [处理器列表与注册入口](../setup.go)

## 人工维护的文件

| 负责的资源或行为 | 文件 |
| --- | --- |
| 实例与实例规格 | [instance.go](instance.go)、[instance_type.go](instance_type.go) |
| Pod 和 Kueue Workload 的准入 | [pod.go](pod.go)、[workload.go](workload.go) |
| KV Cache 后端、池、绑定，以及 Pod 的缓存解析与注入 | [kv_cache_backend.go](kv_cache_backend.go)、[kv_cache_pool.go](kv_cache_pool.go)、[kv_cache_pool_binding.go](kv_cache_pool_binding.go)、[pod_kv_cache.go](pod_kv_cache.go)、[pod_kv_cache_resolve.go](pod_kv_cache_resolve.go)、[pod_kv_cache_inject.go](pod_kv_cache_inject.go) |
| 模型制品、预热和模型存储 | [model_artifact.go](model_artifact.go)、[model_artifact_status.go](model_artifact_status.go)、[model_prefetch.go](model_prefetch.go)、[model_store_binding.go](model_store_binding.go)、[node_model_store.go](node_model_store.go) |
| 模型部署、弹性调整、多角色推理和扩缩容 | [model_deployment.go](model_deployment.go)、[model_deployment_elastic.go](model_deployment_elastic.go)、[model_deployment_epd.go](model_deployment_epd.go)、[model_deployment_scaler.go](model_deployment_scaler.go) |
| 拓扑来源 | [topology_source.go](topology_source.go) |

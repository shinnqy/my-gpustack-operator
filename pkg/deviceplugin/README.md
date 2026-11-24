# pkg/deviceplugin

本目录是节点侧 **Kubernetes Device Plugin 通用引擎** 与 **节点设备账本（Ledger）控制器**，被所有厂商分配器共同复用。

---

## 一、定位与核心作用

如果把各厂商的分配逻辑比作不同品牌的“发卡营业厅”，`pkg/deviceplugin/` 就是它们共用的**统一注册机、收银台和账本系统**。

它的核心目标是**把与具体硬件厂商无关的公共机制全部下沉收口**，避免 NVIDIA、Ascend、AMD、Hygon 等 9 家厂商以及 RDMA 各自重复实现：

1. **统一对接 `kubelet`**：管理 Unix Socket、gRPC Server 生命周期、向 `kubelet` 注册及重启自动重连。
2. **统一维护节点设备账本**：监听本机 `Devices` 和 `Pod`，持续重建 `Devices.status`（记录每张卡被哪个 Pod 占用、Pod 退出后自动释放），并广播变更触发刷新与后台资源回收。
3. **统一处理设备注入策略**：判断节点环境应该走 **CDI 通道** 还是 **环境变量通道**，并严格保证二选一，避免双重注入冲突。
4. **统一处理软切分算账与门槛**：换算容器请求的显存 MiB / 算力百分比、拦截没有剩余显存空间的物理卡。
5. **提供整机 RDMA 设备插件服务**：从 `Devices.spec.interfaces` 实时推导并向 `kubelet` 暴露节点级 RDMA 端点。

---

## 二、与 `pkg/devicemanager/allocator/` 及周边模块的关系

整个节点侧设备管理分为清晰的四层，各层职责边界分明：

```text
                       kubelet (gRPC: ListAndWatch / Allocate)
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  pkg/devicemanager/allocator/   （厂商适配层 / 按厂商动态启停总控）         │
│  - allocator.go：按探测结果启停厂商插件，Linux 节点单独启动 RDMA 插件        │
│  - nvidia/ ascend/ amd/ ...：按模式组装插件，实现各自 Responder 注入与硬件动作│
└───────────────────┬───────────────────────────────────────┬─────────────┘
                    │ 复用通用服务、账本、注入策略、回收循环      │ 调用厂商驱动做 MIG/共享开关等
                    ▼                                       ▼
┌───────────────────────────────────────────┐   ┌─────────────────────────┐
│  pkg/deviceplugin/   （本目录：公共引擎）    │   │  binding/  （CGO 驱动桥） │
│  - gRPC 服务与 kubelet 注册 (serving/server)│   │  - nvml / dcmi / ...    │
│  - 节点账本控制与广播 (DevicesReconciler)    │   └─────────────────────────┘
│  - 注入策略决策 (CDI vs 环境变量)            │
│  - 软切分额度换算与空位校验 / RDMA 插件实现   │
└───────────────────▲───────────────────────┘
                    │ 读取 Devices.spec（由 Detector 上报的硬件与网卡清单）
┌───────────────────┴─────────────────────────────────────────────────────┐
│  pkg/devicemanager/detector/   （探测与监控层：只读硬件、上报 K8s、采指标）  │
└─────────────────────────────────────────────────────────────────────────┘
```

### `pkg/deviceplugin/` 与 `pkg/devicemanager/allocator/` 的分工对照

| 维度 | `pkg/deviceplugin/`（本目录） | `pkg/devicemanager/allocator/` |
| --- | --- | --- |
| **角色** | 通用框架与公共基础设施（厂商无关） | 总控调度 + 各厂商差异化实现（厂商相关） |
| **依赖方向** | 被 `allocator/` 依赖，不反向 import `allocator/` | import 并组合 `pkg/deviceplugin/` 的 `ResourceServer` / `RDMAServer` / `InjectionResolver` / `RunReclaimLoop` |
| **典型职责** | 起 gRPC、向 kubelet 注册、维护全节点账本与分配互斥锁、判断 CDI 还是 envvar、计算软切分显存/算力 | 收到 `Detector` 厂商通知后启停插件；决定某厂商支持哪几种模式；通过 `Responder` 回调填厂商专属环境变量/挂载；调 `binding/` 切 MIG 或开昇腾共享开关 |

---

## 三、文件结构与模块索引

下面按职责列出本目录的实现文件。

### 1. 节点设备账本与资源回收
| 文件 | 作用 |
| --- | --- |
| `controller.go` | `DevicesReconciler`：监听本机 `Devices` 和 `Pod`，重建 `Devices.status` 账本，为同一节点上的分配提供互斥锁，并在设备状态变化时通知插件 |
| `terminated.go` | 判断 Pod 是否彻底终结不再重启，过滤不影响账本的 Pod 更新事件 |
| `kubelet_pods.go` | 查询本机 kubelet 已接收哪些 Pod、哪个容器仍在等待设备 |
| `kubelet_allocations.go` | 对照 kubelet 持有的设备 ID 和 Pod 注解，出现归属冲突时阻止新分配和账本发布 |
| `gc.go` | 对照连续多轮存活 Pod 的 UID，回收已消失 Pod 的切片工作目录 |
| `pod_labels.go` | Chart 与控制器共用的 Pod 标签/索引常量契约，集中定义避免多处漂移 |
| `reclaim.go` | `RunReclaimLoop`：结合账本广播与 60 秒兜底定时器，驱动有状态资源（MIG、sMLU、sGPU 等）的后台回收 |

### 2. 通用 Device Plugin gRPC 服务
| 文件 | 作用 |
| --- | --- |
| `serving.go` | 封装 Unix Socket 监听、gRPC Server 启停、定时巡检 Socket 与向 `kubelet` 自动重注册 |
| `server.go` | `ResourceServer`：面向 `kubelet` 的通用加速器插件服务，注册资源、发布设备清单、推荐候选卡，在 `Allocate` 中预留设备并写 Pod 注解，最后由厂商 `Responder` 构造容器响应 |
| `responder.go` | 定义厂商需要实现的 `Responder` 回调接口（构造 `Allocate` 响应等） |
| `resource.go` | 内部设备资源标识抽象（统一承载加速器、网卡端口等） |
| `types.go` | 重新导出 `k8s.io/kubelet` Device Plugin v1beta1 API 类型别名，减少各厂商包的直接耦合 |

### 3. 容器设备注入策略（CDI 通道 vs 环境变量通道）
| 文件 | 作用 |
| --- | --- |
| `container_engine.go` | 读取节点 containerd 配置，识别容器引擎是否支持 CDI 及默认 RuntimeHandler |
| `cdi.go` | 扫描 `/etc/cdi` 与 `/var/run/cdi` 下的 CDI 规范文件，构造标准 CDI 设备名 |
| `device_injection.go` | `InjectionResolver`：按策略（`envvar` / `cdi-annotations` / `auto`）在环境变量通道和 CDI 通道间**严格二选一**，防止双重注入导致挂载冲突或硬件越权泄露 |
| `inject.go` | 封装软切分预加载库路径（`/etc/gpustack/lib`）、容器工作目录及 `DeviceSpec` 辅助构造方法 |

### 4. 切分额度换算与空位校验
| 文件 | 作用 |
| --- | --- |
| `budget.go` | `SlicedMemoryMib` / `SlicedCoresPercent`：将容器请求的显存与算力配额换算为实际 MiB 与百分比 |
| `sliced_room.go` | 软切分分配门槛检查，拒绝向已无剩余显存空间的物理卡继续分配（支持通过 `GPUSTACK_DEVICE_PLUGIN_SLICED_ALLOCATE_GATE` 开关控制） |
| `allocated.go` | 将 `kubelet` 分配出的设备 ID 映射回 `Devices` 中完整的卡与组级元数据，供厂商注入器读取 |

### 5. 整机 RDMA 设备插件服务
| 文件 | 作用 |
| --- | --- |
| `rdma_server.go` | `rdmaServer`：复用 `serving.go` 面向 `kubelet` 提供 `ListAndWatch`，按模式发布节点级 RDMA 资源 |
| `rdma_endpoint.go` | 从 `Devices.spec.interfaces`（含 PF 与 SR-IOV VF）实时推导可分配的 RDMA 端点及健康状态 |
| `rdma_allocate.go` | 从当前 `Devices.spec.interfaces` 解析 kubelet 选中的 token，按端点去重，返回设备和 `NCCL_IB_HCA` |
| `rdma_devices.go` | 从 Linux sysfs 查找与 RDMA 设备名对应的 `/dev/infiniband/uverbsN` |

---

## 四、学习进度提示

在本仓库的渐进式学习路径中：
- **第 5 章**：随着 `device-manager serve` 接通，本目录作为编译与启动底座整体引入；
- **第 8 章（整卡）** 已接通独占分配。Pod 请求一张卡后，kubelet 从清单选择令牌，插件记录实际卡的归属。Pod 删除后账本恢复满额。
- **第 9～11 章（共享 / 软切 / 硬切）**：完整串联 `budget.go`、`sliced_room.go`、`inject.go`、`reclaim.go`；
- **第 14 章（RDMA）** 已接通端点分配。ListAndWatch 将失败链路标记为 `Unhealthy`。Allocate 复核授予的端点是否仍属于当前模式，只交付这些端点对应的设备，并在宿主机存在时附带一次 `rdma_cm`。

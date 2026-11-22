// Copyright 2026 GPUStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package elasticprofile 负责定义弹性集合通信组所支持的规格配置：operator 渲染、准入和
// 观测的唯一宽度范围。
//
// 上下界在此集中声明一次，所有使用方均从本包读取，而不各自保留数值副本。多出一份副本
// 就会多出一个口径，两者会在无人留意的方向上逐渐偏离：仅放宽准入会导致原生观测拒绝
// 部署已被允许请求的宽度，而仅收窄渲染则会留下一个无人拦截的宽度。需要不同边界的使用方
// 不应重复声明，而应说明为何绑定到其他边界，正如原生观测器所做的那样。
//
// 此处并未放宽规格本身。这些是托管 elastic-EP 规格一直支持的边界，集中声明只是明确
// 其既有定义，而非扩大范围。API 类型自身的 kubebuilder 标记携带了相同的上下界用于
// API server 的模式校验；其下的准入逻辑则负责在本 operator 中强制执行这些边界。
package elasticprofile

const (
	// WidthMin 是最小的弹性集合通信组宽度：引擎工作组加上该规格始终预留的一个 API/DP-master 成员。
	WidthMin = 2

	// WidthMax 是 operator 支持的最大弹性集合通信组宽度（含引擎 rank）。
	// 它限制单次原生观测的原因与限制规格相同：绝不能由不可信的引擎响应决定 operator 生成多少探测流量。
	WidthMax = 64
)

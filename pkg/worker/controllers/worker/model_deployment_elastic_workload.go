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

package worker

import (
	"strconv"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/elasticprofile"
)

// ModelDeploymentElasticBootWidthAnnotation 在 master Pod 上记录引导宽度。
const ModelDeploymentElasticBootWidthAnnotation = "worker.gpustack.ai/elastic-boot-width"

// ModelDeploymentElasticMasterBootWidthAnnotation 是 master Pod 的引导记录注解。
const ModelDeploymentElasticMasterBootWidthAnnotation = ModelDeploymentElasticBootWidthAnnotation

// ModelDeploymentElasticRayNodeIdentityLabel 将 Ray 节点成员身份绑定到所属 Pod UID。
const ModelDeploymentElasticRayNodeIdentityLabel = "gpustack-pod-uid"

// ModelDeploymentElasticHeadCommand 启用原生 vLLM 所需的状态服务。
const ModelDeploymentElasticHeadCommand = "ray start --head --include-dashboard=true"

// 判断是不是弹性部署
// ModelDeploymentElasticRole 返回请求托管弹性配置的角色。
func ModelDeploymentElasticRole(md *workercore.ModelDeployment) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].ElasticEP != nil {
			return &md.Spec.Roles[i]
		}
	}
	return nil
}

// ModelDeploymentElasticBootstrapFromMaster 保留存活 master 已捕获的引导宽度。
// 若实时记录缺失或无效，则阻塞调谐；新创建的 master 使用当前请求宽度。
//
// 该宽度受共享弹性配置边界约束而非使用独立数值，
// 从而确保渲染出的引导配置绝不会超出准入校验允许或原生观测可度量的范围。
func ModelDeploymentElasticBootstrapFromMaster(masterUID, raw string, md *workercore.ModelDeployment) (int32, bool) {
	width := int64(0)
	if masterUID != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, false
		}
		width = parsed
	} else if role := ModelDeploymentElasticRole(md); role != nil {
		width = int64(role.ElasticEP.Width)
	}
	if width < elasticprofile.WidthMin || width > elasticprofile.WidthMax {
		return 0, false
	}
	return int32(width), true
}

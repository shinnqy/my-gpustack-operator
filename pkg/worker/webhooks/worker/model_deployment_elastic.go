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
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/elasticprofile"
)

// modelDeploymentElasticOwnedFlags 是弹性渲染器自行写入的引擎标志。
// 若 role 自身的 argv 中也出现这些条目，就会与 Operator 竞争同一个参数，因此直接拒绝而非合并。
// 该列表设计得尽可能窄：无关的引擎调优参数仍由作者自行控制，无关的 Ray_* 调优键也不会被禁止。
var modelDeploymentElasticOwnedFlags = sets.New(
	"--data-parallel-size",
	"--data-parallel-backend",
	"--data-parallel-address",
	"--enable-expert-parallel",
	"--data-parallel-size-local",
	"--enable-elastic-ep",
	"--enable-eplb",
	"--distributed-executor-backend",
	"--ray-address",
)

// modelDeploymentElasticOwnedEnv 是弹性渲染器自行写入的环境变量条目，拒绝条件与标志相同。
var modelDeploymentElasticOwnedEnv = sets.New(
	"VLLM_DP_SIZE",
	"VLLM_DATA_PARALLEL_SIZE_LOCAL",
	"VLLM_ENABLE_ELASTIC_EP",
	"VLLM_ENABLE_EPLB",
	"VLLM_TENSOR_PARALLEL_SIZE",
	"VLLM_PIPELINE_PARALLEL_SIZE",
	"VLLM_RAY_ADDRESS",
	"RAY_ADDRESS",
	"GPUSTACK_ELASTIC_POD_UID",
	"GPUSTACK_ELASTIC_POD_IP",
	"VLLM_RAY_DP_PACK_STRATEGY",
	"RAY_OVERRIDE_LABELS",
)

// ValidateModelDeploymentElasticEP 结合历史状态校验部署的弹性 EP（elastic-EP）配置。
// old 为 nil 表示创建操作。该检查在每次创建和更新时都会运行；不可变性检查仅在配置此前已存在时触发。
func (r *ModelDeploymentWebhook) ValidateModelDeploymentElasticEP(
	ctx context.Context, md, old *workercore.ModelDeployment,
) field.ErrorList {
	elasticRoles := modelDeploymentElasticRoles(md)
	if len(elasticRoles) == 0 {
		if old != nil && elasticRoleOf(old) != nil {
			return field.ErrorList{field.Forbidden(
				field.NewPath("spec", "roles").Child("elasticEp"), modelDeploymentIdentityMessage,
			)}
		}
		return nil
	}

	errs := validateModelDeploymentElasticShape(md, elasticRoles)
	if old != nil {
		errs = append(errs, validateModelDeploymentElasticIdentity(md, old)...)
	}

	return errs
}

// elasticRoleOf 返回部署中唯一的弹性 role，若不存在则返回 nil。
func elasticRoleOf(md *workercore.ModelDeployment) *workercore.ModelDeploymentRole {
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].ElasticEP != nil {
			return &md.Spec.Roles[i]
		}
	}
	return nil
}

func modelDeploymentElasticRoles(md *workercore.ModelDeployment) []*workercore.ModelDeploymentRole {
	var roles []*workercore.ModelDeploymentRole
	for i := range md.Spec.Roles {
		if md.Spec.Roles[i].ElasticEP != nil {
			roles = append(roles, &md.Spec.Roles[i])
		}
	}
	return roles
}

// modelDeploymentElasticRolePath 返回弹性 role 在列表中的字段路径。
func modelDeploymentElasticRolePath(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) *field.Path {
	for i := range md.Spec.Roles {
		if &md.Spec.Roles[i] == role {
			return field.NewPath("spec", "roles").Index(i)
		}
	}
	return field.NewPath("spec", "roles")
}

// validateModelDeploymentElasticShape 对新对象强制执行配置的结构规则：
// 在 vLLM 引擎上仅有一个弹性 role、单个受管 Server 实例、每个成员使用整张且未切片的加速卡、
// 不接管 argv，且不包含渲染器独占的原生标志或环境变量。
func validateModelDeploymentElasticShape(
	md *workercore.ModelDeployment, elasticRoles []*workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList
	rolesPath := field.NewPath("spec", "roles")

	if md.Spec.Engine.Name != workercore.ModelDeploymentEngineVLLM {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "engine", "name"), md.Spec.Engine.Name,
			"the elastic-EP profile runs the vLLM engine; no other engine carries the elastic contract this profile renders",
		))
	}
	if len(elasticRoles) > 1 {
		errs = append(errs, field.Forbidden(
			rolesPath, "at most one role may carry spec.roles[].elasticEp: the profile is one managed vLLM Server role",
		))
	}
	if len(elasticRoles) > 0 && len(md.Spec.Roles) > 1 {
		errs = append(errs, field.Forbidden(
			rolesPath, "spec.roles[].elasticEp is the deployment's ONLY role: one managed vLLM Server role, "+
				"whose width -- not a second role -- sizes the collective",
		))
	}

	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if role.ElasticEP == nil {
			continue
		}
		rolePath := rolesPath.Index(i)

		if role.ElasticEP.Width < elasticprofile.WidthMin || role.ElasticEP.Width > elasticprofile.WidthMax {
			errs = append(errs, field.Invalid(
				rolePath.Child("elasticEp", "width"), role.ElasticEP.Width,
				fmt.Sprintf("total GPU engines including the reserved master must be between %d and %d",
					elasticprofile.WidthMin, elasticprofile.WidthMax),
			))
		}
		declared, err := workerctrl.ParseModelDeploymentDeclaredParallelism(md.Spec.Engine.Name, role.ExtraArgs, role.Env)
		if err != nil {
			errs = append(errs, field.Invalid(rolePath.Child("extraArgs"), role.ExtraArgs, err.Error()))
		} else {
			if declared.ElasticEPMaxDataParallel > 0 && int64(role.ElasticEP.Width) > int64(declared.ElasticEPMaxDataParallel) {
				errs = append(errs, field.Invalid(rolePath.Child("elasticEp", "width"), role.ElasticEP.Width,
					fmt.Sprintf("must not exceed the startup --elastic-ep-max-dp-size (%d)", declared.ElasticEPMaxDataParallel)))
			}
			if declared.PipelineParallel != 1 || declared.PrefillContextParallel != 1 {
				errs = append(errs, field.Invalid(rolePath.Child("extraArgs"), role.ExtraArgs,
					"each elastic member holds one TP group; pipeline and prefill context parallel sizes must be one"))
			}
			errs = append(errs, validateModelDeploymentElasticResources(role, rolePath, declared.TensorParallel)...)
		}
		if role.Kind != workercore.ModelDeploymentRoleKindServer {
			errs = append(errs, field.Invalid(
				rolePath.Child("kind"), role.Kind,
				"the elastic-EP profile is one managed vLLM Server role; prefill/decode kinds do not compose with it",
			))
		}
		if role.Replicas != 1 {
			errs = append(errs, field.Invalid(
				rolePath.Child("replicas"), role.Replicas,
				"the elastic-EP profile is one serving instance; width, not replicas, sizes the collective",
			))
		}
		if role.ReplicaSize != 1 {
			errs = append(errs, field.Invalid(
				rolePath.Child("size"), role.ReplicaSize,
				"each elastic member is exactly one Pod; the member count is width, not size",
			))
		}
		if len(role.Command) > 0 {
			errs = append(errs, field.Forbidden(
				rolePath.Child("command"),
				"the elastic renderer owns the argv of the managed role; a taken-over command has no elastic rendering",
			))
		}
		errs = append(errs, validateModelDeploymentElasticOwnedKeys(role, rolePath)...)
	}

	return errs
}

// validateModelDeploymentElasticResources 要求每个成员具备完整且固定的 TP 组。
// 每张加速卡都必须是整卡、未切片且未切分的。
func validateModelDeploymentElasticResources(
	role *workercore.ModelDeploymentRole, rolePath *field.Path, tp int,
) field.ErrorList {
	var errs field.ErrorList
	res := role.Resources
	if res == nil {
		return errs
	}
	resPath := rolePath.Child("resources")

	if res.Accelerator != nil && res.Accelerator.CmpInt64(int64(tp)) != 0 {
		errs = append(errs, field.Invalid(
			resPath.Child("accelerator"), res.Accelerator.String(),
			fmt.Sprintf("each elastic member takes exactly %d whole accelerators per Pod", tp),
		))
	}
	if res.AcceleratorSlicedMemoryPercentage != 0 {
		errs = append(errs, field.Forbidden(
			resPath.Child("acceleratorSlicedMemoryPercentage"),
			"the elastic profile takes whole, non-sliced accelerators",
		))
	}
	if res.AcceleratorSlicedCoresPercentage != 0 {
		errs = append(errs, field.Forbidden(
			resPath.Child("acceleratorSlicedCoresPercentage"),
			"the elastic profile takes whole, non-sliced accelerators",
		))
	}
	if res.AcceleratorPartitionedProfile != "" {
		errs = append(errs, field.Forbidden(
			resPath.Child("acceleratorPartitionedProfile"),
			"the elastic profile takes whole, non-partitioned accelerators",
		))
	}

	return errs
}

// validateModelDeploymentElasticOwnedKeys 拒绝渲染器独占的原生标志和环境变量条目。
// 匹配范围很窄：只有与 Operator 自身写入冲突的条目才会被拒绝，无关的调优键仍交由作者配置。
func validateModelDeploymentElasticOwnedKeys(
	role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	for _, arg := range role.ExtraArgs {
		name := strings.SplitN(strings.TrimLeft(arg, "-"), "=", 2)[0]
		switch name {
		case "dp":
			name = "data-parallel-size"
		case "dpl":
			name = "data-parallel-size-local"
		case "dpb":
			name = "data-parallel-backend"
		case "dpa":
			name = "data-parallel-address"
		case "tp":
			name = "tensor-parallel-size"
		case "pp":
			name = "pipeline-parallel-size"
		}
		flag := "--" + name
		if modelDeploymentElasticOwnedFlags.Has(flag) {
			errs = append(errs, field.Forbidden(
				rolePath.Child("extraArgs"), fmt.Sprintf("%s is written by the elastic renderer itself", flag),
			))
		}
	}
	for i := range role.Env {
		if modelDeploymentElasticOwnedEnv.Has(role.Env[i].Name) {
			errs = append(errs, field.Forbidden(
				rolePath.Child("env").Index(i).Child("name"),
				fmt.Sprintf("%s is written by the elastic renderer itself", role.Env[i].Name),
			))
		}
	}

	return errs
}

// validateModelDeploymentElasticIdentity 强制执行配置中冻结的半数属性：存在性、head 调度位置、
// role 的资源池与卡型规格，以及声明的 TP 和宽度上限均属于不可变身份；宽度则是运行期状态。
// 此外，该配置必须始终保留在最初携带它的那个 role 上。
func validateModelDeploymentElasticIdentity(
	md, old *workercore.ModelDeployment,
) field.ErrorList {
	var errs field.ErrorList

	oldElastic := elasticRoleOf(old)
	newElastic := elasticRoleOf(md)
	if oldElastic == nil || newElastic == nil {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "roles").Child("elasticEp"), modelDeploymentIdentityMessage,
		))
		return errs
	}

	oldParallel, oldErr := workerctrl.ParseModelDeploymentDeclaredParallelism(old.Spec.Engine.Name, oldElastic.ExtraArgs, oldElastic.Env)
	newParallel, newErr := workerctrl.ParseModelDeploymentDeclaredParallelism(md.Spec.Engine.Name, newElastic.ExtraArgs, newElastic.Env)
	if oldErr != nil || newErr != nil || oldParallel.TensorParallel != newParallel.TensorParallel {
		errs = append(errs, field.Invalid(
			modelDeploymentElasticRolePath(md, newElastic).Child("extraArgs"),
			newElastic.ExtraArgs, "tensor parallel size is fixed at creation; "+modelDeploymentIdentityMessage,
		))
	}
	if oldErr == nil && newErr == nil && oldParallel.ElasticEPMaxDataParallel != newParallel.ElasticEPMaxDataParallel {
		errs = append(errs, field.Forbidden(
			modelDeploymentElasticRolePath(md, newElastic).Child("extraArgs"),
			"--elastic-ep-max-dp-size is fixed at creation; "+modelDeploymentIdentityMessage,
		))
	}
	if oldElastic.Name != newElastic.Name {
		errs = append(errs, field.Forbidden(
			modelDeploymentElasticRolePath(md, newElastic).Child("name"),
			"the elastic profile must stay on the role it was created with: "+modelDeploymentIdentityMessage,
		))
	}
	// 该配置锁定了常规契约原本允许变动的字段：弹性 role 的成员规格在创建时即固定，
	// 宽度是唯一可变的运行期状态字段。ReplicaSize 保持为 1，且通过结构规则持续禁止自定义 command。
	if newElastic.InstanceType != oldElastic.InstanceType {
		errs = append(errs, field.Invalid(
			modelDeploymentElasticRolePath(md, newElastic).Child("instanceType"),
			newElastic.InstanceType,
			"the elastic-EP profile pins the pool the members were admitted against: "+modelDeploymentIdentityMessage,
		))
	}
	if !kubemeta.DeepEqual(newElastic.Resources, oldElastic.Resources) {
		errs = append(errs, field.Invalid(
			modelDeploymentElasticRolePath(md, newElastic).Child("resources"),
			newElastic.Resources,
			"the elastic-EP profile pins the members' accelerator shape: "+modelDeploymentIdentityMessage,
		))
	}
	if newElastic.ElasticEP.Width < oldElastic.ElasticEP.Width {
		errs = append(errs, field.Forbidden(
			modelDeploymentElasticRolePath(md, newElastic).Child("elasticEp", "width"),
			"elastic-EP scale-down is not supported in this release; keep width unchanged or increase it",
		))
	}

	return errs
}

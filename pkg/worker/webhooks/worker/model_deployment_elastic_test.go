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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/worker/elasticprofile"
)

func elasticRole(mutate func(*workercore.ModelDeploymentRole)) workercore.ModelDeploymentRole {
	role := workercore.ModelDeploymentRole{
		Name:         "vllm-elastic",
		Kind:         workercore.ModelDeploymentRoleKindServer,
		Replicas:     1,
		ReplicaSize:  1,
		InstanceType: "nvidia-gpu",
		ElasticEP: &workercore.ModelDeploymentRoleElasticEP{
			Width: 2,
		},
	}
	if mutate != nil {
		mutate(&role)
	}
	return role
}

func elasticMD(roles ...workercore.ModelDeploymentRole) *workercore.ModelDeployment {
	if len(roles) == 0 {
		roles = []workercore.ModelDeploymentRole{elasticRole(nil)}
	}
	return &workercore.ModelDeployment{
		ObjectMeta: meta.ObjectMeta{Name: "elastic-md", Namespace: "app"},
		Spec: workercore.ModelDeploymentSpec{
			Model: workercore.ModelDeploymentModel{Name: "Qwen/Qwen3-30B-A3B"},
			Engine: workercore.ModelDeploymentEngine{
				Name:  workercore.ModelDeploymentEngineVLLM,
				Image: "vllm/vllm-openai:v0.29.0",
			},
			Roles: roles,
		},
	}
}

func TestValidateModelDeploymentElasticSpec(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		md      func() *workercore.ModelDeployment
		old     func() *workercore.ModelDeployment
		wantErr []string
	}{{
		name: "valid minimal elastic profile admitted",
		md:   func() *workercore.ModelDeployment { return elasticMD() },
	}, {
		name: "width at upper bound admitted",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ElasticEP.Width = elasticprofile.WidthMax
			}))
		},
	}, {
		name: "width below minimum refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ElasticEP.Width = elasticprofile.WidthMin - 1
			}))
		},
		wantErr: []string{"width"},
	}, {
		name: "width above maximum refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ElasticEP.Width = elasticprofile.WidthMax + 1
			}))
		},
		wantErr: []string{"width"},
	}, {
		name: "width edit on update admitted",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Roles[0].ElasticEP.Width = 4
			return md
		},
		old: func() *workercore.ModelDeployment { return elasticMD() },
	}, {
		name: "unchanged width on update admitted",
		md:   func() *workercore.ModelDeployment { return elasticMD() },
		old:  func() *workercore.ModelDeployment { return elasticMD() },
	}, {
		name: "instanceType change on update refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Roles[0].InstanceType = "other-gpu"
			return md
		},
		old:     func() *workercore.ModelDeployment { return elasticMD() },
		wantErr: []string{"spec.roles[0].instanceType", "profile"},
	}, {
		name: "resources change on update refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Roles[0].Resources = &workercore.ModelDeploymentRoleResources{
				Accelerator: resource.NewQuantity(1, resource.DecimalSI),
			}
			return md
		},
		old:     func() *workercore.ModelDeployment { return elasticMD() },
		wantErr: []string{"spec.roles[0].resources", "profile"},
	}, {
		name: "command take-over on update refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Roles[0].Command = []string{"python", "-m", "vllm"}
			return md
		},
		old:     func() *workercore.ModelDeployment { return elasticMD() },
		wantErr: []string{"spec.roles[0].command"},
	}, {
		name: "width decrease on update refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			return md
		},
		old: func() *workercore.ModelDeployment {
			old := elasticMD()
			old.Spec.Roles[0].ElasticEP.Width = 4
			return old
		},
		wantErr: []string{"width", "scale-down is not supported"},
	}, {
		name: "presence removed on update refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Roles[0].ElasticEP = nil
			return md
		},
		old:     func() *workercore.ModelDeployment { return elasticMD() },
		wantErr: []string{"elasticEp"},
	}, {
		name: "second elastic role refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(nil), elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Name = "vllm-elastic-2"
			}))
		},
		wantErr: []string{"elasticEp"},
	}, {
		name: "non-elastic second role refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(nil), workercore.ModelDeploymentRole{
				Name:         "plain-server",
				Kind:         workercore.ModelDeploymentRoleKindServer,
				Replicas:     1,
				InstanceType: "nvidia-gpu",
			})
		},
		wantErr: []string{"elasticEp"},
	}, {
		name: "replicas above one refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Replicas = 2
			}))
		},
		wantErr: []string{"replicas"},
	}, {
		name: "size above one refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ReplicaSize = 2
			}))
		},
		wantErr: []string{"size"},
	}, {
		name: "non-server kind refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Kind = workercore.ModelDeploymentRoleKindPrefill
			}))
		},
		wantErr: []string{"kind"},
	}, {
		name: "non-vLLM engine refused",
		md: func() *workercore.ModelDeployment {
			md := elasticMD()
			md.Spec.Engine.Name = workercore.ModelDeploymentEngineSGLang
			return md
		},
		wantErr: []string{"engine"},
	}, {
		name: "command take-over refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Command = []string{"python", "-m", "vllm"}
			}))
		},
		wantErr: []string{"command"},
	}, {
		name: "multi-accelerator request refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				two := resource.NewQuantity(2, resource.DecimalSI)
				r.Resources = &workercore.ModelDeploymentRoleResources{Accelerator: two}
			}))
		},
		wantErr: []string{"accelerator"},
	}, {
		name: "sliced accelerator request refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				one := resource.NewQuantity(1, resource.DecimalSI)
				r.Resources = &workercore.ModelDeploymentRoleResources{
					Accelerator:                       one,
					AcceleratorSlicedMemoryPercentage: 50,
				}
			}))
		},
		wantErr: []string{"acceleratorSlicedMemoryPercentage"},
	}, {
		name: "partitioned profile refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				one := resource.NewQuantity(1, resource.DecimalSI)
				r.Resources = &workercore.ModelDeploymentRoleResources{
					Accelerator:                   one,
					AcceleratorPartitionedProfile: "3g.40gb",
				}
			}))
		},
		wantErr: []string{"acceleratorPartitionedProfile"},
	}, {
		name: "fabric interface request admitted",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Resources = &workercore.ModelDeploymentRoleResources{
					Interface: resource.NewQuantity(1, resource.DecimalSI),
				}
			}))
		},
	}, {
		name: "operator-owned native flag refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ExtraArgs = []string{"--data-parallel-size=4"}
			}))
		},
		wantErr: []string{"extraArgs"},
	}, {
		name: "operator-owned native env refused",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Env = []workercore.ModelDeploymentEnvVar{{Name: "VLLM_DP_SIZE", Value: "4"}}
			}))
		},
		wantErr: []string{"env"},
	}, {
		name: "unrelated ray tuning env admitted",
		md: func() *workercore.ModelDeployment {
			return elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.Env = []workercore.ModelDeploymentEnvVar{{Name: "RAY_BACKEND_LOG_LEVEL", Value: "debug"}}
			}))
		},
	}, {
		name: "no CPU head instance type required",
		md:   func() *workercore.ModelDeployment { return elasticMD() },
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			webhook := &ModelDeploymentWebhook{}

			var old *workercore.ModelDeployment
			if tc.old != nil {
				old = tc.old()
			}
			errs := webhook.ValidateModelDeploymentElasticEP(context.Background(), tc.md(), old)

			if len(tc.wantErr) == 0 {
				if len(errs) != 0 {
					t.Fatalf("want no errors, got %v", errs)
				}
				return
			}
			if len(errs) == 0 {
				t.Fatalf("want errors matching %v, got none", tc.wantErr)
			}
			joined := ""
			for _, e := range errs {
				joined += e.Field + " " + e.Error() + "\n"
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(joined, want) {
					t.Fatalf("want error mentioning %q, got:\n%s", want, joined)
				}
			}
		})
	}
}

// TestValidateModelDeploymentElasticEPWidthBounds 校验准入宽度范围，覆盖允许的最窄、
// 最宽值及两端紧邻的非法值。
//
// 用例从共享 profile 读取上下限，避免测试另写一套数字。这样当 webhook 和 profile 的宽度约束分歧时，
// 边界用例会指出问题。
func TestValidateModelDeploymentElasticEPWidthBounds(t *testing.T) {
	t.Parallel()

	webhook := &ModelDeploymentWebhook{}

	cases := []struct {
		name     string
		width    int32
		wantRefu bool
	}{
		{name: "the narrowest profile width", width: elasticprofile.WidthMin},
		{name: "the widest profile width", width: elasticprofile.WidthMax},
		{name: "one below the profile", width: elasticprofile.WidthMin - 1, wantRefu: true},
		{name: "one above the profile", width: elasticprofile.WidthMax + 1, wantRefu: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			md := elasticMD(elasticRole(func(r *workercore.ModelDeploymentRole) {
				r.ElasticEP.Width = tc.width
			}))
			errs := webhook.ValidateModelDeploymentElasticEP(context.Background(), md, nil)

			widthRefused := false
			for _, e := range errs {
				if e.Field == "spec.roles[0].elasticEp.width" {
					widthRefused = true
				}
			}
			if widthRefused != tc.wantRefu {
				t.Fatalf("width %d refused=%t, want refused=%t (errors: %v)",
					tc.width, widthRefused, tc.wantRefu, errs)
			}
		})
	}
}

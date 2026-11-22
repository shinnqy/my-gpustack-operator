package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/gen/api/builder"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// projectModule 是本仓库的模块导入路径。\n//\n// 使用同一常量兼顾两处用途，因为两处不一致时不会显式报错：\n//   - 生成器配置的值（builder.Config.Project）；\n//   - 工作目录必须携带的后缀，因为输出基准目录就是将该目录去掉该后缀后\n//     得到的结果。
const projectModule = "gpustack.ai/gpustack"

func main() {
	err := generate()
	if err != nil {
		klog.Fatalf("error generating: %v", err)
	}
}

// resolveProjectDir 返回允许生成器写入的路径，或在不符合条件时拒绝执行。\n//\n// 输出基准目录由该路径去掉 projectModule 后缀得到，因此若目录不以该后缀\n// 结尾，所有生成器的输出层级都会浅一层模块路径。\n//\n// WARNING: 失败时不会干脆中止。go-to-protobuf 会在检测到错误路径前就重写\n// 生成文件，因此该检查必须在首次写入前执行。\n//\n// 该检查依赖以下三个条件：\n//   - 先解析符号链接，因为 os.Getwd 可能返回逻辑路径 $PWD，若存在以模块路径\n//     命名的符号链接，虽能通过检查但工具链实际会在别处运行；\n//   - 将路径分隔符作为后缀的一部分，从而拒绝 "/tmp/mygpustack.ai/gpustack"\n//     而不是将其截断为 "/tmp/my"；\n//   - 返回值使用正斜杠规范化，因为下游的裁剪逻辑是针对斜杠分隔路径的纯字符串\n//     操作。
func resolveProjectDir(pwd string) (string, error) {
	resolved, err := filepath.EvalSymlinks(pwd)
	if err != nil {
		return "", fmt.Errorf("resolve working directory %q: %w", pwd, err)
	}

	normalized := filepath.ToSlash(resolved)

	if !strings.HasSuffix(normalized, "/"+projectModule) {
		return "", fmt.Errorf(
			"working directory %q does not end in the module import path %q: "+
				"the generators derive their output base by trimming it off, so running here "+
				"would write to the wrong tree; run from a checkout whose resolved path ends in %[2]q",
			resolved, projectModule,
		)
	}

	return normalized, nil
}

func generate() error {
	// 准备工作。
	pwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	// REQUIRED: 必须在首次写入前拒绝错误路径；后续所有路径均由该结果派生。
	pwd, err = resolveProjectDir(pwd)
	if err != nil {
		return err
	}

	header, err := os.ReadFile(filepath.Join(pwd, "/hack/boilerplate/go.txt"))
	if err != nil {
		return err
	}

	cfg := builder.Config{
		ProjectDir:    pwd,
		Project:       projectModule,
		ClientsName:   "kubeclients",
		ClientSetName: "kubernetes",
		Header:        stringx.FromBytes(ptr.To(bytes.TrimSpace(header))),
		/*\n\t\t\t指定 CRD API 的包路径。\n\t\t*/
		APIs: []string{
			"gpustack.ai/gpustack/api/worker/v1alpha1",
		},
		/*\n\t\t\t指定 Extension API 的包路径。\n\t\t*/
		ExtensionAPIs: []string{
			"gpustack.ai/gpustack/api/v1",
			"gpustack.ai/gpustack/api/worker/v1",
		},
		/*\n\t\t\t指定 APIs 与 ExtensionAPIs 所依赖的第三方包路径。\n\t\t*/
		MachineryAPIs: []string{
			"k8s.io/apimachinery/pkg/api/resource",
			"k8s.io/apimachinery/pkg/apis/meta/v1",
			"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured",
			"k8s.io/apimachinery/pkg/runtime",
			"k8s.io/apimachinery/pkg/runtime/schema",
			"k8s.io/apimachinery/pkg/types",
			"k8s.io/apimachinery/pkg/version",
			"k8s.io/apimachinery/pkg/util/intstr",
			"k8s.io/api/core/v1",
		},
		/*\n\t\t\t指定嵌入 clientset 的外部 API 包路径。\n\t\t*/
		ExternalAPIs: []string{
			// Kubernetes。
			"k8s.io/api/admission/v1",
			"k8s.io/api/admissionregistration/v1",
			"k8s.io/api/apidiscovery/v2",
			"k8s.io/api/apiserverinternal/v1alpha1",
			"k8s.io/api/apps/v1",
			"k8s.io/api/authentication/v1",
			"k8s.io/api/authorization/v1",
			"k8s.io/api/autoscaling/v1",
			"k8s.io/api/autoscaling/v2",
			"k8s.io/api/batch/v1",
			"k8s.io/api/certificates/v1",
			"k8s.io/api/coordination/v1",
			"k8s.io/api/core/v1",
			"k8s.io/api/discovery/v1",
			"k8s.io/api/events/v1",
			"k8s.io/api/flowcontrol/v1",
			"k8s.io/api/imagepolicy/v1alpha1",
			"k8s.io/api/networking/v1",
			"k8s.io/api/node/v1",
			"k8s.io/api/policy/v1",
			"k8s.io/api/rbac/v1",
			"k8s.io/api/resource/v1",
			"k8s.io/api/scheduling/v1",
			"k8s.io/api/storage/v1",
			"k8s.io/api/storagemigration/v1beta1",
			"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1",
			"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1",
			// Kueue。
			"sigs.k8s.io/kueue/apis/kueue/v1beta2",
			// Node Feature Discovery。
			"sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1",
		},
		/*
			指定 Admission Webhook 的包路径。
		*/
		Webhooks: []string{
			"gpustack.ai/gpustack/pkg/worker/webhooks/worker",
		},
		/*
			指定复数形式的例外映射。
		*/
		PluralExceptions: map[string]string{
			"Endpoints": "Endpoints",
			"Devices":   "Devices",
		},
		/*
			为 proto 生成提供 protobuf 文件的物理路径。
		*/
		ProtoImports: []string{
			// NB(thxCode): code-generator 下的 go-to-protobuf 依赖一个已废弃的项目：
			// https://github.com/gogo/protobuf。
			// 上游已就此提交过 Issue：
			// https://github.com/kubernetes/kubernetes/issues/96564。
			// 为了支持为扩展 API 生成 protobuf 代码，
			// 需要告知 protoc 在何处查找 gogo/protobuf。
			filepath.Join(pwd, "staging"),
		},
	}

	// 执行生成。
	return builder.Generate(cfg)
}

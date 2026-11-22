package builder

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	applygenargs "k8s.io/code-generator/cmd/applyconfiguration-gen/args"
	applygen "k8s.io/code-generator/cmd/applyconfiguration-gen/generators"
	clientgenargs "k8s.io/code-generator/cmd/client-gen/args"
	clientgen "k8s.io/code-generator/cmd/client-gen/generators"
	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	conversionargs "k8s.io/code-generator/cmd/conversion-gen/args"
	conversiongen "k8s.io/code-generator/cmd/conversion-gen/generators"
	deepcopygenargs "k8s.io/code-generator/cmd/deepcopy-gen/args"
	deepcopygen "k8s.io/code-generator/cmd/deepcopy-gen/generators"
	protobufgen "k8s.io/code-generator/cmd/go-to-protobuf/protobuf"
	informergenargs "k8s.io/code-generator/cmd/informer-gen/args"
	informergen "k8s.io/code-generator/cmd/informer-gen/generators"
	listergenargs "k8s.io/code-generator/cmd/lister-gen/args"
	listergen "k8s.io/code-generator/cmd/lister-gen/generators"
	registergenargs "k8s.io/code-generator/cmd/register-gen/args"
	registergen "k8s.io/code-generator/cmd/register-gen/generators"
	gengov2 "k8s.io/gengo/v2"
	generatorv2 "k8s.io/gengo/v2/generator"
	typesv2 "k8s.io/gengo/v2/types"
	klog "k8s.io/klog/v2"
	openapigenargs "k8s.io/kube-openapi/cmd/openapi-gen/args"
	openapigen "k8s.io/kube-openapi/pkg/generators"
	"k8s.io/utils/ptr"

	apireggenargs "gpustack.ai/gpustack/gen/api/generator/cmd/apireg-gen/args"
	apireggen "gpustack.ai/gpustack/gen/api/generator/cmd/apireg-gen/generators"
	crdgenargs "gpustack.ai/gpustack/gen/api/generator/cmd/crd-gen/args"
	crdgen "gpustack.ai/gpustack/gen/api/generator/cmd/crd-gen/generators"
	webhookgenargs "gpustack.ai/gpustack/gen/api/generator/cmd/webhook-gen/args"
	webhookgen "gpustack.ai/gpustack/gen/api/generator/cmd/webhook-gen/generators"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

func Generate(c Config) error {
	if err := c.ValidateAndDefault(); err != nil {
		return fmt.Errorf("validate and default config: %w", err)
	}

	tempHeaderBytes := stringx.ToBytes(ptr.To(stringx.Join("\n\n", c.Header, generatedHeader, "")))
	tempHeaderFile := osx.TempFile("")
	defer func() { _ = os.Remove(tempHeaderFile) }()
	_ = os.WriteFile(
		tempHeaderFile,
		tempHeaderBytes,
		0o600,
	)

	internalAPIs := append(slices.Clone(c.APIs), c.ExtensionAPIs...)

	// 执行 conversion-gen。
	if len(internalAPIs) > 0 {
		klog.Info("executing conversion-gen")

		conversionArgs := conversionargs.Args{
			OutputFile:   "zz_generated.conversion.go",
			BasePeerDirs: conversionargs.DefaultBasePeerDirs,
			GoHeaderFile: tempHeaderFile,
		}

		for i := range c.ExtensionAPIs {
			extensionAPIDir := filepath.Join(
				c.ProjectDir,
				strings.TrimPrefix(c.ExtensionAPIs[i], c.Project),
				conversionArgs.OutputFile,
			)
			_ = osx.DurableRemove(extensionAPIDir)
		}

		err := gengov2.Execute(
			conversiongen.NameSystems(),
			conversiongen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return conversiongen.GetTargets(g, &conversionArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(internalAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute conversion-gen: %w", err)
		}

		klog.Info("executed conversion-gen")
	}

	// 执行 deepcopy-gen。
	if len(internalAPIs) > 0 {
		klog.Info("executing deepcopy-gen")

		deepcopyArgs := deepcopygenargs.Args{
			OutputFile:   "zz_generated.deepcopy.go",
			GoHeaderFile: tempHeaderFile,
		}

		err := gengov2.Execute(
			deepcopygen.NameSystems(),
			deepcopygen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return deepcopygen.GetTargets(g, &deepcopyArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(internalAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute deepcopy-gen: %w", err)
		}

		klog.Info("executed deepcopy-gen")
	}

	// 执行 register-gen。
	if len(internalAPIs) > 0 {
		klog.Info("executing register-gen")

		registerArgs := registergenargs.Args{
			OutputFile:   "zz_generated.register.go",
			GoHeaderFile: tempHeaderFile,
		}

		err := gengov2.Execute(
			registergen.NameSystems(),
			registergen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return registergen.GetTargets(g, &registerArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(internalAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute register-gen: %w", err)
		}

		klog.Info("executed register-gen")
	}

	// 执行 go-to-protobuf。
	if len(internalAPIs) > 0 {
		klog.Info("executing go-to-protobuf")

		// 不再生成 proto message 标记。上游生成器过去会在构建标签下写入另一个\n// 包含空 ProtoMessage 标记的文件，并在标记消失时删除该文件。现在它\n// 两者都不再执行，因此已签入的副本会作为孤立文件残留，不再与 API 类型\n// 匹配。运行前会先移除过时副本，以便在生成器再次输出该文件时直接\n// 重新写入。
		for i := range internalAPIs {
			protomessageFile := filepath.Join(
				c.ProjectDir,
				strings.TrimPrefix(internalAPIs[i], c.Project),
				"generated.protomessage.pb.go",
			)
			if err := osx.DurableRemove(protomessageFile); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove obsolete proto message markers: %w", err)
			}
		}

		protoGenAPIMachineryPackages := make([]string, 0, len(c.MachineryAPIs))
		for _, api := range c.MachineryAPIs {
			protoGenAPIMachineryPackages = append(protoGenAPIMachineryPackages, "-"+api)
		}
		protoGenDropEmbeddedFields := []string{
			"k8s.io/apimachinery/pkg/apis/meta/v1.TypeMeta",
		}

		protocGen := protobufgen.Generator{
			GoHeaderFile:         tempHeaderFile,
			APIMachineryPackages: strings.Join(protoGenAPIMachineryPackages, ","),
			Packages:             strings.Join(internalAPIs, ","),
			OutputDir:            strings.TrimSuffix(c.ProjectDir, c.Project),
			ProtoImport:          c.ProtoImports,
			Conditional:          "!" + gengov2.StdBuildTag,
			DropEmbeddedFields:   strings.Join(protoGenDropEmbeddedFields, ","),
			DropGogoGo:           true,
		}
		protobufgen.Run(&protocGen)

		klog.Info("executed go-to-protobuf")
	}

	// 执行 crd-gen。
	if len(c.APIs) > 0 {
		klog.Info("executing crd-gen")

		crdGenArgs := crdgenargs.Args{
			OutputFile:   "zz_generated.crds.go",
			GoHeaderFile: tempHeaderFile,
		}

		err := gengov2.Execute(
			crdgen.NameSystems(),
			crdgen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return crdgen.GetTargets(g, &crdGenArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(c.APIs),
		)
		if err != nil {
			return fmt.Errorf("execute crd-gen: %w", err)
		}

		klog.Info("executed crd-gen")
	}

	// 执行 apireg-gen。
	if len(c.ExtensionAPIs) > 0 {
		klog.Info("executing apireg-gen")

		apiRegGenArgs := apireggenargs.Args{
			OutputFile:   "zz_generated.apiservice.go",
			GoHeaderFile: tempHeaderFile,
		}

		err := gengov2.Execute(
			apireggen.NameSystems(),
			apireggen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return apireggen.GetTargets(g, &apiRegGenArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(c.ExtensionAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute apireg-gen: %w", err)
		}

		klog.Info("executed apireg-gen")
	}

	// 执行 openapi-gen。
	if len(internalAPIs) > 0 {
		// 按组归类的内部 API。
		groupedInternalAPIs := make(map[string][]string)
		for _, api := range internalAPIs {
			g := path.Dir(api)
			groupedInternalAPIs[g] = append(groupedInternalAPIs[g], api)
		}

		klog.Info("executing openapi-gen")

		// 公共 api/v1 的模型名要先生成，Worker 的 OpenAPI 定义会引用它。
		for _, outputPkg := range slices.Sorted(maps.Keys(groupedInternalAPIs)) {
			internalAPIPkgs := groupedInternalAPIs[outputPkg]
			outputDir := filepath.Join(c.ProjectDir, strings.TrimPrefix(outputPkg, c.Project))
			openapiGenArgs := openapigenargs.Args{
				OutputDir:           outputDir,
				OutputPkg:           outputPkg,
				OutputFile:          "zz_generated.openapi.go",
				GoHeaderFile:        tempHeaderFile,
				ReportFilename:      filepath.Join(outputDir, "zz_generated.openapi.violation_exceptions"),
				OutputModelNameFile: "zz_generated.model_name.go",
			}

			err := gengov2.Execute(
				openapigen.NameSystems(),
				openapigen.DefaultNameSystem(),
				func(g *generatorv2.Context) []generatorv2.Target {
					return openapigen.GetModelNameTargets(g, &openapiGenArgs, tempHeaderBytes)
				},
				gengov2.StdBuildTag,
				slices.Clone(internalAPIPkgs),
			)
			if err != nil {
				return fmt.Errorf("execute openapi-gen for model name: %w", err)
			}

			err = gengov2.Execute(
				openapigen.NameSystems(),
				openapigen.DefaultNameSystem(),
				func(g *generatorv2.Context) []generatorv2.Target {
					return openapigen.GetOpenAPITargets(g, &openapiGenArgs, tempHeaderBytes)
				},
				gengov2.StdBuildTag,
				append(slices.Clone(internalAPIPkgs), c.MachineryAPIs...),
			)
			if err != nil {
				return fmt.Errorf("execute openapi-gen: %w", err)
			}
		}

		klog.Info("executed openapi-gen")
	}

	clientPackageDir := path.Join(c.ProjectDir, strings.TrimPrefix(c.ClientPackage, c.Project))

	// 允许扩展接口，\n// 不再删除整个 clientset。\n// 参见 https://github.com/kubernetes/community/blob/main/contributors/devel/sig-api-machinery/generating-clientset.md。\n// _ = os.RemoveAll(path.Join(clientPackageDir, c.ClientsName))
	allAPIs := append(slices.Clone(internalAPIs), c.ExternalAPIs...)

	// 执行 applyconfiguration-gen。
	applyConfigPkg := path.Join(c.ClientPackage, c.ClientsName, "applyconfiguration")
	{
		klog.Info("executing applyconfiguration-gen")

		applygenExternalConfig := map[typesv2.Name]string{ // nolint:lll
			// 始终包含在 client-go 中生成的 applyconfiguration。\n// 它们足以满足绝大多数使用场景。
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "Condition"}:                "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "DeleteOptions"}:            "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "LabelSelector"}:            "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "LabelSelectorRequirement"}: "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ManagedFieldsEntry"}:       "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ObjectMeta"}:               "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "OwnerReference"}:           "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "TypeMeta"}:                 "k8s.io/client-go/applyconfigurations/meta/v1",
			{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "Preconditions"}:            "k8s.io/client-go/applyconfigurations/meta/v1",
		}

		applygenArgs := applygenargs.Args{
			OutputDir:                   filepath.Join(clientPackageDir, c.ClientsName, "applyconfiguration"),
			OutputPkg:                   applyConfigPkg,
			ExternalApplyConfigurations: applygenExternalConfig,
		}

		err := gengov2.Execute(
			applygen.NameSystems(),
			applygen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return applygen.GetTargets(g, &applygenArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(allAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute applyconfiguration-gen: %w", err)
		}

		klog.Info("executed applyconfiguration-gen")
	}

	pluralExceptionsSlice := func() (ss []string) {
		for k, v := range c.PluralExceptions {
			ss = append(ss, fmt.Sprintf("%s:%s", k, v))
		}
		return ss
	}()

	// 执行 client-gen。
	clientsetPkg := path.Join(c.ClientPackage, c.ClientsName, c.ClientSetName)
	{
		klog.Info("executing client-gen")

		clientsetGrpVers := make([]clientgentypes.GroupVersions, 0, len(internalAPIs)+len(c.ExternalAPIs))
		for _, grpVerPkg := range allAPIs {
			gn := path.Base(path.Dir(grpVerPkg))
			vn := path.Base(grpVerPkg)
			grpVer := clientgentypes.GroupVersions{
				PackageName: gn,
				Group:       clientgentypes.Group(gn),
				Versions: []clientgentypes.PackageVersion{
					{
						Package: grpVerPkg,
						Version: clientgentypes.Version(vn),
					},
				},
			}
			clientsetGrpVers = append(clientsetGrpVers, grpVer)
		}

		clientsetArgs := clientgenargs.Args{
			OutputDir:                 filepath.Join(clientPackageDir, c.ClientsName),
			OutputPkg:                 path.Dir(clientsetPkg),
			GoHeaderFile:              tempHeaderFile,
			Groups:                    clientsetGrpVers,
			ClientsetName:             path.Base(clientsetPkg),
			ClientsetAPIPath:          "/apis",
			ClientsetOnly:             false,
			FakeClient:                true,
			PluralExceptions:          slices.Clone(pluralExceptionsSlice),
			ApplyConfigurationPackage: applyConfigPkg,
			PrefersProtobuf:           true,
		}

		err := gengov2.Execute(
			clientgen.NameSystems(c.PluralExceptions),
			clientgen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return clientgen.GetTargets(g, &clientsetArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(allAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute client-gen: %w", err)
		}

		klog.Info("executed client-gen")
	}

	// 执行 lister-gen。
	listerPkg := path.Join(c.ClientPackage, c.ClientsName, "listers")
	{
		klog.Info("executing lister-gen")

		listerArgs := listergenargs.Args{
			OutputDir:        filepath.Join(clientPackageDir, c.ClientsName, "listers"),
			OutputPkg:        listerPkg,
			GoHeaderFile:     tempHeaderFile,
			PluralExceptions: slices.Clone(pluralExceptionsSlice),
		}

		err := gengov2.Execute(
			listergen.NameSystems(c.PluralExceptions),
			listergen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return listergen.GetTargets(g, &listerArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(allAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute lister-gen: %w", err)
		}

		klog.Info("executed lister-gen")
	}

	// 执行 informer-gen。
	informerPkg := path.Join(c.ClientPackage, c.ClientsName, "informers")
	{
		klog.Info("executing informer-gen")

		informerArgs := informergenargs.Args{
			OutputDir:                 filepath.Join(clientPackageDir, c.ClientsName, "informers"),
			OutputPkg:                 informerPkg,
			GoHeaderFile:              tempHeaderFile,
			VersionedClientSetPackage: clientsetPkg,
			ListersPackage:            listerPkg,
			SingleDirectory:           true,
			PluralExceptions:          slices.Clone(pluralExceptionsSlice),
		}

		err := gengov2.Execute(
			informergen.NameSystems(c.PluralExceptions),
			informergen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return informergen.GetTargets(g, &informerArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(allAPIs),
		)
		if err != nil {
			return fmt.Errorf("execute informer-gen: %w", err)
		}

		klog.Info("executed informer-gen")
	}

	// Webhook 类型引用生成的 clientset。从空目录生成时，先写完客户端再解析这些类型。
	if len(c.Webhooks) > 0 {
		klog.Info("executing webhook-gen")

		webhookGenArgs := webhookgenargs.Args{
			OutputFile:   "zz_generated.webhooks.go",
			GoHeaderFile: tempHeaderFile,
		}

		err := gengov2.Execute(
			webhookgen.NameSystems(),
			webhookgen.DefaultNameSystem(),
			func(g *generatorv2.Context) []generatorv2.Target {
				return webhookgen.GetTargets(g, &webhookGenArgs)
			},
			gengov2.StdBuildTag,
			slices.Clone(c.Webhooks),
		)
		if err != nil {
			return fmt.Errorf("execute webhook-gen: %w", err)
		}

		klog.Info("executed webhook-gen")
	}

	return nil
}

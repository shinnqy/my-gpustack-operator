package generators

import (
	gengov2 "k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/gen/api/generator/cmd/crd-gen/args"
	"gpustack.ai/gpustack/gen/api/generator/utils"
)

// NameSystems 返回此包中生成器使用的命名系统。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public": namer.NewPublicNamer(0),
	}
}

// DefaultNameSystem 返回此包中生成器用于排序待处理类型的\n// 默认命名系统。
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 返回 CRD 生成器写入的目标。\n//\n// 每个输入包对应一个目标。目标会写入其所描述的包所在目录，\n// 因此生成的文件会与其来源类型位于同一目录下。
func GetTargets(context *generator.Context, arguments *args.Args) []generator.Target {
	header, err := utils.Header(gengov2.StdBuildTag, arguments.GoHeaderFile)
	if err != nil {
		klog.Fatalf("Failed loading boilerplate: %v", err)
	}

	targets := []generator.Target{}
	for i := range context.Inputs {
		klog.V(5).Infof("considering pkg %q", context.Inputs[i])
		pkg := context.Universe[context.Inputs[i]]
		if pkg == nil {
			// 例如输入中没有 Go 文件的情况。
			continue
		}

		targets = append(targets, &generator.SimpleTarget{
			PkgName:       pkg.Name,
			PkgPath:       pkg.Path,
			PkgDir:        pkg.Dir,
			HeaderComment: header,
			FilterFunc: func(c *generator.Context, t *types.Type) bool {
				return t.Name.Package == pkg.Path
			},
			GeneratorsFunc: func(c *generator.Context) []generator.Generator {
				return []generator.Generator{
					NewCRDGen(arguments.OutputFile, pkg.Path),
				}
			},
		})
	}

	return targets
}

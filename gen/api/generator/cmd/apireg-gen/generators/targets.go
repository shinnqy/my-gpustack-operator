package generators

import (
	gengov2 "k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/gen/api/generator/cmd/apireg-gen/args"
	"gpustack.ai/gpustack/gen/api/generator/utils"
)

// NameSystems 返回此包中生成器使用的命名系统。
func NameSystems() namer.NameSystems {
	return namer.NameSystems{
		"public": namer.NewPublicNamer(0),
	}
}

// DefaultNameSystem 返回默认命名系统，用于对此包中生成器待处理的类型进行排序。
func DefaultNameSystem() string {
	return "public"
}

// GetTargets 返回 apireg 生成器写入的目标。
//
// 每个输入包对应一个目标。目标会写入其所描述包的目录中，因此生成的文件会
// 位于其源类型定义所在的同一目录。
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
			// 例如，当输入中没有 Go 文件时。
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
					NewAPIRegGen(arguments.OutputFile, pkg.Path),
				}
			},
		})
	}

	return targets
}

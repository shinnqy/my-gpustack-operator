package kubeconfig

import (
	"errors"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LoadRestConfigNonInteractive 按以下规则加载 rest config。
//
//  1. 假设当前运行在 Pod 中，尝试使用挂载的 ServiceAccount 连接
//     Kubernetes 集群。
//  2. 若不满足上述条件，则从推荐的用户主目录配置文件加载。
func LoadRestConfigNonInteractive() (cfgPath string, restCfg *rest.Config, inside bool, err error) {
	// 尝试集群内配置。
	restCfg, err = rest.InClusterConfig()
	switch {
	case err == nil:
		return "", restCfg, true, nil
	case !errors.Is(err, rest.ErrNotInCluster):
		return "", nil, false, err
	}

	// 尝试推荐配置。
	var (
		ld = &clientcmd.ClientConfigLoadingRules{
			Precedence: []string{clientcmd.RecommendedHomeFile},
		}
		od = &clientcmd.ConfigOverrides{}
	)
	restCfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(ld, od).ClientConfig()
	return clientcmd.RecommendedHomeFile, restCfg, false, err
}

// LoadClientConfig 从指定路径加载 client config，
// 给定路径必须存在。
func LoadClientConfig(path string) (clientcmd.ClientConfig, error) {
	if path == "" {
		return nil, errors.New("blank kubeconfig path")
	}

	var (
		ld = &clientcmd.ClientConfigLoadingRules{
			ExplicitPath: path,
		}
		od = &clientcmd.ConfigOverrides{}
	)

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(ld, od), nil
}

// LoadRestConfig 从指定路径加载 rest config，
// 给定路径必须存在。
func LoadRestConfig(path string) (*rest.Config, error) {
	cc, err := LoadClientConfig(path)
	if err != nil {
		return nil, err
	}

	return cc.ClientConfig()
}

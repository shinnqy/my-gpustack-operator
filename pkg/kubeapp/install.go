package kubeapp

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/kubeapp/helm"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// Install 是用于安装应用的函数类型。其最后一个参数表示调用方是否在整次调用期间排除了所有其他进程对该 release 的访问。
type Install func(context.Context, *helm.Client, map[string]any, sets.Set[string], bool) error

// ExecuteInstall 并发执行给定的安装函数，并等待它们全部完成。
//
// exclusive 声明在此期间没有其他进程会操作这些 release。它会传递给各个安装函数，
// 用于在遇到停留在操作中途的 release 时决定是直接修复还是等待其完成。
func ExecuteInstall(
	ctx context.Context,
	restCfg rest.Config,
	disable sets.Set[string],
	namespace string,
	installs []Install,
	globalValuesContext map[string]any,
	exclusive bool,
) error {
	if disable.Has("*") {
		klog.Warningf("!!! all system applications are disabled !!!")
		return nil
	}

	hc, err := helm.NewClient(restCfg, helm.WithDefaultNamespace(namespace))
	if err != nil {
		return fmt.Errorf("create helm client: %w", err)
	}

	// 如果 globalValuesContext 中不存在 Env，则追加通用值，
	// 这些值将传递给所有安装函数。
	if _, ok := globalValuesContext["Env"]; !ok {
		globalValuesContext["Env"] = getCommonEnv(ctx)
	}
	if _, ok := globalValuesContext["ManagedLabel"]; !ok {
		globalValuesContext["ManagedLabel"] = "gpustack.ai/managed"
	}

	// 并发执行安装函数。
	gp := gox.Group()
	for i := range installs {
		gvc := maps.Clone(globalValuesContext)
		install := installs[i]
		gp.Go(func() error {
			err := install(ctx, hc, gvc, disable, exclusive)
			if err != nil {
				return fmt.Errorf("%s: %w", loadInstallerName(install), err)
			}
			return nil
		})
	}
	return gp.Wait()
}

func getCommonEnv(_ context.Context) (env []core.EnvVar) {
	if v := osx.Getenv("ALL_PROXY"); v != "" {
		env = append(env, core.EnvVar{
			Name:  "ALL_PROXY",
			Value: v,
		})
	}

	if v := osx.Getenv("HTTP_PROXY"); v != "" {
		env = append(env, core.EnvVar{
			Name:  "HTTP_PROXY",
			Value: v,
		})
	}

	if v := osx.Getenv("HTTPS_PROXY"); v != "" {
		env = append(env, core.EnvVar{
			Name:  "HTTPS_PROXY",
			Value: v,
		})
	}

	if v := osx.Getenv("NO_PROXY"); v != "" {
		env = append(env, core.EnvVar{
			Name:  "NO_PROXY",
			Value: v,
		})
	}

	return env
}

func loadInstallerName(i Install) string {
	n := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	n = strings.TrimPrefix(strings.TrimSuffix(filepath.Ext(n), "-fm"), ".")
	return stringx.Decamelize(n, true)
}

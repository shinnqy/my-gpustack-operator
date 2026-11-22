package helm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	helmaction "helm.sh/helm/v3/pkg/action"
	helmchartutil "helm.sh/helm/v3/pkg/chartutil"
	helmregistry "helm.sh/helm/v3/pkg/registry"
	helmrelease "helm.sh/helm/v3/pkg/release"
	helmdriver "helm.sh/helm/v3/pkg/storage/driver"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"
	apireg "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubeconfig"
)

type (
	// Client 是一个 Helm 客户端。
	Client struct {
		// getter 是 Kubernetes REST 客户端获取器。
		getter genericclioptions.RESTClientGetter
		// defaultNamespace 是存放 release 信息的默认 Kubernetes Namespace 名称。
		defaultNamespace string
		// Timeout 是操作的超时时间。
		timeout time.Duration
	}

	// ClientOption 是用于配置 Client 的函数。
	ClientOption func(*Client)
)

// WithDefaultNamespace 返回一个 ClientOption，用于设置存放 release 信息的
// 默认 Kubernetes Namespace 名称。
func WithDefaultNamespace(namespace string) func(*Client) {
	return func(c *Client) {
		c.defaultNamespace = namespace
	}
}

// NewClient 根据 Kubernetes rest.Config 创建一个新的 Helm 配置。
//
// 给定的 defaultNamespace 用于存放 release 信息。
func NewClient(restCfg rest.Config, opts ...ClientOption) (*Client, error) {
	restCfg.ContentType = runtime.ContentTypeJSON
	c := &Client{
		getter:           kubeconfig.ConvertRestConfigToRestClientGetter(&restCfg),
		defaultNamespace: core.NamespaceDefault,
		timeout:          30 * time.Minute,
	}
	for i := range opts {
		opts[i](c)
	}

	return c, nil
}

// DefaultNamespace 返回存放 release 信息的默认 Kubernetes Namespace 名称。
func (c *Client) DefaultNamespace() string {
	return c.defaultNamespace
}

// KubeClientSet 返回 Kubernetes clientset。
func (c *Client) KubeClientSet() kubernetes.Interface {
	conf, _ := c.getter.ToRESTConfig()
	return kubernetes.NewForConfigOrDie(conf)
}

// Install 安装指定的 chart，并返回其 values。
//
// 如果对应的 release 已存在，则直接视为完成。
func (c *Client) Install(ctx context.Context, chart *Chart, overrideNamespace ...string) (helmchartutil.Values, error) {
	next := func(r *helmrelease.Release) NextStepType {
		return c.nextStep(ctx, r, chart)
	}
	return c.InstallWith(ctx, chart, next, overrideNamespace...)
}

// nextStep 决定如何处理已找到的指定 chart 的 release。
func (c *Client) nextStep(ctx context.Context, r *helmrelease.Release, chart *Chart) NextStepType {
	switch r.Info.Status {
	case helmrelease.StatusDeployed, helmrelease.StatusSuperseded:
		if r.Chart.Metadata.Version != "" {
			lv := r.Chart.Metadata.Version
			rv := chart.Version
			if lv != rv {
				return NextStepUpgrade
			}
		}
		if rv := r.Config; rv != nil {
			cv, err := chart.Values.GetValues(ctx)
			if err != nil {
				return NextStepDone
			}
			if !reflect.DeepEqual(rv, cv) {
				return NextStepUpgrade
			}
		}
		return NextStepDone
	case helmrelease.StatusPendingInstall, helmrelease.StatusPendingUpgrade, helmrelease.StatusPendingRollback:
		// pending 记录要么对应仍在进行中的操作，要么是中途被终止的进程遗留下来的，
		// 修复前者会破坏正在执行的操作。
		// ExclusiveAccess 用于区分这两种情况；没有它时唯一的依据只有记录的存活时间，
		// 而存在时间短于操作自身超时时间的记录仍可能属于其他并发进程。
		if !chart.ExclusiveAccess && time.Since(r.Info.LastDeployed.Time) <= c.timeout {
			return NextStepRequeue
		}

		// 被遗弃的 pending-install 是唯一一种 Helm 完全无法直接处理的 pending 状态：
		// 只要该记录存在，Helm 的名称检查就会拒绝后续所有安装请求，且没有任何操作能将其清除。
		// 因此需要删除该记录，然后重新安装该 release。
		if r.Info.Status == helmrelease.StatusPendingInstall {
			return _NextStepDiscard
		}

		// 仅通过升级修复（upgrade-only repair）模式下被遗弃的 upgrade/rollback：
		// 丢弃该 pending 记录，使 release 回到最新的历史版本，随后循环会从该版本通过升级完成收敛。
		// 如果改为回滚，则会重新应用该历史版本的清单，从而删除 pending 操作已经接管的所有对象
		// （包括从旧 release 接管的 CRD），这正是 RepairViaUpgradeOnly 要避免的僵局。
		// 其他情况则保留重新安装的行为。
		if chart.RepairViaUpgradeOnly {
			return _NextStepDiscard
		}

		return _NextStepInstall
	default:
		// 处于异常状态（例如 failed）的 release 通常会重新安装，但卸载操作可能会使
		// 带有 finalizer 的 CR 所固定的 Helm 托管 CRD 发生死锁。
		// RepairViaUpgradeOnly 改为通过升级来修复此类 release，参见该字段的文档注释。
		if chart.RepairViaUpgradeOnly {
			return NextStepUpgrade
		}
		return NextStepReinstall
	}
}

// NextStepType 是下一步操作的类型。
type NextStepType uint8

const (
	NextStepDone NextStepType = iota
	NextStepRequeue
	NextStepUpgrade
	NextStepReinstall
	_NextStepInstall
	_NextStepDiscard
)

// NextStepConditionFunc 是根据给定 release 决定下一步操作的函数。
type NextStepConditionFunc func(release *helmrelease.Release) (next NextStepType)

// InstallWith 使用给定的条件函数安装指定的 chart。
//
// 条件函数用于根据给定 release 决定下一步操作，仅在找到该 release 时才会被调用。
func (c *Client) InstallWith(
	ctx context.Context,
	chart *Chart,
	next NextStepConditionFunc,
	overrideNamespace ...string,
) (helmchartutil.Values, error) {
	// 校验。
	if err := chart.Validate(); err != nil {
		return nil, fmt.Errorf("validate chart: %w", err)
	}
	if next == nil {
		return nil, errors.New("next is required")
	}

	// 创建配置。
	namespace := c.defaultNamespace
	if len(overrideNamespace) > 0 {
		namespace = overrideNamespace[0]
	}
	{
		// 确保 Namespace 存在，否则 Helm 将无法创建 release。
		_, err := kubeclientset.Create(
			ctx,
			c.KubeClientSet().CoreV1().Namespaces(),
			&core.Namespace{
				ObjectMeta: meta.ObjectMeta{
					Name: namespace,
				},
			},
		)
		if err != nil {
			return nil, fmt.Errorf("create namespace %s: %w", namespace, err)
		}
	}

	config, err := c.createConfig(namespace)
	if err != nil {
		return nil, fmt.Errorf("create helm config: %w", err)
	}

	// 获取 release。
	g := helmaction.NewGet(config)
	r, err := g.Run(chart.Release)
	if err != nil && !errors.Is(err, helmdriver.ErrReleaseNotFound) {
		return nil, fmt.Errorf("helm get: release %s: %w", chart.Release, err)
	}

	return c.converge(ctx, config, chart, next, namespace, r)
}

// converge 从已读取的 release 记录（不存在时为 nil）开始运行 InstallWith 的下一步循环：
// 它先决策、执行操作，然后重新读取，直到 release 报告完成，届时返回合并后的 values。
// 它与 InstallWith 分离（InstallWith 负责从真实集群构建 Namespace 和操作配置），
// 以便在测试中让该循环直接基于内存中的 Helm 存储运行。
func (c *Client) converge(
	ctx context.Context,
	config *helmaction.Configuration,
	chart *Chart,
	next NextStepConditionFunc,
	namespace string,
	r *helmrelease.Release,
) (helmchartutil.Values, error) {
	logger := klog.Background().WithName(chart.Name).WithValues("release", chart.Release)
	g := helmaction.NewGet(config)
	var err error

	// forceUpgrade 将丢弃被遗弃的 pending upgrade/rollback 后紧接着的 Done 转换为一次升级：
	// 该记录之下的历史版本在版本和 values 上可能看起来已是最新，但被终止的操作其实只应用了一半清单。
	// 该标志仅触发一次：在这次升级之后的 Done 才表示 release 真正收敛。
	forceUpgrade := false
	for {
		n := _NextStepInstall
		if r != nil {
			n = next(r)
			if forceUpgrade {
				// 仅触发一次，且仅在真正执行升级时消费该标志：丢弃记录后紧接着的 Done 会转为升级，
				// 而 release 本身就需要执行的升级（常见的版本变更）也能达到同样目的，因此两者之后都不应保留该标志。
				// requeue 不会执行任何操作，因此会保留该标志直到真正执行升级的那一轮迭代。
				if n == NextStepDone {
					n = NextStepUpgrade
				}
				forceUpgrade = n != NextStepUpgrade
			}
		} else if isApiServiceReady(ctx, c, chart.SkippedInstallationIfApiServiceReady) {
			return nil, nil
		}

		switch n {
		case NextStepRequeue:
			// 重新入队。此处基于 ctx 等待而非直接休眠：该分支负责等待并发进程完成，
			// 一旦调用方被通知停止，就不能在可能已失去锁的情况下继续等待。
			logger.Info("requeueing")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
			}
			r, err = g.Run(chart.Release)
			if err != nil && !errors.Is(err, helmdriver.ErrReleaseNotFound) {
				return nil, fmt.Errorf("helm get: release %s: %w", chart.Release, err)
			}
		case _NextStepDiscard:
			// 丢弃被遗弃的版本记录并重新评估。Helm 对此没有现成的操作：
			// 记录存在时安装会因名称冲突被拒绝，回滚没有可用的已部署版本可回退，
			// 而卸载则会拆毁死掉的进程好不容易创建出的资源。
			// 只删除当前这一条版本记录，这样如果 release 还有更早的版本，就会基于其中最新的版本收敛，而不是从头安装。
			logger.Info("discarding the abandoned release revision")
			forceUpgrade = r.Info.Status != helmrelease.StatusPendingInstall
			if _, err := config.Releases.Delete(chart.Release, r.Version); err != nil {
				return nil, fmt.Errorf("helm delete revision: release %s: %w", chart.Release, err)
			}
			r, err = g.Run(chart.Release)
			if err != nil && !errors.Is(err, helmdriver.ErrReleaseNotFound) {
				return nil, fmt.Errorf("helm get: release %s: %w", chart.Release, err)
			}
		case NextStepUpgrade:
			// 升级。
			logger.Info("upgrading")
			u := c.newUpgrade(config, chart)
			ch, err := chart.Load(ctx, config)
			if err != nil {
				return nil, fmt.Errorf("helm upgrade: load chart: %w", err)
			}
			vs, err := chart.Values.GetValues(ctx)
			if err != nil {
				return nil, fmt.Errorf("helm upgrade: get values: %w", err)
			}
			r, err = u.RunWithContext(ctx, chart.Release, ch, vs)
			if err != nil {
				return nil, fmt.Errorf("helm upgrade: release %s: %w", chart.Release, err)
			}
			logger.Infof("upgraded: %s", r.Info.Status.String())
		case NextStepReinstall:
			// 卸载。
			logger.Info("uninstalling")
			ui := helmaction.NewUninstall(config)
			ui.Timeout = c.timeout
			ui.IgnoreNotFound = true
			ui.KeepHistory = false
			ui.Wait = true
			ui.DeletionPropagation = string(meta.DeletePropagationForeground)
			ur, err := ui.Run(chart.Release)
			if err != nil {
				return nil, fmt.Errorf("helm uninstall: release %s: %w", chart.Release, err)
			}
			// IgnoreNotFound 在 release 不存在时会返回 nil 响应且不返回 error，
			// 这种情况下无需输出任何信息。
			if ur != nil {
				logger.Infof("uninstalled: %s", ur.Info)
			}
			// 该 release 已不复存在，因此接下来执行的是首次安装。
			// switch 的每个 case 都是独立作用域，若不将 r 置为 nil，下面的安装分支仍会拿到卸载前读取的 release。
			r = nil
			fallthrough
		case _NextStepInstall:
			// 安装。
			logger.Info("installing")
			i := c.newInstall(config, chart, namespace, r)
			ch, err := chart.Load(ctx, config)
			if err != nil {
				return nil, fmt.Errorf("helm install: load chart: %w", err)
			}
			vs, err := chart.Values.GetValues(ctx)
			if err != nil {
				return nil, fmt.Errorf("helm install: get values: %w", err)
			}
			r, err = i.RunWithContext(ctx, ch, vs)
			if err != nil {
				return nil, fmt.Errorf("helm install: release %s: %w", chart.Release, err)
			}
			logger.Infof("installed: %s", r.Info.Status.String())
		default:
			return helmchartutil.MergeValues(r.Chart, r.Config)
		}
	}
}

// newInstall 根据已找到的 release 记录（不存在时为 nil）构建 release 的安装操作。
//
// 首次安装不启用 atomic。atomic 隐含 Wait，这会导致原子安装阻塞直到 chart 部署的所有工作负载都变为 Ready；
// 若进程在等待期间被终止（容器的启动探针往往远早于 Helm 超时时间就判定失败），
// 就会留下一个后续任何尝试都无法越过的 pending-install 记录。
// 首次安装时尚未有任何服务在运行，因此没有可回滚的旧版本，也没有理由阻塞：
// 直接应用并返回即可，若留下 failed 状态的 release，下一轮会通过升级路径修复。
// 在已有 release 上覆盖安装则保持 atomic。
func (c *Client) newInstall(
	config *helmaction.Configuration,
	chart *Chart,
	namespace string,
	existing *helmrelease.Release,
) *helmaction.Install {
	i := helmaction.NewInstall(config)
	i.Timeout = c.timeout
	i.ReleaseName = chart.Release
	i.Namespace = namespace
	i.Atomic = existing != nil
	chart.configureInstall(i)

	return i
}

// newUpgrade 构建 release 的升级操作。
//
// 它始终启用 atomic：升级作用于运行中的 release，因此失败时必须回滚到此前正在服务的版本。
//
// 它只做 patch，绝不做 replace。Helm 的 force 选项会用渲染出的清单替换每个对象，
// 即先读取线上对象，再整体回写，这会破坏当前 chart 依赖的两个特性。
// 首先，该写入绑定了读取时的资源版本，只有在没有其他方修改同一对象时才能成功；
// 其次，它只包含 chart 渲染出的内容，会丢弃控制器写入该对象的字段。
// 内置的 Kueue 在这两方面都有涉及：它运行着自己的证书控制器，且 chart 渲染其 CRD 时不包含
// 由该控制器填充的 caBundle。使用 patch 会保留 chart 未渲染的字段，且无需依赖资源版本保持不变，
// 因此无论 Kueue 当时是否正在调谐这些 CRD，升级都能顺利收敛。
func (c *Client) newUpgrade(config *helmaction.Configuration, chart *Chart) *helmaction.Upgrade {
	u := helmaction.NewUpgrade(config)
	u.Timeout = c.timeout
	u.Atomic = true
	u.Recreate = true
	chart.configureUpgrade(u)

	return u
}

// createConfig 创建一个新的 Helm 操作配置。
func (c *Client) createConfig(namespace string) (*helmaction.Configuration, error) {
	logger := klog.Background().WithName("helm")

	// 初始化。
	//
	// NB(thxCode): 借鉴自 helm.sh/helm/cmd/helm/helm.go
	var config helmaction.Configuration
	cd := "secret"
	cdl := func(format string, v ...any) {
		logger.Infof(format, v...)
	}
	err := config.Init(c.getter, namespace, cd, cdl)
	if err != nil {
		return nil, err
	}

	// 填充 registry 客户端。
	//
	// NB(thxCode): 借鉴自 helm.sh/helm/cmd/helm/root.go
	ropts := []helmregistry.ClientOption{
		helmregistry.ClientOptDebug(true),
		helmregistry.ClientOptEnableCache(true),
		helmregistry.ClientOptWriter(logger),
	}
	config.RegistryClient, err = helmregistry.NewClient(ropts...)
	if err != nil {
		return nil, fmt.Errorf("create registry client: %w", err)
	}

	return &config, nil
}

func isApiServiceReady(ctx context.Context, c *Client, apiSvcName string) bool {
	if apiSvcName == "" {
		return false
	}

	svcCli := c.KubeClientSet().ApiregistrationV1().APIServices()

	svc, err := svcCli.Get(ctx, apiSvcName,
		meta.GetOptions{
			ResourceVersion: "0",
		})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false
		}
		return false
	}

	ready := false
	for i := range svc.Status.Conditions {
		if svc.Status.Conditions[i].Type != apireg.Available {
			continue
		}
		ready = svc.Status.Conditions[i].Status == apireg.ConditionTrue
		break
	}
	return ready
}

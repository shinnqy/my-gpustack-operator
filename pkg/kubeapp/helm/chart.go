package helm

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"text/template"

	sprig "github.com/go-task/slim-sprig/v3"
	helmaction "helm.sh/helm/v3/pkg/action"
	helmchart "helm.sh/helm/v3/pkg/chart"
	helmloader "helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"gpustack.ai/gpustack/pkg/utils/bytex"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

type (
	Chart struct {
		// Name 是 Chart 的名称。
		Name string
		// Version 是 Chart 的版本，为符合 SemVer 2 规范的字符串。
		Version string
		// Release 是 Release 的名称。
		Release string
		// Path 是 Chart 压缩包的路径。
		Path string
		// DownloadURL 是下载 Chart 压缩包的 URL。
		// 如果 Path 不存在，将从该 URL 下载 Chart。
		DownloadURL string
		// Values 是要传递给 Chart 的配置值。
		Values ChartValues
		// SkippedCRDsInstallation 定义是否跳过此 Chart 的 CRD 安装。
		//
		// 有时集群中已经安装了相同 API 的 CRD，
		// 该选项用于在全新安装时禁用 CRD 的安装。
		SkippedCRDsInstallation bool
		// SkippedInstallationIfApiServiceReady 是 APIService 的名称，
		// 如果指定该字段，当集群中已存在同名 APIService 时将跳过安装。
		//
		// 有时集群中已经安装了相同的 Chart 但并非同一个 Release，
		// 该选项用于在全新安装时检查是继续安装还是直接跳过。
		SkippedInstallationIfApiServiceReady string
		// RepairViaUpgradeOnly 通过 Helm upgrade 修复处于异常状态的 Release，
		// 而非采用具有破坏性的卸载后重装路径。
		//
		// 如果某个 Chart 的 CRD 由 Helm 模板管理，且其自定义资源带有控制器 finalizer（例如 Kueue），
		// 走卸载路径可能会导致死锁：卸载会移除控制器，但 finalizer 仍会锁住 CR，
		// 导致 CRD 卡在 Terminating 状态而无法在重装时重新创建。使用 upgrade
		// 可以保留控制器以清理 finalizer 并重建缺失的资源，从而使 Release 收敛而不致滞留。
		//
		// 同样的破坏性问题也排除了在修复 pending 记录时使用回滚：回滚会重新应用
		// 上一个已部署修订版本的清单，从而删除 pending 操作已经接管的资源（包括被接管的 CRD），
		// 因此对于被遗弃的 pending-upgrade 或 pending-rollback，修复方式是丢弃该记录
		// 并基于其底层的修订版本执行升级。
		RepairViaUpgradeOnly bool
		// TakeOwnership 会接管集群中已存在但尚未归属此 Release 的对象，
		// 而不是在这些对象上导致操作失败。
		//
		// Helm 默认拒绝在所有权元数据未指向当前 Release 的对象上执行安装或升级，
		// 并以 "invalid ownership metadata" 中止操作。该检查在任何 pre-install/pre-upgrade hook
		// 之前运行，因此 hook 无法修复此问题。启用此选项后，Helm 会跳过该检查，
		// 并在应用变更时重写每个匹配的现有对象的所有权元数据，从而将另一个 Release
		// 创建的对象转移到当前 Release 名下。
		//
		// 接管仅依据名称、Namespace 和 Kind 进行匹配，因此无法区分旧 Release
		// 创建的对象与用户手动创建的对象。仅在明确确认了先前所有者的迁移场景下设置该选项，
		// 切勿无条件启用。
		TakeOwnership bool
		// ExclusiveAccess 声明上一个操作此 Release 的执行方已停止，
		// 且在本次调用运行期间不会有其他方对其进行操作。
		//
		// 正是该字段使处于 pending 状态的 Release 记录变得可处理。遗留为
		// pending-install、pending-upgrade 或 pending-rollback 的记录，要么是仍在进行中的操作，
		// 要么是进程在中途崩溃后的残余，而记录本身无法区分这两种情况——因此若没有此标志，
		// 系统只能按存活时间等待该记录过期，这也是遗留记录会在后续每次尝试中持续存在的原因。
		//
		// 仅持有锁本身并不足以设置该标志：锁只能保证并发方无法启动，
		// 而不能说明前一个执行方已经结束。只有在有明确证据表明前一个执行方已退出时才可设置——
		// 例如它在自身调用返回后释放了锁，或者可以观察到其进程已不再运行。修复正在执行的操作会破坏其状态。
		ExclusiveAccess bool
	}

	ChartValues interface {
		// GetValues 返回用于 Chart 安装的配置值。
		GetValues(ctx context.Context) (map[string]any, error)
	}
)

// Validate 校验 Chart。
func (ch Chart) Validate() error {
	if ch.Name == "" {
		return fmt.Errorf("name is required")
	}
	if ch.Release == "" {
		return fmt.Errorf("release name is required")
	}
	if ch.Path == "" && ch.DownloadURL == "" {
		return fmt.Errorf("path or download URL is required")
	}
	return nil
}

// Load 从本地路径或远程 URL 加载 Chart。
func (ch Chart) Load(_ context.Context, cfg *helmaction.Configuration) (*helmchart.Chart, error) {
	f := ch.Path
	if f != "" && osx.IsEmptyFile(f) {
		f = ""
	}

	if f == "" {
		if ch.DownloadURL == "" {
			return nil, fmt.Errorf("chart path %s is not existed and download URL is not provided", ch.Path)
		}
		_, fn := filepath.Split(ch.DownloadURL)
		f = filepath.Join(filepath.Dir(ch.Path), fn)
		if osx.IsEmptyFile(f) {
			p := helmaction.NewPullWithOpts(helmaction.WithConfig(cfg))
			p.Settings = cli.New()
			p.Version = ch.Version
			p.DestDir = filepath.Dir(ch.Path)
			pr, err := p.Run(ch.DownloadURL)
			if err != nil {
				return nil, fmt.Errorf("pull chart from %s: %s: %w", ch.DownloadURL, pr, err)
			}
		}
	}

	return helmloader.Load(f)
}

// GetValues 返回用于 Chart 安装的配置值。
func (ch Chart) GetValues(ctx context.Context) (map[string]any, error) {
	if ch.Values == nil {
		return nil, nil
	}
	return ch.Values.GetValues(ctx)
}

// configureInstall 将 Chart 声明的选项应用到指定的 install 操作。
//
// 仅设置 Chart 声明的选项；调用方仍需负责该操作的
// release 名称、Namespace、超时时间和原子性设置。
func (ch Chart) configureInstall(i *helmaction.Install) {
	// SkipCRDs 是控制是否安装 Chart 的 crds/ 目录的标志；而 IncludeCRDs 只是
	// 将这些文件一并渲染进清单，两者完全不同。设置后者会导致 Helm 对 crds/ 中的每个 CRD
	// 安装两次：一次在 CRD 阶段（创建时不带所有权元数据），另一次作为 Release 资源
	// （其所有权检查随后会拒绝刚才阶段创建出来的对象）。将 IncludeCRDs 保持为 false
	// 可使 CRD 阶段成为唯一的安装入口，而且该阶段本身允许 CRD 已存在——
	// 这正是集群能够保留其自身 Operator 所安装 CRD 副本的原因。
	i.SkipCRDs = ch.SkippedCRDsInstallation
	i.TakeOwnership = ch.TakeOwnership
}

// configureUpgrade 将 Chart 声明的选项应用到指定的 upgrade 操作。
//
// 仅设置 Chart 声明的选项；调用方仍需负责该操作的
// 超时时间、原子性和重建策略。
func (ch Chart) configureUpgrade(u *helmaction.Upgrade) {
	u.TakeOwnership = ch.TakeOwnership
}

type TemplateValues struct {
	Application   string
	Template      string
	ExtendFuncMap template.FuncMap
	Context       map[string]any
}

func (cv TemplateValues) GetValues(ctx context.Context) (map[string]any, error) {
	tmpl, err := template.New("helm-values-template").
		Funcs(templateFuncMap(cv.ExtendFuncMap)).
		Parse(cv.Template)
	if err != nil {
		return nil, err
	}

	buf := bytex.GetBuffer()
	defer bytex.Put(buf)

	if err = tmpl.Execute(buf, cv.Context); err != nil {
		return nil, err
	}

	if klog.V(3).Enabled() {
		klog.Infof("rendered application values %s:\n%s", cv.Application, buf.String())
	}

	vs := map[string]any{}
	err = yaml.Unmarshal(buf.Bytes(), &vs)
	return vs, err
}

func templateFuncMap(extend template.FuncMap) template.FuncMap {
	fm := sprig.TxtFuncMap()
	fm["toYaml"] = toYAML
	if len(extend) > 0 {
		for k, v := range extend {
			fm[k] = v
		}
	}

	return fm
}

// toYAML 借鉴自 helm.sh/helm/pkg/engine/engine.go。
func toYAML(v any) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		// 在模板内部忽略 error。
		return ""
	}

	return strings.TrimSuffix(string(data), "\n")
}

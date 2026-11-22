package settings

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	storage "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/modelstore"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/system"
)

// 水位线 Setting 的名称。两者各自的准入校验通过名称读取另一方，
// 因为在某个 Setting 声明中直接引用另一个 Setting 会导致初始化循环依赖。
const (
	modelStoreHighWatermarkName = "model-store-high-watermark"
	modelStoreLowWatermarkName  = "model-store-low-watermark"
)

// ModelArtifactDeliveryMode 的取值。
const (
	// ModelArtifactDeliveryEngine 表示由推理引擎自行下载 hub 制品已解析提交版本的内容。
	ModelArtifactDeliveryEngine = "Engine"
	// ModelArtifactDeliveryNode 表示由节点上的 model-manager 插件物化并挂载模型制品。
	ModelArtifactDeliveryNode = "Node"
)

var (
	// ModelArtifactDeliveryMode 定义 Hugging Face 制品权重到达 ModelDeployment 引擎的方式。
	// 该配置需显式指定，绝不根据插件是否安装进行推断：修改它会变更所有 Hugging Face
	// 部署的 Pod，导致各部署滚动更新一次。部署该插件时 Helm chart 会声明为 "Node"，
	// worker 每次启动时也会重新声明该配置。
	ModelArtifactDeliveryMode = settings.NewEditable(
		"model-artifact-delivery-mode",
		"Indicates how a Hugging Face ModelArtifact's weights reach a ModelDeployment: Engine, the engine "+
			"downloads them, or Node, the node's model-manager plugin materializes and mounts them. Node needs "+
			"the plugin installed. Changing it rolls every Hugging Face deployment once.",
		setting.InitializeFromEnv(ModelArtifactDeliveryEngine),
		admitDeliveryMode,
	)

	// ModelStoreHighWatermark 是缓存文件系统的使用率阈值（百分比），超过该阈值时
	// 插件会清理未被引用的内容。当缓存与 kubelet 共用文件系统时，该值必须低于
	// kubelet 的驱逐阈值和镜像垃圾回收阈值；插件会在该限制处对其进行封顶。
	ModelStoreHighWatermark = settings.NewEditable(
		modelStoreHighWatermarkName,
		"Indicates the node model cache's usage percent above which unreferenced models are removed, "+
			"at most 95 and above model-store-low-watermark.",
		setting.InitializeFromEnv("80"),
		setting.AllowInt64InRange(modelstore.MinLowWatermarkPercent+1, modelstore.MaxHighWatermarkPercent),
		admitWatermarkAgainst(modelStoreLowWatermarkName, true),
	)

	// ModelStoreLowWatermark 是垃圾回收执行后将缓存文件系统使用率降到的目标阈值（百分比）。
	ModelStoreLowWatermark = settings.NewEditable(
		modelStoreLowWatermarkName,
		"Indicates the node model cache's usage percent a removal stops at, at least 1 and below "+
			"model-store-high-watermark.",
		setting.InitializeFromEnv("70"),
		setting.AllowInt64InRange(modelstore.MinLowWatermarkPercent, modelstore.MaxHighWatermarkPercent-1),
		admitWatermarkAgainst(modelStoreHighWatermarkName, false),
	)

	// ModelStorePeerSync 控制节点上的插件是否允许从其他节点插件已发布的内容中拉取数据，
	// 而非直接从 hub 拉取。
	ModelStorePeerSync = settings.NewEditable(
		"model-store-peer-sync",
		"Indicates whether a node's plugin may pull a model's content from the plugins of other "+
			"nodes that hold it published, instead of the hub.",
		setting.InitializeFromEnv("true"),
		setting.AllowBool(),
	)

	// ModelStoreDownloadConcurrency 指定节点在所有下载任务中同时运行的 HTTP 请求数上限。
	// 大文件会按分片范围并发拉取，并共享此并发配额。
	ModelStoreDownloadConcurrency = settings.NewEditable(
		"model-store-download-concurrency",
		"Indicates how many HTTP requests a node runs at once across its model downloads, 1 to 64.",
		setting.InitializeFromEnv("8"),
		setting.AllowInt64InRange(modelstore.MinDownloadConcurrency, modelstore.MaxDownloadConcurrency),
	)

	// ModelStoreDownloadBandwidth 是节点的下载速率限制，以每秒字节数表示
	// （例如 "200Mi"）；"0" 表示不限速。
	ModelStoreDownloadBandwidth = settings.NewEditable(
		"model-store-download-bandwidth",
		"Indicates a node's model download rate limit in bytes per second, a quantity such as 200Mi; 0 is unlimited.",
		setting.InitializeFromEnv("0"),
		func(_ context.Context, _, newVal string) error {
			_, err := modelstore.ParseBandwidth(newVal)
			return err
		},
	)
)

// currentValue 按名称直接从 Secret 而非通过 Settings 读取缓存获取另一个 Setting 的存储值：
// 否则在缓存 30 秒有效期内的两次连续修改会基于已被替换的旧值进行校验，
// 若形成交叉错位则会冻结所有节点的配置。无法读取 Secret 时会返回 error，
// 因此准入校验会直接拒绝而非跳过检查。将其定义为变量以便测试在没有 API server 时替换。
var currentValue = func(ctx context.Context, name string) (string, error) {
	sec, err := system.LoopbackKubeClient.Get().CoreV1().Secrets(setting.DelegatedSecretNamespace).
		Get(ctx, setting.DelegatedSecretName, meta.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read setting %s: %w", name, err)
	}
	if v, ok := sec.Data[name]; ok {
		return string(v), nil
	}

	return settings[name].DefaultValue(), nil
}

// csiDriverExists 返回插件对应的 CSIDriver 对象是否存在。该函数直接读取 API server
// 而非 informer 缓存，以避免在刚安装完插件后因缓存尚未同步到 CSIDriver 而误拒修改。
// 将其定义为变量以便测试在没有 API server 时替换。
var csiDriverExists = func(ctx context.Context) (bool, error) {
	err := system.LoopbackCtrlAPIReader.Get().Get(ctx, ctrlcli.ObjectKey{Name: modelstore.DriverName}, &storage.CSIDriver{})
	switch {
	case err == nil:
		return true, nil
	case kerrors.IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

// admitDeliveryMode 允许设为 Engine，并仅在插件的 CSIDriver 存在时允许设为 Node：
// 若缺乏实际投递方，所有 Hugging Face 部署都会一直处于等待状态。
func admitDeliveryMode(ctx context.Context, _, newVal string) error {
	switch newVal {
	case ModelArtifactDeliveryEngine:
		return nil
	case ModelArtifactDeliveryNode:
		ok, err := csiDriverExists(ctx)
		if err != nil {
			return fmt.Errorf("check the CSIDriver %s: %w", modelstore.DriverName, err)
		}
		if !ok {
			return fmt.Errorf("the CSIDriver %s does not exist: install the model-manager plugin "+
				"(chart value modelManager.enabled) before choosing Node delivery", modelstore.DriverName)
		}
		return nil
	default:
		return fmt.Errorf("want %s or %s, got %q", ModelArtifactDeliveryEngine, ModelArtifactDeliveryNode, newVal)
	}
}

// admitWatermarkAgainst 确保高水位线始终高于低水位线：新的高水位线必须大于当前低水位线，
// 新的低水位线必须小于当前高水位线。因此同时调低两者时需先调低低水位线。
func admitWatermarkAgainst(other string, isHigh bool) setting.Admission {
	return func(ctx context.Context, _, newVal string) error {
		v, err := strconv.ParseInt(newVal, 10, 32)
		if err != nil {
			return err
		}
		raw, err := currentValue(ctx, other)
		if err != nil {
			return err
		}
		o, readable := parsePercent(raw)
		switch {
		case !readable:
			// 若另一方的取值无法读取，则交由其自身的校验处理。
		case isHigh && v <= o:
			return fmt.Errorf("the high watermark %d must be above the low watermark %d", v, o)
		case !isHigh && v >= o:
			return fmt.Errorf("the low watermark %d must be below the high watermark %d", v, o)
		}
		return nil
	}
}

func parsePercent(raw string) (int64, bool) {
	v, err := strconv.ParseInt(raw, 10, 32)
	return v, err == nil
}

// ModelStoreLayer 通过 value 函数从 Settings 读取节点模型缓存配置的集群层级：
// 运行时传入 Setting 的当前值，启动时则传入其默认值。
func ModelStoreLayer(value func(setting.Setting) string) (modelstore.Layer, error) {
	var errs []error
	parse32 := func(s setting.Setting) *int32 {
		v, err := strconv.ParseInt(value(s), 10, 32)
		if err != nil {
			errs = append(errs, fmt.Errorf("setting %s: %w", s.Name(), err))
			return nil
		}
		v32 := int32(v)
		return &v32
	}
	str := func(s setting.Setting) *string {
		v := value(s)
		return &v
	}

	peerSync := func(s setting.Setting) *bool {
		raw := value(s)
		if raw == "" {
			return nil
		}
		v, err := strconv.ParseBool(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("setting %s: %w", s.Name(), err))
			return nil
		}
		return &v
	}

	l := modelstore.Layer{
		HighWatermarkPercent: parse32(ModelStoreHighWatermark),
		LowWatermarkPercent:  parse32(ModelStoreLowWatermark),
		DownloadConcurrency:  parse32(ModelStoreDownloadConcurrency),
		PeerSyncEnabled:      peerSync(ModelStorePeerSync),
		HuggingFaceEndpoint:  str(ModelArtifactHuggingFaceEndpoint),
		ModelScopeEndpoint:   str(ModelArtifactModelScopeEndpoint),
		HTTPSProxy:           str(ModelArtifactHTTPSProxy),
		NoProxy:              str(ModelArtifactNoProxy),
		CABundleConfigMap:    str(ModelArtifactCABundle),
	}
	if bw, err := modelstore.ParseBandwidth(value(ModelStoreDownloadBandwidth)); err != nil {
		errs = append(errs, fmt.Errorf("setting %s: %w", ModelStoreDownloadBandwidth.Name(), err))
	} else {
		l.DownloadBytesPerSecond = &bw
	}

	return l, errors.Join(errs...)
}

// ValidateModelStoreDefaults 校验节点模型缓存的 Settings 从环境变量读取的默认值；
// 由于这些值不经过准入校验，若其中存在将被写入所有节点的非法配置，worker 会拒绝启动。
// 此处不检查投递模式所需的 CSIDriver 条件：插件是否存在属于运行时状态，消费方会自行等待。
func ValidateModelStoreDefaults() error {
	if v := ModelArtifactDeliveryMode.DefaultValue(); v != ModelArtifactDeliveryEngine && v != ModelArtifactDeliveryNode {
		return fmt.Errorf("setting %s: want %s or %s, got %q",
			ModelArtifactDeliveryMode.Name(), ModelArtifactDeliveryEngine, ModelArtifactDeliveryNode, v)
	}
	l, err := ModelStoreLayer(setting.Setting.DefaultValue)
	if err != nil {
		return err
	}

	return validateLayer(l)
}

func validateLayer(l modelstore.Layer) error {
	if err := modelstore.Validate(modelstore.Merge(l)); err != nil {
		return fmt.Errorf("the node model cache's settings: %w", err)
	}

	return nil
}

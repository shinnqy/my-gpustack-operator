package worker

import (
	"fmt"
	"path"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/json"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
)

const (
	// KVCacheInjectedAnnotationKey 以 JSON 格式记录该 webhook 的操作结果，便于通过 jsonpath 查询单个字段。
	// 它由 webhook 写入，绝不从提交的对象中读取：用户提供的值相当于伪造了一项并非由其做出的决策记录。
	KVCacheInjectedAnnotationKey = "kvcache." + systemname.LabelPrefix + "injected"

	// observabilityMetricsEnv 和 observabilityBandwidthEnv 在容器未声明时才会被开启。
	// 它们是仅有的两个仍会让位于容器配置而非被覆盖的变量，原因在于它们不改变任何运行结果：
	// 它们仅用于开关指标上报，因此用户设置的值既不会与该 webhook 决定的配置产生二义性，也不值得为此拒绝 Pod。
	observabilityMetricsEnv   = "MC_TE_METRIC"
	observabilityBandwidthEnv = "MC_STORE_CLIENT_METRIC_BANDWIDTH"
)

// ownedArgs 是该 webhook 写入的参数标志。若容器已携带其中之一，说明已有外部配置的 KVCache，
// 强行合并会导致类似“出现两个 --kv-transfer-config，究竟哪个生效”的冲突，且从外部无法诊断。
var ownedArgs = []string{"--kv-transfer-config", "--hicache-storage-backend"}

// ownedEnv 是该 webhook 写入的、用于选择具体机制的环境变量，区别于在机制内部传递具体参数的变量。
// 参数型变量不会触发拒绝，而是直接被覆盖，因为注入是选择加入（opt-in）的，且退出方式是显式的。
//
// SGLANG_HICACHE_MOONCAKE_CONFIG_PATH 不在此处列出而是在别处被拒绝，这是基于实测的考量而非代码风格偏好。
// 该 webhook 不写入该变量，因此它不会产生键名冲突，也无法通过按渲染输出过滤的扫描检出。
// 它的影响在于会使引擎的配置源选择器进入另一条分支，导致所有注入的环境变量虽存在却不被读取——
// 参见包含该实测记录的 inject.ConfigSourceKeys。
var ownedEnv = []string{"MOONCAKE_CONFIG_PATH"}

// injectionRecord 是写回到目标对象上的印记，用于记录该 webhook 所做的决策。
//
// 采用 JSON 格式是为了便于通过 jsonpath 查询单个字段；设立该记录的原因在于下文中的决策不会留下
// 其他可观测痕迹。从外部看，注入了租户与未注入租户的 Pod 并无二致，而这一差异的代价——两个复用域
// 共享同一个驱逐池——不会反映在任何指标上。
type injectionRecord struct {
	// Binding 是该 Pod 指定并由该 webhook 在 Pod 所在命名空间中解析出的 KVCachePoolBinding。
	// 它负责置备复用域和容量，但不提供鉴权，API 契约对此有明确说明——知道其他域名称的工作负载
	// 无论是否有 Binding 都能访问该域（参见 docs/modules/kv-cache/pool.md#limitations 与 #168）。
	//
	// 它不一定就是写入流量最终记账所属的 Binding。若所用引擎构建版本忽略注入的租户信息，
	// 它会以 master 自身的 "default" 名称写入，从而使实际用量计入注册了该域的任意 Binding。
	// 因此，若将此字段当作记账归属对象，可能会误导运维人员查看错误的 status。
	Binding string `json:"binding"`

	// Engine 为实际配置的推理引擎。
	Engine string `json:"engine"`

	// Vehicle 取值为 "file" 或 "environment"。若使用环境变量方式（environment）的 Pod 始终处于冷缓存状态，
	// 则应检查该 Pod 是否自带了配置文件路径——这是本设计唯一允许的静默结果。
	Vehicle string `json:"vehicle"`

	// Domain 是 Binding 声明的复用域标识。
	Domain string `json:"domain,omitempty"`

	// TenantInjected 表示该 webhook 是否向容器中写入了租户标识，它记录的是“已执行的动作”而非“实际结果”。
	// 这一区分正是该字段的核心设计所在。
	//
	// 租户是否生效取决于引擎的构建版本，而准入阶段无法得知这一点，因为它从不检查容器镜像。
	// 较旧的构建版本可能会收到一个它根本不会读取的值。设立该字段正是为了避免在此方向上做出过度承诺，
	// 因此它只声明我们确定的动作，而绝不断言不确定的结果。
	//
	// 为 TRUE 时表示：该 webhook 已提供租户标识。支持该变量的引擎构建会进行隔离，而过旧无法读取
	// 该变量的构建则会静默失效。而在客户端侧失败会是显式的——版本过旧而无法接受该参数的 Mooncake
	// 会直接报错而不是丢弃参数，从而让 Pod 停止启动，避免静默失去隔离性。
	//
	// 为 FALSE 时表示：渲染器未收到复用域，也未写入租户标识。
	TenantInjected bool `json:"tenantInjected"`

	// LaunchProgram 是剥离透明启动器后剩余的可执行程序名。仅当命令行中没有可读取的程序名，
	// 且作者声明启动过程仍会转发追加的参数时（例如镜像 ENTRYPOINT，或封装在单个参数 token 中的命令行），
	// 该字段才为空。任何完全无法解析出程序的启动方式都会被直接拒绝，而不会写入印记。
	LaunchProgram string `json:"launchProgram"`

	// LaunchArgsForwarded 记录是否因作者的显式声明而放行了该 webhook 无法识别为引擎入口点的启动方式。
	LaunchArgsForwarded bool `json:"launchArgsForwarded"`
}

// injectPod 将渲染得到的 Result 应用到 Pod 的一个容器中，并写入操作印记。
//
// 它是此处唯一执行变更的函数，且在最后运行：所有拒绝检查此前均已完成，因此到达此处的 Pod
// 要么被完整注入，要么保持原样。半注入状态的 Pod（例如已添加卷但缺少环境变量）启动后会产生任何字段都无法解释的行为。
func (r *PodKVCacheWebhook) injectPod(pod *core.Pod, res *resolution, out *inject.Result) error {
	ctr, err := targetContainer(pod)
	if err != nil {
		return err
	}
	if err = checkOwnedKeys(pod, ctr, res.Input.Engine, out); err != nil {
		return err
	}
	if err = checkConfigSourceKeys(ctr, res.Input.Engine); err != nil {
		return err
	}
	launch, err := checkLaunchArgs(ctr, res.Input.Engine, pod.Annotations)
	if err != nil {
		return err
	}

	for i := range out.Env {
		// 注入会覆盖工作负载自行声明的同名变量。注入机制采用选择加入模式，退出方式也是显式的——
		// 将标签设为 "false" 或不设置——因此既请求了注入又自行声明了这些变量的 Pod，相当于对同一配置给出了两个答案，
		// 此时应以根据其指定的 Binding 推导出的值为准。
		//
		// 此处采用原地修改而非末尾追加。虽然追加同样会生效——kubelet 会按声明顺序将容器的 env 列表折叠为 map，
		// 因此重名变量中最后一项才是进程实际看到的值——但原地修改能保持 Pod 清单的可读性（每个变量名只保留一项，
		// 直接展示生效的值），并且不依赖于阅读 Pod 配置的人无法直观看到的底层机制。
		found := false
		for j := range ctr.Env {
			if ctr.Env[j].Name == out.Env[i].Name {
				ctr.Env[j] = out.Env[i]
				found = true
			}
		}
		if !found {
			ctr.Env = append(ctr.Env, out.Env[i])
		}
	}
	for _, name := range []string{observabilityMetricsEnv, observabilityBandwidthEnv} {
		if !deviceplugin.ContainerEnvDeclared(ctr, name) {
			ctr.Env = append(ctr.Env, core.EnvVar{Name: name, Value: "1"})
		}
	}
	// 渲染器的默认变量以与上述两项相同的原则让位于容器自身配置：容器内声明的同名变量或指定相同标志的参数，
	// 均视为工作负载自己的设定。容器原有的参数会在追加渲染参数之前读取，因此只有工作负载自行编写的参数
	// 才会使默认值被跳过。
	for _, e := range out.DefaultedEnv {
		if !deviceplugin.ContainerEnvDeclared(ctr, e.Name) {
			ctr.Env = append(ctr.Env, e)
		}
	}
	var defaultedArgs []string
	for _, group := range out.DefaultedArgs {
		if _, found := containerFlag(ctr, res.Input.Engine, group[0]); found {
			continue
		}
		defaultedArgs = append(defaultedArgs, group...)
	}

	ctr.Args = append(ctr.Args, out.Args...)
	ctr.Args = append(ctr.Args, defaultedArgs...)
	ctr.VolumeMounts = append(ctr.VolumeMounts, out.VolumeMounts...)
	pod.Spec.Volumes = append(pod.Spec.Volumes, out.Volumes...)

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	for k, v := range out.PodAnnotations {
		pod.Annotations[k] = v
	}

	vehicle := "environment"
	if len(out.Volumes) > 0 {
		vehicle = "file"
	}
	record, err := json.Marshal(injectionRecord{
		Binding:             pod.Annotations[KVCacheBindingAnnotationKey],
		Engine:              string(res.Input.Engine),
		Vehicle:             vehicle,
		Domain:              res.Isolation.Domain,
		TenantInjected:      out.TenantInjected,
		LaunchProgram:       launch.program,
		LaunchArgsForwarded: launch.argsForwarded,
	})
	if err != nil {
		// 不可达分支，保留此处而非省略是因为省略就意味着忽略 error 返回值。injectionRecord 仅包含字符串和布尔值，
		// 而 encoding/json 仅在遇到无法表示的值（如 channel、函数、循环引用、NaN 和无穷大）时才会失败，
		// 该结构体不可能包含这些值。因此没有任何测试覆盖此分支；若要为此编写测试，就必须向无需此类字段的记录中
		// 硬塞入一个可能序列化失败的字段。此处也不应 panic：该代码位于准入路径上，panic 会导致整个 webhook 宕机
		// 进而影响所有 Pod，而不是仅让单个 Pod 失败。
		return fmt.Errorf("marshal the injection record: %w", err)
	}
	pod.Annotations[KVCacheInjectedAnnotationKey] = string(record)

	return nil
}

// targetContainer 选择要注入的目标容器，无法确定时直接拒绝而非猜测。
//
// 若只有一个容器，则选择该容器；若有多个容器，调用方必须显式指定名称，因为默认选取第一个容器
// 极易导致配置被注入到 sidecar 中，而实际工作负载却运行在主容器里——其表现是“产物已注入但功能未生效”，
// 从 Pod 外部根本无法诊断。
//
// init 容器绝不作为候选，显式指定 init 容器也会被拒绝。init 容器在工作负载启动前就已退出，
// 对其配置缓存没有任何意义。
func targetContainer(pod *core.Pod) (*core.Container, error) {
	named := pod.Annotations[KVCacheContainerAnnotationKey]

	if named == "" {
		switch len(pod.Spec.Containers) {
		case 1:
			return &pod.Spec.Containers[0], nil
		case 0:
			return nil, fmt.Errorf("the Pod has no containers to inject into")
		default:
			return nil, fmt.Errorf("the Pod has %d containers (%s), so annotation %q must name the "+
				"one running the engine; this webhook never picks the first",
				len(pod.Spec.Containers), strings.Join(containerNames(pod.Spec.Containers), ", "),
				KVCacheContainerAnnotationKey)
		}
	}

	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == named {
			return &pod.Spec.Containers[i], nil
		}
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == named {
			return nil, fmt.Errorf("annotation %q names %q, which is an init container: it finishes "+
				"before the workload starts, so configuring it would cache nothing. Name one of: %s",
				KVCacheContainerAnnotationKey, named,
				strings.Join(containerNames(pod.Spec.Containers), ", "))
		}
	}
	return nil, fmt.Errorf("annotation %q names %q, which is not a container of this Pod. Name one "+
		"of: %s", KVCacheContainerAnnotationKey, named,
		strings.Join(containerNames(pod.Spec.Containers), ", "))
}

// checkConfigSourceKeys 拒绝已经选择了本 webhook 不会写入的配置源（SOURCE）的容器。
//
// 它与 checkOwnedKeys 分开实现，因为两者拒绝的对象不同。owned 键是本 webhook 所渲染配置的第二个来源，
// 那里的扫描会按本次渲染实际写入的内容进行过滤——渲染本身不打算添加的键不会产生冲突。而此处的键
// 不会与任何键发生名义上的冲突：本 webhook 并不写入这类键，但这正是它危险的地方。
// 引擎通过互斥分支读取存储配置，该键会引导引擎进入另一分支，从而导致所有注入的环境变量所在分支
// 永不执行。结果 Pod 带着一整套无人读取的环境变量运行，而注入记录却显示注入成功。
//
// 这些键按引擎区分，来自完成实测的对应包，因为一个键只对读取它的引擎起分支选择作用。
func checkConfigSourceKeys(ctr *core.Container, engine inject.Engine) error {
	env, args := inject.ConfigSourceKeys(engine)

	for _, name := range env {
		if deviceplugin.ContainerEnvDeclared(ctr, name) {
			return fmt.Errorf("container %q sets %s, which points engine %q at a configuration file "+
				"of its own; this webhook configures the store through the environment, and that "+
				"branch is then never read -- every injected variable would be present and unused",
				ctr.Name, name, engine)
		}
	}
	for _, flag := range args {
		if spelled, found := containerFlag(ctr, engine, flag); found {
			return fmt.Errorf("container %q passes %s, which outranks the environment engine %q "+
				"would otherwise read its store configuration from, so the injected variables would "+
				"be present and unused", ctr.Name, spelled, engine)
		}
	}

	return nil
}

// checkOwnedKeys 拒绝已携带用于选择 KVCache 机制的键，或存在与本 webhook 所拥有名称或路径冲突的卷或挂载点的容器。
//
// 此处选择拒绝而非合并，因为上述每一种情况都会产生无报错的二义性：同一命令行出现两个连接器标志，
// 或同一路径存在两个挂载点。如需自行接管配置，必须显式退出注入——将注入标签设为 "false" 或不设置该标签。
func checkOwnedKeys(pod *core.Pod, ctr *core.Container, engine inject.Engine, out *inject.Result) error {
	// 只有本次渲染实际会写入的项才可能发生冲突。owned 列表涵盖所有引擎，若不过滤地全量扫描，会因本次渲染
	// 根本不打算添加的键而误拒 Pod：例如 SGLang 容器设置了 MOONCAKE_CONFIG_PATH（SGLang 使用
	// SGLANG_HICACHE_MOONCAKE_CONFIG_PATH 选择配置文件，因此该变量对它无意义），或 vLLM 容器传入了
	// SGLang 的 --hicache-storage-backend。两者都不会构成同一配置的第二个来源，而这正是此处执行拒绝的前提。
	for _, name := range ownedEnv {
		if !slices.ContainsFunc(out.Env, func(e core.EnvVar) bool { return e.Name == name }) {
			continue
		}
		if deviceplugin.ContainerEnvDeclared(ctr, name) {
			return fmt.Errorf("container %q already sets %s, so it already has a KV cache configured; "+
				"this webhook refuses to merge rather than leave two sources for one setting",
				ctr.Name, name)
		}
	}
	for _, flag := range ownedArgs {
		if !slices.Contains(out.Args, flag) {
			continue
		}
		if spelled, found := containerFlag(ctr, engine, flag); found {
			return fmt.Errorf("container %q already passes %s, so it already has a KV cache "+
				"configured; two of them on one command line is undiagnosable", ctr.Name, spelled)
		}
	}
	// 拒绝 dtype 标志的原因不同于上述两项：它并不意味着缓存已在别处配置，而是指定元素数据类型，
	// 且同一复用域上的所有引擎都必须使用 Binding 中指定的 dtype 写入数据。
	if slices.Contains(out.Args, inject.KVCacheDtypeArg) {
		if spelled, found := containerFlag(ctr, engine, inject.KVCacheDtypeArg); found {
			return fmt.Errorf("container %q already passes %s, but the KV cache dtype comes from "+
				"the KVCachePoolBinding: the store key does not carry it, so engines writing two "+
				"dtypes into one domain read each other's blocks wrong; remove it, and name a "+
				"Binding declaring the dtype you want", ctr.Name, spelled)
		}
	}

	// 除容器的挂载列表外，还会检查 Pod 自身的卷列表。Pod 上可能已存在同名的卷但未被目标容器挂载，
	// 若直接追加第二个同名卷，API server 会拒绝请求，且其报错既不会指出本 webhook，也不会指出冲突的清单行。
	for i := range out.Volumes {
		for j := range pod.Spec.Volumes {
			if pod.Spec.Volumes[j].Name == out.Volumes[i].Name {
				return fmt.Errorf("the Pod already declares a volume named %q, which this webhook owns",
					out.Volumes[i].Name)
			}
		}
	}

	for i := range out.VolumeMounts {
		for j := range ctr.VolumeMounts {
			switch {
			case ctr.VolumeMounts[j].Name == out.VolumeMounts[i].Name:
				return fmt.Errorf("container %q already mounts a volume named %q, which this webhook "+
					"owns", ctr.Name, out.VolumeMounts[i].Name)
			case ctr.VolumeMounts[j].MountPath == out.VolumeMounts[i].MountPath:
				return fmt.Errorf("container %q already mounts something at %q, which this webhook "+
					"owns", ctr.Name, out.VolumeMounts[i].MountPath)
			}
		}
	}
	return nil
}

// checkLaunchArgs 拒绝既未声明 command 也未声明 args 的容器。
//
// 向空的 args 追加参数并非真正的“追加”：Kubernetes 会把单独的 args 视为完整命令行并完全丢弃镜像的 CMD，
// 导致引擎丢失自身的启动参数。该 webhook 无法查看镜像内容，也无法恢复这些参数。
//
// 为何“仅设置 command 而无 args”不属于同一种情况——在此写明是因为代码评审已三次提出该疑问，而评审意见解决后
// 回复不会保留在代码中。每次的质疑都是：设置了 `command` 但省略 `args` 时仍会运行镜像的 CMD，因此向 args 追加参数
// 会静默替换掉 CMD。实际上 Kubernetes 对四种组合的解析规则如下：
//
//	command unset, args unset  -> image ENTRYPOINT + image CMD
//	command unset, args set    -> image ENTRYPOINT + args        (CMD dropped)
//	command SET,   args unset  -> command alone                  (ENTRYPOINT and CMD BOTH dropped)
//	command set,   args set    -> command + args
//
// 因此在第三行中根本没有 CMD 可供替换：早在 webhook 追加任何内容之前，设置 command 就已经将其丢弃了。
// 丢弃它的是容器作者，而非注入逻辑。这一行也解释了为什么下方的拒绝条件在“两者均未声明”时触发，而非在
// “args 为空”时触发——仅设置了 command 的容器本身是完整的，向其 args 追加参数正是本 webhook 设计的常规场景。
//
// 仅包含启动器的 COMMAND 是第三行中的特例，会被拒绝。例如 `tini --` 配合空的 args 会解析出空的参数列表：
// 容器完全未指定要执行的程序，导致此处追加的参数变成启动器 exec 执行的命令，从而把连接器标志当作程序来运行。
// 拒绝这种情况不会影响任何本可正常启动的容器——没有目标程序的启动器自身就会启动失败——而放行它则会把
// 无法启动的容器变成能启动却运行错误命令的容器。这一条件比“args 为空”更窄：若 `tini --` 在 args 中带有程序命令，
// 它会解析出该程序，属于上述可正常追加参数的常规情况。
type launchCheck struct {
	program       string
	argsForwarded bool
}

func checkLaunchArgs(ctr *core.Container, engine inject.Engine, annotations map[string]string) (launchCheck, error) {
	if len(ctr.Command) == 0 && len(ctr.Args) == 0 {
		return launchCheck{}, fmt.Errorf("container %q declares neither command nor args, so it runs the image's own "+
			"ENTRYPOINT and CMD. Both engines need a flag on the command line, and setting args while "+
			"command is unset makes Kubernetes discard the image's CMD. Set command to the engine executable "+
			"and copy the image's launch arguments into args; this overrides the image ENTRYPOINT too, so "+
			"the command must start the engine directly",
			ctr.Name)
	}
	declaresForwarding, err := declaresLaunchArgsForwarding(annotations)
	if err != nil {
		return launchCheck{}, err
	}
	if len(ctr.Command) == 0 {
		if declaresForwarding {
			return launchCheck{argsForwarded: true}, nil
		}
		return launchCheck{}, fmt.Errorf("container %q has args but no command, so its image ENTRYPOINT is "+
			"not visible to this webhook. Set command to the engine executable and leave its arguments in "+
			"args, or set annotation %q to %q only if the image ENTRYPOINT forwards appended arguments to "+
			"the engine", ctr.Name, KVCacheLaunchArgsForwardedAnnotationKey, "true")
	}

	argv, insideOneString := shellLaunchArgv(slices.Concat(ctr.Command, ctr.Args))
	if err := checkShellWrapper(ctr, argv, insideOneString, declaresForwarding); err != nil {
		return launchCheck{}, err
	}
	if insideOneString {
		return launchCheck{argsForwarded: declaresForwarding}, nil
	}
	if len(argv) == 0 {
		return launchCheck{}, fmt.Errorf("container %q runs the launcher %q with no program after "+
			"it, so its command line starts no engine and whatever this webhook appends to args "+
			"would be taken by that launcher as the command to run - the connector flag itself "+
			"would be executed as a program. Put the engine executable and its arguments after the "+
			"launcher, or in command and args directly", ctr.Name, path.Base(ctr.Command[0]))
	}

	program := path.Base(argv[0])
	if resolvedEngine, recognised := launchEngine(argv); recognised {
		if !launchMatchesEngine(resolvedEngine, engine) {
			return launchCheck{}, fmt.Errorf("container %q resolves to the %q engine, but the selected cache "+
				"runtime is %q. The injected configuration must match the engine that starts; correct the "+
				"engine or manufacturer annotation, or launch that engine directly", ctr.Name, resolvedEngine, engine)
		}
		return launchCheck{program: program}, nil
	}
	if declaresForwarding {
		return launchCheck{program: program, argsForwarded: true}, nil
	}
	return launchCheck{}, fmt.Errorf("container %q resolves to %q, which this webhook cannot identify "+
		"as a supported engine launch. Launch the engine directly - put its executable in command and "+
		"its arguments in args - or set annotation %q to %q only if this launcher forwards appended "+
		"arguments to the engine", ctr.Name, program, KVCacheLaunchArgsForwardedAnnotationKey, "true")
}

func declaresLaunchArgsForwarding(annotations map[string]string) (bool, error) {
	value, declared := annotations[KVCacheLaunchArgsForwardedAnnotationKey]
	if !declared {
		return false, nil
	}
	if value != "true" {
		return false, fmt.Errorf("annotation %q must be %q when declared: it is an author statement "+
			"that the launch forwards appended arguments to the engine", KVCacheLaunchArgsForwardedAnnotationKey, "true")
	}
	return true, nil
}

func launchEngine(argv []string) (inject.Engine, bool) {
	switch path.Base(argv[0]) {
	case "vllm":
		return inject.EngineVLLM, true
	case "python3":
		if len(argv) >= 3 && argv[1] == "-m" && argv[2] == "sglang.launch_server" {
			return inject.EngineSGLang, true
		}
		return "", false
	default:
		return "", false
	}
}

func launchMatchesEngine(resolvedEngine, declaredEngine inject.Engine) bool {
	if resolvedEngine == inject.EngineVLLM {
		return declaredEngine == inject.EngineVLLM || declaredEngine == inject.EngineVLLMAscend
	}
	return resolvedEngine == declaredEngine
}

// shellNames 是将 "-c" 解释为“下一个参数为完整脚本”的解释器名称列表。
var shellNames = []string{"sh", "bash", "dash", "ash", "zsh", "ksh"}

// checkShellWrapper 拒绝那些无法证明能将追加参数透传给引擎的容器。它采用 fail-closed 策略：
// “无法判断”意味着附带说明信息予以拒绝，而非放行。
//
// 以下三种形式会被拒绝，且拒绝信息会指明具体属于哪一种，因为读者的后续处理方式各不相同：
//
//	shell 的 -c              追加的标志会变成 $0 和 $1，永远不会传给引擎
//	单 token 内的命令        `env -S "sh -c ..."` —— 命令行上没有可供检查的内容
//	以脚本作为入口程序       脚本是否透传 "$@" 取决于镜像内部文件，而非命令行
//
// 第三种形式涵盖了同一启动方式的两种写法。`./run.sh` 和 `sh /app/run.sh` 都会把相同的追加
// 标志交给同一个无法读取的文件，因此拒绝前者而放行后者只会让人改用能绕过检查的写法。
//
// 下方的启动器枚举并不完备。该解析器未收录的启动器会解析为其自身，随后引擎检查会将其拒绝，
// 除非作者明确声明它会透传追加参数。这样一来，未收录的启动器会触发显式拒绝，而不是被静默放行。
//
// 在 `sh -c <script>` 之后，其余所有参数都是位置参数：标志会成为 $0，其值成为 $1，而 shell
// 不会将它们传给其执行的命令。这是实测结果而非臆测——`sh -c 'echo "$0 [$*]"' --kv-config VAL`
// 会输出 `--kv-config [VAL]`。此时 Pod 仍会启动，标记显示已注入，投射文件也已挂载，但连接器
// 根本不会被启用：这种静默失效在外在表现上就是缓存无法命中。
//
// 检查针对的是拼接后的 argv，这正是其正确性的核心所在。Kubernetes 会按顺序在 command 后
// 追加 args，因此以下两种写法会产生完全相同的进程：
//
//	command: ["/bin/sh", "-c"]  args: ["<script>"]
//	command: ["/bin/sh"]        args: ["-c", "<script>"]
//
// 仅针对 `command` 的检查会捕获第一种写法却漏掉第二种。单独的某一个字段都无法决定最终行为；
// 真正决定行为的是两者的拼接结果。
//
// 两个条件缺一不可，其中第一个条件用于将拒绝范围严格限定在必要情形内。`-c` 对普通程序而言是
// 常见选项——`command: ["myapp", "-c", "config.yaml"]` 很普遍且在此并无问题。要求 argv[0]
// 必须为 shell，能将拒绝范围精确收敛到行为受 POSIX 保证的情形，而非依赖“未见反例”来推断。
func checkShellWrapper(ctr *core.Container, argv []string, insideOneString, declaresForwarding bool) error {
	if insideOneString {
		if declaresForwarding {
			return nil
		}
		return fmt.Errorf("container %q hides its command line inside a single argument, so this "+
			"webhook cannot tell what would run or whether appending to args would reach it. "+
			"Launch the engine directly - put its executable in command and its arguments in args - "+
			"or add the connector flag inside that argument yourself", ctr.Name)
	}
	if len(argv) == 0 {
		return nil
	}
	program := path.Base(argv[0])
	if !slices.Contains(shellNames, program) {
		// 启动器可能是任意脚本，其透明性取决于脚本正文而非命令行。任何启动器枚举都无法覆盖这一点：
		// 这并非解析器尚未收录的启动器，而是准入阶段完全无法读取其行为的启动器。
		return scriptByPath(ctr.Name, program, declaresForwarding)
	}
	// shell 在遇到第一个操作数时会停止解析选项，此处的扫描逻辑也是如此：在
	// `sh /app/run.sh -c config` 中，-c 属于脚本而非 shell，因此不属于此处要拒绝的形式。
	// 扫描停止的位置即为 shell 的命令文件，该文件将接受与直接执行脚本相同的检查。
	for i := 1; i < len(argv); i++ {
		arg := argv[i]
		switch {
		case arg == "--":
			// 选项扫描结束且未遇到 -c；其后紧跟的即为命令文件。
			if i+1 < len(argv) {
				return scriptByPath(ctr.Name, path.Base(argv[i+1]), declaresForwarding)
			}

			return nil
		case len(arg) < 2 || (arg[0] != '-' && arg[0] != '+'):
			// 即命令文件本身。单独的 "-" 也是一个操作数，而非选项组合。
			return scriptByPath(ctr.Name, path.Base(arg), declaresForwarding)
		case isShellCommandFlag(arg):
			// 命中拒绝逻辑。必须在下方的操作数规则之前检查，因为一个选项组合可能同时具备两种属性——
			// `-co` 既是命令模式，其中的 -o 依然接收一个操作数。
		case slices.Contains(shellOperandLong, arg):
			// 一种长选项，其操作数为下一个 token，且该操作数是启动配置文件而非命令文件：
			// `bash --rcfile /tmp/x -c '<script>'` 依然会解析后面的 -c。跳过该操作数才能让扫描继续；
			// 若在此停止，则会错误放行此类启动方式。
			i++
			continue
		case arg[len(arg)-1] == 'o' || arg[len(arg)-1] == 'O':
			// -o 和 bash 的 -O 会接收一个选项名作为操作数，以它们结尾的选项组合也是如此（例如 -euo pipefail）。
			// 该操作数绝非命令文件，因此扫描必须将其跳过，而不是在此停止。
			i++
			continue
		default:
			continue
		}
		return fmt.Errorf("container %q runs %q through a shell's %q, and anything this webhook "+
			"appends to args would become the shell's positional parameters ($0, $1) rather than "+
			"reaching the engine. The Pod would start, record itself as injected, and never enable "+
			"the connector. Launch the engine directly - put its executable in command and its "+
			"arguments in args - or add the connector flag to the script yourself",
			ctr.Name, program, arg)
	}
	return nil
}

// scriptByPath 拒绝以 shell 脚本作为入口程序的启动方式，并放行其他情况。
//
// 追加参数能否到达引擎取决于脚本是否透传 "$@"，而这属于镜像内部文件的内容，而非命令行上的
// token。因此此处直接拒绝而不做猜测，这正是 fail-closed 的意义所在：将“无法判断”从静默放行
// 转为显式报错信息。其代价是确实会透传 "$@" 的脚本也会被拒绝，而报错信息给出的补救方案与
// shell 情形相同。
//
// 两种写法都会调用到此处，因为两者的不确定性完全一致：`./run.sh` 直接运行脚本，`sh /app/run.sh`
// 将同一文件交给解释器执行，无论哪种方式，追加的标志都会变成该脚本的 $1。
//
// .sh 后缀只是一种约定而非绝对保证，因此仅以 .sh 结尾的程序也会被拒绝。无后缀的脚本会通过
// 此辅助函数，随后启动检查会因其属于未识别程序而将其拒绝，除非作者声明支持透传。准入阶段无法
// 读取文件内容，而后缀名是命令行上唯一与脚本相关的信号。
func scriptByPath(ctrName, program string, declaresForwarding bool) error {
	if !strings.HasSuffix(program, ".sh") {
		return nil
	}
	if declaresForwarding {
		return nil
	}

	return fmt.Errorf("container %q runs the script %q, and whether anything this webhook "+
		"appends to args reaches the engine depends on whether that script forwards its "+
		"arguments - which admission cannot read, because it is inside the image. Launch "+
		"the engine directly - put its executable in command and its arguments in args - "+
		"or add the connector flag inside the script yourself", ctrName, program)
}

// launcherGrammar 仅描述启动器选项语法中在此处关心的部分：哪些选项会消耗紧随其后的 token
// 作为操作数。除此之外的其他内容均为普通标志或目标命令。
type launcherGrammar struct {
	// operandShort 保存以单个短横线开头、且以独立 token 作为操作数的短选项字母。
	operandShort string
	// operandLong 保存接收独立操作数的长选项。--opt=value 形式无需收录，因为其值已包含在同一个
	// token 内。
	operandLong []string
	// opaqueShort 保存以单个短横线开头、且其操作数为整条命令行的短选项字母，而非普通参数值。
	// 下方启动器中只有 env(1) 的 -S 属于此类：它会将操作数拆分为程序及其参数。
	//
	// 将其与 operandShort 分开是因为跳过此类选项后已无内容可检——目标程序就隐藏在被跳过的 token
	// 内部。因此，一旦遇到此类选项，无论其后还跟有什么，解析都会以“无法判断”结束，而非解析出某个程序。
	opaqueShort string
	// opaqueLong 保存上述同类选项的长选项写法。与 operandLong 不同，它同样涵盖 --opt=value
	// 形式，因为无论哪种写法，命令行都包含在单个 token 内部。
	opaqueLong []string
	// positionalOperands 表示在启动器自身选项与目标命令之间，有多少个属于启动器自身的位置操作数，
	// 例如 `timeout DURATION COMMAND` 有 1 个。0 表示第一个非选项 token 即为目标命令，这也是
	// 其余所有条目的形式。
	//
	// 带有位置操作数的启动器此前正因这个原因而无法解析。只要明确该计数即可解析，例如 `gosu USER` /
	// `chroot DIR` 的计数均为 1——但它们并未列在下方：这一缺口属于一个待决的评审讨论，需由人工
	// 决定是对其进行解析还是收窄文档承诺的范围，在此悄然补齐相当于通过改代码代替人工决策来回答该问题。
	positionalOperands int
}

// shellLaunchers 会 exec 其自身参数之后的内容，因此位于其后的 shell 最终仍会成为读取 -c
// 的进程。它们的语法是“选项在前、命令在后”，这使其具备可解析性；而先接收位置操作数的启动器
// 则不然（参见 shellLaunchArgv）。
//
// 本映射、各启动器的选项字母以及下方的 shellOperandLong 构成三个枚举集合，且三者均非完备集合：
// 哪些程序属于启动器、启动器的哪些选项会消耗一个 token、以及 shell 的哪些选项会消耗一个 token。
// 此处缺失的条目会解析为启动器自身，除非作者声明该程序会透传追加参数，否则引擎检查会将其拒绝。
//
// 每个条目都严格对照对应程序自身的选项列表编写，而非做泛化处理，因为条目有误会在两个方向上
// 掩盖 shell：若预设的操作数过少，`env -C /tmp sh -c ...` 会把 /tmp 误报为程序；若预设过多，
// `env -i sh -c ...` 则会把 -c 误报为程序。
var shellLaunchers = map[string]launcherGrammar{
	// GNU coreutils env(1)：-u NAME、-C DIR、-S STRING、-a ARG 及其对应的四种长选项形式，
	// getopt_long 也接受将参数拆分为独立 token。它的三个 --*-signal 选项接收可选操作数
	// （`[=SIG]`），因此分开写时不会消耗后续 token，未列在此处是正确的。
	//
	// -S 是个特例，位于 opaque 字段中：它的操作数是一条命令行而非普通值，因此不能简单跳过。
	"env": {
		operandShort: "uCa",
		operandLong:  []string{"--unset", "--chdir", "--argv0"},
		opaqueShort:  "S",
		opaqueLong:   []string{"--split-string"},
	},
	// tini(1) 使用标准 getopt 解析参数，因此只有 -p SIGNAL 和 -e EXIT_CODE 会消耗后续 token。
	"tini": {operandShort: "pe"},
	// dumb-init(1)：-r/--rewrite 接收一个信号重映射参数。
	"dumb-init": {operandShort: "r", operandLong: []string{"--rewrite"}},
	// 多调用二进制文件。它们的第一个操作数指定要运行的 applet，因此 `busybox sh -c ...` 会运行
	// 此检查必须识别出的 shell。该操作数看起来像 gosu 的 user 或 chroot 的目录那样属于位置参数，
	// 但它本身就是目标程序而非传给程序的参数，这正是此类程序可解析而那两个不可解析的原因。它们
	// 的任何选项都不消耗后续 token。
	"busybox": {},
	"toybox":  {},
	// timeout(1) 的用法为 `timeout [OPTION] DURATION COMMAND [ARG]...`，因此在其选项和命令之间
	// 存在一个属于其自身的操作数。若无此计数，`timeout 30 sh -c 'vllm ...'` 会解析为 `timeout`
	// ——并非 shell——从而被放行，而追加的标志会成为 shell 的 $0。它是容器命令行中常见的包装器，
	// 且与 gosu/chroot 不同，它并不在此解析器已知会刻意放行的形式列表中。
	"timeout": {
		operandShort:       "sk",
		operandLong:        []string{"--signal", "--kill-after"},
		positionalOperands: 1,
	},
}

// shellOperandLong 列出了以“下一个 token”作为操作数的 shell 长选项。在上述指定的 shell 中，
// 只有 bash 的 --rcfile 及其同义选项 --init-file 属于此类；这些 shell 接收的其他长选项均为
// 普通标志。bash 自行解析这些选项而不使用 getopt_long，且不接受 --opt=value 形式，因此没有
// 内联写法需要剥离——即便有人写成那样，它也会作为未识别标志跳过，扫描依然能到达其后的 -c。
var shellOperandLong = []string{"--rcfile", "--init-file"}

// shellLaunchArgv 解析透明的启动器前缀，使下方的检查看到的是实际运行的程序，而非命令行上的
// 第一个词。
//
// 引入该函数是因为仅检查 argv[0] 已被绕过三次：-c 从 command 移入 args、属于脚本而非 shell
// 的 -c，以及 `env sh -c`。第三种情况并非需要追加第四个特例——它说明命令行的第一个词并不总是
// 目标程序。因此这里选择解析前缀，而非针对特定前缀做穷举拦截。
//
// 适用范围说明（此处存在已知边界）：有一类形式不会被解析，即启动器自身第一个位置操作数属于
// 参数而非目标程序的情况，例如 `gosu USER sh -c` 或 `chroot DIR sh -c`。此处没有任何依据
// 将该操作数与命令区分开来。它会保留为 argv[0]，除非作者声明透传，否则会被引擎检查拒绝。
// 多调用二进制文件的 applet 名称不属于此类情况；参见下方 busybox 的说明。
//
// insideOneString 表示启动器接收到的命令行被包裹在单个 token 中，此解析器无法深入查看，
// 即 `env -S "sh -c ..."` 及其各类写法。这种情况下解析出的 resolved argv 为 nil；若没有
// 第二个返回值，它将与“无程序可运行”无法区分，而这两种情况应当得到不同反馈：无程序可运行会
// 因未声明程序而被拒绝，隐藏在字符串中的命令则因不可读而被拒绝——且只有后者允许作者通过透传
// 声明来承担责任。哪个 token 属于不透明内容由启动器自身的语法决定，而非取决于 argv 恰好在
// 何处结束——否则 `env -S "sh -c ..." trailing` 会解析为 `trailing` 并被放行，而隐藏的
// shell 却会实际执行。
func shellLaunchArgv(argv []string) (resolved []string, insideOneString bool) {
	for len(argv) > 0 {
		grammar, ok := shellLaunchers[path.Base(argv[0])]
		if !ok {
			return argv, false
		}

		var opaque bool
		argv, opaque = stripLauncherOperands(argv[1:], grammar)
		if opaque {
			return nil, true
		}
	}

	return argv, false
}

// stripLauncherOperands 剥离单个启动器自身的参数——NAME=value 赋值、启动器标志以及其选项
// 消耗的操作数——并返回从即将 exec 的目标命令开始的 argv。
//
// opaque 表示启动器被要求执行一条包含在单个 token 内部的命令行，在此处的启动器中特指
// `env -S "sh -c ..."`。此时 argv 上不存在可供检查的程序，因此返回该标记而非将其归并为空
// argv：调用方对两者都会拒绝，但会给出不同的报错信息，且只有前者允许作者通过透传声明放行。
//
// 空结果并不等于“无法判断”，在此将两者混为一谈会丢失调用方赖以区分拒绝原因的关键差异。
// 空结果表示容器未声明任何程序；opaque 表示程序位于本解析器无法读取的 token 内部。读者的
// 后续操作不同，因此提示信息也必须不同。
//
// 空结果也并非最常见的情况，这一点值得说明，因为两者的表现形式很相似。本 operator 生成的
// runner 镜像系列中，有 56/60 将 `tini --` 作为 entrypoint，并在 ARGS 中携带真正的命令。
// Kubernetes 在此之前会拼接 command 和 args，因此这些情形会解析到真正的命令而非空，依然
// 保持可追加参数的状态。解析结果为空的 argv，仅指整个容器声明里只有启动器本身的情况。
//
// 缺少操作数的选项——例如以 `env -u` 作为最后一个 token——同样解析为空，原因相同：env
// 会自行报错失败，其中并不存在可供 shell 藏身的命令行。因此它按空结果而非隐藏命令被拒绝，
// 这正是第二个返回值的用途所在。此前将其与 -S 混为一谈时，曾用描述命令的报错信息将其拒绝。
// line it does not have.
//
// 空的不透明操作数同样不属于不透明情形，此处由选项的值而非选项本身决定——这是本解析器中唯一
// 如此处理的地方。空字符串不会拆出任何词，因此启动器会直接运行其后跟随的内容，解析必须继续，
// 而不能在一个并不存在的命令行处停下。在 GNU coreutils 9.11 和 BSD env 上的实测结果一致：
// `env -S "" echo hi` 输出 hi，`env --split-string= echo hi` 输出 hi，而后面不带任何内容的
// `env -S ""` 不会启动任何程序——它会打印环境变量并以 0 退出。
//
// 跳过它而非报告 opaque 决定了三种情况的走向，其中原本只有第一种会报错：`env -S ""` 解析为
// 空并因未声明程序而被拒绝，此时透传声明不再能将其放行；`env -S "" vllm serve` 解析为 vllm
// 并被放行，而此前报告 opaque 会误拒一个正常运行的启动命令；`env -S "" sh -c` 则会触达
// shell 拒绝逻辑，精确指出真正吞掉追加标志的元凶。
func stripLauncherOperands(argv []string, grammar launcherGrammar) (resolved []string, opaque bool) {
	// 属于启动器自身、位于命令之前的位置操作数，遇到时即消耗掉。未声明此类操作数的启动器行为与
	// 此前完全一致：第一个非选项 token 即为目标命令。
	positional := grammar.positionalOperands
	for len(argv) > 0 {
		arg := argv[0]
		takesNext := false
		switch {
		case arg == "--":
			return argv[1:], false
		case strings.HasPrefix(arg, "--"):
			name, value, inline := strings.Cut(arg, "=")
			if slices.Contains(grammar.opaqueLong, name) {
				if inline && value == "" {
					argv = argv[1:]

					continue
				}
				if !inline && len(argv) > 1 && argv[1] == "" {
					argv = argv[2:]

					continue
				}
				return nil, inline || len(argv) > 1
			}
			takesNext = !inline && slices.Contains(grammar.operandLong, name)
		case arg != "" && arg[0] == '-':
			// 单独的 "-" 不包含任何字母，因此不消耗后续 token；env(1) 将其视为 -i。
			separated, isOpaque := bundleOperand(arg[1:], grammar)
			if isOpaque {
				if separated && len(argv) > 1 && argv[1] == "" {
					argv = argv[2:]

					continue
				}
				// 不透明选项只有在确实拥有操作数时才视为不透明。内联写法——`-Ssh -c vllm` 或
				// `--split-string=...`——将命令行直接携带在当前 token 内部；分离写法则将其携带在下一个
				// token 中（如果存在的话）。若缺少操作数，则根本不存在命令行，因此这属于空结果而非隐藏命令，
				// 与缺少操作数的普通选项处理方式完全一致。
				return nil, !separated || len(argv) > 1
			}
			takesNext = separated
		case strings.Contains(arg, "="):
			// NAME=value 形式的环境变量赋值，env(1) 允许将其放在命令之前。
		case positional > 0:
			// 属于启动器而非命令的操作数：例如 timeout 的 DURATION。在 default 分支之前将其消耗掉，
			// 以便其后的 token 仍能作为程序接受检查。
			positional--
		default:
			return argv, false
		}
		if takesNext {
			if len(argv) < 2 {
				// 操作数缺失，因此此处既没有命令，也没有可能藏有命令的 token。同上，按空结果处理。
				return nil, false
			}
			argv = argv[2:]

			continue
		}
		argv = argv[1:]
	}

	return argv, false
}

// bundleOperand 对短选项组合进行分类：判断它是否需要从后续 token 读取操作数，以及该操作数
// 是否为本解析器无法查看内部的命令行。
//
// 第一个接收操作数的字母起决定性作用，因为若组合中还有剩余字符，该字母会将其直接消耗——例如
// `-uNAME` 会取消设置 NAME——因此只有以此类字母结尾的选项组合才会读取下一个 token。这与
// shell 扫描对 -o 的规则一致，任一方向出错都会掩盖 shell。
//
// 对于不透明字母同样会报告 takesNext，调用方需要该信息而非仅仅一个布尔标志：`-Ssh -c vllm`
// 将命令行携带在自身 token 内，`-S 'sh -c vllm'` 携带在下一个 token 内，而后面没有任何
// 内容的 `-S` 则完全不携带命令行——这是与前两者截然不同的结果。
func bundleOperand(bundle string, grammar launcherGrammar) (takesNext, opaque bool) {
	for i, letter := range bundle {
		last := i == len(bundle)-1
		switch {
		case strings.ContainsRune(grammar.opaqueShort, letter):
			return last, true
		case strings.ContainsRune(grammar.operandShort, letter):
			return last, false
		}
	}

	return false, false
}

// isShellCommandFlag 报告 shell 选项组合中是否包含 "c"，涵盖 -c 以及包装脚本可能使用的组合
// 形式，例如 -lc 或 -euc。长选项绝不属于此类。
func isShellCommandFlag(arg string) bool {
	if !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "--") || len(arg) < 2 {
		return false
	}
	return strings.ContainsRune(arg[1:], 'c')
}

// hasFlag 报告标志是否出现在参数列表中，包括 --flag=value 形式。
// containerFlag 报告容器是否以引擎自身解析器能够识别为该标志的任意拼写传入了 flag，并描述
// 触发匹配的条目。除 args 外还会扫描 command，因为用户可能将标志写在任意一处。
//
// vLLM-Ascend 是由 vLLM 自身解析器拉起的插件，因此它识别的是 vLLM 的标志拼写。
func containerFlag(ctr *core.Container, engine inject.Engine, flag string) (string, bool) {
	parser := string(engine)
	switch engine {
	case inject.EngineVLLM, inject.EngineVLLMAscend:
		parser = workercore.ModelDeploymentEngineVLLM
	case inject.EngineSGLang:
		parser = workercore.ModelDeploymentEngineSGLang
	}

	for _, arg := range slices.Concat(ctr.Args, ctr.Command) {
		if _, found := workerctrl.ModelDeploymentResolveArg(parser, arg, []string{flag}); !found {
			continue
		}
		if name := workerctrl.ModelDeploymentArgName(arg); name != flag {
			return fmt.Sprintf("%s, which engine %q reads as %s", name, engine, flag), true
		}
		return flag, true
	}

	return "", false
}

// containerNames 列出容器名称，以便在拒绝时为读者提供可操作的定位信息。
func containerNames(ctrs []core.Container) []string {
	names := make([]string, 0, len(ctrs))
	for i := range ctrs {
		names = append(names, ctrs[i].Name)
	}
	return names
}

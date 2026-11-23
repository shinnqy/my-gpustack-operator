package preflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// DefaultHostRoot 是容器中默认挂载宿主机根文件系统的位置。
const DefaultHostRoot = "/host"

// chrootPath 使用绝对路径。非登录 shell 的 PATH 可能不含 /usr/sbin。
const chrootPath = "/usr/sbin/chroot"

// hostRootMarkers 是宿主机根目录应具备的标志路径，一起检查可避免把误建的普通目录当成宿主机。
var hostRootMarkers = []string{"etc", "proc", "usr"}

// containerdSockets 按顺序列出常见 socket，包含 k3s 和 RKE2 的路径。
var containerdSockets = []string{
	"/run/containerd/containerd.sock",
	"/run/k3s/containerd/containerd.sock",
	"/var/run/containerd/containerd.sock",
}

// hostRuntimes 按优先顺序列出探针可使用的宿主机容器工具。
var hostRuntimes = []string{"docker", "nerdctl", "ctr"}

// containerdRuntimes 中的工具需要指定 socket 和命名空间，Docker 使用自己的守护进程配置。
var containerdRuntimes = []string{"nerdctl", "ctr"}

// errNoHostRuntime 表示当前环境无法驱动容器运行时。相关步骤仍能输出可复现的命令。
var errNoHostRuntime = errors.New("no container runtime found on the host")

// hostExec 借助 chroot 调用宿主机安装的工具和动态库。探针镜像无需内置每种容器运行时和厂商 CLI。
type hostExec struct {
	root string
	// run 负责实际执行命令，参数构造与读取结果共用这一个入口。
	run func(ctx context.Context, name string, args ...string) ([]byte, error)
}

func newHostExec(root string) *hostExec {
	return &hostExec{root: root, run: runCommand}
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err == nil {
		return out, nil
	}
	// 命令失败时附上实际输出，没有输出时仅保留原错误，避免产生空的错误详情。
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		return out, fmt.Errorf("%w: %s", err, msg)
	}
	return out, err
}

// Validate 检查根目录和宿主机标志路径是否可访问，并指出缺少哪一项。未挂载的空目录也可能存在。
func (h *hostExec) Validate() error {
	if h.root == "" {
		return errors.New("no host root configured")
	}

	info, err := os.Stat(h.root)
	if err != nil {
		return fmt.Errorf("host root %q is not readable: %w", h.root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("host root %q is not a directory", h.root)
	}

	for _, marker := range hostRootMarkers {
		if _, err := os.Stat(filepath.Join(h.root, marker)); err != nil {
			return fmt.Errorf("host root %q carries no %q, so it is not a host root: "+
				"mount the host's / into the container at this path", h.root, marker)
		}
	}
	return nil
}

// Command 构造从宿主机执行指定命令的参数。实际执行和报告展示使用同一组参数。
func (h *hostExec) Command(name string, args ...string) []string {
	return append([]string{chrootPath, h.root, name}, args...)
}

// Run 从宿主机执行命令并返回输出。
func (h *hostExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	argv := h.Command(name, args...)
	return h.run(ctx, argv[0], argv[1:]...)
}

// Has 判断宿主机上是否有指定可执行文件。传给 shell 的名字单独转义。
func (h *hostExec) Has(ctx context.Context, name string) bool {
	_, err := h.Run(ctx, "sh", "-c", "command -v "+shellQuote(name))
	return err == nil
}

// hostRuntime 保存识别出的宿主机运行时及其连接方式。
type hostRuntime struct {
	// Name 为宿主机上实际执行的工具名。
	Name string
	// Socket 指定 containerd 守护进程地址，使探针创建和清理针对同一实例。
	Socket string
	// Namespace 是探针容器使用的 containerd 命名空间，隔开普通工作负载。
	Namespace string
	// NetworkWarning 记录容器和宿主机网络命名空间不一致可能造成的镜像拉取问题，已在本地的镜像仍可使用。
	NetworkWarning string
	// NerdctlAbsent 记录选用 ctr 时宿主机没有 nerdctl 的情况，用于准确说明替代命令需要安装什么。
	NerdctlAbsent bool
}

// networkWarning 说明 chroot 后宿主机 DNS 和当前网络命名空间不匹配的原因及处理办法。
const networkWarning = "this container does not share the host's network namespace, so a host CLI " +
	"asked to pull an image will fail to resolve names: the host's /etc/resolv.conf is read " +
	"through the chroot while nothing answers on it here. Re-run with host networking."

// ResolveRuntime 优先尊重显式指定的运行时，否则读取 kubelet 实际使用的 CRI 端点。节点没有给出端点时，
// 再按已知 CLI 顺序查找。
func (h *hostExec) ResolveRuntime(ctx context.Context, want string) (*hostRuntime, error) {
	if want != "" {
		// 即使同名程序存在，也必须是本命令支持的运行时，才能使用相应参数格式。
		if !slices.Contains(hostRuntimes, want) {
			return nil, fmt.Errorf("%w: --runtime %s is not a container runtime this command "+
				"drives -- it drives %s", errNoHostRuntime, want, strings.Join(hostRuntimes, ", "))
		}
		if !h.Has(ctx, want) {
			return nil, fmt.Errorf("%w: --runtime %s is not on the host", errNoHostRuntime, want)
		}
		rt := h.describeRuntime(want, "")
		// 显式选用 ctr 时同样检查 nerdctl 是否存在，后面的命令说明依赖这个事实。
		if want == "ctr" && !h.Has(ctx, "nerdctl") {
			rt.NerdctlAbsent = true
		}
		return rt, nil
	}

	endpoint, found, err := h.kubeletCRIEndpoint()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errNoHostRuntime, err)
	}
	if found {
		name, socket, known := runtimeForEndpoint(endpoint)
		if !known {
			return nil, fmt.Errorf("%w: the kubelet is configured against %s, which is a CRI this "+
				"command carries no client for -- it drives docker and containerd only",
				errNoHostRuntime, endpoint)
		}
		if h.Has(ctx, name) {
			return h.describeRuntime(name, socket), nil
		}
		// containerd 端点已知但没有 nerdctl 时，保留 ctr 和实际 socket 信息，生成命令时再说明限制。
		if name == "nerdctl" && h.Has(ctx, "ctr") {
			rt := h.describeRuntime("ctr", socket)
			rt.NerdctlAbsent = true
			return rt, nil
		}
		return nil, fmt.Errorf("%w: the kubelet is configured against %s, and this host carries no %s "+
			"to drive it with", errNoHostRuntime, endpoint, name)
	}

	for _, name := range hostRuntimes {
		if h.Has(ctx, name) {
			// ctr 在查找顺序中排最后，走到这里时已经确认没有 nerdctl。
			if name == "ctr" {
				rt := h.describeRuntime(name, "")
				rt.NerdctlAbsent = true
				return rt, nil
			}
			return h.describeRuntime(name, ""), nil
		}
	}
	return nil, fmt.Errorf("%w: probed %s", errNoHostRuntime, strings.Join(hostRuntimes, ", "))
}

// kubeletConfigSource 表示宿主机上的一个配置来源。精确路径和 glob 模式二选一，
// 目录来源还需要按 kubelet 的顺序合并配置。
type kubeletConfigSource struct {
	pattern   string
	path      string
	flags     bool
	dropInDir bool
}

// where 返回适合放入诊断消息的来源名称，可以是显式路径或搜索模式。
func (s kubeletConfigSource) where() string {
	if s.path != "" {
		return s.path
	}
	return s.pattern
}

// resolve 列出这个来源在宿主机根目录下对应的位置。
func (s kubeletConfigSource) resolve(root string) ([]string, error) {
	if s.path != "" {
		return []string{filepath.Join(root, s.path)}, nil
	}
	return filepath.Glob(filepath.Join(root, s.pattern))
}

// kubeletConfigSources 是未发现运行中的 kubelet 时，各发行版保存配置的常见位置。
// 按顺序检查单个配置文件和配置片段目录。
var kubeletConfigSources = []kubeletConfigSource{
	{pattern: "var/lib/kubelet/kubeadm-flags.env", flags: true},
	{pattern: "var/lib/kubelet/config.yaml"},
	{pattern: "var/lib/rancher/*/agent/etc/kubelet.conf.d", dropInDir: true},
}

// kubeletNamedSources 从运行中 kubelet 的参数取出其实际使用的文件和目录。优先读取补充配置，再看主配置文件。
func kubeletNamedSources(argv []string) []kubeletConfigSource {
	var sources []kubeletConfigSource
	if dir, ok := kubeletFlagValue(argv, kubeletConfigDirFlag); ok {
		sources = append(sources, kubeletConfigSource{path: dir, dropInDir: true})
	}
	if file, ok := kubeletFlagValue(argv, kubeletConfigFlag); ok {
		sources = append(sources, kubeletConfigSource{path: file})
	}
	return sources
}

const (
	// kubeletExecutable 用进程命令第一个参数的文件名识别 kubelet。有些发行版把它嵌入其他进程，
	// 此时可能找不到独立的命令行。
	kubeletExecutable = "kubelet"
	// kubeletConfigFlag 和 kubeletConfigDirFlag 分别定位主配置文件和叠加配置目录。
	kubeletConfigFlag    = "--config"
	kubeletConfigDirFlag = "--config-dir"
	// hostProcDir 是相对于宿主机根目录的进程表路径。
	hostProcDir = "proc"
	// kubeletCommandLinePlace 是报告中描述 kubelet 命令行来源的名称。
	kubeletCommandLinePlace = "the kubelet's own command line"
)

// hostKubeletCommandLine 扫描宿主机进程表，返回正在运行的 kubelet 参数，并区分没有这个进程和进程表无法读取。
func hostKubeletCommandLine(root string) (argv []string, running bool, err error) {
	procDir := filepath.Join(root, hostProcDir)
	// 找不到 proc 目录或其中没有进程时，应报告宿主机进程表不可见，避免误用默认配置路径。
	notVisible := func(cause error) error {
		return fmt.Errorf("the host's process table is not visible at %s, so the kubelet this "+
			"node runs cannot be identified: mount the host's root with the mounts under it, so "+
			"that the host's own /proc comes with it: %w", procDir, cause)
	}

	entries, err := os.ReadDir(procDir)
	if err != nil {
		return nil, false, notVisible(err)
	}

	var (
		processes int
		lowest    int
		found     []string
	)
	for _, entry := range entries {
		pid, pidErr := strconv.Atoi(entry.Name())
		if pidErr != nil || pid <= 0 {
			continue
		}
		processes++
		if found != nil && pid > lowest {
			continue
		}
		body, readErr := os.ReadFile(filepath.Join(procDir, entry.Name(), "cmdline"))
		if readErr != nil {
			// 在枚举后已退出的进程可以跳过。其他读取错误需按是否影响结论处理。
			if errors.Is(readErr, fs.ErrNotExist) {
				continue
			}
			return nil, false, notVisible(readErr)
		}
		words := commandLineWords(body)
		if len(words) == 0 || filepath.Base(words[0]) != kubeletExecutable {
			continue
		}
		found, lowest = words, pid
	}
	if processes == 0 {
		return nil, false, notVisible(errors.New("it lists no process"))
	}
	return found, found != nil, nil
}

// commandLineWords 按内核的 NUL 分隔方式拆开参数，并移除末尾分隔符产生的空项。
func commandLineWords(body []byte) []string {
	var words []string
	for word := range strings.SplitSeq(string(body), "\x00") {
		if word != "" {
			words = append(words, word)
		}
	}
	return words
}

// kubeletFlagValue 同时支持 --flag=value 和 --flag value。重复选项按最后一次赋值处理。
func kubeletFlagValue(argv []string, flag string) (string, bool) {
	var (
		value string
		found bool
	)
	for i, arg := range argv {
		var token string
		switch {
		case strings.HasPrefix(arg, flag+"="):
			token = strings.TrimPrefix(arg, flag+"=")
		case arg == flag && i+1 < len(argv):
			token = argv[i+1]
		default:
			continue
		}
		if token = kubeletSettingValue(token); token != "" {
			value, found = token, true
		}
	}
	return value, found
}

// kubeletDropInExtension 是 kubelet 从配置片段目录加载的文件后缀。
const kubeletDropInExtension = ".conf"

// kubeletDropInFiles 按 kubelet 的目录遍历和合并顺序收集 .conf 文件，保证覆盖顺序一致。
func kubeletDropInFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != kubeletDropInExtension {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// kubeletSetting 保存一个设置的命令行名称与 YAML 字段名，使读取不同来源时共用同一份定义。
type kubeletSetting struct{ flag, field string }

// key 根据当前来源返回相应的字段拼写。
func (s kubeletSetting) key(flags bool) string {
	if flags {
		return s.flag
	}
	return s.field
}

// name 去掉命令行形式末尾的分隔符，便于匹配独立的参数名。
func (s kubeletSetting) name() string {
	return strings.TrimSuffix(s.flag, "=")
}

var (
	// criEndpointSetting 指定 kubelet 使用的容器运行时端点。
	criEndpointSetting = kubeletSetting{
		flag:  "--container-runtime-endpoint=",
		field: "containerRuntimeEndpoint:",
	}
	// topologyPolicySetting 指定 kubelet 对需要 NUMA 亲和设备的容器采用的拓扑策略。
	topologyPolicySetting = kubeletSetting{
		flag:  "--topology-manager-policy=",
		field: "topologyManagerPolicy:",
	}
)

// kubeletReading 保存一次配置读取的结果，区分找到值、搜索不完整和多个配置互相冲突。
type kubeletReading struct {
	// Value 和 Found 表示已读到明确赋值。找遍所有来源仍无结果时，Found 为假。
	Value string
	Found bool
	// UnreadablePath 和 UnreadableErr 指出已经定位但无法读取的配置，以免使用其他文件猜测有效值。
	UnreadablePath string
	UnreadableErr  error
	// UnsearchablePattern 和 UnsearchableErr 说明哪个配置源未能完整搜索，包括宿主机进程表不可见的情况。
	UnsearchablePattern string
	UnsearchableErr     error
	// Conflict 记录同一搜索模式下不同配置给出的冲突值，ConflictPattern 指明该模式。
	Conflict        []string
	ConflictPattern string
	// Searched 按访问顺序记录检查过的位置，用于解释为何最终没有找到配置。
	Searched []string
	// CommandLineRead 区分已核对运行中 kubelet 的命令行和仅搜索发行版常见路径这两种情况。
	CommandLineRead bool
}

// readKubeletSetting 按运行中 kubelet 的命令行、叠加配置、主配置的有效优先级读取指定配置，
// 只在无法找到运行的 kubelet 时查发行版默认位置。
func readKubeletSetting(root string, setting kubeletSetting) kubeletReading {
	argv, running, cmdErr := hostKubeletCommandLine(root)
	if cmdErr != nil {
		return kubeletReading{
			UnsearchablePattern: filepath.Join(root, hostProcDir),
			UnsearchableErr:     cmdErr,
		}
	}

	sources := kubeletConfigSources
	searched := make([]string, 0, len(sources)+1)
	if running {
		// kubelet 在合并配置文件之后会再次应用命令行参数，因此显式命令行取值优先。
		if value, ok := kubeletFlagValue(argv, setting.name()); ok {
			return kubeletReading{Value: value, Found: true}
		}
		sources = kubeletNamedSources(argv)
		searched = append(searched, kubeletCommandLinePlace)
	}

	for _, src := range sources {
		searched = append(searched, src.where())

		matches, globErr := src.resolve(root)
		if globErr != nil {
			// 构造出的匹配模式无效时，报告搜索失败，避免把未搜索当成未配置。
			return kubeletReading{
				UnsearchablePattern: filepath.Join(root, src.pattern),
				UnsearchableErr:     globErr,
			}
		}

		// 一个配置目录内的片段依次覆盖。不同目录可能各属于一个独立的部署，要分别求值后检查冲突。
		byUnit := map[string]string{}
		var units []string
		for _, match := range matches {
			files := []string{match}
			if src.dropInDir {
				walked, walkErr := kubeletDropInFiles(match)
				if walkErr != nil {
					// 有目录未能遍历完，整个来源就无法确定。不能把已经读到的一部分作为最终配置。
					return kubeletReading{
						UnsearchablePattern: match,
						UnsearchableErr:     walkErr,
					}
				}
				files = walked
			}
			for _, path := range files {
				body, readErr := os.ReadFile(path)
				if readErr != nil {
					return kubeletReading{UnreadablePath: path, UnreadableErr: readErr}
				}
				value, ok := valueAfter(string(body), setting.key(src.flags))
				if !ok {
					continue
				}
				if _, seen := byUnit[match]; !seen {
					units = append(units, match)
				}
				byUnit[match] = value
			}
		}

		var answers []string
		for _, unit := range units {
			if !slices.Contains(answers, byUnit[unit]) {
				answers = append(answers, byUnit[unit])
			}
		}
		switch len(answers) {
		case 0:
			continue
		case 1:
			return kubeletReading{Value: answers[0], Found: true}
		default:
			return kubeletReading{Conflict: answers, ConflictPattern: src.where()}
		}
	}
	return kubeletReading{Searched: searched, CommandLineRead: running}
}

// kubeletCRIEndpoint 读取 kubelet 使用的 CRI 端点。只有确实找不到设置时才能回退到探测宿主机工具，
// 读取失败和配置冲突都需保留。
func (h *hostExec) kubeletCRIEndpoint() (endpoint string, found bool, err error) {
	reading := readKubeletSetting(h.root, criEndpointSetting)
	switch {
	case reading.UnsearchableErr != nil:
		// 未能开始或完成搜索时给出对应错误，而不是把节点误判为没有配置运行时。
		return "", false, fmt.Errorf(
			"the kubelet configuration search under %s could not run, so which runtime its "+
				"kubelet talks to cannot be established: %w",
			reading.UnsearchablePattern, reading.UnsearchableErr,
		)
	case reading.UnreadableErr != nil:
		// 命中配置但文件读不到时，真实端点仍可能在其中，需要报告该路径。
		return "", false, fmt.Errorf(
			"this host carries a kubelet configuration at %s that could not be read, so "+
				"which runtime its kubelet talks to cannot be established: %w",
			reading.UnreadablePath, reading.UnreadableErr,
		)
	case reading.Conflict != nil:
		return "", false, fmt.Errorf(
			"this host names more than one kubelet CRI endpoint under %s (%s), and only one of them "+
				"belongs to the kubelet that is running: name the runtime with --runtime",
			reading.ConflictPattern, strings.Join(reading.Conflict, ", "),
		)
	default:
		return reading.Value, reading.Found, nil
	}
}

// valueAfter 逐行读取指定字段的有效值，处理引号、注释和重复赋值，避免匹配到别的字段或被注释的配置。
func valueAfter(body, key string) (string, bool) {
	var (
		value string
		found bool
	)
	for line := range strings.SplitSeq(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		i := strings.LastIndex(trimmed, key)
		if i < 0 {
			continue
		}
		fields := strings.Fields(trimmed[i+len(key):])
		if len(fields) == 0 {
			continue
		}
		if token := kubeletSettingValue(fields[0]); token != "" {
			value, found = token, true
		}
	}
	return value, found
}

// kubeletSettingValue 统一移除包围值的引号及端点的 unix:// 前缀，使文件和命令行中的写法可以比较。
func kubeletSettingValue(token string) string {
	return strings.TrimPrefix(strings.Trim(token, `"'`), "unix://")
}

// runtimeForEndpoint 将已知 CRI 端点映射到宿主机 CLI 和 socket。遇到不支持的 CRI 时明确返回未知。
func runtimeForEndpoint(endpoint string) (name, socket string, known bool) {
	switch {
	case strings.Contains(endpoint, "docker"):
		return "docker", "", true
	case strings.Contains(endpoint, "containerd"):
		return "nerdctl", endpoint, true
	}
	return "", "", false
}

// describeRuntime 补齐 containerd 的 socket、命名空间及网络情况。Docker 自行定位守护进程。
func (h *hostExec) describeRuntime(name, socket string) *hostRuntime {
	rt := &hostRuntime{Name: name}

	// 在选择运行时就检查网络命名空间，给镜像拉取失败留下可定位的原因。
	if shared, known := h.networkNamespaceShared(); known && !shared {
		rt.NetworkWarning = networkWarning
	}

	if !slices.Contains(containerdRuntimes, name) {
		return rt
	}

	rt.Namespace = containerdNamespace
	if socket != "" {
		rt.Socket = socket
		return rt
	}
	for _, sock := range containerdSockets {
		if _, err := os.Stat(filepath.Join(h.root, sock)); err == nil {
			rt.Socket = sock
			break
		}
	}
	return rt
}

// containerdNamespace 是预检容器的专用命名空间，便于在正确的范围内发现和清理容器。
const containerdNamespace = "gpustack-preflight"

// networkNamespaceShared 比较当前进程与宿主机网络命名空间，同时返回是否能确定结果。
// chroot 本身不会切换网络命名空间。
func (h *hostExec) networkNamespaceShared() (shared, known bool) {
	ours, err := os.Readlink("/proc/self/ns/net")
	if err != nil {
		return false, false
	}
	theirs, err := os.Readlink(filepath.Join(h.root, "proc/1/ns/net"))
	if err != nil {
		return false, false
	}
	return ours == theirs, true
}

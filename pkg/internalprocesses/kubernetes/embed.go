package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/kubeconfig"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/netx"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

var embeddedKubeConfigPath = filepath.Join(clientcmd.RecommendedConfigDir, "k3s.yaml")

// Embedded 表示由 k3s 驱动的嵌入式 Kubernetes 集群。
type Embedded struct{}

func (Embedded) Start(ctx context.Context) error {
	// 校验是否以特权模式运行。
	if !system.IsRunningAsPrivileged() {
		if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
			return errors.New(`require "securityContext.privileged" feature of gpustack pod`)
		}

		return errors.New(`require "--privileged" flag to run gpustack container`)
	}

	// NB(thxCode): 在 cgroup v2 下，
	// 为了能够多次嵌套运行 gpustack，
	// 我们需要为 k3s 创建专用的 cgroup。
	cgroupRoot := "/"

	// 启用嵌套 cgroup v2，
	// 参考 https://github.com/moby/moby/issues/43093。
	if osx.ExistsFile("/sys/fs/cgroup/cgroup.controllers") {
		// 将进程从根组移动到 /init 组，
		// 否则写入 subtree_control 会因 EBUSY 而失败。
		if !osx.ExistsFile("/sys/fs/cgroup/init/cgroup.procs") {
			err := osx.Copy(
				"/sys/fs/cgroup/cgroup.procs",
				"/sys/fs/cgroup/init/cgroup.procs",
			)
			if err != nil {
				return fmt.Errorf("error moving processes to root init group: %w", err)
			}
		}
		// 启用控制器以允许嵌套。
		err := osx.Copy(
			"/sys/fs/cgroup/cgroup.controllers",
			"/sys/fs/cgroup/cgroup.subtree_control",
			osx.CopyWithModifier(func(data []byte) ([]byte, error) {
				if len(data) == 0 {
					return data, nil
				}
				cs := strings.Split(stringx.FromBytes(&data), " ")
				s := "+" + stringx.Join(" +", cs...)

				return stringx.ToBytes(&s), nil
			}),
		)
		if err != nil {
			return fmt.Errorf("error enabling root group controllers: %w", err)
		}

		// 计算特定的 cgroup 根目录。
		cgroupRoot = "/k3s-" + stringx.SumByFNV64a(system.PrimarySubnet.Get())

		// 创建特定的 cgroup 根目录。
		err = os.Mkdir(path.Join("/sys/fs/cgroup", cgroupRoot), 0o755)
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("error creating cgroup: %w", err)
		}

		// 启用运行 k3s 所需的控制器。
		err = osx.Copy(
			"/sys/fs/cgroup/cgroup.subtree_control",
			path.Join("/sys/fs/cgroup", cgroupRoot, "cgroup.subtree_control"),
			osx.CopyWithModifier(func(data []byte) ([]byte, error) {
				if len(data) == 0 {
					return data, nil
				}
				cs := strings.Split(stringx.FromBytes(&data), " ")
				s := "+" + stringx.Join(" +", cs...)

				return stringx.ToBytes(&s), nil
			}),
		)
		if err != nil {
			return fmt.Errorf("error enabling %q group controllers: %w", cgroupRoot, err)
		}
	}

	var (
		k3sDataDir       = system.SubDataDir("k3s")
		k3sServerDataDir = filepath.Join(k3sDataDir, "server")
	)

	// 将离线镜像链接到 k3s agent 数据目录，
	// 例如 /etc/gpustack/k3s/agent/images -> /var/lib/gpustack/operator/k3s/agent/images。
	if imgConfDir := system.SubConfDir("k3s", "agent", "images"); osx.ExistsDir(imgConfDir) {
		k3sAgentImageDir := filepath.Join(k3sDataDir, "agent", "images")
		err := osx.Link(
			imgConfDir,
			k3sAgentImageDir,
			osx.LinkEvenIfNotFound(false, 0o766),
			osx.LinkInReplace(),
		)
		if err != nil {
			return fmt.Errorf("error link agent image: %w", err)
		}
	}

	// 重置服务端数据库。
	if osx.ExistsDir(filepath.Join(k3sServerDataDir, "db", "etcd")) {
		_ = os.Remove(filepath.Join(k3sServerDataDir, "db", "reset-flag")) // 清理重置标记。

		cmdArgs := []string{
			"server",
			"--cluster-reset",
			"--data-dir=" + k3sDataDir,
		}
		if err := runK3sWith(ctx, cmdArgs); err != nil {
			return err
		}
	}

	cmdArgs := []string{
		"server",
		"--cluster-init",
		"--etcd-disable-snapshots",
		"--disable=traefik,servicelb,metrics-server",
		"--disable-cloud-controller",
		"--disable-network-policy",
		"--disable-helm-controller",
		"--flannel-backend=host-gw",
		"--node-name=local",
		"--data-dir=" + k3sDataDir,
		"--write-kubeconfig=" + embeddedKubeConfigPath,
		"--kubelet-arg=cgroup-root=" + cgroupRoot,
		"--kubelet-arg=system-reserved=cpu=300m,memory=256Mi",
		"--kubelet-arg=kube-reserved=cpu=200m,memory=256Mi",
		"--kube-apiserver-arg=service-node-port-range=30000-30100",
	}

	// NB(thxCode): 对于嵌入式集群，
	// 为避免在 Kubernetes 集群中运行时与默认集群 CIDR 冲突，
	// 需要调整嵌入式集群的 CIDR。
	if v := system.PrimarySubnet.Get(); v != "" {
		sn := netx.MustIPv4FromCIDR(v)
		cls := sn.Next().Next() // 偏移 2 个位置以获取集群 CIDR。
		svc := cls.Next()       // 在集群 CIDR 基础上偏移 1 个位置以获取服务 CIDR。
		cmdArgs = append(cmdArgs,
			"--cluster-cidr="+cls.String(),
			"--service-cidr="+svc.String())

		// NB(thxCode): 目前无法重置已加入 Kubernetes Node 的 PodCIDR，
		// 即便使用 k3s 引入的 `--cluster-reset` 也不行。
		// 嵌入式 Kubernetes 集群仅用于演示目的，
		// 不保证可用于生产环境。
		// 为了能在可变的网络环境中重启，
		// 此处将其与先前的集群 CIDR 进行比较，以决定是否清除数据目录。
		clsFp := filepath.Join(k3sServerDataDir, "cluster.cidr")
		if osx.ExistsFile(clsFp) {
			bs, _ := os.ReadFile(clsFp)
			if string(bs) != cls.String() {
				_ = os.RemoveAll(filepath.Join(k3sServerDataDir, "db", "etcd"))
			}
		}
		_ = os.WriteFile(clsFp, []byte(cls.String()), 0o600)
	}

	return runK3sWith(ctx, cmdArgs)
}

func (Embedded) GetConfig(ctx context.Context) (string, *rest.Config, error) {
	err := waitx.RetryUntilSuccessWithTimeout(ctx, time.Second, 300*time.Second, true,
		func(ctx context.Context) error {
			if osx.ExistsFile(embeddedKubeConfigPath) {
				return nil
			}
			return errors.New("wait for embedded kubeconfig")
		})
	if err != nil {
		return "", nil, err
	}

	cfg, err := kubeconfig.LoadRestConfig(embeddedKubeConfigPath)
	if err != nil {
		return "", nil, err
	}

	return embeddedKubeConfigPath, cfg, err
}

func runK3sWith(ctx context.Context, cmdArgs []string) error {
	const cmdName = "k3s"

	logger := klog.Background().WithName(cmdName)
	logger.Infof("run: %s %s", cmdName, stringx.Join(" ", cmdArgs...))
	cmd := exec.CommandContext(ctx, cmdName, cmdArgs...)
	cmd.SysProcAttr = getSysProcAttr()
	cmd.Stdout = logger.V(5)
	cmd.Stderr = logger.V(5)

	err := cmd.Run()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

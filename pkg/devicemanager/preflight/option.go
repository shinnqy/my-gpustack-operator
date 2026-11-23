package preflight

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/nodefeature"
)

type Options struct {
	// 预检选项。
	NoPCICheck    bool
	Manufacturers []string
	DryRun        bool
	ProbeImage    string
	HostRoot      string
	Runtime       string
}

func NewOptions() *Options {
	return &Options{
		// 默认探测范围和宿主机根目录。
		NoPCICheck:    false,
		Manufacturers: nodefeature.GetKnownAcceleratableManufacturers(),
		HostRoot:      DefaultHostRoot,
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// 注册一次性预检的命令行参数。
	fs.BoolVar(&o.NoPCICheck, "no-pci-check", o.NoPCICheck,
		"disable pci check.")
	fs.StringSliceVar(&o.Manufacturers, "manufacturer", o.Manufacturers,
		"comma separated list of manufacturers to preflight.")
	fs.BoolVar(&o.DryRun, "dry-run", o.DryRun,
		"print the container steps instead of taking them, and write nothing to the host; each "+
			"answer then reports what would have run, complete and runnable as printed.")
	fs.StringVar(&o.ProbeImage, "probe-image", o.ProbeImage,
		"the image the probe containers run, overriding the default derived from the accelerator "+
			"family the detect pass reported; required for a family that has no default.")
	fs.StringVar(&o.HostRoot, "host-root", o.HostRoot,
		"where the host's own root filesystem is mounted into this container; "+
			"the host's container and vendor CLIs are invoked by entering it.")
	fs.StringVar(&o.Runtime, "runtime", o.Runtime,
		"the host container runtime to drive, overriding what was resolved; empty follows the "+
			"kubelet's own CRI endpoint, falling back to probing "+strings.Join(hostRuntimes, ", ")+
			" on a host that names none.")
}

// Validate 校验厂商名和可驱动的容器运行时。拼写错误应当直接反馈给调用方，避免将其当作节点缺少运行时。
func (o *Options) Validate(_ context.Context) error {
	// 逐项检查用户提供的探测参数。
	if len(o.Manufacturers) != 0 {
		knownManufacturers := nodefeature.GetKnownAcceleratableManufacturers()
		if !sets.New[string](knownManufacturers...).HasAll(o.Manufacturers...) {
			return errors.New("--manufacturer: should be a comma separated list of valid manufacturers, " +
				"valid manufacturers: " + strings.Join(knownManufacturers, ","))
		}
	}

	if o.Runtime != "" && !slices.Contains(hostRuntimes, o.Runtime) {
		return errors.New("--runtime: should be a container runtime this command drives, " +
			"valid runtimes: " + strings.Join(hostRuntimes, ","))
	}

	return nil
}

func (o *Options) Complete(_ context.Context) (*Config, error) {
	return &Config{
		NoPCICheck:    o.NoPCICheck,
		Manufacturers: sets.New(o.Manufacturers...),
		DryRun:        o.DryRun,
		ProbeImage:    o.ProbeImage,
		HostRoot:      o.HostRoot,
		Runtime:       o.Runtime,
	}, nil
}

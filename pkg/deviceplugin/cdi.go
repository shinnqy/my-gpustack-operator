package deviceplugin

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	yaml "gopkg.in/yaml.v3"
)

// IMPORTANT: 生成标准 CDI 设备名（如 nvidia.com/gpu=...）

// CDISpecDirs 是容器推理引擎按自身优先级顺序加载规范的目录： /etc/cdi 存放管理员或手动放置的包文件。而
// /var/run/cdi 则存放因节点硬件变更而由生成器重新更新的内容。因此，第二个目录中的规范会覆盖第一个目录中相
// 同名称的文件。
//
// 它们是变量而非常量，因此测试可以将其指向一个临时的树。
var CDISpecDirs = []string{"/etc/cdi", "/var/run/cdi"}

// CDIDeviceName 表示由 CDI 设备所请求的完全限定名称：包括制造商类型以及设备自身的标识符，对于这里的每个制
// 造商而言，即为检测器记录到的加速器 ID。
func CDIDeviceName(kind, id string) string {
	return kind + "=" + id
}

// CDIDeviceNames 会根据标识符的顺序，为每个标识符渲染一个完全限定的名称。
func CDIDeviceNames(kind string, ids []string) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, CDIDeviceName(kind, id))
	}

	return names
}

// CDISpecs 是节点加载的规范名称，因为此运算符可以查看它们。
type CDISpecs struct {
	// Names 包含加载的规范中所有的完全限定设备名称。
	Names map[string]struct{}
	// Unreadable 报告是否任何规范无法被读取或解析。无法解析的规范并非什么都没有：若将两者混为一谈，一
	// 个格式错误的文件就会让原本良好的加速器变成“没有规范名称”，而实际上这并非一个确定的答案。
	Unreadable bool
	// Dirs 是曾经存在并被读取的规范目录，用于记录相关决策的证据。
	Dirs []string
}

// 缺失功能会返回所加载规范中未包含的完全限定名称，排序后用户看到的消息在相同的运行环境中不会发生变化。
func (s CDISpecs) Missing(names []string) []string {
	var missing []string
	for _, name := range names {
		if _, ok := s.Names[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)

	return missing
}

// cdiSpecDocument 是规范中需要包含的部分：它所声明的版本与类型，以及该类型下所命名的设备。
type cdiSpecDocument struct {
	Version string `yaml:"cdiVersion"`
	Kind    string `yaml:"kind"`
	Devices []struct {
		Name string `yaml:"name"`
	} `yaml:"devices"`
}

// LoadCDISpecs 会读取节点的规范目录。
//
// 它读取名为 FILES 的规范文件，而非仅读取目录。Device Manager 使用 DirectoryOrCreate 挂载这些目录，因此挂
// 载操作本身会在完全没有 CDI 的主机上创建一个空目录——将目录的存在作为证据，就能得出该软件在运行过的每个节
// 点上都有 CDI 的结论。
//
// 文件按名称在各个目录中建立关联，因此后续目录的规范会替换同名称的先前目录，而不会对其进行扩展。这是推理
// 引擎自身的规则，而“合并”操作会处理该引擎无法解析的设备：这正是生成器用新一组加速器重写过时的手工放置文
// 件的情况。
func LoadCDISpecs() CDISpecs {
	specs := CDISpecs{Names: make(map[string]struct{})}

	byFile := make(map[string][]string)
	for _, dir := range CDISpecDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			// 不存在的目录什么也不识别，这是事实。而存在的且无法列出的目录则识别一些未知的内
			// 容，这也是不可能的——因此只有第二种情况是无法读取的。它无法做到的是找不到之前存
			// 在的目录所携带的名称：其文件中哪些文件被其他文件覆盖，这正是无法读取的内容。
			if !os.IsNotExist(err) {
				specs.Unreadable = true
			}

			continue
		}
		specs.Dirs = append(specs.Dirs, dir)

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			switch filepath.Ext(entry.Name()) {
			case ".yaml", ".yml", ".json":
			default:
				continue
			}
			names, err := readCDISpec(filepath.Join(dir, entry.Name()))
			if err != nil {
				specs.Unreadable = true
				// 此文件会覆盖之前目录中同名的文件，无论该文件是否可读取。若保留之前的条
				// 目，就会违背推理引擎所制定的规范，这属于“旧文件”情况，而密钥机制的存在
				// 正是为了避免这种状况——只是现在替换文件的内容尚不可知。
				delete(byFile, entry.Name())

				continue
			}
			byFile[entry.Name()] = names
		}
	}

	for _, names := range byFile {
		for _, name := range names {
			specs.Names[name] = struct{}{}
		}
	}

	return specs
}

// readCDISpec 返回一个规范文件所包含的完全限定设备名称。
func readCDISpec(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// YAML 是 JSON 的扩展集，因此一个 Decode 实例可以处理这两种格式。
	var doc cdiSpecDocument
	if err = yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	// 这两个字段，因为一份未声明为容器的文档，推理引擎就不会加载——而从文档中读取到的、该引擎从未加载
	// 过的名称，正是会导致一个运行中的节点进入一个没有内容的通道的误判情况。
	if doc.Version == "" || doc.Kind == "" {
		return nil, fmt.Errorf("%q declares no cdiVersion or no kind, so it is not a specification "+
			"the container engine loads", path)
	}

	names := make([]string, 0, len(doc.Devices))
	for i := range doc.Devices {
		if doc.Devices[i].Name == "" {
			continue
		}
		names = append(names, CDIDeviceName(doc.Kind, doc.Devices[i].Name))
	}

	return names, nil
}

// SetCDIRequest 要求容器引擎将名为 CDI 的组件注入到该容器中。
//
// 请求使用的是注解，而非响应中指定的 CDI 字段：该字段需要满足 Kubernetes 版本 >= 1.31，而较旧的 kubelet
// 会默默忽略这个未知字段，而这正是整个渠道旨在避免的故障。注解的后缀只需在写入同一个容器的插件中唯一即可，
// 因此它可以用来标识请求方。
func SetCDIRequest(resp *ContainerAllocateResponse, requester string, names []string) {
	if resp.Annotations == nil {
		resp.Annotations = make(map[string]string, 1)
	}
	resp.Annotations["cdi.k8s.io/"+requester] = strings.Join(names, ",")
}

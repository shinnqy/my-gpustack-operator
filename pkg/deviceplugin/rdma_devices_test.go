package deviceplugin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// verbsFixture 在临时目录建立与内核对应的 sysfs 层级。
// 测试通过目录、符号链接和 ibdev 属性验证路径解析。
type verbsFixture struct {
	t    *testing.T
	root string
}

func newVerbsFixture(t *testing.T) *verbsFixture {
	t.Helper()
	return &verbsFixture{t: t, root: t.TempDir()}
}

func (f *verbsFixture) write(rel, content string) {
	f.t.Helper()
	p := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		f.t.Fatalf("write %s: %v", p, err)
	}
}

func (f *verbsFixture) mkdir(rel string) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Join(f.root, rel), 0o755); err != nil {
		f.t.Fatalf("mkdir %s: %v", rel, err)
	}
}

// symlink 让 rel 指向指定位置。相对目标按测试根目录解释，绝对目标保留其原始位置。
func (f *verbsFixture) symlink(target, rel string) {
	f.t.Helper()
	p := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(f.root, target)
	}
	if err := os.Symlink(target, p); err != nil {
		f.t.Fatalf("symlink %s -> %s: %v", p, target, err)
	}
}

// addVerbsClassEntry 在 devices 子树建立实际条目，再从 verbs 类目录创建指向它的链接。
// ibdev 属性和内核输出一样以换行结束。
func (f *verbsFixture) addVerbsClassEntry(entry, ibdev string) {
	f.t.Helper()
	realDir := filepath.Join("devices/virtual/infiniband_verbs", entry)
	f.write(filepath.Join(realDir, ibdevAttr), ibdev+"\n")
	f.symlink(realDir, filepath.Join(sysfsVerbsClassDir, entry))
}

// addVerbsDeviceTreeEntry 按硬件父设备的布局创建条目，
// uverbsN 位于父设备下的 infiniband_verbs 类目录中。
func (f *verbsFixture) addVerbsDeviceTreeEntry(parentRel, entry, ibdev string) {
	f.t.Helper()
	f.write(filepath.Join(parentRel, sysfsVerbsClassName, entry, ibdevAttr), ibdev+"\n")
}

// addRDMADevice 建立内核使用的双向关联，类条目指向 RDMA 设备目录，
// 设备目录的 device 链接再指回硬件父设备。
func (f *verbsFixture) addRDMADevice(name, parentRel string) {
	f.t.Helper()
	rdmaRel := filepath.Join(parentRel, "infiniband", name)
	f.mkdir(rdmaRel)
	f.symlink(rdmaRel, filepath.Join(sysfsInfinibandDir, name))
	f.symlink(parentRel, filepath.Join(rdmaRel, "device"))
}

const fixturePCIRel = "devices/pci0000:00/0000:00:07.0"

// TestVerbsDevicePath 检查两种布局能否各自解析设备、两者都有映射时优先采用哪一条，
// 以及找不到设备时是否给出查找位置。
func TestVerbsDevicePath(t *testing.T) {
	testCases := []struct {
		name string
		root func(f *verbsFixture) string
		// build 创建本用例所需的目录和属性。
		build func(f *verbsFixture)
		// device 是需要解析的 RDMA 设备名。
		device string
		// wantPath 是成功时应返回的字符设备路径。
		wantPath string
		// wantErr 是错误消息中必须同时出现的文字。
		wantErr []string
	}{
		{
			name: "the verbs class layout answers",
			build: func(f *verbsFixture) {
				// 类目录还包含 abi_version 等普通属性，遍历时要跳过这些非 uverbs 项。
				f.write(filepath.Join(sysfsVerbsClassDir, "abi_version"), "6")
				f.addVerbsClassEntry("uverbs0", "mlx5_1")
				f.addVerbsClassEntry("uverbs1", "mlx5_0")
			},
			device:   "mlx5_0",
			wantPath: devInfinibandDir + "/uverbs1",
		},
		{
			name: "the device tree layout answers when the verbs class is absent",
			build: func(f *verbsFixture) {
				f.addRDMADevice("mlx5_0", fixturePCIRel)
				f.addVerbsDeviceTreeEntry(fixturePCIRel, "uverbs0", "mlx5_0")
				f.addVerbsDeviceTreeEntry(fixturePCIRel, "uverbs1", "mlx5_1")
			},
			device:   "mlx5_0",
			wantPath: devInfinibandDir + "/uverbs0",
		},
		{
			name: "the class layout answers first when both hold the mapping",
			build: func(f *verbsFixture) {
				f.addRDMADevice("mlx5_0", fixturePCIRel)
				f.addVerbsDeviceTreeEntry(fixturePCIRel, "uverbs0", "mlx5_0")
				f.addVerbsClassEntry("uverbs3", "mlx5_0")
			},
			device:   "mlx5_0",
			wantPath: devInfinibandDir + "/uverbs3",
		},
		{
			name: "a clean match later in the directory answers despite an unreadable sibling",
			build: func(f *verbsFixture) {
				// 排在前面的 uverbs0 无法读取，后面的 uverbs1 能提供明确匹配，应当返回这个已确认的结果。
				f.symlink("/nonexistent-escape-target",
					filepath.Join(sysfsVerbsClassDir, "uverbs0"))
				f.addVerbsClassEntry("uverbs1", "mlx5_0")
			},
			device:   "mlx5_0",
			wantPath: devInfinibandDir + "/uverbs1",
		},
		{
			name: "a name under neither layout fails naming both",
			build: func(f *verbsFixture) {
				f.addVerbsClassEntry("uverbs0", "mlx5_1")
				f.addRDMADevice("mlx5_1", fixturePCIRel)
				f.addVerbsDeviceTreeEntry(fixturePCIRel, "uverbs0", "mlx5_1")
			},
			device: "mlx5_9",
			wantErr: []string{
				sysfsVerbsClassDir,
				sysfsVerbsClassName + `" directory under`,
				filepath.Join(sysfsInfinibandDir, "mlx5_9"),
			},
		},
		{
			name:   "an empty tree fails naming both",
			build:  func(f *verbsFixture) {},
			device: "mlx5_0",
			wantErr: []string{
				sysfsVerbsClassDir,
				sysfsVerbsClassName + `" directory under`,
			},
		},
		{
			name:    "an empty RDMA device name is refused",
			build:   func(f *verbsFixture) {},
			device:  "",
			wantErr: []string{"name is empty"},
		},
		{
			name: "an unresolvable sysfs root is refused",
			root: func(f *verbsFixture) string {
				return filepath.Join(f.root, "absent")
			},
			build:   func(f *verbsFixture) {},
			device:  "mlx5_0",
			wantErr: []string{"resolve the sysfs root"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newVerbsFixture(t)
			tc.build(f)
			root := f.root
			if tc.root != nil {
				root = tc.root(f)
			}

			got, err := verbsDevicePath(root, tc.device)

			switch {
			case tc.wantPath != "":
				if err != nil {
					t.Fatalf("verbsDevicePath(%q): unexpected error: %v", tc.device, err)
				}
				if got != tc.wantPath {
					t.Errorf("verbsDevicePath(%q) = %q, want %q", tc.device, got, tc.wantPath)
				}
			case len(tc.wantErr) > 0:
				if err == nil {
					t.Fatalf("verbsDevicePath(%q) = %q, want an error", tc.device, got)
				}
				for _, sub := range tc.wantErr {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q does not name %q", err.Error(), sub)
					}
				}
			default:
				t.Fatal("the case states no expected outcome")
			}
		})
	}
}

package preflight

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

// inImageLibDir 是镜像内厂商预加载库的位置。独立运行的预检需要将相应厂商的文件放到宿主机供容器挂载。
var inImageLibDir = "/etc/gpustack/lib"

// StageResult 保存单个厂商库目录的准备结果。调用方能据此避免使用尚未写入宿主机的挂载源。
type StageResult struct {
	// Failed 表示通过宿主机根目录写入失败，此时不能启动依赖该目录的探针。
	Failed bool
	// Reason 在准备失败时说明具体的路径或错误。
	Reason string
}

// StageLib 将镜像内对应厂商的库树复制到挂载的宿主机根目录下，目标与设备分配器使用的库路径一致。
func StageLib(hostRoot, manufacturer string) StageResult {
	res := StageResult{}

	src := filepath.Join(inImageLibDir, manufacturer)
	if !osx.ExistsDir(src) {
		res.Failed = true
		res.Reason = fmt.Sprintf("this image carries no %q to stage", src)
		return res
	}

	dst := filepath.Join(hostRoot, deviceplugin.OperatorLibDir, manufacturer)
	if err := copyTree(src, dst); err != nil {
		res.Failed = true
		res.Reason = fmt.Sprintf("write %q: %v", dst, err)
	}
	return res
}

// copyTree 递归复制文件并保留权限，使库仍可读取、工具仍可执行。
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		return copyFile(path, target)
	})
}

// copyFile 在目标目录创建临时文件，复制内容和权限后原子替换目标。读取者不会看到写到一半的文件。
func copyFile(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".stage-*")
	if err != nil {
		return err
	}
	// 发生错误时移除尚未完成替换的临时文件，保留原目标文件。
	defer func() {
		_ = out.Close()
		_ = os.Remove(out.Name())
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Chmod(info.Mode()); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	return os.Rename(out.Name(), dst)
}

package version

import "fmt"

var (
	Version   = "dev"
	GitCommit = "HEAD"
)

// Get 返回版本号和构建时的提交标识。
func Get() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}

package version

import (
	"fmt"
	"runtime"

	"golang.org/x/mod/semver"
)

var (
	Version   = "dev"
	GitCommit = "HEAD"
)

// Get 返回版本号和构建时的提交标识。
func Get() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}

func GetUserAgent() string {
	return GetUserAgentWith("gpustack-operator")
}

func GetUserAgentWith(name string) string {
	return "gpustack.ai/" + name + "; version=" + Get() + "; os=" + runtime.GOOS + "; arch=" + runtime.GOARCH
}

func MajorMinor() string {
	vXy := semver.MajorMinor(Version)
	if vXy == "" {
		return "0.0"
	}

	return vXy
}

func IsValid() bool {
	return semver.IsValid(Version)
}

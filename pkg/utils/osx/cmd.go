package osx

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// RetrieveArgsFromEnvInto 按选中的命令读取环境变量，补充未显式提供的参数。
func RetrieveArgsFromEnvInto(c *cobra.Command) {
	const (
		argPrefix    = "--"
		envKeyPrefix = "GPUSTACK_"
	)

	var (
		cmdName = filepath.Base(os.Args[0])
		sc      *cobra.Command
	)

	for _, c := range c.Commands() {
		if c.Name() == cmdName {
			if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], argPrefix) {
				break
			}

			newArgs := make([]string, len(os.Args)+1)
			newArgs[0] = os.Args[0]
			newArgs[1] = cmdName
			copy(newArgs[2:], os.Args[1:])
			os.Args = newArgs

			sc = c

			break
		}
	}

	if len(os.Args) >= 2 && sc == nil {
		for _, c := range c.Commands() {
			if c.Name() == os.Args[1] {
				sc = c
				break
			}
		}
	}

	var (
		envPrefix = envKeyPrefix
		flags     = c.Flags()
	)

	if sc != nil {
		envPrefix = envPrefix + strings.ToUpper(sc.Name()) + "_"
		flags = sc.Flags()
	}

	ignores := map[string]bool{"help": true, "v": true, "version": true}
	settings := make(map[string]bool)

	if len(os.Args) > 2 {
		for _, v := range os.Args[2:] {
			if strings.HasPrefix(v, argPrefix) {
				vs := strings.SplitN(v, "=", 2)
				settings[vs[0]] = true
			}
		}
	}

	envArgs := make([]string, 0, len(os.Environ())*2)

	for _, v := range os.Environ() {
		if v2 := strings.TrimPrefix(v, envPrefix); v == v2 {
			continue
		} else {
			v = v2
		}

		vs := strings.SplitN(v, "=", 2)
		if len(vs) != 2 {
			continue
		}

		var (
			fn = strings.ReplaceAll(strings.ToLower(vs[0]), "_", "-")
			ek = argPrefix + fn
		)

		if ignores[fn] || flags.Lookup(fn) == nil || settings[ek] {
			continue
		}

		ev := vs[1]
		if ev2 := os.ExpandEnv(ev); ev2 != "" && ev != ev2 {
			ev = ev2
		}

		envArgs = append(envArgs, ek, ev)
	}

	if len(envArgs) == 0 {
		return
	}

	newArgs := make([]string, len(os.Args)+len(envArgs))
	copy(newArgs, os.Args)
	copy(newArgs[len(os.Args):], envArgs)
	os.Args = newArgs
}

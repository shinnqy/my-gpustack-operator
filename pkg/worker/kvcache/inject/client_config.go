// This file holds what every engine is told, and the artifact only the vLLM family takes: its config
// file. "The vLLM family" is two engines with two connector names and two readers closed at
// different key sets, which is why the values below are described per reader rather than per family.
//
// The values below are rendered for a Mooncake client that stores nothing of its own. That is a role
// rather than a size setting: a client with a local buffer and no global segment can Get and Put and
// contributes no memory, which is what an inference engine needs. Declaring the other shapes by
// accident is the failure this file is arranged to prevent, so the keys that declare the role are
// written together and never one at a time.
package inject

import (
	"gpustack.ai/gpustack/pkg/systemname"
)

// ClientConfigAnnotationKey carries the rendered client configuration on the Pod that consumes it, for
// engines that take a file. The file is a downwardAPI projection of this annotation, so the caller
// creates no ConfigMap, needs no RBAC for one, and leaves nothing to garbage-collect: the
// configuration's lifetime is exactly the Pod's.
//
// It is declared here rather than beside the annotations a caller READS, because this one is an output
// of rendering. A caller that refuses a user-supplied value for it reads the constant from here.
const ClientConfigAnnotationKey = "kvcache." + systemname.LabelPrefix + "client-config"

const (
	// ConfigVolumeName and ConfigMountPath are where the rendered file lands. The mount is read-only:
	// nothing in the container has any reason to write it, and a writable projection would let a
	// process edit the record of what was injected.
	ConfigVolumeName = "gpustack-kvcache-config"
	ConfigMountPath  = "/etc/gpustack/kvcache"

	// ConfigFileName is the file's name inside the mount, and ConfigFilePath the two joined - the
	// value the engine's own path variable is set to.
	ConfigFileName = "mooncake.json"
	ConfigFilePath = ConfigMountPath + "/" + ConfigFileName
)

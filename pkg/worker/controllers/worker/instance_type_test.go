package worker

import (
	"context"
	"fmt"
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

// flavorOpt 调整测试 ResourceFlavor 的附加信息，可用来标记加速器或设置指定的单位资源。
type flavorOpt func(notes map[string]string)

func accelerated(manufacturer string) flavorOpt {
	return func(notes map[string]string) {
		notes["acceleratable"] = "true"
		notes["manufacturer"] = manufacturer
	}
}

// newNodesFlavor 构造资源类别为 nodes 的 ResourceFlavor，
// 带上调度所需的特征键、加速器标记、系统、架构和 .count、.capacity 标签，以及 flavor 的附加信息。
// 设备数使用 d 后缀。
func newNodesFlavor(name, key string, count, capacity int64, opts ...flavorOpt) *kueue.ResourceFlavor {
	notes := map[string]string{
		"acceleratable": "false",
		"manufacturer":  "generic",
		"product":       "",
		"family":        "",
		"memory":        "",
	}
	for _, o := range opts {
		o(notes)
	}
	acceleratable := notes["acceleratable"] == "true"
	keyLabel := featureKeyLabel(acceleratable, key)
	labels := map[string]string{
		keyLabel:                                      "true",
		nodefeature.NodeAcceleratableLabelKey:         notes["acceleratable"],
		core.LabelOSStable:                            "linux",
		core.LabelArchStable:                          "amd64",
		keyLabel + _ResourceFlavorCountLabelSuffix:    itoa(count),
		keyLabel + _ResourceFlavorCapacityLabelSuffix: itoa(capacity),
	}
	// 加速器 flavor 还可以具有 general.<gKey> 选择标签，该键没有 .capacity。
	// 测试可以用临时标记要求添加标签，该标记不会写入对象的附加信息。
	if gk := notes["_generalKey"]; gk != "" {
		labels[nodefeature.GeneralFeatureLabelPrefix+gk] = "true"
		delete(notes, "_generalKey")
	}
	rf := &kueue.ResourceFlavor{
		ObjectMeta: meta.ObjectMeta{Name: name, Labels: labels},
	}
	systemmeta.NoteResource(rf, _ResourceFlavorResType, notes)
	return rf
}

// nodeQueueName 返回关闭 CPU 厂商感知时 flavor 所属的池名，ClusterQueue 和 InstanceType 共用它。
// 纯 CPU 池合并到 generic，加速器池按加速器键区分，统一使用 gpustack--<key>-linux-amd64 格式。
func nodeQueueName(key string) string {
	return fmt.Sprintf("gpustack--%s-linux-amd64", key)
}

func getClusterQueue(t *testing.T, cli ctrlcli.Client, name string) (*kueue.ClusterQueue, error) {
	t.Helper()
	cq := new(kueue.ClusterQueue)
	err := cli.Get(context.Background(), ctrlcli.ObjectKey{Name: name}, cq)
	return cq, err
}

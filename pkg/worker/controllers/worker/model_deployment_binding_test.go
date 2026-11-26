package worker

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// newRenderPool 提供部署测试所引用的资源池及其客户端地址。
func newRenderPool() *workercore.KVCachePool {
	return &workercore.KVCachePool{
		ObjectMeta: meta.ObjectMeta{Name: "shared"},
		Spec:       workercore.KVCachePoolSpec{Backends: []string{"kvcb"}},
		Status:     workercore.KVCachePoolStatus{ClientEndpoint: "master:50051"},
	}
}

func newRenderBackend() *workercore.KVCacheBackend {
	return &workercore.KVCacheBackend{
		ObjectMeta: meta.ObjectMeta{Name: "kvcb"},
		Spec: workercore.KVCacheBackendSpec{
			Transport: workercore.KVCacheBackendTransport{Protocol: "TCP"},
		},
	}
}

func newRenderBinding() *workercore.KVCachePoolBinding {
	kvcpb := &workercore.KVCachePoolBinding{
		ObjectMeta: meta.ObjectMeta{Name: "shared-kv", Namespace: "team-a"},
		Spec: workercore.KVCachePoolBindingSpec{
			PoolRef: workercore.KVCachePoolBindingPoolReference{Name: "shared"},
			Domain:  workercore.KVCachePoolBindingDomain{Name: "chat", BlockSize: 256, Dtype: "bfloat16"},
			Quota:   workercore.KVCachePoolBindingQuota{Ceiling: resource.MustParse("100Gi")},
		},
		Status: workercore.KVCachePoolBindingStatus{
			Phase:        KVCachePoolPhaseReady,
			PhaseMessage: "the master reports this domain's figures",
		},
	}
	KVCachePoolBindingConditionQuotaGranted.True(kvcpb, "Granted", "the master granted 100Gi")

	return kvcpb
}

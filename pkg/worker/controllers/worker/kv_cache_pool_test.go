package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

func newTestKVCachePool(name string, backends ...string) *workercore.KVCachePool {
	return &workercore.KVCachePool{
		ObjectMeta: meta.ObjectMeta{Name: name},
		Spec: workercore.KVCachePoolSpec{
			Backends: backends,
			Quota:    workercore.KVCachePoolQuota{Total: resource.MustParse("100Ti")},
		},
	}
}

func newTestKVCachePoolBinding(namespace, name, pool string) *workercore.KVCachePoolBinding {
	return &workercore.KVCachePoolBinding{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name},
		Spec: workercore.KVCachePoolBindingSpec{
			PoolRef: workercore.KVCachePoolBindingPoolReference{Name: pool},
			Domain: workercore.KVCachePoolBindingDomain{
				Name:      name,
				BlockSize: 16,
				Dtype:     "bfloat16",
			},
			// 设置该值，因为该字段是必填项，其零值为 1，而 Webhook 会拒绝此值。如果将其设为零，
			// 那将是一个不会被集群接受的对象。
			Quota: workercore.KVCachePoolBindingQuota{Ceiling: resource.MustParse("1Ti")},
		},
	}
}

// TestObserveKVCachePoolQuotaWithinTotal 用于确定资源池的裁决结果：其已授予的所有上限是否仍符合所声明的总量。
//
// “错误”是值得阅读的裁决，并非过错：该商店按比例为所有租户提供服务，每个绑定的 status.effectiveQuota 会报告
// 实际授予的数量，没有任何绑定被拒绝或因此受到指责。该总和仅包含那些上限被写入账本的绑定，因此释放的绑定和
// 有争议的领域都未被包含在内。
func TestObserveKVCachePoolQuotaWithinTotal(t *testing.T) {
	// binding 会在其自身的复用域下构建一个 fixture，因此两种情况不会在同一个 fixture 上发生冲突。
	binding := func(domain, ceiling string) workercore.KVCachePoolBinding {
		kvcpb := *newTestKVCachePoolBinding("team-a", domain, "shared")
		kvcpb.Spec.Domain.Name = domain
		kvcpb.Spec.Quota.Ceiling = resource.MustParse(ceiling)
		return kvcpb
	}

	cases := []struct {
		name       string
		total      string
		bindings   []workercore.KVCachePoolBinding
		mutate     func(master *kvCachePoolMaster)
		wantTrue   bool
		wantReason string
		wantMsg    []string
	}{
		{
			name:       "two bindings at half the total each",
			total:      "16Gi",
			bindings:   []workercore.KVCachePoolBinding{binding("chat", "8Gi"), binding("batch", "8Gi")},
			wantTrue:   true,
			wantReason: "WithinTotal",
			wantMsg:    []string{"sum to 16Gi", "total of 16Gi"},
		},
		{
			name:  "a third binding taking the sum past the total",
			total: "16Gi",
			bindings: []workercore.KVCachePoolBinding{
				binding("chat", "8Gi"), binding("batch", "8Gi"), binding("eval", "2Gi"),
			},
			wantReason: KVCachePoolReasonOversubscribed,
			wantMsg:    []string{"sum to 18Gi", "total of 16Gi"},
		},
		{
			// 其声明已结束：只要排空操作持续进行，在已解决超订阅问题的删除操作之后，判定结果仍为“假”。
			name:  "a releasing binding is not counted",
			total: "16Gi",
			bindings: func() []workercore.KVCachePoolBinding {
				live := binding("chat", "16Gi")
				draining := binding("batch", "16Gi")
				draining.DeletionTimestamp = &meta.Time{Time: time.Now()}
				return []workercore.KVCachePoolBinding{live, draining}
			}(),
			wantTrue:   true,
			wantReason: "WithinTotal",
		},
		{
			// Two Bindings 将一个缓存绑定到单一域中，导致该缓存从目标租户中消失；若对域进行计数，
			// 其上限也会被重复计算两次。
			name:  "a contested domain is not counted",
			total: "16Gi",
			bindings: []workercore.KVCachePoolBinding{
				binding("chat", "16Gi"),
				func() workercore.KVCachePoolBinding {
					second := binding("chat", "16Gi")
					second.Namespace = "team-b"
					return second
				}(),
			},
			mutate: func(master *kvCachePoolMaster) {
				master.contested["chat"] = []workercore.KVCachePoolBindingReference{
					{Namespace: "team-a", Name: "chat"},
					{Namespace: "team-b", Name: "chat"},
				}
			},
			wantTrue:   true,
			wantReason: "WithinTotal",
		},
		{
			// 观察阶段会先跳过它，因此裁决必须一致，否则双方会就资源池的内容产生分歧。
			name:  "a binding with no domain is not counted",
			total: "16Gi",
			bindings: []workercore.KVCachePoolBinding{
				binding("chat", "8Gi"), binding("", "16Gi"),
			},
			wantTrue:   true,
			wantReason: "WithinTotal",
		},
		{
			name:       "a pool nobody bound",
			total:      "16Gi",
			wantTrue:   true,
			wantReason: "WithinTotal",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kvcp := newTestKVCachePool("shared", "mooncake-dram")
			kvcp.Spec.Quota.Total = resource.MustParse(c.total)
			holder := &workercore.KVCachePool{ObjectMeta: *kvcp.ObjectMeta.DeepCopy()}

			master := &kvCachePoolMaster{
				bindings:  map[string][]workercore.KVCachePoolBinding{"shared": c.bindings},
				contested: map[string][]workercore.KVCachePoolBindingReference{},
			}
			if c.mutate != nil {
				c.mutate(master)
			}

			observeKVCachePoolQuotaWithinTotal(kvcp, holder, master)

			condition := KVCachePoolConditionQuotaWithinTotal
			require.True(t, condition.Exists(holder))
			if c.wantTrue {
				assert.True(t, condition.IsTrue(holder))
			} else {
				assert.True(t, condition.IsFalse(holder))
			}
			assert.Equal(t, c.wantReason, condition.GetReason(holder))
			for _, want := range c.wantMsg {
				assert.Contains(t, condition.GetMessage(holder), want)
			}

			// 这一裁决仅属于资源池：没有绑定会对其所接受的上限产生自身的错误。
			for i := range c.bindings {
				assert.Empty(t, c.bindings[i].Status.Conditions)
			}
		})
	}
}

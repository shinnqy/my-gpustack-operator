package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/utils/certs"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const (
	k8sManagedLabel      = "certs.gpustack.ai/managed"
	k8sManagedGroupLabel = "certs.gpustack.ai/group"

	k8sManagedNameSumAnno  = "certs.gpustack.ai/name-sum"
	k8sManagedNameAnno     = "certs.gpustack.ai/name"
	k8sManagedValueSumAnno = "certs.gpustack.ai/value-sum"
	k8sManagedValueKey     = "value"
)

// k8sCache 使用 Kubernetes Secret 存储证书数据来实现 certs.Cache。
type k8sCache struct {
	logger klog.Logger
	cli    certs.SecretInterface
	inf    cache.SharedIndexInformer
	grp    string
}

// NewK8sCache 使用给定的客户端创建一个新的 k8sCache 实例。
func NewK8sCache(ctx context.Context, group string, cli certs.SecretInterface) (certs.Cache, error) {
	// 从 context 中解析 logger，使调用方能够捕获该缓存的日志，
	// 而无需修改全局 klog（在下方启动的 informer goroutine 持续输出日志期间修改全局 klog 并不安全）。
	lg := klog.FromContext(ctx).WithName("certs").WithName("k8s")

	lw := func() *cache.ListWatch {
		labelSelector := labels.FormatLabels(map[string]string{
			k8sManagedLabel:      "true",
			k8sManagedGroupLabel: group,
		})

		return &cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options meta.ListOptions) (runtime.Object, error) {
				options.ResourceVersion = "0"
				options.LabelSelector = labelSelector
				return cli.List(ctx, options)
			},
			WatchFuncWithContext: func(ctx context.Context, options meta.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labelSelector
				return cli.Watch(ctx, options)
			},
		}
	}()

	inf := cache.NewSharedIndexInformer(lw, &core.Secret{}, 1*time.Hour,
		map[string]cache.IndexFunc{
			"_": func(obj any) ([]string, error) {
				s, ok := obj.(*core.Secret)
				if !ok {
					return nil, errors.New("object is not a secret")
				}

				if s.DeletionTimestamp != nil ||
					s.Type != core.SecretTypeOpaque ||
					s.Annotations == nil || s.Data == nil {
					return nil, nil
				}

				annos, data := s.Annotations, s.Data

				if annos[k8sManagedNameAnno] == "" || annos[k8sManagedNameSumAnno] == "" ||
					data[k8sManagedValueKey] == nil || annos[k8sManagedValueSumAnno] == "" {
					return nil, nil
				}

				if sumName(annos[k8sManagedNameAnno]) != annos[k8sManagedNameSumAnno] ||
					sumValue(data[k8sManagedValueKey]) != annos[k8sManagedValueSumAnno] {
					lg.Error(nil, "invalid key", "key", annos[k8sManagedNameAnno])
					return nil, nil
				}

				// 仅对持有该 key 的 Secret 建立索引，从而使一个 key 最多解析到一个 Secret。
				// 其他声称拥有该 key 的 Secret（例如旧版本以随机生成名称创建的 Secret）则不予处理。
				if s.Name != secretName(group, annos[k8sManagedNameAnno]) {
					return nil, nil
				}

				return []string{annos[k8sManagedNameAnno]}, nil
			},
		})

	gox.Go(func() {
		inf.Run(ctx.Done())
	})

	// 等待 informer 完成同步。
	{
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
			return k8sCache{}, errors.New("sync informer")
		}
	}

	return k8sCache{
		logger: lg,
		cli:    cli,
		inf:    inf,
		grp:    group,
	}, nil
}

// Get 从指定名称的 Secret 中读取证书数据。
func (k k8sCache) Get(_ context.Context, name string) ([]byte, error) {
	if name == "" {
		return nil, certs.ErrCacheMiss
	}

	// 获取已存在的 Secret。
	sec := k.get(name)
	if sec == nil || sec.DeletionTimestamp != nil {
		return nil, certs.ErrCacheMiss
	}

	return sec.Data[k8sManagedValueKey], nil
}

// Put 将证书数据写入指定名称的 Secret。
//
// 持有某个 key 的 Secret 以该 key 命名，因此并发写入同一 key 的写入方会创建或更新同一个 Secret，
// 以先到者为准。
func (k k8sCache) Put(ctx context.Context, name string, data []byte) error {
	if name == "" || len(data) == 0 {
		return nil
	}

	sec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Name: secretName(k.grp, name),
			Annotations: map[string]string{
				k8sManagedNameAnno:     name,
				k8sManagedNameSumAnno:  sumName(name),
				k8sManagedValueSumAnno: sumValue(data),
			},
			Labels: map[string]string{
				k8sManagedLabel:      "true",
				k8sManagedGroupLabel: k.grp,
			},
		},
		Type: core.SecretTypeOpaque,
		Data: map[string][]byte{
			k8sManagedValueKey: data,
		},
	}

	secAlignFn := func(aSec *core.Secret) (_ *core.Secret, skip bool, err error) {
		skip = true
		// 对齐 annotations。
		for ak, av := range sec.Annotations {
			if aSec.Annotations[ak] == av {
				continue
			}
			if aSec.Annotations == nil {
				aSec.Annotations = make(map[string]string, len(sec.Annotations))
			}
			aSec.Annotations[ak] = av
			skip = false
		}
		// 对齐 labels。
		for lk, lv := range sec.Labels {
			if aSec.Labels[lk] == lv {
				continue
			}
			if aSec.Labels == nil {
				aSec.Labels = make(map[string]string, len(sec.Labels))
			}
			aSec.Labels[lk] = lv
			skip = false
		}
		// 对齐值。
		if !bytes.Equal(aSec.Data[k8sManagedValueKey], data) {
			if aSec.Data == nil {
				aSec.Data = make(map[string][]byte, 1)
			}
			aSec.Data[k8sManagedValueKey] = data
			skip = false
		}
		return aSec, skip, err
	}

	asec, err := kubeclientset.Update(ctx, k.cli, sec,
		kubeclientset.WithCreateIfNotExisted[*core.Secret](),
		kubeclientset.WithUpdateAlign(secAlignFn))
	if err != nil {
		return fmt.Errorf("put secret %q: %w", sec.Name, err)
	}

	k.logger.V(4).Info("put secret", "object", klog.KObj(asec))

	return nil
}

// Delete 移除指定名称的 Secret。
func (k k8sCache) Delete(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}

	// 删除已存在的 Secret。
	sn := secretName(k.grp, name)
	err := k.cli.Delete(ctx, sn,
		meta.DeleteOptions{
			PropagationPolicy: ptr.To(meta.DeletePropagationBackground),
		})
	if err != nil && !kerrors.IsNotFound(err) {
		return fmt.Errorf("delete secret %q: %w", sn, err)
	}

	k.logger.V(4).Info("deleted secret", "name", sn)

	return nil
}

func (k k8sCache) get(name string) *core.Secret {
	if name == "" {
		return nil
	}

	secs, err := k.inf.GetIndexer().ByIndex("_", name)
	if err != nil {
		k.logger.Error(err, "get indexed cached secrets")
		return nil
	}

	// 仅索引以该 key 命名的 Secret，因此最多只有一个。
	if len(secs) == 0 {
		return nil
	}

	return secs[0].(*core.Secret)
}

// secretName 返回用于存储给定 group 中指定 key 的 Secret 名称。
//
// 由于 key 不受 Kubernetes 对象名称规范的限制，因此名称采用摘要值。
func secretName(group, key string) string {
	return "gpustack-cert-" + stringx.SumByFNV64a(group, "/", key)
}

func sumName(k string) string {
	return "fnv64a:" + stringx.SumByFNV64a(k)
}

func sumValue(v []byte) string {
	return "sha224:" + stringx.SumBytesBySHA224(v)
}

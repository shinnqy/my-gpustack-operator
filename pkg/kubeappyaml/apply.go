package kubeappyaml

import (
	"context"
	"fmt"
	"io"
	"strings"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/jonboulle/clockwork"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/openapi/cached"
	"k8s.io/client-go/openapi3"
	"k8s.io/client-go/rest"
	kubectlapply "k8s.io/kubectl/pkg/cmd/apply"
	kubectlutil "k8s.io/kubectl/pkg/util"
	kubectlopenapi "k8s.io/kubectl/pkg/util/openapi"

	"gpustack.ai/gpustack/pkg/kubeconfig"
	"gpustack.ai/gpustack/pkg/utils/funcx"
)

// Apply 将指定的 YAML 内容应用到由给定 REST 配置指向的集群。
func Apply(ctx context.Context, content string, restCfg rest.Config) error {
	restCliGetter := kubeconfig.ConvertRestConfigToRestClientGetter(&restCfg)

	return ApplyWithRestClientGetter(ctx, content, restCliGetter)
}

// ApplyWithRestClientGetter 将指定的 YAML 内容应用到由给定 RESTClientGetter 指向的集群。
func ApplyWithRestClientGetter(ctx context.Context, content string, restCliGetter genericclioptions.RESTClientGetter) error {
	restDisCli := funcx.NoError(restCliGetter.ToDiscoveryClient())

	infos, err := resource.
		NewBuilder(restCliGetter).
		Unstructured().
		Flatten().
		ContinueOnError().
		Stream(strings.NewReader(content), "MEMORY").
		Do().
		Infos()
	if err != nil {
		return fmt.Errorf("build resources from yaml: %w", err)
	}

	openapiGetter := openAPIGetter{parser: kubectlopenapi.NewOpenAPIParser(kubectlopenapi.NewOpenAPIGetter(restDisCli))}
	openapiv3Root := openapi3.NewRoot(cached.NewClient(restDisCli.OpenAPIV3()))
	for i := range infos {
		aerr := applyOneObject(infos[i], openapiGetter, openapiv3Root)
		if aerr != nil {
			err = multierror.Append(err, aerr)
		}
	}

	return err
}

func applyOneObject(info *resource.Info, openapiGetter kubectlopenapi.OpenAPIResourcesGetter, openapiv3Root openapi3.Root) error {
	helper := resource.NewHelper(info.Client, info.Mapping)

	modified, err := kubectlutil.GetModifiedConfiguration(info.Object, true, unstructured.UnstructuredJSONScheme)
	if err != nil {
		return fmt.Errorf("get modified configuration: %w", err)
	}

	// 如果资源不存在，则创建它。
	if err = info.Get(); err != nil {
		if !kerrors.IsNotFound(err) {
			return fmt.Errorf("get object: %w", err)
		}

		if u, ok := info.Object.(runtime.Unstructured); ok {
			pruneNullsFromMap(u.UnstructuredContent())
		}

		createdObj, err := helper.Create(info.Namespace, true, info.Object)
		if err != nil {
			return fmt.Errorf("create object: %w", err)
		}
		_ = info.Refresh(createdObj, true)

		return nil
	}

	// 如果对象已存在，则需要对其执行 patch 更新。
	patcher := &kubectlapply.Patcher{
		Mapping:           info.Mapping,
		Helper:            helper,
		Overwrite:         true,
		BackOff:           clockwork.NewRealClock(),
		Force:             false,
		CascadingStrategy: "background",
		Timeout:           0,
		GracePeriod:       -1,
		OpenAPIGetter:     openapiGetter,
		OpenAPIV3Root:     openapiv3Root,
		Retries:           5,
	}
	_, patchedObj, err := patcher.Patch(info.Object, modified, info.Source, info.Namespace, info.Name, io.Discard)
	if err != nil {
		return fmt.Errorf("patch object: %w", err)
	}
	_ = info.Refresh(patchedObj, true)

	return nil
}

type openAPIGetter struct {
	parser *kubectlopenapi.CachedOpenAPIParser
}

func (o openAPIGetter) OpenAPISchema() (kubectlopenapi.Resources, error) {
	return o.parser.Parse()
}

func pruneNullsFromMap(data map[string]any) {
	for k, v := range data {
		if v == nil {
			delete(data, k)
		} else {
			pruneNulls(v)
		}
	}
}

func pruneNullsFromSlice(data []any) {
	for _, v := range data {
		pruneNulls(v)
	}
}

func pruneNulls(v any) {
	switch v := v.(type) {
	case map[string]any:
		pruneNullsFromMap(v)
	case []any:
		pruneNullsFromSlice(v)
	}
}

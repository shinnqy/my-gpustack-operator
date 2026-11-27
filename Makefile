.PHONY: build test generate api audit chart image

OUT ?= gpustack-operator
GUIDE ?= $(HOME)/Documents/opensource/gpustack-operator/guide
HELM ?= helm
CHART := deploy/gpustack-operator/chart
DIST ?= .dist
VERSION ?= v0.0.0-guide
COMMIT ?=
IMAGE ?= gpustack-operator:guide-final
PLATFORM ?= linux/$(shell go env GOARCH)

# 厂商驱动由探测器按需加载，Linux 程序启动时不能提前解析所有厂商符号。
ifeq ($(shell go env GOOS),linux)
device_link_flags := -extldflags=-Wl,-z,lazy
endif

build:
	CGO_ENABLED=1 go build -mod=readonly -trimpath -buildvcs=false \
		-ldflags="-s -w $(device_link_flags) -X gpustack.ai/gpustack/pkg/utils/version.Version=$(VERSION) $(if $(COMMIT),-X gpustack.ai/gpustack/pkg/utils/version.GitCommit=$(COMMIT))" -o "$(OUT)" ./cmd/gpustack-operator

test:
	CGO_ENABLED=1 go test -mod=readonly -buildvcs=false $(if $(device_link_flags),-ldflags="$(device_link_flags)") ./...

generate:
	@bash hack/generate.sh

api: generate

audit:
	go run -mod=readonly -buildvcs=false ./hack/check/chapter-source -guide "$(GUIDE)"

chart:
	mkdir -p "$(DIST)"
	$(HELM) lint "$(CHART)"
	$(HELM) package "$(CHART)" --version "$(patsubst v%,%,$(VERSION))" \
		--app-version "$(patsubst v%,%,$(VERSION))" --destination "$(DIST)"

image:
	BUILDX_GIT_INFO=false BUILDX_GIT_LABELS=false docker buildx build --load --platform "$(PLATFORM)" \
		--build-arg GPUSTACK_GIT_VERSION="$(VERSION)" --build-arg GPUSTACK_GIT_COMMIT="$(COMMIT)" \
		-f pack/gpustack-operator/Dockerfile -t "$(IMAGE)" .

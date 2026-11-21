.PHONY: build test generate api

OUT ?= gpustack-operator

# 厂商驱动由探测器按需加载，Linux 程序启动时不能提前解析所有厂商符号。
ifeq ($(shell go env GOOS),linux)
device_link_flags := -ldflags='-extldflags=-Wl,-z,lazy'
endif

build:
	CGO_ENABLED=1 go build -buildvcs=false $(device_link_flags) -o "$(OUT)" ./cmd/gpustack-operator

test:
	CGO_ENABLED=1 go test -buildvcs=false $(device_link_flags) ./...

generate:
	@bash hack/generate.sh

api: generate

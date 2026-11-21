.PHONY: build generate api

build:
	go build ./cmd/gpustack-operator

generate:
	@bash hack/generate.sh

api: generate

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
source "${ROOT_DIR}/hack/lib/log.sh"
source "${ROOT_DIR}/hack/lib/util.sh"
source "${ROOT_DIR}/hack/lib/proto.sh"
source "${ROOT_DIR}/hack/lib/style.sh"

mkdir -p "${ROOT_DIR}/.sbin/protoc/bin"
gpustack::protoc::protoc::validate
gpustack::protoc::protoc_gen_go::validate
gpustack::protoc::protoc_gen_gogo::validate
gpustack::lint::goimports::validate

export PATH="${ROOT_DIR}/.sbin:${ROOT_DIR}/.sbin/protoc/bin:${PATH}"
export GOWORK=off
exec python3 "${ROOT_DIR}/hack/generate_api.py"

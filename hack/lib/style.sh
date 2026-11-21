#!/usr/bin/env bash

goimports_version=${GOIMPORT_VERSION:-"v0.49.0"}

function gpustack::lint::goimports::install() {
  local bin="${ROOT_DIR}/.sbin"
  mkdir -p "${bin}"
  GOBIN="${bin}" go install "golang.org/x/tools/cmd/goimports@${goimports_version}"
}

function gpustack::lint::goimports::validate() {
  # goimports has no version flag (-version and -V both exit 2 with the usage text), so the pin
  # is compared against the module version `go install` stamped into the binary. Existence alone
  # would keep a .sbin populated before a pin bump, and hack/generate.sh runs goimports over the
  # generated tree -- its output is inside the baseline the API drift gate compares against.
  # shellcheck disable=SC2046
  if [[ -n "$(command -v $(gpustack::lint::goimports::bin))" ]]; then
    # shellcheck disable=SC2046
    if gpustack::util::go_module_version_is \
      "$(gpustack::util::go_module_version $(gpustack::lint::goimports::bin))" \
      "${goimports_version}"; then
      return 0
    fi
  fi

  gpustack::log::info "installing goimports"
  if gpustack::lint::goimports::install; then
    gpustack::log::info "goimports installed"
    return 0
  fi
  gpustack::log::error "no goimports available"
  return 1
}

function gpustack::lint::goimports::bin() {
  local bin="goimports"
  if [[ -f "${ROOT_DIR}/.sbin/goimports" ]]; then
    bin="${ROOT_DIR}/.sbin/goimports"
  fi
  echo -n "${bin}"
}

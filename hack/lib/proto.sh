#!/usr/bin/env bash

protoc_version=${PROTOC_VERSION:-"v33.5"}
protoc_gen_go_version=${PROTOC_GEN_GO_VERSION:-"v1.36.11"}
protoc_gen_gogo_version=${PROTOC_GEN_GOGO_VERSION:-"f67b8970b736"}

function gpustack::protoc::protoc::install() {
  local os
  os="$(gpustack::util::get_raw_os)"
  if [[ "${os}" == "darwin" ]]; then
    os="osx"
  fi
  local arch
  arch="$(gpustack::util::get_raw_arch)"
  if [[ "${arch}" == "arm64" ]]; then
    arch="aarch_64"
  fi
  if [[ "${arch}" == "amd64" ]]; then
    arch="x86_64"
  fi
  local url
  url="https://github.com/protocolbuffers/protobuf/releases/download/${protoc_version}/protoc-${protoc_version#v}-${os}-${arch}.zip"
  gpustack::log::info "downloading protoc ${protoc_version} from ${url}"
  curl --retry 3 --retry-all-errors --retry-delay 3 \
    -o /tmp/protoc.zip \
    -sSfL "${url}"
  unzip -qou /tmp/protoc.zip -d "${ROOT_DIR}/.sbin/protoc"
  chmod a+x "${ROOT_DIR}/.sbin/protoc/bin/protoc"
  file "${ROOT_DIR}/.sbin/protoc/bin/protoc"
}

function gpustack::protoc::protoc::validate() {
  # shellcheck disable=SC2046
  if [[ -n "$(command -v $(gpustack::protoc::protoc::bin))" ]]; then
    if [[ "$($(gpustack::protoc::protoc::bin) --version 2>&1)" == "libprotoc ${protoc_version#v}" ]]; then
      return 0
    fi
  fi

  gpustack::log::info "installing protoc ${protoc_version}"
  if gpustack::protoc::protoc::install; then
    gpustack::log::info "protoc $($(gpustack::protoc::protoc::bin) --version 2>&1 | cut -d " " -f 2 2>&1 | head -n 1)"
    return 0
  fi
  gpustack::log::error "no protoc available"
  return 1
}

function gpustack::protoc::protoc::bin() {
  local bin="protoc"
  if [[ -f "${ROOT_DIR}/.sbin/protoc/bin/protoc" ]]; then
    bin="${ROOT_DIR}/.sbin/protoc/bin/protoc"
  fi
  echo -n "${bin}"
}

function gpustack::protoc::protoc_gen_go::install() {
  local os
  os="$(gpustack::util::get_raw_os)"
  local arch
  arch="$(gpustack::util::get_raw_arch)"
  local url
  url="https://github.com/protocolbuffers/protobuf-go/releases/download/${protoc_gen_go_version}/protoc-gen-go.${protoc_gen_go_version}.${os}.${arch}.tar.gz"
  gpustack::log::info "downloading protoc-gen-go ${protoc_gen_go_version} from ${url}"
  curl --retry 3 --retry-all-errors --retry-delay 3 \
    -o /tmp/protoc-gen.tar.gz \
    -sSfL "${url}"
  tar -zxvf /tmp/protoc-gen.tar.gz \
    --directory "${ROOT_DIR}/.sbin/protoc/bin" \
    --no-same-owner \
    --exclude ./LICENSE \
    --exclude ./README.md
  chmod a+x "${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-go"
  file "${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-go"
}

function gpustack::protoc::protoc_gen_go::validate() {
  # shellcheck disable=SC2046
  if [[ -n "$(command -v $(gpustack::protoc::protoc_gen_go::bin))" ]]; then
    if [[ "$($(gpustack::protoc::protoc_gen_go::bin) --version 2>&1)" == "protoc-gen-go ${protoc_gen_go_version}" ]]; then
      return 0
    fi
  fi

  gpustack::log::info "installing protoc-gen-go ${protoc_gen_go_version}"
  if gpustack::protoc::protoc_gen_go::install; then
    gpustack::log::info "protoc-gen-go installed"
    return 0
  fi
  gpustack::log::error "no protoc-gen-go available"
  return 1
}

function gpustack::protoc::protoc_gen_go::bin() {
  local bin="protoc-gen-go"
  if [[ -f "${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-go" ]]; then
    bin="${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-go"
  fi
  echo -n "${bin}"
}

function gpustack::protoc::protoc_gen_gogo::install() {
  local bin="${ROOT_DIR}/.sbin/protoc/bin"
  mkdir -p "${bin}"
  GOBIN="${bin}" go install "github.com/gogo/protobuf/protoc-gen-gogo@${protoc_gen_gogo_version}"
}

function gpustack::protoc::protoc_gen_gogo::validate() {
  # protoc-gen-gogo has no version flag -- it is a protoc plugin and answers --version with
  # "no files to generate" on exit 1 -- so the pin is compared against the module version
  # `go install` stamped into the binary. Existence alone would keep a .sbin populated before a
  # pin bump, and `make generate` would run the generator the bump replaced.
  # shellcheck disable=SC2046
  if [[ -n "$(command -v $(gpustack::protoc::protoc_gen_gogo::bin))" ]]; then
    # shellcheck disable=SC2046
    if gpustack::util::go_module_version_is \
      "$(gpustack::util::go_module_version $(gpustack::protoc::protoc_gen_gogo::bin))" \
      "${protoc_gen_gogo_version}"; then
      return 0
    fi
  fi

  gpustack::log::info "installing protoc-gen-gogo"
  if gpustack::protoc::protoc_gen_gogo::install; then
    gpustack::log::info "protoc-gen-gogo installed"
    return 0
  fi
  gpustack::log::error "no protoc-gen-gogo available"
  return 1
}

function gpustack::protoc::protoc_gen_gogo::bin() {
  local bin="protoc-gen-gogo"
  if [[ -f "${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-gogo" ]]; then
    bin="${ROOT_DIR}/.sbin/protoc/bin/protoc-gen-gogo"
  fi
  echo -n "${bin}"
}

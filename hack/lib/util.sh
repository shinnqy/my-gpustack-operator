#!/usr/bin/env bash


function gpustack::util::get_raw_os() {
  local os
  os=$(echo -n "$(uname -s)" | tr '[:upper:]' '[:lower:]')

  case ${os} in
  cygwin_nt*) os="windows" ;;
  mingw*) os="windows" ;;
  msys_nt*) os="windows" ;;
  esac

  echo -n "${os}"
}

function gpustack::util::get_raw_arch() {
  local arch
  arch=$(uname -m)

  case ${arch} in
  armv5*) arch="armv5" ;;
  armv6*) arch="armv6" ;;
  armv7*)
    if [[ "${1:-}" == "--full-name" ]]; then
      arch="armv7"
    else
      arch="arm"
    fi
    ;;
  aarch64) arch="arm64" ;;
  x86) arch="386" ;;
  i686) arch="386" ;;
  i386) arch="386" ;;
  x86_64) arch="amd64" ;;
  esac

  echo -n "${arch}"
}

function gpustack::util::go_module_version() {
  local path
  path="$(command -v "${1}" 2>/dev/null)"
  if [[ -z "${path}" ]]; then
    return 0
  fi
  go version -m "${path}" 2>/dev/null | awk '$1 == "mod" { print $3; exit }'
}

function gpustack::util::go_module_version_is() {
  local installed="${1}" pin="${2}" commit
  if [[ -z "${installed}" ]] || [[ -z "${pin}" ]]; then
    return 1
  fi
  if [[ "${installed}" == "${pin}" ]]; then
    return 0
  fi

  commit="${installed##*-}"
  if [[ "${commit}" == "${installed}" ]]; then
    return 1
  fi
  if [[ ${#commit} -lt 7 ]] || [[ ${#pin} -lt 7 ]]; then
    return 1
  fi
  if [[ "${pin}" == "${commit}"* ]] || [[ "${commit}" == "${pin}"* ]]; then
    return 0
  fi
  return 1
}

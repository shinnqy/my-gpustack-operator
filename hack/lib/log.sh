#!/usr/bin/env bash

log_level="${LOG_LEVEL:-"debug"}"
log_colorful="${LOG_COLORFUL:-"true"}"

gpustack::log::info() {
  [[ ${log_level} == "debug" ]] || [[ ${log_level} == "info" ]] || return 0
  local message="${2:-}"

  local timestamp
  timestamp="$(date +"[%m%d %H:%M:%S]")"
  if [[ ${log_colorful} == "true" ]]; then
    echo -e "\033[34m[INFO]\033[0m ${timestamp} ${1-}" >&2
  else
    echo -e "[INFO] ${timestamp} ${1-}" >&2
  fi
  shift 1
  for message; do
    echo -e "       ${message}" >&2
  done
}

gpustack::log::error() {
  local message="${2:-}"

  local timestamp
  timestamp="$(date +"[%m%d %H:%M:%S]")"
  if [[ ${log_colorful} == "true" ]]; then
    echo -e "\033[31m[ERRO]\033[0m ${timestamp} ${1-}" >&2
  else
    echo -e "[ERRO] ${timestamp} ${1-}" >&2
  fi
  shift 1
  for message; do
    echo -e "       ${message}" >&2
  done
}

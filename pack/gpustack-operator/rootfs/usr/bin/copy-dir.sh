#!/usr/bin/env bash
#
# 参数依次为源目录与主机目标目录。
#
# 递归同步普通文件并保留相对路径，按需创建目录。仅在目标缺失或校验和不同时复制。
#
# 按校验和避免重复写入，防止覆盖运行中容器已经映射的库文件。
#
set -euo pipefail

src="${1:?usage: copy-dir.sh <src> <dst>}"
dst="${2:?usage: copy-dir.sh <src> <dst>}"

if [[ ! -d "${src}" ]]; then
    echo "copy-dir.sh: source directory not found: ${src}" >&2
    exit 1
fi

mkdir -p "${dst}"

# 用 NUL 分隔相对路径，遍历时保留文件名中的空格和换行。
while IFS= read -r -d '' rel; do
    s="${src}/${rel}"
    d="${dst}/${rel}"
    if [[ -f "${d}" ]]; then
        s_sum="$(sha256sum "${s}" | awk '{ print $1 }')"
        d_sum="$(sha256sum "${d}" | awk '{ print $1 }')"
        if [[ "${s_sum}" == "${d_sum}" ]]; then
            continue
        fi
    fi
    mkdir -p "$(dirname "${d}")"
    cp -f --preserve=mode,timestamps "${s}" "${d}"
done < <(cd "${src}" && find . -type f -printf '%P\0')

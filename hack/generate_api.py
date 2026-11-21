"""从手写类型运行项目的 Go 生成器；只写回 Worker 使用的客户端包。"""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
CLIENTS = Path("pkg/kubeclients")
WEBHOOK = Path("pkg/worker/webhooks/worker/zz_generated.webhooks.go")
GENERATED_HEADER = re.compile(rb"^// Code generated [^\n]*DO NOT EDIT\.[ \t\r]*$", re.MULTILINE)


def generated_files(root):
    """从生成标记识别输出，保留手写的 SwaggerDoc 和 client 扩展。"""
    files = set()
    for directory in ("api", CLIENTS):
        for path in (root / directory).rglob("*"):
            if path.name in {"generated.proto", "zz_generated.openapi.violation_exceptions"}:
                files.add(path.relative_to(root))
            elif path.suffix == ".go" and path.is_file():
                with path.open("rb") as stream:
                    if GENERATED_HEADER.search(stream.read(1024)):
                        files.add(path.relative_to(root))
    if (root / WEBHOOK).is_file():
        files.add(WEBHOOK)
    return files


def generate(work):
    # go-to-protobuf 用物理目录末尾的模块路径确定输出位置。
    work.mkdir(parents=True)
    for name in ("api", "cmd", "gen", "hack", "pkg", "staging"):
        shutil.copytree(ROOT / name, work / name)
    for name in ("go.mod", "go.sum"):
        shutil.copyfile(ROOT / name, work / name)
    for relative in generated_files(work):
        (work / relative).unlink()

    subprocess.run(["go", "run", "-mod=readonly", "./gen/api"], cwd=work, check=True)
    # typed client 又会导入 applyconfiguration；按实际 Go 依赖保留这两层。
    packages = subprocess.check_output(
        ["go", "list", "-mod=readonly", "-deps", "-f",
         "{{if .Module}}{{if .Module.Main}}{{.ImportPath}}{{end}}{{end}}",
         "./cmd/gpustack-operator"],
        cwd=work, text=True,
    )
    prefix = "gpustack.ai/gpustack/"
    used = {Path(line[len(prefix):]) for line in packages.splitlines() if line.startswith(prefix)}
    return {p for p in generated_files(work)
            if CLIENTS not in p.parents or p.parent in used}


def main():
    with tempfile.TemporaryDirectory(prefix="gpustack-api-") as scratch:
        work = Path(scratch) / "gpustack.ai" / "gpustack"
        previous = generated_files(ROOT)
        current = generate(work)
        for path in current - previous:
            if (ROOT / path).exists():
                raise FileExistsError(f"生成路径已有手写文件 {path}")
        for path in sorted(current):
            source = work / path
            target = ROOT / path
            if target.is_file() and target.read_bytes() == source.read_bytes():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            # 临时文件与目标位于同一文件系统，中断不会留下半个 Go 文件。
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as temp:
                temp_path = Path(temp.name)
            try:
                shutil.copy2(source, temp_path)
                os.replace(temp_path, target)
            finally:
                temp_path.unlink(missing_ok=True)
        for path in sorted(previous - current):
            (ROOT / path).unlink()
        print(f"已生成并核对 {len(current)} 个 API 和客户端文件")


if __name__ == "__main__":
    main()

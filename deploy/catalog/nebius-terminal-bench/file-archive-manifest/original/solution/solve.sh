#!/usr/bin/env bash
set -euo pipefail

cat > /app/build_manifest.py <<'PY'
#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path


def excluded(relative: Path) -> bool:
    parts = relative.parts
    if any(part.startswith(".") for part in parts):
        return True
    if any(part in {"tmp", "backup", "backups"} for part in parts):
        return True
    return relative.name.endswith(".tmp")


def build_manifest(root: Path) -> dict:
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if excluded(relative):
            continue
        data = path.read_bytes()
        files.append(
            {
                "path": relative.as_posix(),
                "size_bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    return {"files": files}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = Path(args.root)
    output = Path(args.output)
    output.write_text(json.dumps(build_manifest(root), sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
PY

python /app/build_manifest.py --root /app/archive_src --output /app/archive_manifest.json

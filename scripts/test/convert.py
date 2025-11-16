#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def main() -> None:
    payload = json.loads(sys.stdin.read() or "{}")
    inputs = payload.get("input_files") or []
    out_dir = Path(payload.get("extra_args", ["./output"])[0]).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for index, src in enumerate(inputs or [out_dir / "placeholder.txt"]):
        dest = out_dir / f"converted_{index}.dat"
        dest.write_text(f"Converted from {src}\n")
        outputs.append(str(dest))
    json.dump({"output_files": outputs, "is_success": True, "error_message": None}, sys.stdout)


if __name__ == "__main__":
    main()

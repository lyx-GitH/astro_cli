#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def main() -> None:
    payload = json.loads(sys.stdin.read() or "{}")
    inputs = payload.get("input_files") or []
    outputs = []
    for index, src in enumerate(inputs or ["./input.txt"]):
        dest = Path(src).with_suffix(f".filtered{index}")
        dest.write_text(f"Filtered: {src}\n")
        outputs.append(str(dest))
    json.dump({"output_files": outputs, "is_success": True, "error_message": None}, sys.stdout)


if __name__ == "__main__":
    main()

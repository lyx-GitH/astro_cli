#!/usr/bin/env python3
from __future__ import annotations
import json
import sys
from pathlib import Path


def main() -> None:
    payload = json.loads(sys.stdin.read() or "{}")
    inputs = payload.get("input_files") or []
    buffer_path = payload.get("output_buffer")
    output = Path(payload.get("extra_args", ["./aggregate.out"])[0]).resolve()
    content = "\n".join(inputs) if inputs else "no inputs"
    output.write_text(f"Aggregated:\n{content}\n")
    _write_result(buffer_path, {"output_files": [str(output)], "is_success": True, "error_message": None})


def _write_result(buffer_path: str | None, result: dict) -> None:
    if not buffer_path:
        json.dump(result, sys.stdout)
        return
    Path(buffer_path).write_text(json.dumps(result))


if __name__ == "__main__":
    main()

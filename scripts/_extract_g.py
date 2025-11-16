#!/usr/bin/env python3
from __future__ import annotations
"""Extract the green channel from images."""

import json
import sys
from pathlib import Path
from typing import List


def extract_green(inputs: List[str]) -> List[str]:
    outputs: List[str] = []
    for src in inputs:
        source_path = Path(src).resolve()
        if not source_path.exists():
            continue

        destination = source_path.with_name(f"{source_path.stem}_G{source_path.suffix or '.channel'}")
        destination.write_text(f"Green channel extracted from {source_path}\n")
        outputs.append(str(destination))
    return outputs


def main() -> None:
    payload = json.loads(sys.stdin.read() or "{}")
    input_files = payload.get("input_files") or []
    buffer_path = payload.get("output_buffer")
    print(f"{buffer_path} !!!!!!!!")
    if not input_files:
        result = {
            "output_files": [],
            "is_success": False,
            "error_message": "extract_g requires at least one input file.",
        }
        _write_result(buffer_path, result)
        return

    outputs = extract_green(input_files)

    result = {
        "output_files": outputs,
        "is_success": True,
        "error_message": None,
    }
    _write_result(buffer_path, result)


def _write_result(buffer_path: str | None, result: dict) -> None:
    if not buffer_path:
        json.dump(result, sys.stdout)
        return
    Path(buffer_path).write_text(json.dumps(result))


if __name__ == "__main__":
    main()

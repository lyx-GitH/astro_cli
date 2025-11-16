#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def main() -> None:
    payload = json.loads(sys.stdin.read() or "{}")
    inputs = payload.get("input_files") or []
    output = Path(payload.get("extra_args", ["./aggregate.out"])[0]).resolve()
    content = "\n".join(inputs) if inputs else "no inputs"
    output.write_text(f"Aggregated:\n{content}\n")
    json.dump({"output_files": [str(output)], "is_success": True, "error_message": None}, sys.stdout)


if __name__ == "__main__":
    main()

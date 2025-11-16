from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    from . import Engine, visualize
except ImportError:  # pragma: no cover - fallback when running as script
    import os
    import pathlib

    current_dir = pathlib.Path(__file__).resolve().parent
    sys.path.append(str(current_dir.parent))
    from astro_cli import Engine, visualize  # type: ignore


def main() -> None:
    engine = Engine()
    engine.context.scripts_path = Path('/Users/liuyuxuan/astro_cli/astro_cli/scripts')
    engine.context.path = Path('/Users/liuyuxuan/astro_cli/astro_cli/')
    print("Astro CLI interactive mode. Type 'exit' or Ctrl-D to quit.")

    while True:
        try:
            command = input("astro> ").strip()
        except EOFError:
            print()
            break

        if not command:
            continue
        if command.lower() in {"exit", "quit"}:
            break

        try:
            functor = engine.parse(command)
        except Exception as exc:  # noqa: BLE001
            print(f"[parse error] {exc}")
            continue

        print("Functor tree:")
        print(visualize(functor))

        try:
            result = engine.execute(functor)
        except Exception as exc:  # noqa: BLE001
            print(f"[execution error] {exc}")
            continue

        print("Result:")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

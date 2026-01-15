from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from . import Context, visualize
except ImportError:  # pragma: no cover - fallback when running as script
    import pathlib

    current_dir = pathlib.Path(__file__).resolve().parent
    sys.path.append(str(current_dir.parent))
    from astro_cli import Context, visualize  # type: ignore


def main() -> None:
    args = _parse_args()
    scripts_path = Path(args.scripts_path).resolve() if args.scripts_path else None
    context = Context(path=Path.cwd(), scripts_path=scripts_path)
    engine = context.engine
    verbose = args.verbose

    print(
        f"Starting Astro CLI with path={context.path} "
        f"scripts_path={context.scripts_path} debug={args.debug} verbose={verbose}"
    )
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
            functor = engine.parse(context, command)
        except Exception as exc:  # noqa: BLE001
            print(f"[parse error] {exc}")
            continue

        if args.debug:
            print("Functor tree:")
            print(visualize(functor))

        try:
            result = engine.execute(context, functor)
        except Exception as exc:  # noqa: BLE001
            print(f"[execution error] {exc}")
            continue

        _print_result(result, verbose)


def _print_result(result: dict, verbose: bool) -> None:
    """Print execution result based on verbosity setting."""
    if verbose:
        print("Result:")
        print(json.dumps(result, indent=2))
    else:
        is_success = result.get("is_success", False)
        if is_success:
            output_files = result.get("output_files", [])
            if output_files:
                for f in output_files:
                    print(f)
        else:
            error_message = result.get("error_message", "Unknown error")
            print(f"[error] {error_message}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Astro CLI interactive shell.")
    parser.add_argument(
        "--scripts_path",
        type=str,
        default=None,
        help="Directory containing user-defined scripts.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print parsed functor tree before executing commands.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full JSON output instead of simplified result.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()

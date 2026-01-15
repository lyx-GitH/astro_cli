from __future__ import annotations

import argparse
import glob
import json
import os
import readline
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

    # Set up tab completion
    completer = Completer(context)
    readline.set_completer(completer.complete)
    readline.set_completer_delims(" \t\n;")
    readline.parse_and_bind("tab: complete")

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


class Completer:
    """Tab completion for commands, file paths, and Unix commands."""

    def __init__(self, context: Context) -> None:
        self.context = context
        self._matches: list[str] = []
        self._unix_commands: list[str] | None = None

    def _get_unix_commands(self) -> list[str]:
        """Get available Unix commands from PATH."""
        if self._unix_commands is not None:
            return self._unix_commands
        commands: set[str] = set()
        path_dirs = os.environ.get("PATH", "").split(os.pathsep)
        for d in path_dirs:
            if os.path.isdir(d):
                try:
                    for f in os.listdir(d):
                        full_path = os.path.join(d, f)
                        if os.access(full_path, os.X_OK):
                            commands.add(f)
                except PermissionError:
                    continue
        self._unix_commands = sorted(commands)
        return self._unix_commands

    def _get_commands(self) -> list[str]:
        """Get all available commands (system, user scripts, Unix)."""
        commands: list[str] = []
        # System commands (prefixed with :)
        commands.extend(f":{name}" for name in self.context.system_funcs.keys())
        # User scripts
        if self.context.scripts_path.exists():
            for script in self.context.scripts_path.glob("*.py"):
                commands.append(script.stem)
        # Unix commands
        commands.extend(self._get_unix_commands())
        return commands

    def _get_file_completions(self, text: str) -> list[str]:
        """Get file path completions."""
        if not text:
            text = "./"
        # Handle ~ expansion
        expanded = os.path.expanduser(text)
        # Use glob to find matches
        pattern = expanded + "*"
        matches = glob.glob(pattern)
        completions: list[str] = []
        for match in matches:
            # Add trailing slash for directories
            if os.path.isdir(match):
                match += "/"
            # Convert back to use ~ if original used it
            if text.startswith("~"):
                home = os.path.expanduser("~")
                if match.startswith(home):
                    match = "~" + match[len(home):]
            completions.append(match)
        return sorted(completions)

    def complete(self, text: str, state: int) -> str | None:
        """Return the next possible completion for text."""
        if state == 0:
            line = readline.get_line_buffer()
            begidx = readline.get_begidx()
            # Check if we're completing the first word (command position)
            prefix = line[:begidx].lstrip()
            if not prefix:
                # Command position: complete commands
                commands = self._get_commands()
                if text:
                    self._matches = [c for c in commands if c.startswith(text)]
                else:
                    self._matches = commands
            else:
                # Argument position: complete file paths
                self._matches = self._get_file_completions(text)
        try:
            return self._matches[state]
        except IndexError:
            return None


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

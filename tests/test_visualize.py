from __future__ import annotations

from astro_cli import Context, parse, visualize


def build_context() -> Context:
    return Context(
        path=".",
        scripts_path="astro_cli/scripts",
        system_funcs={
            "set": lambda payload, ctx: {
                "output_files": [],
                "is_success": True,
                "error_message": None,
            }
        },
    )


def showcase(commands: list[str]) -> None:
    ctx = build_context()
    for cmd in commands:
        functor = parse(cmd, ctx)
        tree = visualize(functor)
        print(f"Command: {cmd}")
        print(tree)
        print("-" * 40)


def main() -> None:
    commands = [
        "ls -la | pwd",
        "test/filter image1.png image2.png -strength high | test/aggregate -o ./tmp/agg.txt",
        "(ls -1, test/convert ./tmp -preset fast) | (ls -1, test/convert ./tmp -preset fast) | test/aggregate -o ./tmp/all.out",
        "(test/filter photo.jpg -strength low, test/convert ./tmp -preset slow) | test/aggregate -o ./tmp/report.out",
        ":set mode debug | ls -l",
    ]
    showcase(commands)


if __name__ == "__main__":
    main()

from __future__ import annotations

from typing import List

from .functors import (
    BuiltinFunctor,
    Functor,
    ParallelFunctor,
    SequentialFunctor,
    SystemFunctor,
    UserDefinedFunctor,
)


def visualize(functor: Functor) -> str:
    """
    Produce a human-readable tree representation of a functor hierarchy.
    """
    lines: List[str] = []
    _render(functor, lines, 0)
    return "\n".join(lines)


def _render(functor: Functor, lines: List[str], depth: int) -> None:
    indent = "  " * depth
    descriptor = _describe_functor(functor)
    lines.append(f"{indent}{descriptor}")

    if isinstance(functor, SequentialFunctor):
        for child in functor.functors:
            _render(child, lines, depth + 1)
    elif isinstance(functor, ParallelFunctor):
        for child in functor.functors:
            _render(child, lines, depth + 1)


def _describe_functor(functor: Functor) -> str:
    base = f"{functor.name} ({functor.__class__.__name__})"
    details: List[str] = []

    default_inputs = getattr(functor, "_default_input_files", None)
    if default_inputs:
        details.append(f"inputs={list(default_inputs)}")

    default_args = getattr(functor, "_default_extra_args", None)
    if default_args:
        details.append(f"args={list(default_args)}")

    if isinstance(functor, BuiltinFunctor):
        details.append(f"command={functor.command}")
    elif isinstance(functor, UserDefinedFunctor):
        details.append(f"script={functor.script_path}")
    elif isinstance(functor, SystemFunctor):
        details.append("system")

    if not details:
        return base

    return f"{base} [{' | '.join(details)}]"

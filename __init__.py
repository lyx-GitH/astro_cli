"""Core primitives for the Astro CLI prototype."""

from .functors import (
    BuiltinFunctor,
    Context,
    Functor,
    FunctorExecutionError,
    SystemFunctor,
    UserDefinedFunctor,
    pipe_process,
)

__all__ = [
    "Context",
    "Functor",
    "BuiltinFunctor",
    "UserDefinedFunctor",
    "SystemFunctor",
    "FunctorExecutionError",
    "pipe_process",
]

"""Core primitives for the Astro CLI prototype."""

from .context import Context
from .functors import (
    BuiltinFunctor,
    Functor,
    FunctorExecutionError,
    ParallelFunctor,
    SequentialFunctor,
    SystemFunctor,
    UserDefinedFunctor,
)
from .parser import parse
from .pipeline import pipe_process

__all__ = [
    "Context",
    "Functor",
    "BuiltinFunctor",
    "UserDefinedFunctor",
    "SequentialFunctor",
    "ParallelFunctor",
    "SystemFunctor",
    "FunctorExecutionError",
    "parse",
    "pipe_process",
]

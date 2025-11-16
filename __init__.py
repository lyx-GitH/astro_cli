"""Core primitives for the Astro CLI prototype."""

from .functors import BuiltinFunctor, Functor, FunctorExecutionError, UserDefinedFunctor, pipe_process

__all__ = ["Functor", "BuiltinFunctor", "UserDefinedFunctor", "FunctorExecutionError", "pipe_process"]

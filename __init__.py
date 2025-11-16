"""Core primitives for the Astro CLI prototype."""

from .functors import Functor, FunctorExecutionError, pipe_process

__all__ = ["Functor", "FunctorExecutionError", "pipe_process"]

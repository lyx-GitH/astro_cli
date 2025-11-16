from __future__ import annotations

from typing import Any, Mapping

from .context import Context
from .parser import parse
from .pipeline import pipe_process


class Engine:
    """CLI engine that parses commands and executes functor pipelines."""

    def __init__(self, context: Context | None = None) -> None:
        self.context = context or Context()

    def run(self, command: str, payload: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        functor = self.parse(command)
        return self.execute(functor, payload)

    def parse(self, command: str):
        return parse(command, self.context)

    def execute(self, functor, payload: Mapping[str, Any] | None = None):
        return functor(self.context, payload)

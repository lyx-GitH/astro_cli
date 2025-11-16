from __future__ import annotations

from typing import Any, Mapping, TYPE_CHECKING

from .parser import parse
from .pipeline import pipe_process

if TYPE_CHECKING:
    from .context import Context
    from .functors import Functor


class Engine:
    """CLI engine that parses commands and executes functor pipelines."""

    def run(
        self,
        context: "Context",
        command: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        functor = self.parse(context, command)
        return self.execute(context, functor, payload)

    def parse(self, context: "Context", command: str) -> "Functor":
        return parse(command, context)

    def execute(
        self,
        context: "Context",
        functor: "Functor",
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        return functor(context, payload)

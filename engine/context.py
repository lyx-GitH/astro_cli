from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Mapping, MutableMapping, TYPE_CHECKING

JsonMapping = MutableMapping[str, Any]
InputPayload = Mapping[str, Any]
OutputPayload = Dict[str, Any]

INPUT_FIELDS = {"input_files", "extra_args"}
OUTPUT_FIELDS = {"output_files", "is_success", "error_message"}

if TYPE_CHECKING:
    from .engine import Engine
    from .functors import Functor


SystemFunc = Callable[[JsonMapping, "Context"], OutputPayload]


class Context:
    """Holds CLI runtime state such as current path, history, and system functions."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        system_funcs: Mapping[str, SystemFunc] | None = None,
        scripts_path: str | Path | None = None,
        engine: "Engine" | None = None,
    ) -> None:
        base_path = Path(path).resolve() if path else Path.cwd()
        self.path = base_path
        self.history: list[str] = []
        self.system_funcs: dict[str, SystemFunc] = dict(system_funcs or {})
        self.scripts_path = Path(scripts_path).resolve() if scripts_path else base_path / "scripts"
        if engine is None:
            from .engine import Engine as EngineClass

            engine = EngineClass()
        self.engine = engine

    def record_history(self, functor: "Functor", input_payload: JsonMapping) -> None:
        entry = self._serialize_history(functor, input_payload)
        self.history.append(entry)

    def _serialize_history(self, functor: "Functor", input_payload: JsonMapping) -> str:
        joined_input = " ".join(input_payload.get("input_files", []))
        joined_args = " ".join(input_payload.get("extra_args", []))
        parts = [functor.name]
        if joined_input:
            parts.append(joined_input)
        if joined_args:
            parts.append(joined_args)
        return " ".join(parts)

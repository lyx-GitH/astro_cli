from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Union

JsonMapping = MutableMapping[str, Any]


class FunctorExecutionError(RuntimeError):
    """Raised when a functor produces malformed data."""


class Functor(ABC):
    """Base class for CLI commands wrapped as functors."""

    def __init__(
        self,
        name: str,
        default_input_files: Sequence[str] | None = None,
    ) -> None:
        self.name = name
        self._default_input_files = list(default_input_files) if default_input_files else None

    def __call__(self, payload: Mapping[str, Any] | None = None) -> Dict[str, Any]:
        """Normalize the input payload, execute the functor, and validate the output."""
        normalized = self._normalize_input(payload)
        output = self.execute(normalized)
        self._validate_output(output)
        return output

    def _normalize_input(self, payload: Mapping[str, Any] | None) -> JsonMapping:
        normalized: JsonMapping = dict(payload or {})
        files = normalized.get("input_files")
        if not files:
            if self._default_input_files is not None:
                normalized["input_files"] = list(self._default_input_files)
            else:
                normalized["input_files"] = [str(Path.cwd())]
        return normalized

    def _validate_output(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            raise FunctorExecutionError(f"Functor '{self.name}' returned a non-mapping payload.")

        if "is_success" not in payload:
            raise FunctorExecutionError(f"Functor '{self.name}' did not include 'is_success'.")

        if "output_files" not in payload:
            raise FunctorExecutionError(f"Functor '{self.name}' did not include 'output_files'.")

    @abstractmethod
    def execute(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        """Perform the command's work using normalized JSON input."""


def pipe_process(functors: Sequence[Functor], input_json: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    """
    Sequentially execute functors, piping output_files to the next command's input_files.
    """
    if not functors:
        raise ValueError("pipe_process requires at least one functor.")

    current_payload: JsonMapping = dict(input_json or {})
    if not current_payload.get("input_files"):
        current_payload["input_files"] = [str(Path.cwd())]

    last_output: Dict[str, Any] = dict(current_payload)
    for functor in functors:
        result = functor(current_payload)

        if not result.get("is_success"):
            failure_output = dict(result)
            failure_output.setdefault("failed_at", functor.name)
            return failure_output

        output_files = result.get("output_files")
        if not output_files:
            raise FunctorExecutionError(
                f"Functor '{functor.name}' produced an empty 'output_files' value."
            )

        current_payload = dict(result)
        current_payload["input_files"] = output_files
        last_output = result

    return last_output

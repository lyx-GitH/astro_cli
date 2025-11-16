from __future__ import annotations

from abc import ABC, abstractmethod
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, MutableMapping, Sequence

JsonMapping = MutableMapping[str, Any]
InputPayload = Mapping[str, Any]
OutputPayload = Dict[str, Any]

INPUT_FIELDS = {"input_files", "extra_args"}
OUTPUT_FIELDS = {"output_files", "is_success", "error_message"}

SystemFunc = Callable[[JsonMapping, "Context"], OutputPayload]


class Context:
    """Holds CLI runtime state such as current path, history, and system functions."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        system_funcs: Mapping[str, SystemFunc] | None = None,
    ) -> None:
        self.path = Path(path).resolve() if path else Path.cwd()
        self.history: list[str] = []
        self.system_funcs: dict[str, SystemFunc] = dict(system_funcs or {})

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


class FunctorExecutionError(RuntimeError):
    """Raised when a functor produces malformed data."""


class Functor(ABC):
    """Base class for CLI commands wrapped as functors."""

    def __init__(
        self,
        name: str,
        *,
        default_input_files: Sequence[str] | None = None,
        default_extra_args: Sequence[str] | None = None,
    ) -> None:
        self.name = name
        self._default_input_files = list(default_input_files) if default_input_files is not None else None
        self._default_extra_args = list(default_extra_args) if default_extra_args is not None else None

    def __call__(self, context: Context, payload: InputPayload | None = None) -> OutputPayload:
        """Normalize the input payload, execute the functor, and validate the output."""
        normalized = self._normalize_input(context, payload)
        output = self.execute(context, normalized)
        self._validate_output(output)
        if self._should_record_history():
            context.record_history(functor=self, input_payload=normalized)
        return output

    def _normalize_input(self, context: Context, payload: InputPayload | None) -> JsonMapping:
        if payload:
            unknown = set(payload.keys()) - INPUT_FIELDS
            if unknown:
                raise FunctorExecutionError(
                    f"Functor '{self.name}' received unsupported fields: {', '.join(sorted(unknown))}"
                )

        normalized: JsonMapping = {
            "input_files": list(payload.get("input_files", [])) if payload else [],
            "extra_args": list(payload.get("extra_args", [])) if payload else [],
        }

        if not normalized["input_files"]:
            if self._default_input_files is not None:
                normalized["input_files"] = list(self._default_input_files)
            else:
                normalized["input_files"] = [str(context.path)]

        if not normalized["extra_args"] and self._default_extra_args is not None:
            normalized["extra_args"] = list(self._default_extra_args)

        return normalized

    def _validate_output(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            raise FunctorExecutionError(f"Functor '{self.name}' returned a non-mapping payload.")

        missing = OUTPUT_FIELDS - payload.keys()
        if missing - {"error_message"}:
            raise FunctorExecutionError(
                f"Functor '{self.name}' output missing fields: {', '.join(sorted(missing))}"
            )

        unknown = set(payload.keys()) - OUTPUT_FIELDS
        if unknown:
            raise FunctorExecutionError(
                f"Functor '{self.name}' output included unsupported fields: {', '.join(sorted(unknown))}"
            )

    def _should_record_history(self) -> bool:
        return True

    @abstractmethod
    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        """Perform the command's work using normalized JSON input."""


class BuiltinFunctor(Functor):
    """Functor implementation for built-in shell commands."""

    def __init__(
        self,
        name: str,
        command: Sequence[str],
        *,
        cwd: str | Path | None = None,
        default_extra_args: Sequence[str] | None = None,
    ) -> None:
        super().__init__(name, default_input_files=[], default_extra_args=default_extra_args)
        self.command = list(command)
        self.cwd = str(cwd) if cwd else None

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        input_files = payload.get("input_files", [])
        extra_args = payload.get("extra_args", [])

        cmd = list(self.command)
        if extra_args:
            cmd.extend(extra_args)

        stdin_data = "\n".join(input_files) if input_files else None

        try:
            completed = subprocess.run(
                cmd,
                input=stdin_data,
                capture_output=True,
                text=True,
                check=False,
                cwd=self.cwd or str(context.path),
            )
        except FileNotFoundError as exc:
            return {
                "output_files": input_files,
                "is_success": False,
                "error_message": str(exc),
            }

        is_success = completed.returncode == 0
        stdout_lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        output_files = stdout_lines if stdout_lines else list(input_files)

        error_message = completed.stderr.strip() if not is_success else ""

        return {
            "output_files": output_files,
            "is_success": is_success,
            "error_message": error_message or None,
        }


class UserDefinedFunctor(Functor):
    """Functor that executes a Python script located in the scripts directory."""

    def __init__(
        self,
        name: str,
        script_path: str | Path,
        *,
        python_executable: str | None = None,
        cwd: str | Path | None = None,
    ) -> None:
        super().__init__(name)
        self.script_path = str(script_path)
        self.python_executable = python_executable or sys.executable
        self.cwd = str(cwd) if cwd else None

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        serialized = json.dumps(payload)
        try:
            completed = subprocess.run(
                [self.python_executable, self.script_path],
                input=serialized,
                text=True,
                capture_output=True,
                check=False,
                cwd=self.cwd or str(context.path),
            )
        except FileNotFoundError as exc:
            return {
                "output_files": payload.get("input_files", []),
                "is_success": False,
                "error_message": str(exc),
            }

        if completed.returncode != 0:
            return {
                "output_files": payload.get("input_files", []),
                "is_success": False,
                "error_message": completed.stderr.strip() or "Script execution failed.",
            }

        try:
            output_payload = json.loads(completed.stdout or "{}")
        except json.JSONDecodeError as exc:
            return {
                "output_files": payload.get("input_files", []),
                "is_success": False,
                "error_message": f"Invalid JSON output: {exc}",
            }

        return output_payload


class SystemFunctor(Functor):
    """Functor that delegates to context-registered system functions."""

    def __init__(self, name: str) -> None:
        super().__init__(name)

    def _should_record_history(self) -> bool:
        return False

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        handler = context.system_funcs.get(self.name)
        if handler is None:
            raise FunctorExecutionError(
                f"System functor '{self.name}' is not registered in the context."
            )
        return handler(payload, context)


def _normalize_initial_payload(context: Context, payload: InputPayload | None) -> JsonMapping:
    normalized: JsonMapping = {
        "input_files": [str(context.path)],
        "extra_args": [],
    }
    if not payload:
        return normalized

    unknown = set(payload.keys()) - INPUT_FIELDS
    if unknown:
        raise FunctorExecutionError(
            f"Pipeline received unsupported fields: {', '.join(sorted(unknown))}"
        )

    if "input_files" in payload:
        normalized["input_files"] = list(payload["input_files"])

    if "extra_args" in payload:
        normalized["extra_args"] = list(payload["extra_args"])

    return normalized


def pipe_process(
    functors: Sequence[Functor],
    context: Context,
    input_json: InputPayload | None = None,
) -> OutputPayload:
    """
    Sequentially execute functors, piping output_files to the next command's input_files.
    """
    if not functors:
        raise ValueError("pipe_process requires at least one functor.")

    current_payload = _normalize_initial_payload(context, input_json)
    last_output: OutputPayload = {
        "output_files": list(current_payload["input_files"]),
        "is_success": True,
        "error_message": None,
    }

    for functor in functors:
        result = functor(context, current_payload)

        if not result.get("is_success"):
            error_message = result.get("error_message") or "Unknown error."
            print(f"[pipe] {functor.name} failed: {error_message}", file=sys.stderr)
            failure_output = dict(result)
            failure_output.setdefault("failed_at", functor.name)
            return failure_output

        output_files = result.get("output_files") or []
        if not output_files:
            raise FunctorExecutionError(
                f"Functor '{functor.name}' produced an empty 'output_files' value."
            )

        current_payload = {
            "input_files": list(output_files),
            "extra_args": [],
        }
        last_output = result

    return last_output

from __future__ import annotations

from abc import ABC, abstractmethod
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Sequence

JsonMapping = MutableMapping[str, Any]
InputPayload = Mapping[str, Any]
OutputPayload = Dict[str, Any]

INPUT_FIELDS = {"input_files", "extra_args"}
OUTPUT_FIELDS = {"output_files", "is_success", "error_message"}


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
        self._default_input_files = list(default_input_files) if default_input_files else None
        self._default_extra_args = list(default_extra_args) if default_extra_args else None

    def __call__(self, payload: InputPayload | None = None) -> OutputPayload:
        """Normalize the input payload, execute the functor, and validate the output."""
        normalized = self._normalize_input(payload)
        output = self.execute(normalized)
        self._validate_output(output)
        return output

    def _normalize_input(self, payload: InputPayload | None) -> JsonMapping:
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

        if not normalized["input_files"] and self._default_input_files is not None:
            normalized["input_files"] = list(self._default_input_files)

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

    @abstractmethod
    def execute(self, payload: JsonMapping) -> OutputPayload:
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

    def execute(self, payload: JsonMapping) -> OutputPayload:
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
                cwd=self.cwd,
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
        super().__init__(name, default_input_files=[str(Path.cwd())])
        self.script_path = str(script_path)
        self.python_executable = python_executable or sys.executable
        self.cwd = str(cwd) if cwd else None

    def execute(self, payload: JsonMapping) -> OutputPayload:
        serialized = json.dumps(payload)
        try:
            completed = subprocess.run(
                [self.python_executable, self.script_path],
                input=serialized,
                text=True,
                capture_output=True,
                check=False,
                cwd=self.cwd,
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

def _normalize_initial_payload(payload: InputPayload | None) -> JsonMapping:
    normalized: JsonMapping = {
        "input_files": [],
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


def pipe_process(functors: Sequence[Functor], input_json: InputPayload | None = None) -> OutputPayload:
    """
    Sequentially execute functors, piping output_files to the next command's input_files.
    """
    if not functors:
        raise ValueError("pipe_process requires at least one functor.")

    current_payload = _normalize_initial_payload(input_json)
    last_output: OutputPayload = {
        "output_files": list(current_payload["input_files"]),
        "is_success": True,
        "error_message": None,
    }

    for functor in functors:
        result = functor(current_payload)

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

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
import json
import os
import pickle
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from .context import Context, INPUT_FIELDS, OUTPUT_FIELDS, InputPayload, JsonMapping, OutputPayload


def _execute_functor_parallel(
    functor: "Functor",
    context_kwargs: Dict[str, Any],
    payload: JsonMapping,
) -> OutputPayload:
    worker_context = Context(**context_kwargs)
    return functor(worker_context, payload)


def _serialize_context_for_parallel(context: Context) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "path": str(context.path),
        "scripts_path": str(context.scripts_path),
    }
    if context.system_funcs:
        picklable_funcs: Dict[str, Any] = {}
        for name, handler in context.system_funcs.items():
            try:
                pickle.dumps(handler)
            except Exception:
                continue
            picklable_funcs[name] = handler
        if picklable_funcs:
            data["system_funcs"] = picklable_funcs
    return data

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
        default_input_files: Sequence[str] | None = None,
        default_extra_args: Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            name,
            default_input_files=default_input_files,
            default_extra_args=default_extra_args,
        )
        self.script_path = str(script_path)
        self.python_executable = python_executable or sys.executable
        self.cwd = str(cwd) if cwd else None

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        buffer_path = self._create_output_buffer()
        payload_with_buffer = dict(payload)
        payload_with_buffer["output_buffer"] = buffer_path
        print(f"{buffer_path=}")
        serialized = json.dumps(payload_with_buffer)
        try:
            completed = subprocess.run(
                [self.python_executable, self.script_path],
                input=serialized,
                text=True,
                capture_output=False, # prints in the user scripts will be emitted
                check=False,
                cwd=self.cwd or str(context.path),
            )
        except FileNotFoundError as exc:
            self._cleanup_buffer(buffer_path)
            return {
                "output_files": payload.get("input_files", []),
                "is_success": False,
                "error_message": str(exc),
            }

        output_payload, buffer_error = self._load_output_buffer(buffer_path)
        self._cleanup_buffer(buffer_path)

        if buffer_error:
            return {
                "output_files": payload.get("input_files", []),
                "is_success": False,
                "error_message": buffer_error,
            }

        if completed.returncode != 0 and output_payload.get("is_success", True):
            return {
                "output_files": output_payload.get("output_files") or payload.get("input_files", []),
                "is_success": False,
                "error_message": completed.stderr.strip() or "Script execution failed.",
            }

        return output_payload

    def _create_output_buffer(self) -> str:
        buffer = tempfile.NamedTemporaryFile(delete=False)
        buffer_path = buffer.name
        buffer.close()
        return buffer_path

    def _load_output_buffer(self, buffer_path: str) -> tuple[OutputPayload | None, str | None]:
        try:
            raw = Path(buffer_path).read_text().strip()
        except FileNotFoundError:
            return None, "Script did not produce an output buffer."

        if not raw:
            return None, "Script wrote an empty output buffer."

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, f"Invalid JSON output: {exc}"

        return payload, None

    def _cleanup_buffer(self, buffer_path: str) -> None:
        try:
            os.remove(buffer_path)
        except FileNotFoundError:
            pass


class SystemFunctor(Functor):
    """Functor that delegates to context-registered system functions."""

    def __init__(self, name: str, *, default_extra_args: Sequence[str] | None = None) -> None:
        super().__init__(name, default_input_files=[], default_extra_args=default_extra_args)

    def _should_record_history(self) -> bool:
        return False

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        handler = context.system_funcs.get(self.name)
        if handler is None:
            raise FunctorExecutionError(
                f"System functor '{self.name}' is not registered in the context."
            )
        return handler(payload, context)


class SequentialFunctor(Functor):
    """Functor that composes a sequence of sub-functors sequentially."""

    def __init__(self, name: str, functors: Sequence[Functor]) -> None:
        if not functors:
            raise ValueError("SequentialFunctor requires at least one sub-functor.")
        super().__init__(name)
        self.functors = list(functors)

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        current_payload: JsonMapping = {
            "input_files": list(payload.get("input_files", [])),
            "extra_args": list(payload.get("extra_args", [])),
        }
        last_result: OutputPayload | None = None

        for functor in self.functors:
            result = functor(context, current_payload)
            if not result.get("is_success"):
                return result

            output_files = result.get("output_files") or []
            if not output_files:
                raise FunctorExecutionError(
                    f"Functor '{functor.name}' produced an empty 'output_files' value."
                )

            current_payload = {
                "input_files": list(output_files),
                "extra_args": [],
            }
            last_result = result

        return last_result or {
            "output_files": list(payload.get("input_files", [])),
            "is_success": True,
            "error_message": None,
        }


class ParallelFunctor(Functor):
    """Functor that executes multiple sub-functors in parallel and aggregates their outputs."""

    def __init__(self, name: str, functors: Sequence[Functor]) -> None:
        if not functors:
            raise ValueError("ParallelFunctor requires at least one sub-functor.")
        super().__init__(name)
        self.functors = list(functors)

    def execute(self, context: Context, payload: JsonMapping) -> OutputPayload:
        base_input_files = list(payload.get("input_files", []))
        base_extra_args = list(payload.get("extra_args", []))

        context_kwargs = _serialize_context_for_parallel(context)

        tasks = []
        with ProcessPoolExecutor(max_workers=len(self.functors)) as executor:
            for functor in self.functors:
                functor_payload: JsonMapping = {
                    "input_files": list(base_input_files),
                    "extra_args": list(base_extra_args),
                }
                future = executor.submit(
                    _execute_functor_parallel,
                    functor,
                    context_kwargs,
                    functor_payload,
                )
                tasks.append((functor, future))

        combined_outputs: list[str] = []
        errors: list[str] = []

        for functor, future in tasks:
            try:
                result = future.result()
            except Exception as exc:
                errors.append(f"{functor.name}: {exc}")
                continue

            if not result.get("is_success"):
                message = result.get("error_message") or "Unknown error."
                errors.append(f"{functor.name}: {message}")
                continue

            output_files = result.get("output_files") or []
            if not output_files:
                raise FunctorExecutionError(
                    f"Functor '{functor.name}' produced an empty 'output_files' value."
                )
            combined_outputs.extend(output_files)

        if errors:
            return {
                "output_files": list(base_input_files),
                "is_success": False,
                "error_message": "; ".join(errors),
            }

        return {
            "output_files": combined_outputs,
            "is_success": True,
            "error_message": None,
        }

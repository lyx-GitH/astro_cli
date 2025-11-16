from __future__ import annotations

import sys
from typing import Sequence

from .context import Context, INPUT_FIELDS, InputPayload, JsonMapping, OutputPayload
from .functors import Functor, FunctorExecutionError


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

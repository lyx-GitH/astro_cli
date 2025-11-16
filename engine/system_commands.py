from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any, Dict, List, MutableMapping

JsonMapping = MutableMapping[str, Any]


def get_default_system_funcs():
    return {
        "history": history_command,
        "run": run_command,
        "list": list_command,
    }


def history_command(payload: JsonMapping, context) -> Dict:
    entries = list(context.history)
    return {
        "output_files": entries,
        "is_success": True,
        "error_message": None,
    }


def run_command(payload: JsonMapping, context) -> Dict:
    commands = payload.get("extra_args", [])
    if not commands:
        return {
            "output_files": [],
            "is_success": False,
            "error_message": "run requires commands in extra_args.",
        }

    last_result: Dict | None = None
    for cmd in commands:
        result = context.engine.run(context, cmd)
        if not result.get("is_success"):
            return {
                "output_files": result.get("output_files", []),
                "is_success": False,
                "error_message": f"Command '{cmd}' failed: {result.get('error_message')}",
            }
        last_result = result

    return {
        "output_files": list(last_result.get("output_files", [])) if last_result else [],
        "is_success": True,
        "error_message": None,
    }


def list_command(payload: JsonMapping, context) -> Dict:
    targets: List[str] = []
    if payload.get("input_files"):
        targets.extend(payload["input_files"])
    if payload.get("extra_args"):
        targets.extend(payload["extra_args"])
    if not targets:
        targets.append(str(context.path))

    listed: List[str] = []
    missing: List[str] = []
    for target in targets:
        path = Path(target).expanduser()
        if not path.is_absolute():
            path = (context.path / path).resolve()
        if not path.exists():
            missing.append(str(path))
            continue
        if path.is_dir():
            children = sorted(path.iterdir(), key=lambda p: p.name)
            for child in children:
                listed.append(str(child.resolve()))
        else:
            listed.append(str(path.resolve()))

    return {
        "output_files": listed,
        "is_success": not missing,
        "error_message": None if not missing else f"Missing: {', '.join(missing)}",
    }


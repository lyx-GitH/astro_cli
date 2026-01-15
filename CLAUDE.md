# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Astro CLI is a Python-based terminal tool for image-processing workflows that combines shell commands, user-defined Python scripts, and custom pipeline syntax (sequential `|` and parallel `,` operators). Users can chain tasks like channel extraction, resizing, and conversion directly from the command line.

## Running the CLI

Start the interactive shell:
```bash
python3 -m astro_cli.main --scripts_path <path-to-scripts> [--debug]
```

- `--scripts_path`: Directory for user scripts (defaults to `./scripts/`)
- `--debug`: Print the functor tree before execution

## Architecture

### Core Components

**Engine Flow**: `main.py` → `Engine` → `Parser` → `Functor` execution

1. **Parser** (`engine/parser.py`): Tokenizes command strings and builds a functor tree
   - Handles operators: `|` (sequential), `,` (parallel), `()` (grouping)
   - Distinguishes between system commands (`:prefix`), user scripts (from `scripts_path`), and built-in shell commands
   - Example: `(ls -l, resize ./img -w 100) | convert -o ./out`

2. **Functors** (`engine/functors.py`): Abstract representation of executable commands
   - `Functor`: Base class with normalized JSON input/output contract
   - `BuiltinFunctor`: Wraps shell commands via `subprocess`
   - `UserDefinedFunctor`: Executes Python scripts with JSON I/O via temporary buffer files
   - `SystemFunctor`: Delegates to context-registered system functions (`:history`, `:run`, `:list`)
   - `SequentialFunctor`: Chains functors where output feeds into next input
   - `ParallelFunctor`: Executes functors concurrently using `ProcessPoolExecutor`

3. **Context** (`engine/context.py`): Runtime state holder
   - Manages current path, scripts path, command history
   - Registers system functions (`:history`, `:run`, `:list`)
   - Holds reference to the Engine instance

4. **Engine** (`engine/engine.py`): Orchestrates parsing and execution
   - `parse()`: Converts command string to functor tree
   - `execute()`: Runs functor with optional payload
   - `run()`: Combines parse + execute

### JSON Contract

All functors use a standardized JSON payload format:

**Input** (to functors):
```json
{
  "input_files": ["path1", "path2"],
  "extra_args": ["-flag", "value"]
}
```

**Output** (from functors):
```json
{
  "output_files": ["result1", "result2"],
  "is_success": true,
  "error_message": null
}
```

### Command Types

1. **Built-in shell commands**: Standard Unix commands like `ls`, `pwd`
2. **User scripts**: Python files in `scripts_path`, invoked by filename without `.py`
3. **System commands**: Prefixed with `:` (`:history`, `:run`, `:list`)

### User Script Guidelines

When creating user scripts in `scripts/`:

1. **Filename = Command name**: `scripts/resize.py` becomes command `resize`
2. **Input**: Read JSON from stdin with keys: `input_files`, `extra_args`, `output_buffer`
3. **Output**: Write result JSON to the file path specified in `output_buffer` (not stdout)
4. **Argument parsing**: Tokens before first `-` flag are `input_files`, rest are `extra_args`
5. **Logging**: Use stdout/stderr freely for logs; only the buffer file is parsed

Example script structure (see `scripts/_extract_r.py`):
```python
#!/usr/bin/env python3
import json
import sys
from pathlib import Path

def main():
    payload = json.loads(sys.stdin.read() or "{}")
    input_files = payload.get("input_files", [])
    buffer_path = payload.get("output_buffer")

    # Process input_files...
    outputs = process(input_files)

    result = {
        "output_files": outputs,
        "is_success": True,
        "error_message": None
    }
    Path(buffer_path).write_text(json.dumps(result))
```

## Testing

Run tests:
```bash
python3 -m pytest astro_cli/tests/
```

Run specific test:
```bash
python3 -m pytest astro_cli/tests/test_visualize.py::test_name
```

## Key Implementation Details

- **Parallel execution** uses `ProcessPoolExecutor` with context serialization (pickles system functions)
- **History tracking** records functor name + normalized input (except system commands)
- **Output buffer pattern** for user scripts ensures clean separation between logs and results
- **Error propagation**: Sequential pipelines short-circuit on first failure; parallel functors collect all errors
- **Default input**: When no input files specified, defaults to current working directory path

## Common Patterns

**Sequential pipeline** (output → next input):
```
ls | grep .py | head -n 5
```

**Parallel execution** (same input to multiple functors):
```
(extract_r, extract_g, extract_b) img.png
```

**Mixed operators**:
```
ls *.png | (resize -w 100, resize -w 200) | convert -o output/
```

**System commands**:
```
:history              # Show command history
:run "ls" "pwd"      # Execute multiple commands
:list scripts/       # List directory contents
```

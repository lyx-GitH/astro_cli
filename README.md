## Astro CLI (English)

### Introduction
Astro CLI is a Python-based terminal tool for image-processing workflows. It combines shell commands, user-defined Python scripts, and custom pipeline syntax (sequential + parallel operators) so that users can chain tasks like channel extraction, resizing, and conversion directly from the command line. The project is designed to be script-extensible, with a consistent JSON contract between the engine and user scripts.

### Getting Started
1. Ensure Python 3.10+ is available.  
2. Run the shell:
   ```bash
   python3 -m astro_cli.main --scripts_path <path-to-scripts> [--debug]
   ```
   - `--scripts_path` (optional): directory for user scripts; defaults to `./scripts/`.  
   - `--debug` prints the functor tree before execution.

### Supported Commands
Currently supported:
1. Built-in shell commands (e.g., `ls`, `pwd`).  
2. User scripts placed under `scripts_path`.  
3. System commands prefixed with `:` (currently `:history`, `:run`, `:list`).

### Grammar
- Command form: `name arg1 arg2 ...`.  
- Operators:
  - `|` sequential pipeline (output → next input).  
  - `,` parallel execution (same input for each).  
  - Parentheses `()` to group, e.g. `(cmd1, cmd2) | cmd3`.

Example:
```
(ls -l, resize ./img -w 100) | convert -o ./out
```

### Adding Scripts
Follow `astro_cli/prompts/script_guidelines.txt`:
1. Place `<name>.py` inside `scripts_path` (command is `name`).  
2. Script input JSON (from stdin) includes:
   - `input_files`
   - `extra_args`
   - `output_buffer` (file path to write results)
3. Write the result JSON **to `output_buffer`** only:
   ```json
   {"output_files": [...], "is_success": true, "error_message": null}
   ```
4. Stdout/stderr are free for logs.  
5. Before the first flag-like arg (`-foo`), tokens are treated as `input_files`; the rest become `extra_args`.

---

## Astro CLI（中文）

### 项目简介
Astro CLI 是一个基于 Python 的终端工具，用于处理图像工作流。它支持将系统命令、用户自定义脚本以及自定义管道语法（顺序/并行）组合在一起，让用户在命令行中完成通道提取、尺寸转换等操作。通过统一的 JSON 输入/输出协议，脚本能够方便地扩展整个处理流程。

### 如何启动
1. 确保已安装 Python 3.10+。  
2. 运行交互式终端：
   ```bash
   python3 -m astro_cli.main --scripts_path <脚本目录> [--debug]
   ```
   - `--scripts_path` 可选，默认 `./scripts/`。  
   - `--debug` 显示解析出的 Functor 树。

### 支持的命令
目前支持：
1. 系统内置命令（如 `ls`、`pwd`）。  
2. `scripts_path` 下的用户脚本。  
3. 以 `:` 开头的系统命令，当前提供 `:history`、`:run`、`:list`。

### 语法
- 命令形式：`命令名 参数1 参数2 ...`  
- 运算符：
  - `|` 顺序管道。  
  - `,` 并行执行。  
  - `()` 用于分组，如 `(cmd1, cmd2) | cmd3`。

示例：
```
(ls -l, resize ./img -w 100) | convert -o ./out
```

### 如何添加脚本
遵循 `astro_cli/prompts/script_guidelines.txt`：
1. 在 `scripts_path` 下放置 `<名称>.py`，命令即为 `名称`。  
2. 脚本通过 stdin 读取 JSON，包含：
   - `input_files`  
   - `extra_args`  
   - `output_buffer`（必须写入结果 JSON 的文件路径）
3. 结果 JSON 只写入 `output_buffer`：
   ```json
   {"output_files": [...], "is_success": true, "error_message": null}
   ```
4. stdout/stderr 可用于日志。  
5. 第一个 `-` 参数之前为输入文件，其后都视为额外参数。

遵循上述规则即可新增脚本并参与管道。

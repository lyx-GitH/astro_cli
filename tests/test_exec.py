from __future__ import annotations

import tempfile
from pathlib import Path

from astro_cli import Context
from astro_cli.engine.system_commands import exec_command


def build_context(tmp_path: Path | None = None) -> Context:
    """Build a test context."""
    path = tmp_path or Path.cwd()
    return Context(path=path, scripts_path=path / "scripts")


class TestExecCommand:
    """Tests for the :exec system command."""

    def test_exec_single_command(self, tmp_path: Path) -> None:
        """Test executing a single command from input_files."""
        context = build_context(tmp_path)
        payload = {"input_files": ["pwd"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True
        assert result["error_message"] is None
        assert len(result["output_files"]) > 0

    def test_exec_multiple_commands(self, tmp_path: Path) -> None:
        """Test executing multiple commands sequentially."""
        context = build_context(tmp_path)
        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        payload = {"input_files": ["pwd", "ls"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True
        assert result["error_message"] is None

    def test_exec_skips_empty_lines(self, tmp_path: Path) -> None:
        """Test that empty lines are skipped."""
        context = build_context(tmp_path)
        payload = {"input_files": ["", "pwd", "  ", "ls"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True

    def test_exec_skips_comments(self, tmp_path: Path) -> None:
        """Test that comment lines (starting with #) are skipped."""
        context = build_context(tmp_path)
        payload = {"input_files": ["# this is a comment", "pwd", "# another comment"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True

    def test_exec_empty_input_fails(self, tmp_path: Path) -> None:
        """Test that empty input_files returns error."""
        context = build_context(tmp_path)
        payload = {"input_files": []}

        result = exec_command(payload, context)

        assert result["is_success"] is False
        assert "requires commands" in result["error_message"]

    def test_exec_only_comments_succeeds(self, tmp_path: Path) -> None:
        """Test that input with only comments succeeds with empty output."""
        context = build_context(tmp_path)
        payload = {"input_files": ["# comment 1", "# comment 2"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True
        assert result["output_files"] == []

    def test_exec_failed_command_stops(self, tmp_path: Path) -> None:
        """Test that a failed command stops execution and reports error."""
        context = build_context(tmp_path)
        payload = {"input_files": ["pwd", "nonexistent_command_xyz", "ls"]}

        result = exec_command(payload, context)

        assert result["is_success"] is False
        assert "nonexistent_command_xyz" in result["error_message"]

    def test_exec_collects_all_outputs(self, tmp_path: Path) -> None:
        """Test that outputs from all commands are collected."""
        context = build_context(tmp_path)
        # Create test files
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.txt").write_text("b")

        payload = {"input_files": ["ls"]}

        result = exec_command(payload, context)

        assert result["is_success"] is True
        # ls should output the files
        assert len(result["output_files"]) >= 2


class TestExecInPipeline:
    """Tests for :exec used in pipeline with engine."""

    def test_pipeline_with_exec(self, tmp_path: Path) -> None:
        """Test cat file | :exec pipeline pattern."""
        context = build_context(tmp_path)

        # Create a commands file
        cmd_file = tmp_path / "commands.txt"
        cmd_file.write_text("pwd\nls\n")

        # Run: cat commands.txt | :exec
        result = context.engine.run(context, f"cat {cmd_file} | :exec")

        assert result["is_success"] is True

    def test_history_to_exec(self, tmp_path: Path) -> None:
        """Test :history | :exec to replay commands."""
        context = build_context(tmp_path)

        # Run some commands to build history
        context.engine.run(context, "pwd")
        context.engine.run(context, "ls")

        # Now replay via :history | :exec
        result = context.engine.run(context, ":history | :exec")

        assert result["is_success"] is True

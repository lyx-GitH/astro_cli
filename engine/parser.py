from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .context import Context
from .functors import (
    BuiltinFunctor,
    Functor,
    ParallelFunctor,
    SequentialFunctor,
    SystemFunctor,
    UserDefinedFunctor,
)


class ParseError(ValueError):
    """Raised when parsing a CLI command string fails."""


def parse(command: str, context: "Context") -> Functor:
    tokens = _tokenize(command)
    if not tokens:
        raise ParseError("Empty command.")
    parser = _Parser(tokens, context)
    result = parser.parse_expression()
    parser.expect_end()
    return result


def _tokenize(command: str) -> List[str]:
    tokens: List[str] = []
    current: List[str] = []
    quote: str | None = None
    escaped = False

    for ch in command:
        if quote:
            if escaped:
                current.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                tokens.append("".join(current))
                current = []
                quote = None
            else:
                current.append(ch)
            continue

        if ch in ("'", '"'):
            if current:
                tokens.append("".join(current))
                current = []
            quote = ch
            continue

        if ch.isspace():
            if current:
                tokens.append("".join(current))
                current = []
            continue

        if ch in "|(),":
            if current:
                tokens.append("".join(current))
                current = []
            tokens.append(ch)
        else:
            current.append(ch)

    if quote:
        raise ParseError("Unterminated quote in command.")

    if current:
        tokens.append("".join(current))

    return tokens


@dataclass
class _Parser:
    tokens: Sequence[str]
    context: "Context"
    pos: int = 0

    def parse_expression(self) -> Functor:
        return self._parse_parallel()

    def expect_end(self) -> None:
        if self.pos != len(self.tokens):
            raise ParseError(f"Unexpected token: {self.tokens[self.pos]!r}")

    def _parse_parallel(self) -> Functor:
        functors = [self._parse_sequential()]
        while self._peek() == ",":
            self._consume(",")
            functors.append(self._parse_sequential())
        if len(functors) == 1:
            return functors[0]
        name = f"parallel({', '.join(f.name for f in functors)})"
        return ParallelFunctor(name, functors)

    def _parse_sequential(self) -> Functor:
        functors = [self._parse_block()]
        while self._peek() == "|":
            self._consume("|")
            functors.append(self._parse_block())
        if len(functors) == 1:
            return functors[0]
        name = f"sequential({' | '.join(f.name for f in functors)})"
        return SequentialFunctor(name, functors)

    def _parse_block(self) -> Functor:
        token = self._peek()
        if token == "(":
            self._consume("(")
            functor = self._parse_parallel()
            self._consume(")")
            return functor
        if token in {")", "|", ",", None}:
            raise ParseError("Unexpected block boundary.")
        return self._parse_command()

    def _parse_command(self) -> Functor:
        name = self._consume_word()
        args: List[str] = []
        while True:
            token = self._peek()
            if token is None or token in {"|", ",", ")"}:
                break
            if token == "(":
                raise ParseError(f"Unexpected token '{token}' in command arguments.")
            args.append(self._consume_word())
        return self._create_functor(name, args)

    def _create_functor(self, name: str, args: Sequence[str]) -> Functor:
        if name.startswith(":"):
            return self._create_system_functor(name, args)

        script_functor = self._try_create_user_functor(name, args)
        if script_functor:
            return script_functor

        return self._create_builtin_functor(name, args)

    def _create_system_functor(self, raw_name: str, args: Sequence[str]) -> Functor:
        if raw_name == ":":
            raise ParseError("System command name is missing.")
        command_name = raw_name[1:]
        return SystemFunctor(command_name, default_extra_args=list(args))

    def _try_create_user_functor(self, name: str, args: Sequence[str]) -> Functor | None:
        script_path = (self.context.scripts_path / f"{name}.py").resolve()
        if not script_path.is_file():
            return None

        input_files, extra_args = self._split_user_args(args)
        if extra_args and not extra_args[0].startswith("-"):
            raise ParseError("User command extra arguments must start with '-'.")

        return UserDefinedFunctor(
            name,
            script_path,
            default_input_files=input_files or None,
            default_extra_args=extra_args or None,
        )

    def _split_user_args(self, args: Sequence[str]) -> tuple[List[str], List[str]]:
        input_files: List[str] = []
        extra_args: List[str] = []
        for arg in args:
            if extra_args:
                extra_args.append(arg)
                continue
            if arg.startswith("-"):
                extra_args.append(arg)
            else:
                input_files.append(arg)
        return input_files, extra_args

    def _create_builtin_functor(self, name: str, args: Sequence[str]) -> Functor:
        command = [name]
        return BuiltinFunctor(
            name,
            command,
            default_extra_args=list(args) if args else None,
        )

    def _peek(self) -> str | None:
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _consume(self, expected: str) -> None:
        token = self._peek()
        if token != expected:
            raise ParseError(f"Expected '{expected}' but found '{token}'.")
        self.pos += 1

    def _consume_word(self) -> str:
        token = self._peek()
        if token is None:
            raise ParseError("Unexpected end of input.")
        if token in {"|", ",", ")", "("}:
            raise ParseError(f"Unexpected token '{token}'.")
        self.pos += 1
        return token

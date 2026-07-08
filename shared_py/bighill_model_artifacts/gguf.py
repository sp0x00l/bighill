from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GGUFInspection:
    path: str
    architecture: str
    tensor_count: int
    metadata_count: int
    alignment: int
    chat_template_present: bool
    chat_template: str
    stop_tokens: list[str]


def inspect_gguf(path: str | Path) -> GGUFInspection:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise ValueError(f"GGUF file does not exist: {source}")

    from gguf import GGUFReader

    reader = GGUFReader(str(source))
    architecture = _field_string(reader, "general.architecture")
    if not architecture:
        raise ValueError("GGUF metadata is missing general.architecture")

    alignment = _field_int(reader, "general.alignment", 32)
    if alignment <= 0:
        raise ValueError("GGUF metadata general.alignment must be positive")

    tensors = list(getattr(reader, "tensors", []) or [])
    if not tensors:
        raise ValueError("GGUF file does not contain tensor metadata")

    file_size = source.stat().st_size
    for tensor in tensors:
        offset = int(getattr(tensor, "data_offset", -1))
        n_bytes = int(getattr(tensor, "n_bytes", 0))
        if offset < 0 or n_bytes <= 0 or offset+n_bytes > file_size:
            name = getattr(tensor, "name", "<unknown>")
            raise ValueError(f"GGUF tensor {name!r} has an invalid data range")
        if offset % alignment != 0:
            name = getattr(tensor, "name", "<unknown>")
            raise ValueError(f"GGUF tensor {name!r} is not aligned to {alignment} bytes")

    chat_template = _field_string(reader, "tokenizer.chat_template")
    stop_tokens = _stop_tokens(reader)
    fields = getattr(reader, "fields", {}) or {}
    return GGUFInspection(
        path=str(source),
        architecture=architecture,
        tensor_count=len(tensors),
        metadata_count=len(fields),
        alignment=alignment,
        chat_template_present=bool(chat_template.strip()),
        chat_template=chat_template,
        stop_tokens=stop_tokens,
    )


def _field_string(reader: Any, key: str) -> str:
    field = _field(reader, key)
    if field is None:
        return ""
    value = _field_value(field)
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _field_int(reader: Any, key: str, default: int) -> int:
    field = _field(reader, key)
    if field is None:
        return default
    value = _field_value(field)
    if value is None:
        return default
    return int(value)


def _field_values(reader: Any, key: str) -> list[Any]:
    field = _field(reader, key)
    if field is None:
        return []
    contents = getattr(field, "contents", None)
    if contents is not None:
        values = contents() if callable(contents) else contents
        if not isinstance(values, (list, tuple)):
            return [_scalar(values)]
        return [_scalar(value) for value in values]
    data = getattr(field, "data", None)
    if data is not None:
        if isinstance(data, (list, tuple)):
            return [_scalar(value) for value in data]
        return [_scalar(data)]
    value = getattr(field, "value", None)
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [_scalar(item) for item in value]
    return [_scalar(value)]


def _field(reader: Any, key: str) -> Any:
    getter = getattr(reader, "get_field", None)
    if getter is not None:
        field = getter(key)
        if field is not None:
            return field
    fields = getattr(reader, "fields", {}) or {}
    return fields.get(key)


def _stop_tokens(reader: Any) -> list[str]:
    tokens = [_decode_token(value) for value in _field_values(reader, "tokenizer.ggml.tokens")]
    token_ids = _token_ids(reader)
    out: list[str] = []
    seen: set[str] = set()
    for token_id in token_ids:
        if token_id < 0 or token_id >= len(tokens):
            continue
        token = tokens[token_id].strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _token_ids(reader: Any) -> list[int]:
    keys = (
        "tokenizer.ggml.eos_token_id",
        "tokenizer.ggml.eot_token_id",
        "tokenizer.ggml.eom_token_id",
        "tokenizer.ggml.eos_token_ids",
    )
    out: list[int] = []
    seen: set[int] = set()
    for key in keys:
        for value in _field_values(reader, key):
            if isinstance(value, (list, tuple)):
                candidates = value
            else:
                candidates = (value,)
            for candidate in candidates:
                try:
                    token_id = int(candidate)
                except (TypeError, ValueError):
                    continue
                if token_id in seen:
                    continue
                seen.add(token_id)
                out.append(token_id)
    return out


def _decode_token(value: Any) -> str:
    scalar = _scalar(value)
    if isinstance(scalar, bytes):
        return scalar.decode("utf-8", errors="replace")
    if isinstance(scalar, bytearray):
        return bytes(scalar).decode("utf-8", errors="replace")
    return str(scalar)


def _field_value(field: Any) -> Any:
    contents = getattr(field, "contents", None)
    if contents is not None:
        if callable(contents):
            return _scalar(contents())
        if len(contents) == 0:
            return None
        return _scalar(contents[0])
    data = getattr(field, "data", None)
    if data is not None:
        if isinstance(data, (list, tuple)):
            if not data:
                return None
            return _scalar(data[0])
        return _scalar(data)
    value = getattr(field, "value", None)
    return _scalar(value)


def _scalar(value: Any) -> Any:
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect and validate a GGUF model artifact")
    parser.add_argument("path", help="Path to a GGUF file")
    parser.add_argument("--require-chat-template", action="store_true")
    args = parser.parse_args()

    inspection = inspect_gguf(args.path)
    if args.require_chat_template and not inspection.chat_template_present:
        raise ValueError("GGUF metadata is missing tokenizer.chat_template")
    print(json.dumps(asdict(inspection), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

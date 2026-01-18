from __future__ import annotations


def hash8(schema_hash: str) -> str:
    return schema_hash[:8]


def template_db_name(prefix: str, schema_hash: str) -> str:
    return f"{prefix}_template_{hash8(schema_hash)}"


def clone_db_name(prefix: str, schema_hash: str, index: int) -> str:
    return f"{prefix}_{hash8(schema_hash)}_{index:02d}"

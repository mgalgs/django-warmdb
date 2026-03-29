from __future__ import annotations


def hash8(schema_hash: str) -> str:
    return schema_hash[:8]


def template_db_name(prefix: str, schema_hash: str) -> str:
    return f"{prefix}_template_{hash8(schema_hash)}"


def clone_db_name(prefix: str, schema_hash: str, index: int) -> str:
    return f"{prefix}_{hash8(schema_hash)}_{index:02d}"


def snapshot_db_name(label: str) -> str:
    return f"warmdb_snap_{label}"


def snapshot_clone_db_name(snapshot_label: str, clone_label: str) -> str:
    return f"warmdb_snapclone_{snapshot_label}_{clone_label}"

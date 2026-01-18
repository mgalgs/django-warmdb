from __future__ import annotations

from pathlib import Path

from warmdb.schema import schema_hash_from_migrations


def test_schema_hash_changes_with_bytes(tmp_path: Path):
    p1 = tmp_path / "0001_initial.py"
    p2 = tmp_path / "0002_more.py"

    p1.write_text("# a\n", encoding="utf-8")
    p2.write_text("# b\n", encoding="utf-8")

    h1 = schema_hash_from_migrations([p1, p2])

    p2.write_text("# b2\n", encoding="utf-8")

    h2 = schema_hash_from_migrations([p1, p2])

    assert h1 != h2

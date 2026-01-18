from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import django


def hash_paths(paths: Iterable[Path]) -> str:
    h = hashlib.sha256()

    for path in sorted({p.resolve() for p in paths}, key=lambda p: str(p)):
        h.update(str(path).encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")

    return h.hexdigest()


def schema_hash_from_migrations(migration_files: Iterable[Path]) -> str:
    h = hashlib.sha256()
    h.update(f"django:{django.get_version()}".encode("utf-8"))
    h.update(b"\0")

    # Mix in migration contents.
    files_hash = hash_paths(migration_files)
    h.update(files_hash.encode("ascii"))

    return h.hexdigest()

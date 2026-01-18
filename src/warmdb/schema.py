from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import django


def schema_hash_from_migration_files(files: Iterable[tuple[str, Path]]) -> str:
    """Compute a schema hash from migration file identity + bytes.

    The `identity` string must be stable across machines (e.g. "app_label:migrations/0001_initial.py").
    """

    h = hashlib.sha256()
    h.update(f"django:{django.get_version()}".encode("utf-8"))
    h.update(b"\0")

    normalized: dict[str, Path] = {}
    for identity, path in files:
        normalized[str(identity)] = Path(path)

    for identity in sorted(normalized.keys()):
        path = normalized[identity]
        h.update(identity.encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")

    return h.hexdigest()

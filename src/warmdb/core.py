from __future__ import annotations

import contextlib
import datetime as _dt
from pathlib import Path
from typing import Iterable

from django.apps import apps
from django.conf import settings
from django.core.management import call_command
from django.db import connections

from .exceptions import WarmDBNoReadyDB, WarmDBNotInitialized, WarmDBSchemaChanged
from .naming import clone_db_name, template_db_name
from .postgres import create_database_from_template, drop_database
from .schema import schema_hash_from_migrations
from .state import (
    DBRow,
    STATUS_READY,
    WarmDBState,
)


def state_path() -> Path:
    return Path(settings.BASE_DIR) / "warmdb_state.sqlite3"


def migration_files_for_installed_apps() -> list[Path]:
    files: list[Path] = []

    for cfg in apps.get_app_configs():
        mig_dir = Path(cfg.path) / "migrations"
        if not mig_dir.is_dir():
            continue

        for p in mig_dir.glob("*.py"):
            if p.name.startswith("__"):
                # include __init__.py? doesn't matter much; keep hashing stable by excluding
                continue
            files.append(p)

    return files


def compute_schema_hash() -> str:
    return schema_hash_from_migrations(migration_files_for_installed_apps())


@contextlib.contextmanager
def _override_database_name(alias: str, name: str):
    original = settings.DATABASES[alias].get("NAME")
    settings.DATABASES[alias]["NAME"] = name
    connections[alias].close()
    try:
        yield
    finally:
        settings.DATABASES[alias]["NAME"] = original
        connections[alias].close()


def load_state_or_fail(state: WarmDBState) -> None:
    if not state.exists():
        raise WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )


def ensure_schema_hash_matches(state: WarmDBState, current_schema_hash: str) -> None:
    stored = state.get_meta("schema_hash")
    if stored is None:
        raise WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )
    if stored != current_schema_hash:
        raise WarmDBSchemaChanged(
            "Schema changed since warmdb init.\nRun: manage.py warmdb invalidate && manage.py warmdb init"
        )


def init_pool(
    *,
    alias: str = "default",
    pool_size: int = 5,
    force: bool = False,
    prefix: str = "warmdb",
) -> None:
    state = WarmDBState(state_path())
    state.ensure_schema()

    current_hash = compute_schema_hash()
    stored_hash = state.get_meta("schema_hash")

    if force or (stored_hash is not None and stored_hash != current_hash):
        # Best-effort clean.
        invalidate_pool(alias=alias)
        state.ensure_schema()

    tmpl = template_db_name(prefix, current_hash)
    drop_database(alias, tmpl)

    with connections[alias]._nodb_cursor():
        # Ensure nodb connection is available early (sanity).
        pass

    # Re-create and migrate template.
    from .postgres import create_database

    create_database(alias, tmpl)

    with _override_database_name(alias, tmpl):
        call_command("migrate", database=alias, interactive=False, verbosity=1)

    clones = [clone_db_name(prefix, current_hash, i) for i in range(1, pool_size + 1)]

    for c in clones:
        drop_database(alias, c)
        create_database_from_template(alias, c, tmpl)

    state.set_meta("schema_hash", current_hash)
    state.set_meta("template_db_name", tmpl)
    state.set_meta("pool_size", str(pool_size))
    state.set_meta("prefix", prefix)
    state.set_meta("created_at", _dt.datetime.now(tz=_dt.timezone.utc).isoformat())

    state.upsert_dbs(
        DBRow(
            name=c,
            status=STATUS_READY,
            allocated_to_pid=None,
            allocated_at=None,
            last_error=None,
            schema_hash=current_hash,
        )
        for c in clones
    )


def invalidate_pool(*, alias: str = "default") -> None:
    state = WarmDBState(state_path())
    if state.exists():
        template = state.get_meta("template_db_name")
        for r in state.list_dbs():
            drop_database(alias, r.name)
        if template:
            drop_database(alias, template)
        state.clear()


def allocate_clone(*, alias: str = "default") -> tuple[str, str]:
    state = WarmDBState(state_path())
    load_state_or_fail(state)

    current_hash = compute_schema_hash()
    ensure_schema_hash_matches(state, current_hash)

    name = state.allocate_ready()
    if name is None:
        raise WarmDBNoReadyDB(
            "No warmdb databases are ready. Run: manage.py warmdb status"
        )

    template = state.get_meta("template_db_name")
    if not template:
        raise WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )

    return name, template

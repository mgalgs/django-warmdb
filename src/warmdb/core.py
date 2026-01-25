from __future__ import annotations

import contextlib
import datetime as _dt
from pathlib import Path
from typing import Callable, Iterable

from django.apps import apps
from django.conf import settings
from django.core.management import call_command
from django.db import connections

from .exceptions import WarmDBNoReadyDB, WarmDBNotInitialized, WarmDBSchemaChanged
from .naming import clone_db_name, template_db_name
from .postgres import create_database_from_template, drop_database
from .schema import schema_hash_from_migration_files
from .state import (
    DBRow,
    STATUS_CONSUMED,
    STATUS_ERROR,
    STATUS_READY,
    WarmDBState,
)


def state_path() -> Path:
    return Path(settings.BASE_DIR) / "warmdb_state.sqlite3"


def migration_files_for_installed_apps() -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []

    for cfg in apps.get_app_configs():
        mig_dir = Path(cfg.path) / "migrations"
        if not mig_dir.is_dir():
            continue

        for p in mig_dir.glob("*.py"):
            if p.name.startswith("__"):
                # include __init__.py? doesn't matter much; keep hashing stable by excluding
                continue

            rel = p.relative_to(Path(cfg.path)).as_posix()
            identity = f"{cfg.label}:{rel}"
            files.append((identity, p))

    return files


def compute_schema_hash() -> str:
    return schema_hash_from_migration_files(migration_files_for_installed_apps())


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
            "Schema changed since warmdb init.\nRun: manage.py warmdb refresh"
        )


def init_pool(
    *,
    alias: str = "default",
    pool_size: int = 5,
    force: bool = False,
    prefix: str = "warmdb",
    log: Callable[[str], None] | None = None,
) -> None:
    state = WarmDBState(state_path())
    state.ensure_schema()

    current_hash = compute_schema_hash()
    stored_hash = state.get_meta("schema_hash")

    if log:
        log(
            "warmdb init: "
            f"current_schema_hash={current_hash} stored_schema_hash={stored_hash} "
            f"pool_size={pool_size} prefix={prefix} force={force}"
        )

    if force or (stored_hash is not None and stored_hash != current_hash):
        # Best-effort clean.
        if log:
            log("warmdb init: invalidating existing pool")
        invalidate_pool(alias=alias)
        state.ensure_schema()

    tmpl = template_db_name(prefix, current_hash)
    if log:
        log(f"warmdb init: creating template {tmpl}")
    drop_database(alias, tmpl)

    with connections[alias]._nodb_cursor():
        # Ensure nodb connection is available early (sanity).
        pass

    # Re-create and migrate template.
    from .postgres import create_database

    create_database(alias, tmpl)

    if log:
        log(f"warmdb init: running migrations on template {tmpl}")

    with _override_database_name(alias, tmpl):
        call_command("migrate", database=alias, interactive=False, verbosity=1)

    clones = [clone_db_name(prefix, current_hash, i) for i in range(1, pool_size + 1)]

    if log:
        log(f"warmdb init: creating {len(clones)} clones from template {tmpl}")

    for c in clones:
        if log:
            log(f"warmdb init: cloning {c}")
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

    if log:
        log("warmdb init: pool ready")


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
            "No warmdb databases are ready. Pool exhausted. Run: manage.py warmdb refresh"
        )

    template = state.get_meta("template_db_name")
    if not template:
        raise WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )

    return name, template


def refresh_pool(
    *,
    alias: str = "default",
    log: Callable[[str], None] | None = None,
) -> None:
    """Refresh the warmdb pool.

    If the schema hash matches the stored state, repopulate consumed/error clones.
    If the schema hash differs, auto-invalidate and reinitialize the pool.
    """

    state = WarmDBState(state_path())
    load_state_or_fail(state)

    current_hash = compute_schema_hash()
    stored_hash = state.get_meta("schema_hash")

    if log:
        log(
            "warmdb refresh: "
            f"current_schema_hash={current_hash} stored_schema_hash={stored_hash}"
        )

    # Schema changed: full reinit.
    if stored_hash != current_hash:
        prefix = state.get_meta("prefix") or "warmdb"
        pool_size = int(state.get_meta("pool_size") or "5")

        if log:
            log(
                "warmdb refresh: schema changed; reinitializing pool "
                f"(pool_size={pool_size} prefix={prefix})"
            )

        invalidate_pool(alias=alias)
        if log:
            init_pool(alias=alias, pool_size=pool_size, prefix=prefix, log=log)
        else:
            init_pool(alias=alias, pool_size=pool_size, prefix=prefix)
        return

    template = state.get_meta("template_db_name")
    if not template:
        raise WarmDBNotInitialized(
            "warmdb is not initialized. Run: manage.py warmdb init"
        )

    prefix = state.get_meta("prefix") or "warmdb"
    pool_size = int(state.get_meta("pool_size") or "5")

    existing_clones = state.list_dbs()

    to_recreate = [
        clone
        for clone in existing_clones
        if clone.status in (STATUS_CONSUMED, STATUS_ERROR)
    ]

    if log:
        consumed = sum(1 for db in existing_clones if db.status == STATUS_CONSUMED)
        error = sum(1 for db in existing_clones if db.status == STATUS_ERROR)
        log(
            "warmdb refresh: "
            f"{len(to_recreate)} clones to recreate "
            f"({consumed} consumed, {error} error); "
            f"existing={len(existing_clones)} target_pool_size={pool_size}"
        )

    # Repopulate consumed/error clones.
    for clone in to_recreate:
        if log:
            log(f"warmdb refresh: recreating {clone.name} (was {clone.status})")
        drop_database(alias, clone.name)
        create_database_from_template(alias, clone.name, template)
        state.mark_ready(clone.name)

    # Backfill missing rows if state is short for any reason.
    if len(existing_clones) < pool_size:
        for i in range(len(existing_clones) + 1, pool_size + 1):
            name = clone_db_name(prefix, current_hash, i)
            if log:
                log(f"warmdb refresh: creating missing clone {name}")
            create_database_from_template(alias, name, template)
            state.upsert_dbs(
                [
                    DBRow(
                        name=name,
                        status=STATUS_READY,
                        allocated_to_pid=None,
                        allocated_at=None,
                        last_error=None,
                        schema_hash=current_hash,
                    )
                ]
            )

    if log:
        log("warmdb refresh: done")

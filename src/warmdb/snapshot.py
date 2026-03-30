from __future__ import annotations

import datetime as _dt
from typing import Callable

from django.conf import settings

from .exceptions import WarmDBSnapshotHasClones, WarmDBSnapshotNotFound
from .naming import snapshot_clone_db_name, snapshot_db_name
from .postgres import create_database_from_template, drop_database, terminate_sessions
from .state import SnapshotCloneRow, SnapshotRow, WarmDBState


def _get_state() -> WarmDBState:
    from .core import state_path

    state = WarmDBState(state_path())
    state.ensure_schema()
    state.ensure_snapshot_schema()
    return state


def create_snapshot(
    *,
    alias: str = "default",
    label: str | None = None,
    log: Callable[[str], None] | None = None,
) -> SnapshotRow:
    state = _get_state()

    source_db = settings.DATABASES[alias]["NAME"]

    if label is None:
        label = _dt.datetime.now().strftime("%Y%m%d_%H%M")

    db_name = snapshot_db_name(label)

    if log:
        log(
            f"Warning: active connections to '{source_db}' will be terminated.\n"
            "Stop your dev server before creating a snapshot."
        )
        log(f"Creating snapshot '{label}' from '{source_db}'...")

    terminate_sessions(alias, source_db)
    create_database_from_template(alias, db_name, source_db)
    state.add_snapshot(db_name, label, source_db)

    snap = state.get_snapshot(label)
    assert snap is not None

    if log:
        log(f"Snapshot created: label={label} db={db_name}")

    return snap


def clone_snapshot(
    *,
    alias: str = "default",
    clone_label: str,
    snapshot_label: str | None = None,
    log: Callable[[str], None] | None = None,
) -> SnapshotCloneRow:
    state = _get_state()

    if snapshot_label is not None:
        snap = state.get_snapshot(snapshot_label)
        if snap is None:
            raise WarmDBSnapshotNotFound(
                f"Snapshot '{snapshot_label}' not found. "
                "Run: manage.py warmdb snapshot list"
            )
    else:
        snap = state.get_latest_snapshot()
        if snap is None:
            raise WarmDBSnapshotNotFound(
                "No snapshots exist. " "Run: manage.py warmdb snapshot create"
            )

    existing = state.get_clone(clone_label)
    if existing is not None:
        if log:
            log(f"Clone '{clone_label}' already exists, reusing (db={existing.name})")
        return existing

    db_name = snapshot_clone_db_name(snap.label, clone_label)

    if log:
        log(f"Cloning snapshot '{snap.label}' as '{clone_label}'...")

    terminate_sessions(alias, snap.name)
    create_database_from_template(alias, db_name, snap.name)
    state.add_clone(db_name, snap.label, clone_label)

    clone = state.get_clone(clone_label)
    assert clone is not None

    if log:
        log(f"Clone created: db={db_name}")

    return clone


def drop_clone(
    *,
    alias: str = "default",
    clone_label: str,
    log: Callable[[str], None] | None = None,
) -> None:
    state = _get_state()

    clone = state.get_clone(clone_label)
    if clone is None:
        raise WarmDBSnapshotNotFound(
            f"Clone '{clone_label}' not found. " "Run: manage.py warmdb snapshot status"
        )

    if log:
        log(f"Dropping clone '{clone_label}' (db={clone.name})...")

    drop_database(alias, clone.name)
    state.remove_clone(clone_label)

    if log:
        log(f"Clone '{clone_label}' dropped.")


def drop_snapshot(
    *,
    alias: str = "default",
    label: str,
    cascade: bool = False,
    log: Callable[[str], None] | None = None,
) -> None:
    state = _get_state()

    snap = state.get_snapshot(label)
    if snap is None:
        raise WarmDBSnapshotNotFound(
            f"Snapshot '{label}' not found. " "Run: manage.py warmdb snapshot list"
        )

    clones = state.list_clones(snapshot_label=label)

    if clones and not cascade:
        clone_labels = ", ".join(c.clone_label for c in clones)
        raise WarmDBSnapshotHasClones(
            f"Snapshot '{label}' has {len(clones)} clone(s): {clone_labels}\n"
            "Use --cascade to drop them, or drop clones individually first."
        )

    for clone in clones:
        if log:
            log(f"Dropping clone '{clone.clone_label}' (db={clone.name})...")
        drop_database(alias, clone.name)
        state.remove_clone(clone.clone_label)

    if log:
        log(f"Dropping snapshot '{label}' (db={snap.name})...")

    drop_database(alias, snap.name)
    state.remove_snapshot(label)

    if log:
        log(f"Snapshot '{label}' dropped.")


def list_snapshots() -> list[SnapshotRow]:
    state = _get_state()
    return state.list_snapshots()


def list_clones(snapshot_label: str | None = None) -> list[SnapshotCloneRow]:
    state = _get_state()
    return state.list_clones(snapshot_label=snapshot_label)


def prune_snapshots(
    *,
    alias: str = "default",
    keep: int = 1,
    cascade: bool = False,
    log: Callable[[str], None] | None = None,
) -> None:
    state = _get_state()
    snapshots = state.list_snapshots()

    if len(snapshots) <= keep:
        if log:
            log(f"Nothing to prune ({len(snapshots)} snapshot(s), keeping {keep}).")
        return

    to_prune = snapshots[: len(snapshots) - keep]

    for snap in to_prune:
        clones = state.list_clones(snapshot_label=snap.label)
        if clones and not cascade:
            clone_labels = ", ".join(c.clone_label for c in clones)
            if log:
                log(
                    f"Skipping snapshot '{snap.label}': "
                    f"has {len(clones)} clone(s): {clone_labels} "
                    "(use --cascade to force)"
                )
            continue

        drop_snapshot(alias=alias, label=snap.label, cascade=cascade, log=log)

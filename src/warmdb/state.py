from __future__ import annotations

import datetime as _dt
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, Optional

if TYPE_CHECKING:
    import contextlib as _ctx

STATUS_INITIALIZING = "initializing"
STATUS_READY = "ready"
STATUS_IN_USE = "in-use"
STATUS_CONSUMED = "consumed"
STATUS_ERROR = "error"

# Operation types for locking
OP_NONE = "none"
OP_INIT = "init"
OP_REFRESH = "refresh"
OP_INVALIDATE = "invalidate"


@dataclass(frozen=True)
class SnapshotRow:
    name: str
    label: str
    source_db: str
    created_at: str


@dataclass(frozen=True)
class SnapshotCloneRow:
    name: str
    snapshot_label: str
    clone_label: str
    created_at: str


@dataclass(frozen=True)
class DBRow:
    name: str
    status: str
    allocated_to_pid: int | None
    allocated_at: str | None
    last_error: str | None
    schema_hash: str


class WarmDBState:
    def __init__(self, path: Path):
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dbs (
                  name TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  allocated_to_pid INTEGER,
                  allocated_at TEXT,
                  last_error TEXT,
                  schema_hash TEXT NOT NULL
                );
                """
            )

    def get_meta(self, key: str) -> str | None:
        if not self.exists():
            return None
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        self.ensure_schema()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def list_dbs(self) -> list[DBRow]:
        if not self.exists():
            return []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, status, allocated_to_pid, allocated_at, last_error, schema_hash FROM dbs ORDER BY name"
            ).fetchall()
        return [
            DBRow(
                name=r["name"],
                status=r["status"],
                allocated_to_pid=r["allocated_to_pid"],
                allocated_at=r["allocated_at"],
                last_error=r["last_error"],
                schema_hash=r["schema_hash"],
            )
            for r in rows
        ]

    def upsert_dbs(self, rows: Iterable[DBRow]) -> None:
        self.ensure_schema()
        with self.connect() as conn:
            for r in rows:
                conn.execute(
                    """
                    INSERT INTO dbs(name, status, allocated_to_pid, allocated_at, last_error, schema_hash)
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                      status=excluded.status,
                      allocated_to_pid=excluded.allocated_to_pid,
                      allocated_at=excluded.allocated_at,
                      last_error=excluded.last_error,
                      schema_hash=excluded.schema_hash
                    """,
                    (
                        r.name,
                        r.status,
                        r.allocated_to_pid,
                        r.allocated_at,
                        r.last_error,
                        r.schema_hash,
                    ),
                )

    def mark_ready(self, name: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL, last_error=NULL WHERE name=?",
                (STATUS_READY, name),
            )

    def mark_consumed(self, name: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL, last_error=NULL WHERE name=?",
                (STATUS_CONSUMED, name),
            )

    def mark_error(self, name: str, error: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, last_error=? WHERE name=?",
                (STATUS_ERROR, error, name),
            )

    def _pid_alive(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def reclaim_stale_in_use(self, *, ttl_seconds: int | None = 4 * 60 * 60) -> int:
        """Reclaim clones stuck as in-use by dead/stale processes.

        Not wrapped in BEGIN IMMEDIATE because the UPDATE is idempotent —
        two concurrent callers reclaiming the same row is harmless.
        """
        if not self.exists():
            return 0

        now = _dt.datetime.now(tz=_dt.timezone.utc)
        reclaimed = 0

        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, allocated_to_pid, allocated_at FROM dbs WHERE status=?",
                (STATUS_IN_USE,),
            ).fetchall()

            for r in rows:
                pid = r["allocated_to_pid"]
                allocated_at = r["allocated_at"]

                stale = False

                if pid is None or not self._pid_alive(int(pid)):
                    stale = True

                if not stale and ttl_seconds is not None and allocated_at:
                    try:
                        ts = _dt.datetime.fromisoformat(str(allocated_at))
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=_dt.timezone.utc)
                        if (now - ts).total_seconds() > ttl_seconds:
                            stale = True
                    except ValueError:
                        stale = True

                if stale:
                    conn.execute(
                        "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                        (STATUS_READY, r["name"]),
                    )
                    reclaimed += 1

        return reclaimed

    def allocate_ready(self, *, pid: int | None = None) -> str | None:
        """Atomically allocate a ready DB and mark it in-use."""

        if not self.exists():
            return None

        pid = os.getpid() if pid is None else pid
        allocated_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Best-effort cleanup while we hold the lock.
                self._reclaim_stale_in_use_locked(conn)

                row = conn.execute(
                    "SELECT name FROM dbs WHERE status=? ORDER BY name LIMIT 1",
                    (STATUS_READY,),
                ).fetchone()

                if row is None:
                    conn.execute("ROLLBACK")
                    return None

                name = str(row["name"])
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=?, allocated_at=? WHERE name=?",
                    (STATUS_IN_USE, int(pid), allocated_at, name),
                )
                conn.execute("COMMIT")
                return name
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _reclaim_stale_in_use_locked(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            "SELECT name, allocated_to_pid FROM dbs WHERE status=?",
            (STATUS_IN_USE,),
        ).fetchall()

        for r in rows:
            pid = r["allocated_to_pid"]
            if pid is None:
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                    (STATUS_READY, r["name"]),
                )
                continue

            if not self._pid_alive(int(pid)):
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                    (STATUS_READY, r["name"]),
                )

    def get_operation(self) -> tuple[str, int | None, str | None] | None:
        """Get current operation: (operation_type, pid, started_at) or None."""
        if not self.exists():
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key=?",
                ("operation",),
            ).fetchone()
            if not row:
                return None
            return self._parse_operation_value(str(row["value"]))

    @staticmethod
    def _parse_operation_value(
        value: str,
    ) -> tuple[str, int | None, str | None] | None:
        """Parse an operation meta value into (op_type, pid, started_at).

        Supports both legacy 3-part (op:pid:ts) and new 4-part
        (op:pid:ts:token) formats.
        """
        parts = value.split(":", 3)
        if len(parts) < 3:
            return None
        op_type, pid_str, started_at = parts[0], parts[1], parts[2]
        return (op_type, int(pid_str) if pid_str else None, started_at)

    def clear_operation(self, *, token: str | None = None) -> None:
        """Clear the current operation, only if *token* matches (when given)."""
        if not self.exists():
            return
        with self.connect() as conn:
            if token is not None:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key=?",
                    ("operation",),
                ).fetchone()
                if not row or not str(row["value"]).endswith(f":{token}"):
                    return
            conn.execute("DELETE FROM meta WHERE key=?", ("operation",))

    def is_operation_valid(self, operation: tuple[str, int | None, str | None]) -> bool:
        """Check if an operation entry is still valid (PID alive and not stale)."""
        if operation is None:
            return False
        op_type, pid, started_at = operation
        if pid is None:
            return False
        # Check if PID is alive
        if not self._pid_alive(int(pid)):
            return False
        # Check if operation is stale (more than 2 hours)
        if started_at:
            try:
                ts = _dt.datetime.fromisoformat(str(started_at))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=_dt.timezone.utc)
                if (
                    _dt.datetime.now(tz=_dt.timezone.utc) - ts
                ).total_seconds() > 2 * 60 * 60:
                    return False
            except ValueError:
                return False
        return True

    def acquire_operation_lock(
        self,
        op_type: str,
        *,
        wait_timeout_seconds: int = 60,
        poll_interval_seconds: float = 0.5,
    ) -> str:
        """Acquire an operation lock atomically. Returns a token for release.

        Uses ``BEGIN IMMEDIATE`` so that the check-and-set happens inside a
        single SQLite write transaction, preventing two processes from both
        observing "free" and both proceeding.

        Raises:
            RuntimeError: if timeout is exceeded
        """
        start = time.time()
        pid = os.getpid()
        token = secrets.token_hex(16)

        while True:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    row = conn.execute(
                        "SELECT value FROM meta WHERE key=?",
                        ("operation",),
                    ).fetchone()

                    current = None
                    if row:
                        current = self._parse_operation_value(str(row["value"]))

                    if current is None or not self.is_operation_valid(current):
                        started_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
                        conn.execute(
                            "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                            (
                                "operation",
                                f"{op_type}:{pid}:{started_at}:{token}",
                            ),
                        )
                        conn.execute("COMMIT")
                        return token

                    conn.execute("ROLLBACK")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            if time.time() - start >= wait_timeout_seconds:
                held_by_op, held_by_pid, held_since = current  # type: ignore[misc]
                raise RuntimeError(
                    f"Cannot acquire {op_type} lock: operation {held_by_op} "
                    f"held by PID {held_by_pid} since {held_since} "
                    f"(timeout after {wait_timeout_seconds}s)"
                )
            time.sleep(poll_interval_seconds)

    # ── Snapshot state ────────────────────────────────────────────

    def ensure_snapshot_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                  name TEXT PRIMARY KEY,
                  label TEXT NOT NULL UNIQUE,
                  source_db TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshot_clones (
                  name TEXT PRIMARY KEY,
                  snapshot_label TEXT NOT NULL,
                  clone_label TEXT NOT NULL UNIQUE,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY (snapshot_label) REFERENCES snapshots(label)
                );
                """
            )

    def add_snapshot(self, name: str, label: str, source_db: str) -> None:
        self.ensure_snapshot_schema()
        created_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO snapshots(name, label, source_db, created_at) VALUES(?, ?, ?, ?)",
                (name, label, source_db, created_at),
            )

    def get_snapshot(self, label: str) -> SnapshotRow | None:
        if not self.exists():
            return None
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name, label, source_db, created_at FROM snapshots WHERE label=?",
                (label,),
            ).fetchone()
            if row is None:
                return None
            return SnapshotRow(
                name=row["name"],
                label=row["label"],
                source_db=row["source_db"],
                created_at=row["created_at"],
            )

    def get_latest_snapshot(self) -> SnapshotRow | None:
        if not self.exists():
            return None
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name, label, source_db, created_at FROM snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return SnapshotRow(
                name=row["name"],
                label=row["label"],
                source_db=row["source_db"],
                created_at=row["created_at"],
            )

    def list_snapshots(self) -> list[SnapshotRow]:
        if not self.exists():
            return []
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, label, source_db, created_at FROM snapshots ORDER BY created_at"
            ).fetchall()
        return [
            SnapshotRow(
                name=r["name"],
                label=r["label"],
                source_db=r["source_db"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    def remove_snapshot(self, label: str) -> None:
        if not self.exists():
            return
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            conn.execute("DELETE FROM snapshots WHERE label=?", (label,))

    def add_clone(self, name: str, snapshot_label: str, clone_label: str) -> None:
        self.ensure_snapshot_schema()
        created_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO snapshot_clones(name, snapshot_label, clone_label, created_at) VALUES(?, ?, ?, ?)",
                (name, snapshot_label, clone_label, created_at),
            )

    def get_clone(self, clone_label: str) -> SnapshotCloneRow | None:
        if not self.exists():
            return None
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name, snapshot_label, clone_label, created_at FROM snapshot_clones WHERE clone_label=?",
                (clone_label,),
            ).fetchone()
            if row is None:
                return None
            return SnapshotCloneRow(
                name=row["name"],
                snapshot_label=row["snapshot_label"],
                clone_label=row["clone_label"],
                created_at=row["created_at"],
            )

    def list_clones(self, snapshot_label: str | None = None) -> list[SnapshotCloneRow]:
        if not self.exists():
            return []
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            if snapshot_label is not None:
                rows = conn.execute(
                    "SELECT name, snapshot_label, clone_label, created_at FROM snapshot_clones WHERE snapshot_label=? ORDER BY created_at",
                    (snapshot_label,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT name, snapshot_label, clone_label, created_at FROM snapshot_clones ORDER BY created_at"
                ).fetchall()
        return [
            SnapshotCloneRow(
                name=r["name"],
                snapshot_label=r["snapshot_label"],
                clone_label=r["clone_label"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    def remove_clone(self, clone_label: str) -> None:
        if not self.exists():
            return
        self.ensure_snapshot_schema()
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM snapshot_clones WHERE clone_label=?", (clone_label,)
            )

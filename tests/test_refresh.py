from __future__ import annotations

from pathlib import Path

from warmdb import core
from warmdb.state import DBRow, STATUS_CONSUMED, STATUS_ERROR, STATUS_READY, WarmDBState


def test_refresh_schema_mismatch_triggers_reinit(tmp_path: Path, monkeypatch):
    state_file = tmp_path / "warmdb_state.sqlite3"

    monkeypatch.setattr(core, "state_path", lambda: state_file)
    monkeypatch.setattr(core, "compute_schema_hash", lambda: "new")

    state = WarmDBState(state_file)
    state.ensure_schema()
    state.set_meta("schema_hash", "old")
    state.set_meta("pool_size", "3")
    state.set_meta("prefix", "warmdb_x")

    calls: list[tuple[str, object]] = []

    def fake_invalidate_pool(*, alias: str = "default") -> None:
        calls.append(("invalidate", alias))

    def fake_init_pool(
        *,
        alias: str = "default",
        pool_size: int = 5,
        prefix: str = "warmdb",
        force: bool = False,
        log=None,
    ) -> None:
        calls.append(
            (
                "init",
                {
                    "alias": alias,
                    "pool_size": pool_size,
                    "prefix": prefix,
                    "force": force,
                },
            )
        )

    monkeypatch.setattr(core, "invalidate_pool", fake_invalidate_pool)
    monkeypatch.setattr(core, "init_pool", fake_init_pool)

    core.refresh_pool(alias="default")

    assert calls == [
        ("invalidate", "default"),
        (
            "init",
            {
                "alias": "default",
                "pool_size": 3,
                "prefix": "warmdb_x",
                "force": False,
            },
        ),
    ]


def test_refresh_recreates_consumed_and_error(tmp_path: Path, monkeypatch):
    state_file = tmp_path / "warmdb_state.sqlite3"

    monkeypatch.setattr(core, "state_path", lambda: state_file)
    monkeypatch.setattr(core, "compute_schema_hash", lambda: "h")

    state = WarmDBState(state_file)
    state.ensure_schema()
    state.set_meta("schema_hash", "h")
    state.set_meta("template_db_name", "tmpl")
    state.set_meta("pool_size", "2")
    state.set_meta("prefix", "warmdb_x")

    state.upsert_dbs(
        [
            DBRow(
                name="warmdb_x_h_01",
                status=STATUS_READY,
                allocated_to_pid=None,
                allocated_at=None,
                last_error=None,
                schema_hash="h",
            ),
            DBRow(
                name="warmdb_x_h_02",
                status=STATUS_CONSUMED,
                allocated_to_pid=None,
                allocated_at=None,
                last_error=None,
                schema_hash="h",
            ),
            DBRow(
                name="warmdb_x_h_03",
                status=STATUS_ERROR,
                allocated_to_pid=None,
                allocated_at=None,
                last_error="boom",
                schema_hash="h",
            ),
        ]
    )

    calls: list[tuple[str, str]] = []

    def fake_drop_database(alias: str, dbname: str) -> None:
        calls.append(("drop", dbname))

    def fake_create_database_from_template(
        alias: str, dbname: str, template_dbname: str
    ) -> None:
        calls.append(("create", dbname))

    monkeypatch.setattr(core, "drop_database", fake_drop_database)
    monkeypatch.setattr(
        core, "create_database_from_template", fake_create_database_from_template
    )

    core.refresh_pool(alias="default")

    assert ("drop", "warmdb_x_h_02") in calls
    assert ("create", "warmdb_x_h_02") in calls
    assert ("drop", "warmdb_x_h_03") in calls
    assert ("create", "warmdb_x_h_03") in calls

    rows = {r.name: r for r in WarmDBState(state_file).list_dbs()}
    assert rows["warmdb_x_h_01"].status == STATUS_READY
    assert rows["warmdb_x_h_02"].status == STATUS_READY
    assert rows["warmdb_x_h_03"].status == STATUS_READY
    assert rows["warmdb_x_h_03"].last_error is None

from __future__ import annotations

from django.core.management.base import BaseCommand

from warmdb.core import init_pool, invalidate_pool, refresh_pool
from warmdb.state import (
    STATUS_CONSUMED,
    STATUS_ERROR,
    STATUS_IN_USE,
    STATUS_READY,
    WarmDBState,
)


class Command(BaseCommand):
    help = "Manage warm Postgres DB pool for Django tests"

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest="subcommand")
        sub.required = True

        # ── init ──────────────────────────────────────────────
        init_parser = sub.add_parser("init", help="Initialize the DB pool")
        init_parser.add_argument("--pool-size", type=int, default=5)
        init_parser.add_argument("--force", action="store_true")
        init_parser.add_argument("--prefix", default="warmdb")

        # ── status ────────────────────────────────────────────
        sub.add_parser("status", help="Show pool status")

        # ── invalidate ────────────────────────────────────────
        sub.add_parser("invalidate", help="Drop all databases and clear state")

        # ── refresh ───────────────────────────────────────────
        sub.add_parser("refresh", help="Refresh the pool")

        # ── snapshot ──────────────────────────────────────────
        snap_parser = sub.add_parser("snapshot", help="Manage database snapshots")
        snap_sub = snap_parser.add_subparsers(dest="snap_subcommand")
        snap_sub.required = True

        snap_create = snap_sub.add_parser("create", help="Create a snapshot")
        snap_create.add_argument("--label", default=None)

        snap_sub.add_parser("list", help="List snapshots")

        snap_clone = snap_sub.add_parser("clone", help="Clone a snapshot")
        snap_clone.add_argument(
            "clone_label", help="Label for the clone (e.g. worktree name)"
        )
        snap_clone.add_argument("--snapshot", dest="snapshot_label", default=None)

        snap_drop_clone = snap_sub.add_parser(
            "drop-clone", help="Drop a snapshot clone"
        )
        snap_drop_clone.add_argument("clone_label", help="Clone label to drop")

        snap_drop = snap_sub.add_parser("drop", help="Drop a snapshot")
        snap_drop.add_argument("label", help="Snapshot label to drop")
        snap_drop.add_argument("--cascade", action="store_true")

        snap_prune = snap_sub.add_parser("prune", help="Prune old snapshots")
        snap_prune.add_argument("--keep", type=int, default=1)
        snap_prune.add_argument("--cascade", action="store_true")

        snap_sub.add_parser("status", help="Show snapshot status")

    def handle(self, *args, **options):
        subcommand = options["subcommand"]

        if subcommand == "init":
            log = self.stdout.write if int(options.get("verbosity", 1)) >= 1 else None
            init_pool(
                pool_size=int(options["pool_size"]),
                force=bool(options["force"]),
                prefix=str(options["prefix"]),
                log=log,
            )
            self.stdout.write(self.style.SUCCESS("warmdb init complete"))
            return

        if subcommand == "status":
            self._handle_pool_status()
            return

        if subcommand == "invalidate":
            invalidate_pool()
            self.stdout.write(self.style.SUCCESS("warmdb invalidated"))
            return

        if subcommand == "refresh":
            refresh_pool(log=self.stdout.write)
            self.stdout.write(self.style.SUCCESS("warmdb pool refreshed"))
            return

        if subcommand == "snapshot":
            self._handle_snapshot(options)
            return

    def _handle_pool_status(self):
        from warmdb.core import state_path

        state = WarmDBState(state_path())
        if not state.exists():
            self.stdout.write("warmdb is not initialized. Run: manage.py warmdb init")
            return

        schema_hash = state.get_meta("schema_hash")
        template = state.get_meta("template_db_name")

        self.stdout.write(f"template: {template}")
        self.stdout.write(f"schema_hash: {schema_hash}")

        dbs = state.list_dbs()
        for r in dbs:
            extra = ""
            if r.status == STATUS_IN_USE:
                extra = f" pid={r.allocated_to_pid} at={r.allocated_at}"
            if r.status == STATUS_ERROR and r.last_error:
                extra = f" error={r.last_error}"
            self.stdout.write(f"{r.name}: {r.status}{extra}")

        ready = sum(1 for db in dbs if db.status == STATUS_READY)
        in_use = sum(1 for db in dbs if db.status == STATUS_IN_USE)
        consumed = sum(1 for db in dbs if db.status == STATUS_CONSUMED)
        error = sum(1 for db in dbs if db.status == STATUS_ERROR)
        self.stdout.write(
            f"\nSummary: {ready} ready, {in_use} in-use, {consumed} consumed, {error} error"
        )

    def _handle_snapshot(self, options):
        from warmdb.snapshot import (
            clone_snapshot,
            create_snapshot,
            drop_clone,
            drop_snapshot,
            list_clones,
            list_snapshots,
            prune_snapshots,
        )

        snap_sub = options["snap_subcommand"]
        log = self.stdout.write if int(options.get("verbosity", 1)) >= 1 else None

        if snap_sub == "create":
            snap = create_snapshot(label=options.get("label"), log=log)
            self.stdout.write(
                self.style.SUCCESS(f"Snapshot '{snap.label}' created (db={snap.name})")
            )
            return

        if snap_sub == "list":
            snapshots = list_snapshots()
            if not snapshots:
                self.stdout.write("No snapshots.")
                return
            for s in snapshots:
                clones = list_clones(snapshot_label=s.label)
                self.stdout.write(
                    f"{s.label}: db={s.name} source={s.source_db} "
                    f"created={s.created_at} clones={len(clones)}"
                )
            return

        if snap_sub == "clone":
            clone = clone_snapshot(
                clone_label=options["clone_label"],
                snapshot_label=options.get("snapshot_label"),
                log=log,
            )
            self.stdout.write(clone.name)
            return

        if snap_sub == "drop-clone":
            drop_clone(clone_label=options["clone_label"], log=log)
            self.stdout.write(self.style.SUCCESS("Clone dropped."))
            return

        if snap_sub == "drop":
            drop_snapshot(
                label=options["label"],
                cascade=bool(options.get("cascade")),
                log=log,
            )
            self.stdout.write(self.style.SUCCESS("Snapshot dropped."))
            return

        if snap_sub == "prune":
            prune_snapshots(
                keep=int(options.get("keep", 1)),
                cascade=bool(options.get("cascade")),
                log=log,
            )
            self.stdout.write(self.style.SUCCESS("Prune complete."))
            return

        if snap_sub == "status":
            snapshots = list_snapshots()
            if not snapshots:
                self.stdout.write("No snapshots.")
                return
            for s in snapshots:
                self.stdout.write(
                    f"\nSnapshot: {s.label}"
                    f"\n  db: {s.name}"
                    f"\n  source: {s.source_db}"
                    f"\n  created: {s.created_at}"
                )
                clones = list_clones(snapshot_label=s.label)
                if clones:
                    for c in clones:
                        self.stdout.write(
                            f"  clone: {c.clone_label} db={c.name} created={c.created_at}"
                        )
                else:
                    self.stdout.write("  (no clones)")
            return

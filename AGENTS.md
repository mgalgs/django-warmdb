# AGENTS.md (a.k.a. CLAUDE.md)

This file provides guidance to agentic coding tools when working with code in this repository.

## Build and Development Commands

```bash
# Install dependencies (uses uv)
uv venv --python 3.12 --python-preference=only-managed --seed
uv pip install -e '.[dev]'

# Run tests
uv run pytest

# Run a single test
uv run pytest tests/test_schema_hash.py -k test_name

# Format code
uv run black .

# Run pre-commit hooks
uv run pre-commit run --all-files

# Install git hooks
uv run pre-commit install
```

### Integration Tests

Integration tests require a real Postgres instance and are opt-in:

```bash
# Start Postgres via Docker (optional)
docker run --rm -d --name warmdb-postgres -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16

# Run integration tests
export WARMDB_INTEGRATION=1
export WARMDB_PGHOST=localhost
export WARMDB_PGPORT=15432
export WARMDB_PGUSER=postgres
export WARMDB_PGPASSWORD=postgres
export WARMDB_PGDATABASE=postgres
uv run pytest -m integration
```

## Architecture

django-warmdb provides a pre-migrated Postgres database pool to speed up Django test runs by eliminating migration time.

### Core Components

- **`core.py`**: Main orchestration - `init_pool()` creates template + clones, `allocate_clone()` hands out a ready DB, `invalidate_pool()` tears down everything
- **`runner.py`**: `WarmDBDiscoverRunner` extends Django's `DiscoverRunner` - allocates a clone in `setup_databases()`, recycles it back to pristine state in `teardown_databases()`
- **`state.py`**: SQLite-backed state tracking (`WarmDBState` class) - tracks clone statuses (ready/in-use/error), handles PID-based stale detection for concurrency safety
- **`schema.py`**: Computes SHA256 hash from all migration files + Django version for schema change detection
- **`postgres.py`**: Low-level Postgres operations using `CREATE DATABASE ... TEMPLATE ...`
- **`naming.py`**: Database naming conventions (`warmdb_template_<hash8>`, `warmdb_<hash8>_01`, etc.)

### Management Command

`manage.py warmdb <subcommand>`:
- `init --pool-size N`: Create template DB, run migrations, clone N databases
- `status`: Show template, schema hash, and clone states
- `invalidate`: Drop all databases and clear local state

### State Storage

State is stored in `{settings.BASE_DIR}/warmdb_state.sqlite3` with two tables:
- `meta`: Key-value store (schema_hash, template_db_name, pool_size, prefix, created_at)
- `dbs`: Clone database records with status, PID allocation, timestamps

### Flow

1. `warmdb init` computes schema hash, creates migrated template, clones N databases
2. Test run calls `allocate_clone()` which atomically grabs a ready clone
3. After tests, `teardown_databases()` drops and re-creates clone from template
4. Schema hash is checked on each test run; mismatch raises `WarmDBSchemaChanged`

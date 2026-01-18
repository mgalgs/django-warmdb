# django-warmdb

Pre-warmed Postgres database pool for faster `manage.py test`.

`django-warmdb` keeps a *migrated* template database and a pool of cloned databases. Test runs allocate a ready clone instantly, run tests without running migrations, then recycle the clone back to a pristine state.

## Status

Alpha / prototype.

## How it works

1. `warmdb init` creates a **template** database and runs migrations once.
2. It creates `N` **clone** databases via Postgres `CREATE DATABASE ... TEMPLATE ...`.
3. The custom test runner allocates one ready clone (sqlite-backed, concurrency-safe).
4. On teardown, the clone is dropped and re-created from the template.

## Requirements

- Python **3.10+**
- Django **>= 3.2**
- Postgres (only backend supported)
- Postgres role with permissions to `CREATE DATABASE` and `DROP DATABASE`

## Installation

```bash
pip install django-warmdb
```

## Usage

1) Enable the app and test runner:

```py
# settings.py
INSTALLED_APPS += ["warmdb"]
TEST_RUNNER = "warmdb.runner.WarmDBDiscoverRunner"
```

2) Initialize the pool:

```bash
python manage.py warmdb init --pool-size 5
```

3) Run tests:

```bash
python manage.py test
```

4) Check status:

```bash
python manage.py warmdb status
```

5) Invalidate everything (drop template + clones, remove local sqlite state):

```bash
python manage.py warmdb invalidate
```

## State file

State is stored in a local sqlite database:

- `Path(settings.BASE_DIR) / "warmdb_state.sqlite3"`

This makes the state location stable regardless of the current working directory.

## Schema change detection

On `warmdb init`, we compute a `schema_hash` from:

- all `migrations/*.py` file identities and contents across `INSTALLED_APPS`
- Django version

On `manage.py test`, we recompute and fail fast if it differs:

```text
Schema changed since warmdb init.
Run: manage.py warmdb invalidate && manage.py warmdb init
```

Additionally, the runner checks for unapplied migrations on the allocated clone using Django's migration system.

## Development

This repo uses `uv` for environment management.

```bash
uv venv --python 3.12 --python-preference=only-managed --seed
uv pip install -e '.[dev]'
uv run pytest
uv run black .
uv run pre-commit run --all-files
```

Install git hooks:

```bash
uv run pre-commit install
```

### Integration test (optional)

There is an opt-in integration test that shells out to an example Django project under `tests/example_project`.

Enable it with:

If you don't already have Postgres running locally, a quick one-liner using Docker is:

```bash
docker run --rm -d --name warmdb-postgres -e POSTGRES_PASSWORD=postgres -p 15432:5432 postgres:16
```

Then run:

```bash
export WARMDB_INTEGRATION=1
export WARMDB_PGHOST=localhost
export WARMDB_PGPORT=15432
export WARMDB_PGUSER=postgres
export WARMDB_PGPASSWORD=postgres
export WARMDB_PGDATABASE=postgres
uv run pytest -m integration
```

## Comparison with similar solutions

### Django built-ins: `--keepdb`
- `--keepdb` avoids dropping the test database between runs, but **initial creation and migrations** still happen the first time (or when schema changes).
- `django-warmdb` targets the slow part: **migrate once**, then **clone instantly** for each run.

### `pytest-django`
- `pytest-django` provides excellent test ergonomics and integrates with `pytest`.
- It doesn't, by itself, provide a pre-migrated Postgres clone pool. You can combine it with `--reuse-db`/`--create-db`, but schema setup is still typically migration-driven.

### Template DB + custom scripts
Many teams maintain ad-hoc scripts that:
- build a migrated template DB
- clone it before tests
- drop afterwards

`django-warmdb` packages this pattern into:
- a standard Django management command (`warmdb init|status|invalidate`)
- a test runner that integrates with `manage.py test`
- local sqlite state to support a pool and basic concurrency.

### Postgres features (snapshotting / ZFS / filesystem-level clones)
Some environments can snapshot and clone Postgres data directories very quickly, but that typically requires infrastructure-level support and operational complexity. `django-warmdb` sticks to standard Postgres `CREATE DATABASE ... TEMPLATE ...`.

## License

MIT

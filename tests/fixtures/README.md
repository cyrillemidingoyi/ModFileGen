# Test fixtures

The MasterInput test database is versioned as text and rebuilt for each test
session. Do not commit `.db` files here.

## Layout

```
fixtures/
  build_fixtures.py          export and rebuild script
  sources/
    masterinput/
      schema.sql             CREATE TABLE and CREATE INDEX of the input tables
      <Table>.csv            rows of each input table
```

Only input tables are stored. Output tables (`SummaryOutput`,
`*DailyOutput`, `SticsProfile`) are created by the converters, and their
columns depend on the output configuration.

## CSV conventions

- UTF-8, comma separator, one header row, rows in the original rowid order.
- SQL `NULL` is written as the literal `NULL`. An empty field is an empty
  string, which is a different value.
- Floats are written with Python `repr`, so they round-trip exactly.

## Using the fixture in tests

`tests/conftest.py` provides two pytest fixtures:

- `masterinput_db`: the database built once per session. Treat it as
  read-only.
- `masterinput_copy`: a private copy for a test that writes to the database.

## Rebuilding a database by hand

```bash
python tests/fixtures/build_fixtures.py build --out /tmp/MasterInput.db
```

## Changing the fixture

Edit the CSV files, or `schema.sql` for a schema change, then run the tests.
Every change is then visible in the git diff, which documents the migration.

To regenerate all sources from a database instead:

```bash
python tests/fixtures/build_fixtures.py export --from path/to/MasterInput.db
```

This overwrites `schema.sql` and every CSV file.

## Provenance

| Date | Source | Change |
|---|---|---|
| 2026-10-03 | `tests/stics_successive/MasterInput.db` | Initial export of the 26 input tables. The rebuilt database is identical to the source in values, storage types, schema and indexes. |

The initial content covers: one point and one soil, years 2000 to 2002,
4 simulations, successive seasons, two-crop association, non-zero irrigation,
and parameter overrides by management and by soil. Every management uses
fertilisation policy `0`, meaning no fertiliser input; non-zero mineral and
organic policies exist in the operation tables but are not used yet.

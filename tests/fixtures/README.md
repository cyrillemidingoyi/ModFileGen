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
    modelsdictionary/        ModelsDictionary, Variables table only
      schema.sql
      Variables.csv
    celsius_v32_template/    CELSIUS V32 model database used as template
      schema.sql
      <Table>.csv
```

The CELSIUS V32 template keeps its reference tables whole (cultivars,
species, stages, residues...). The converter empties and refills the
simulation tables and reads the first row of some of them as default values:
those tables keep only their first row, and the other simulation tables and
the output tables are stored empty. Converting the MasterInput fixture into
this reduced template gives the same database as with the full template.

Only input tables are stored. Output tables (`SummaryOutput`,
`*DailyOutput`, `SticsProfile`) are created by the converters, and their
columns depend on the output configuration.

## CSV conventions

- UTF-8, comma separator, one header row, rows in the original rowid order.
- SQL `NULL` is written as the literal `NULL`. An empty field is an empty
  string, which is a different value.
- Floats are written with Python `repr`, so they round-trip exactly.

## Using the fixture in tests

`tests/conftest.py` provides four pytest fixtures:

- `masterinput_db`: the MasterInput database built once per session. Treat
  it as read-only.
- `masterinput_copy`: a private copy for a test that writes to the database.
- `modelsdictionary_db`: the ModelsDictionary built once per session, with
  only the `Variables` table that the converters read. Treat it as read-only.
- `celsius_v32_template_db`: the CELSIUS V32 template built once per session.
  Treat it as read-only; copy it before converting into it.

## Rebuilding a database by hand

```bash
python tests/fixtures/build_fixtures.py build --out /tmp/MasterInput.db
python tests/fixtures/build_fixtures.py build --fixture celsius_v32_template \
    --out /tmp/celsius_model_input.db
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
| 2026-10-03 | `migrations/20260911_add_soil_ssat.sql` and `migrations/20261003_align_masterinput_with_lowinput_schema.sql` | Schema aligned with the reference input schema of `LowInput/blindphase_corrected/MasterInput.db`: `SeasonYearOffset` replaces `SowingYearOffset`, `Soil.Ssat`, `InitialConditions.option` (`simple`) and `NH4initf`, new `InitialConditionsLayers` and `dailyobs` tables (empty), `RAclimateD.rhum` as REAL plus `vapeurp` and `co2`. New values are NULL except `option`. |
| 2026-10-04 | MasterInput `ListCultivars` | `testcult` and `testcult2` map to the CELSIUS V32 cultivars `20.1` (maize OPV_BEOU) and `2.1` (peanut ara28-206). |
| 2026-10-04 | `tests/dssatsuccessive/celsius_model_input.db` | Initial export of the CELSIUS V32 template (24 tables, 81 cultivars), reduced as described above. |
| 2026-10-04 | `LowInput/blindphase_corrected/ModelsDictionaryArise.db` | Initial export of the `Variables` table (3069 rows); the other tables of that database are not read by the converters. |

The initial content covers: one point and one soil, years 2000 to 2002,
4 simulations, successive seasons, two-crop association, non-zero irrigation,
and parameter overrides by management and by soil. Every management uses
fertilisation policy `0`, meaning no fertiliser input; non-zero mineral and
organic policies exist in the operation tables but are not used yet.

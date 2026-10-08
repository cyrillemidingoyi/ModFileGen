# CELSIUS V32 converter

This converter replaces the legacy Datamill CELSIUS database transformation
for V32. It starts from a valid V32 SQLite template, preserves its static model
parameter tables, and rebuilds all simulation-specific tables from
`MasterInput.db`.

Configuration keys in `modfilegen.GlobalVariables`:

- `celsius_version`: `v32`;
- `celsius_mode`: `standard` or `successive`;
- `dbMasterInput`: source MasterInput database;
- `dbModelsDictionary`: ModelsDictionary database used to resolve CELSIUS soil
  defaults and soil-specific parameter overrides for any matching `Soil`
  parameter;
- `dbCelsiusV32Template`: valid V32 SQLite template (fallback: `dbCelsius`);
- `celsiusV32Output`: generated database path (optional);
- `celsiusV32Executable`: executable name/path (default: `celsiusV32`);
- `celsiusIdsim`: optional simulation id or list of ids to convert;
- `runCelsiusV32`: `1` to execute the model, `0` to generate only.
- `nthreads`: number of `celsiusV32` processes run in parallel (default `1`);
- `dailyoutput`: `1` to store daily `OutputD_*` rows, otherwise only
  `OutputSynt` is written (default `0`);
- `dt`: `0` to import `OutputSynt` into MasterInput `SummaryOutput`.

With `nthreads > 1`, simulations are split in `ChampTri` order into balanced
parts that never cut a chain of successive simulations (a chain starts at
`codesuite=0`). Each process runs the unmodified engine on its own copy of the
converted database, reduced to its simulations, their management rows and
their weather. Outputs are then appended to the main database in simulation
order and the temporary worker directory is removed. On failure, the worker
databases and logs are kept and their path is reported.

The conversion also indexes the lookups the engine issues for every
simulation (`Dweather`, `Tech_perCrop`, `Soil_layers`, operation lists and
`StadePheno`).

In successive mode, one MasterInput experiment is expanded by `SeasonOrder`.
The first generated season has `codesuite=0`; following seasons have
`codesuite=1`. Crops sharing a `SeasonOrder` are emitted as one association
with `PlantOrder` mapped to `NumCrop`; CELSIUS V32 supports at most two.
`SeasonYearOffset` determines each season year (with legacy
`SowingYearOffset` fallback). The ordered management pattern is repeated for
each `idsim` until the next season's first operation would fall after the
`SimUnitList` end. The first occurrence keeps the experiment start, the last
keeps the experiment end, and each intermediate boundary is the first
management operation of the next occurrence; `DHarvest` does not partition a
successive rotation.

Static parameter tables such as `Cultivars`, `PlantSpecies`, `StadePheno`,
`General_Parameters`, `Mulch`, `ListResidus`, and `CO2Yearly` must already be
present and populated in the template.

For each copied soil, `Soil.CsurNhum` is computed as
`MasterInput.Soil.OrganicC / MasterInput.Soil.OrganicNStock`; conversion fails
with a soil-specific error if `OrganicNStock` is zero.

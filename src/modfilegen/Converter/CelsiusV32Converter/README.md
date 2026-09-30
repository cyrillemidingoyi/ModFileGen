# CELSIUS V32 converter

This converter replaces the legacy Datamill CELSIUS database transformation
for V32. It starts from a valid V32 SQLite template, preserves its static model
parameter tables, and rebuilds all simulation-specific tables from
`MasterInput.db`.

Configuration keys in `modfilegen.GlobalVariables`:

- `celsius_version`: `v32`;
- `celsius_mode`: `standard` or `successive`;
- `dbMasterInput`: source MasterInput database;
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

Static parameter tables such as `Cultivars`, `PlantSpecies`, `StadePheno`,
`General_Parameters`, `Mulch`, `ListResidus`, and `CO2Yearly` must already be
present and populated in the template.

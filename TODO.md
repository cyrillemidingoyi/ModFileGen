# ModFileGen TODO

This file is the project's persistent task list. Keep tasks short, actionable,
and linked to an issue or pull request when one exists.

## In progress

- [ ] Complete soil-specific parameter support for DSSAT using
  `ParameterResolver` and `SoilDataRepository`.
- [ ] Verify the STICS v11 `q0` computed strategy in standard and successive
  notebook runs, including multi-worker runs.

## Next

- [ ] Integrate the common output configuration service into each converter and replace per-model hard-coded `SummaryOutput` mappings.
- [ ] Extend dynamic DSSAT synthesis import from `Summary.OUT` to aggregated `PlantGro.OUT`, `SoilNi.OUT`, `SoilOrg.OUT`, `N2O.OUT`, and `WaterBal.OUT` fields.
- [ ] Re-run all LowInput multi-observation cultivars with constrained AgMIP
  Step 7 before exporting the final optimized cultivar directory.
- [ ] Extend the validated Tensift IA55 training database to the remaining
  sowing-date, irrigation, and nitrogen treatments.
- [ ] Move DSSAT automatic-irrigation parameters (including `ITHRL`) from global
  model defaults to simulation-specific policies.
- [ ] Extend prefetched data access to STICS v11 initial conditions (`ficini`).
- [ ] Replace broad periodic cache clearing with bounded caches where memory
  measurements show it is useful.

## Tests and maintenance

- [ ] DSSAT standard: a `STOP 99` in one simulation aborts its whole chunk and
  the main loop discards the chunk, including simulations that already
  succeeded; skip only the failing simulation and report it.
- [ ] Make the text fixtures runnable by every model and add opt-in
  `integration` tests that run the executables: extend the fixture weather to
  2004 for the DSSAT successive rotation, version the STICS cultivar files
  and DSSAT genotype files, then start with a CELSIUS V32 run.
- [ ] Version `migrations/` again: the committed `.gitignore` rule `migrations/`
  hides the schema migration scripts that `AGENTS.md` requires.
- [ ] Add a reference MasterInput input schema to the package and a test that
  keeps `tests/fixtures/sources/masterinput/schema.sql` identical to it.
- [ ] Add named scenarios to the text MasterInput fixture (`SCN_*` idMangt):
  mineral and organic fertilisation using the existing non-zero policies,
  two soils, point overrides.
- [ ] Move converter tests from the per-model fixture databases to the
  `masterinput_db` / `masterinput_copy` pytest fixtures.
- [ ] Republish `celsiusV32` from the optimized Celsius ADODB layer and check a
  two-crop association run writes identical `OutputD_2` rows before and after.
- [ ] Regenerate `tests/celsius/output_v32_standard/celsius_v32_standard.db`
  (still uses `__TECH` technique identifiers) and document the fixture migration.
- [ ] Add mineral and organic fertilisation cases to the CELSIUS V32 standard
  and successive fixtures and verify that `celsiusV32` applies the inputs
  (the Access reference has `fertiminON` and `fertiorgON` disabled everywhere).
- [ ] Validate CELSIUS V32 generated databases end to end against the Access
  V32 reference for standard, associated-crop, and successive simulations.
- [ ] Extend CELSIUS V32 `SummaryOutput` import to expose the second crop of an
  association without losing `PlantOrder` identity.
- [ ] Add an end-to-end DSSAT regression run proving that standard dates keep
  the legacy extended-DOY convention and successive SQX dates use
  `SowingYearOffset`.
- [ ] Define a realistic automatic-irrigation threshold (`ratiol`) and related
  management overrides before using the fixture for agronomic validation.
- [ ] Add a non-zero irrigation fixture case and verify the generated
  `fictec1.txt` in a standard STICS v11 run.
- [ ] Add a multi-point standard STICS v11 regression test proving that the
  station cache does not reuse `PointParameterOverrides` across `idPoint`.
- [ ] Run the complete pytest suite in an environment containing the project
  development dependencies.
- [ ] Add end-to-end tests proving that simulations sharing an `idSoil` reuse
  prefetched soil data and generated soil-file content.
- [ ] Add regression tests for default, computed, and explicit `q0` values.

## Completed

- [x] Drop DSSAT daily output rows outside StartYear/StartDay to
  EndYear/EndDay (standard and successive), so days DSSAT simulates after the
  simulation end, possibly on rollover weather, never reach `DssatDailyOutput`.

- [x] Skip, without deleting them from `SimUnitList`, the simulations whose
  StartYear/StartDay to EndYear/EndDay period is not covered by consecutive
  `RAclimateD` days of their `idPoint` (`modfilegen.weather_coverage`), in every
  converter; skipped simulations are printed and listed in
  `skipped_simulations_<model>.csv` in the output directory.

- [x] Add a `modelsdictionary` text fixture (`Variables` table only, exported
  from `LowInput/blindphase_corrected/ModelsDictionaryArise.db`) and the
  `modelsdictionary_db` pytest fixture.

- [x] Move `test_celsius_v32_converter.py` to the text fixtures: MasterInput
  cultivars map to CELSIUS V32 codes, and a reduced CELSIUS V32 template is
  versioned in `tests/fixtures/sources/celsius_v32_template`.

- [x] Align the text MasterInput fixture with the LowInput reference input schema
  (`migrations/20261003_align_masterinput_with_lowinput_schema.sql`) and let
  DSSAT successive read `SeasonYearOffset`, keeping `SowingYearOffset` as a
  legacy fallback like STICS v11 and CELSIUS V32.

- [x] Move `test_dssatsuccessiveconverter.py` to the `masterinput_db` text
  fixture, selecting the `ROT_MAIZE_PEANUT_3Y` rotation, and expect the extra
  boundary weather year for its day-120 start.

- [x] Align DSSAT successive synthesis with the standard DSSAT configurable
  pipeline: retain raw `Summary.OUT` fields, transform them through the active
  output selection with per-run `SDAT` date context, and write the same
  canonical columns to CSV and `SummaryOutput` for both `dt` modes.

- [x] Base DSSAT successive summary time and every phenological date on each
  model-estimated `Summary.OUT` `SDAT`, including cross-year and leap-year
  offsets for both implicit repeated cycles and explicit management seasons.

- [x] Resolve DSSAT successive latitude and longitude exclusively from the
  `Coordinates` row linked by `idPoint` for both `dt` modes, leaving missing
  coordinates null without parsing `Idsim`.

- [x] Use the shared `CoordinateResolver` in standard DSSAT for both `dt`
  modes, with `StartYear` as `time` and no coordinate parsing from `Idsim`.

- [x] Use the shared `CoordinateResolver` in STICS v9, STICS v11 standard and
  successive, CELSIUS v3, and CELSIUS V32; all summary CSV and database paths
  now source `lat`/`lon` from `Coordinates` independently of `dt`.

- [x] Make every converter write `SummaryOutput` the same way through
  `OutputConfiguration.replace_model_summary_rows`: create or extend the table,
  replace only the current model's rows; DSSAT successive and STICS v11 no
  longer fail when the table is missing.

- [x] Version the MasterInput test fixture as text (`tests/fixtures/sources/masterinput`:
  `schema.sql` plus one CSV per input table), rebuilt per pytest session; the
  initial export from `tests/stics_successive/MasterInput.db` round-trips exactly.

- [x] Use the simulation identifier as the CELSIUS V32 `IdTech_Com`, dropping
  the `__TECH` suffix to match the Access V32 convention.

- [x] Fix the CELSIUS V32 engine fertilisation reading in the CelsiusV32
  repository: read `FertiOrga_List` instead of `FertiOrg_List`, and check
  `IdTech_Com` against `sIdTec` instead of `sIdSim` for mineral and organic
  inputs; Access/VB.NET regression passes on Windows and Linux.

- [x] Populate lon, lat and time in canonical CELSIUS outputs for dt=1 by
  decoding the shared lat_lon_year simulation identifier convention.

- [x] Write canonical selected outputs to a Celsius CSV independently of dt,
  while keeping OutputSynt as the raw model output.

- [x] Canonicalize STICS result CSV files for both dt modes and preserve
  canonical rows consistently when resuming an interrupted run.

- [x] Document packaged and user-provided output catalogues, named output
  selections, the default legacy selection, and model availability rules.

- [x] Replace CELSIUS v3 and v32 hard-coded SummaryOutput mappings with the
  shared configurable transformation and map transpiration from SigmaTranspiMC.

- [x] Rewrite DSSAT standard result CSVs with canonical shared-variable columns only, while keeping raw `Summary.OUT` fields internal to transformation.

- [x] Write STICS standard result CSVs with canonical selected columns only, keeping raw model fields in an internal temporary CSV and preserving resume compatibility.

- [x] Map DSSAT fresh yield from `FHWAM` (kg/ha) to `FreshYield` (t/ha), treating only the DSSAT `-99` sentinel as missing and preserving zero.

- [x] Replace DSSAT standard hard-coded `SummaryOutput` mappings with the shared configurable transformation for `Summary.OUT`.

- [x] Mark STICS v9 end-of-cycle `msrac(n)` as invalid for `RootBiomass`, pending derivation from daily output.

- [x] Validate the expanded configurable `rap.mod` against the STICS v9 executable, including derived soil mineral N from `azomes + ammomes`.

- [x] Support derived multi-field output mappings and map STICS v9 soil mineral N as `azomes + ammomes`, aligned with STICS v11 `SMNmes`.

- [x] Transform STICS v9 report fields through the shared output catalog and dynamically extend and populate `SummaryOutput` from the selected variables.

- [x] Generate STICS v9 `rap.mod` from the configured output selection, excluding unavailable, invalid, and duplicate mappings while preserving template control lines.

- [x] Remove the obsolete universal-wheel setting so Python-3-only wheels are tagged correctly.

- [x] Add the common output configuration service: load and validate the three packaged YAML catalogs, resolve selections and model mappings, apply generic conversions, and create or extend `SummaryOutput`.

- [x] Run CELSIUS V32 simulations in parallel (`nthreads`) on reduced
  per-process copies that keep successive chains whole, merge outputs in
  simulation order, and index the per-simulation lookups at conversion.

- [x] Make CELSIUS V32 runs usable on the full standard fixture: fix the
  `phenoCTphot` stage overflow on crop death (Celsius VBA), write daily output
  only when `dailyoutput == 1`, and make the Celsius ADODB layer append daily rows without
  reloading the table, in one transaction per simulation.

- [x] Add a native CELSIUS V32 converter that replaces the Datamill mapping,
  supports standard and successive `SeasonOrder`, preserves two-crop
  associations through `PlantOrder`, converts dated management operations,
  runs `celsiusV32`, and imports first-crop synthesis outputs.

- [x] Rebuild the empty DSSAT successive `IrrigationFOperations` fixture table
  with the shared LowInput schema and lookup indexes.

- [x] Make STICS v11 successive single-offset patterns repeat annually, with
  each seasonal end equal to sowing plus `DHarvest`, capped by the global end.

- [x] Document the three-hour Morocco rainfed-wheat training protocol covering
  four climate-sensitivity scenarios, DSSAT execution, paired yield inference,
  interpretation limits, and participant deliverables.

- [x] Extend the Morocco climate-training notebook with scenario-specific
  phenology, phase-resolved DSSAT water stress, and reproductive heat-exposure
  analyses validated against the 30-season successive outputs.

- [x] Add a reusable migration for the soil, crop-management, and point
  parameter override tables and their required unique lookup indexes.

- [x] Keep DSSAT standard date handling unchanged while making successive SQX
  management dates honor `StartYear + SowingYearOffset`.

- [x] Read DSSAT `SSAT` from the shared soil-level `Soil.Ssat` value, with
  backward-compatible fallback to `Soil.Wfc * 1.01 / 100`.

- [x] Retain LowInput cultivar initial parameters whenever calibration does
  not strictly improve the global `imats` MSE, while preserving raw optimizer
  values for diagnostics.

- [x] Preserve each LowInput cultivar's initial `stdrpmat/stlevdrp` ratio
  during every STICS evaluation in AgMIP Step 7.

- [x] Rebuild the LowInput cultivar optimization summary from both newly
  selected runs and previously saved per-cultivar RDS results.

- [x] Make the LowInput sorghum/millet calibration start one repetition from
  each cultivar's initial `stlevdrp` and use the Linux STICS binary that
  correctly applies `param.sti` forcing.

- [x] Export all STICS technical intervention tables to the summary workbook,
  including dynamic `julapI_n` and `doseI_n` irrigation columns.

- [x] Synchronize LowInput copied STICS cultivar filenames, database cultivar
  codes, and internal `codevar` values.

- [x] Import LowInput STICS `codlocirrig` management overrides from observed
  irrigation types.

- [x] Accept repeated STICS intervention-table headers when converting
  irrigated `fictec` text files to XML.

- [x] Import LowInput manual irrigation schedules from
  `data_corrected/irrigated_amount.xlsx` as sowing-relative operations while
  preserving rainfed managements.

- [x] Split LowInput STICS cultivars by GDD class and observed cultivar name,
  with shared variants for identical names and collision-safe suffixes.

- [x] Add the explicit ZIM_I1 (GDD 2000) and ZIM_I2 (GDD 1900) STICS
  cultivar variants for sites absent from `GDD_site.csv`.

- [x] Move STICS v11 `interrang` values from the shared `CropManagement`
  schema to model-specific `fictec1`/`fictec2` management overrides.

- [x] Keep STICS v11 albedo in the physical `Soil.albedo` column rather than
  treating it as a `SoilParameterOverrides` parameter.

- [x] Add generic `PointParameterOverrides`, preload them by `idPoint`, and
  apply them to every dictionary-backed STICS v11 `station` parameter.

- [x] Separate the standard STICS v11 soil and station caches so station files
  are keyed by `idPoint` and mixed-crop status.

- [x] Seed the successive STICS fixture with a `codecalirrig = 1` override for
  season 3 of `ROT_MAIZE_PEANUT_3Y`.

- [x] Add prefetched, model-specific crop-management parameter overrides keyed
  by `(idMangt, SeasonOrder, PlantOrder)` for every dictionary-backed STICS v11
  `fictec1` and `fictec2` parameter.

- [x] Force `codecalirrig = 2` and `codedateappH2O = 2` whenever STICS v11
  manual irrigation operations are generated.

- [x] Verify a complete three-season STICS v11 successive run containing
  rainfed seasons and a season with three manual irrigation interventions.

- [x] Fix STICS v11 manual-irrigation serialization by repeating the table
  header before every intervention, as required by the STICS text reader.

- [x] Integrate prefetched, sowing-relative `IrrigationFOperations` into the
  standard and successive STICS v11 input-generation paths.

- [x] Extend the STICS successive MasterInput fixture with the documented
  `IrrigationFOperations` schema for sowing-relative date and irrigation dose.

- [x] Finalize generated climate-training databases with an explicit SQLite
  WAL checkpoint, DELETE journal mode, and closed connections.

- [x] Add a validated training notebook that derives HIST, +2 °C, −20% rain,
  and combined climate-sensitivity databases from a source MasterInput, with
  a common monthly-climatology comparison figure and an initial four-scenario
  yield-distribution analysis plus paired annual absolute and relative yield
  differences against HIST, 95% confidence intervals, paired t/Wilcoxon tests,
  and Holm multiple-comparison correction.

- [x] Export the extra boundary weather year required by DSSAT successive
  sequences that start after January 1.

- [x] Allow textual irrigation and inorganic-fertilization policy identifiers
  in DSSAT successive treatment levels.

- [x] Migrate the Tutorial `MasterInput.db` fixture with the management and
  cultivar columns required by standard and successive DSSAT simulations.

- [x] Create and validate the initial 30-season Tensift wheat training case
  with cultivar Karim and the global IA55 automatic-irrigation rule.

- [x] Add `ParameterResolver.prefetch()` for STICS v11 `paramsol` defaults and
  soil-specific overrides.
- [x] Add `SoilDataRepository` for prefetched `Soil`, `RunoffTypes`, and
  `SoilLayers` data.
- [x] Add the global STICS v11 `q0` strategy with explicit overrides taking
  priority.
- [x] Share the STICS successive `param.sol` cache within each worker batch.

## Working convention

Move completed items to **Completed** rather than deleting them. Review this
file before starting a feature and after finishing one. Tasks that become large
should be moved to an issue while retaining a short link here.

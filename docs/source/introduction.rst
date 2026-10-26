Introduction
============

**Describe your simulations once. Run them with every crop model.**

ModFileGen turns one shared database into ready-to-run inputs for STICS,
DSSAT, CELSIUS and APSIM, runs the models, and brings their results back into
the same database, with the same variable names and units for every model.
Whether you compare models on one field or simulate thousands of grid points
over decades, you work with one dataset and one workflow instead of one
pipeline per model.

Why ModFileGen
--------------

Each crop model has its own files, formats, units and conventions. Running
several models on the same question usually means writing and maintaining a
converter per model, checking that every model received the same soil, the
same weather and the same management, then reconciling outputs that never
share a name or a unit. That effort grows with every model, every site and
every scenario, and it is where inconsistencies hide.

ModFileGen removes that effort:

- **One description of the world.** Points and weather, soils and their
  layers, initial conditions, crop managements, rotations, fertilisation,
  irrigation and tillage live in a single, documented SQLite database
  (:doc:`database`).
- **Every model gets the same inputs.** Each converter translates that
  description into the model's own files, units and conventions, so
  differences between models come from the models, not from their inputs.
- **Comparable results by construction.** Outputs are written to a shared
  ``SummaryOutput`` table through a common catalogue of variables, units and
  conversions that you can extend (:doc:`output_configuration`).

How it works
------------

.. code-block:: text

    MasterInput (your data)          ModelsDictionary (model parameters)
              │                                   │
              └──────────────┬────────────────────┘
                             ▼
                  ModFileGen converters
       (soil, weather, management, cultivar, options...)
                             │
        ┌───────────┬────────┴───────┬────────────┐
        ▼           ▼                ▼            ▼
      STICS       DSSAT           CELSIUS       APSIM
        │           │                │            │
        └───────────┴────────┬───────┴────────────┘
                             ▼
       SummaryOutput and daily outputs, back in MasterInput

1. You fill **MasterInput** with model-independent data.
2. **ModelsDictionary** provides, for each model, the parameters that your
   data does not describe, with their default values.
3. ModFileGen generates the input files of each simulation, runs the model
   and collects its outputs.

What you get
------------

Many models, one interface
    STICS v9 and v11, DSSAT 4.7 and 4.8, CELSIUS v3 and V32, and APSIM, each
    started with one call (``run_stics()``, ``run_dssat()``,
    ``run_celsius()``, ``run_apsim()``).

Rotations and intercropping
    Standard simulations, successive seasons chained in one run (STICS v11,
    DSSAT, CELSIUS V32), and crop associations (STICS v11, CELSIUS V32).

Built for scale
    Simulations are split across parallel workers, inputs shared by many
    simulations (soils, weather, parameters) are read once per batch, and runs
    work the same way on a laptop or on a computing cluster.

Your data first, model defaults second
    Measured soil properties always win over model defaults. When a soil, a
    soil layer, a management or a point needs its own model parameter, an
    override table sets it without touching the model defaults (see
    :doc:`configuration`).

Safe by default
    A simulation whose weather does not cover its whole period is not run and
    is reported, instead of failing silently or producing results on missing
    days. Inconsistent inputs, such as an override on a layer the soil does
    not have, raise explicit errors.

Transparent and reproducible
    The whole input description is in two SQLite databases you can version,
    share and query. Every estimation choice (for example the soil albedo or
    the STICS ``q0``) is an explicit, documented option.

What you can do with it
-----------------------

- **Multi-model ensembles**: run several crop models on the same sites and
  scenarios and compare their results variable by variable.
- **Large-scale studies**: simulate gridded regions or long time series in
  parallel, and read all results from one table.
- **Scenario analysis**: change sowing dates, fertilisation, irrigation,
  cultivars or rotations in the database and rerun every model.
- **Calibration and evaluation**: store observations next to the
  simulations and evaluate the models on a common basis.

A first look
------------

.. code-block:: python

    from modfilegen import GlobalVariables
    from modfilegen.Converter import run_dssat, run_stics

    GlobalVariables["dbMasterInput"] = "data/MasterInput.db"
    GlobalVariables["dbModelsDictionary"] = "data/ModelsDictionary.db"
    GlobalVariables["directorypath"] = "results"   # result CSV files
    GlobalVariables["tempDir"] = "work"            # generated model inputs
    GlobalVariables["nthreads"] = 8                # parallel workers
    GlobalVariables["dt"] = 0                      # also fill SummaryOutput

    GlobalVariables["pltfolder"] = "cultivars/dssat"
    run_dssat()                                    # DSSAT, standard simulations

    GlobalVariables["pltfolder"] = "cultivars/stics"
    GlobalVariables["stics_version"] = "v11"
    GlobalVariables["stics_mode"] = "successive"
    GlobalVariables["albedoStrategy"] = "texture"
    run_stics()                                    # STICS v11, rotations

Both runs read the same database and write their results, with the same
variable names and units, to the same ``SummaryOutput`` table.

Where to go next
----------------

- :doc:`installation` to install the package.
- :doc:`usage` for complete examples.
- :doc:`configuration` for every option and the rules that combine your data
  with model defaults.
- :doc:`database` for the description of every table and column.
- :doc:`output_configuration` to choose and extend the shared outputs.
- :doc:`models` for the supported models.

ModFileGen is developed by the LIMA team and is open source under the MIT
license. Contributions and feedback are welcome (:doc:`contributing`).

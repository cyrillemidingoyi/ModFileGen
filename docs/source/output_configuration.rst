Output configuration
====================

ModFileGen uses a common output catalogue to expose model results with the
same column names and units across STICS, DSSAT, and CELSIUS. The configuration
also controls which shared variables are written to ``SummaryOutput``.

Default configuration
---------------------

The package includes three default YAML files:

``output_variables.yml``
    Defines each shared variable, its output column, type, unit, and
    model-specific correspondence and conversion.

``output_selections.yml``
    Defines named lists of shared variables.

``profile_variables.yml``
    Defines profile variables, such as soil variables reported by layer.

When no custom path is provided, ModFileGen loads these packaged files
automatically. The default selection name is ``legacy``. Therefore, the
following setting is optional:

.. code-block:: python

    GlobalVariables["outputSelection"] = "legacy"

Configuration file versus selection name
----------------------------------------

``outputSelectionsConfig`` and ``outputSelection`` have different roles:

``outputSelectionsConfig``
    Is the path to the YAML file containing one or more named selections.

``outputSelection``
    Is the name of the selection to use from that file. If it is omitted,
    ModFileGen uses ``legacy``.

For example:

.. code-block:: python

    from modfilegen import GlobalVariables

    GlobalVariables["outputSelectionsConfig"] = (
        "my_config/output_selections.yml"
    )
    GlobalVariables["outputSelection"] = "nitrogen"

The referenced file may contain several selections:

.. code-block:: yaml

    version: 1

    selections:
      minimal:
        variables:
          - planting
          - maturity
          - yield

      nitrogen:
        variables:
          - planting
          - maturity
          - yield
          - grain_n_at_maturity
          - soil_nitrogen
          - cumulative_n_leached

This separation lets users switch between output sets without maintaining a
different configuration file for every run.

Selecting existing variables
----------------------------

To select variables that already exist in the packaged catalogue, users only
need a custom ``output_selections.yml`` file. The packaged variable and profile
catalogues remain active:

.. code-block:: python

    GlobalVariables["outputSelectionsConfig"] = (
        "my_config/output_selections.yml"
    )
    GlobalVariables["outputSelection"] = "nitrogen"

The selected variables determine the shared columns written to
``SummaryOutput``. Identity columns such as ``Model``, ``IdSim``,
``SeasonOrder``, and ``PlantOrder`` are managed separately and do not need to
be listed in a selection.

Providing all three custom files
--------------------------------

Users who need to add mappings or profile variables can provide all three
configuration files:

.. code-block:: python

    GlobalVariables["outputVariablesConfig"] = (
        "my_config/output_variables.yml"
    )
    GlobalVariables["outputSelectionsConfig"] = (
        "my_config/output_selections.yml"
    )
    GlobalVariables["profileVariablesConfig"] = (
        "my_config/profile_variables.yml"
    )
    GlobalVariables["outputSelection"] = "my_selection"

Custom files should normally be copied into the user's project. Users should
not edit files inside the installed ``modfilegen`` package because package
updates may replace them.

Adding a new shared variable
----------------------------

A new shared variable must first be declared in
``output_variables.yml``. Its key can then be included in a named selection.
For example:

.. code-block:: yaml

    variables:
      harvest_index:
        column: HarvestIndex
        description: Harvest index
        type: REAL
        unit: ratio
        required: false
        models:
          dssat:
            source: Summary.OUT
            field: HIAM
            source_unit: ratio
            scale: 1.0
            offset: 0.0
            status: verified

The selection refers to the variable key, not the output column:

.. code-block:: yaml

    selections:
      my_selection:
        variables:
          - planting
          - maturity
          - yield
          - harvest_index

Model availability
------------------

Selecting a variable does not guarantee that every crop model produces it.
The result depends on the model mapping and source file:

* STICS v9 uses the selection to build ``rap.mod`` and request the configured
  report variables.
* DSSAT transforms fields from the configured DSSAT output modules. A module
  must be enabled and parsed before its fields can be populated.
* CELSIUS selects and transforms fields already produced in ``OutputSynt``.

For CELSIUS v3 and v32, ``OutputSynt`` remains the raw model output and
ModFileGen also writes a canonical ``*_celsius.csv`` file for every completed
run. This CSV is produced for both values of ``dt``. Setting ``dt = 0``
additionally writes the same canonical rows to ``SummaryOutput``.

When a selected optional variable has no valid mapping for a model, its shared
column is written as ``NULL``. A missing required source field is reported as
a configuration error. Raw model outputs remain model-specific; the
configuration controls the canonical values stored in ``SummaryOutput``.

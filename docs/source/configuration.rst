Configuration
=============

ModFileGen is configured through the ``GlobalVariables`` dictionary. Set the
options before calling a converter:

.. code-block:: python

    from modfilegen import GlobalVariables
    from modfilegen.Converter import run_dssat

    GlobalVariables["dbMasterInput"] = "path/to/MasterInput.db"
    GlobalVariables["dbModelsDictionary"] = "path/to/ModelsDictionary.db"
    GlobalVariables["directorypath"] = "path/to/output"
    GlobalVariables["albedoStrategy"] = "texture"
    run_dssat()

Options not set keep the default listed below. Text values are compared
without case or surrounding spaces, and an unknown value raises a
``ValueError``.

Databases and directories
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Meaning
   * - ``dbMasterInput``
     - Path of the MasterInput database (simulations, soils, weather,
       managements). Required.
   * - ``dbModelsDictionary``
     - Path of the ModelsDictionary database (model parameter defaults).
       Required.
   * - ``directorypath``
     - Directory receiving the result CSV files and the reports, such as
       ``skipped_simulations_<model>.csv``. Default: current directory.
   * - ``tempDir``
     - Working directory where the model input files are generated and the
       models are run.
   * - ``pltfolder``
     - Directory of the model cultivar files (STICS plant files, DSSAT
       cultivar files).
   * - ``package``
     - Path of the ModFileGen package data (STICS parameter files). Default:
       the installed package.

Model selection
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Option
     - Values
     - Meaning
   * - ``stics_version``
     - ``v9``, ``v11``
     - STICS version used by ``run_stics()``. No default.
   * - ``stics_mode``
     - ``standard`` (default), ``successive``
     - ``successive`` chains the seasons of a rotation; STICS v11 only.
   * - ``dssat_mode``
     - ``standard`` (default), ``successive``
     - ``successive`` runs a rotation as one DSSAT sequence.
   * - ``dssat_version``
     - ``v47`` (default), ``v48``
     - DSSAT executable and batch file version.
   * - ``celsius_version``
     - ``v3`` (default), ``v32``
     - CELSIUS version used by ``run_celsius()``.
   * - ``celsius_mode``
     - ``standard`` (default), ``successive``
     - ``successive`` is supported by CELSIUS V32 only.

CELSIUS V32 and APSIM have further options, described in their converter
README files (for example ``dbCelsiusV32Template``, ``celsiusV32Executable``,
``runCelsiusV32``, ``apsim_path``, ``template_apsimx``).

Runs and outputs
----------------

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Option
     - Values
     - Meaning
   * - ``nthreads``
     - integer
     - Number of parallel worker processes.
   * - ``parts``
     - integer, default ``1``
     - The simulations are split into ``parts × nthreads`` chunks.
   * - ``dt``
     - ``0``, ``1`` (default)
     - ``1``: results are written to CSV files only and each simulation
       working directory is deleted after its run. ``0``: results are also
       written to the MasterInput ``SummaryOutput`` table and the working
       directories are kept.
   * - ``dailyoutput``
     - ``0`` (default), ``1``
     - ``1`` also stores the daily model outputs in MasterInput.
   * - ``thirdyear``
     - ``0`` (default), ``1``
     - DSSAT standard: also writes the weather file of the second year after
       the simulation start year.
   * - ``resume_stics``
     - ``0`` (default), ``1``
     - STICS v9: ``1`` resumes an interrupted run and keeps the results
       already written.
   * - ``outputSelection`` and the output catalogue options
     - see :doc:`output_configuration`
     - Shared output variables written to ``SummaryOutput``.

Estimation choices
------------------

``albedoStrategy``
    Soil albedo used by every model. ``"measured"`` (default) uses
    ``Soil.albedo``. ``"texture"`` re-estimates it from
    ``Soil.SoilTextureType``:

    ===================  =======
    Texture              Albedo
    ===================  =======
    clay                 0.28
    clay loam            0.20
    clayey loam          0.20
    loam                 0.225
    loamy sand           0.25
    any other or empty   0.30
    ===================  =======

    The texture is compared without case or surrounding spaces. The choice is
    applied once when soils are read, so STICS, DSSAT and APSIM receive the
    same value.

``sticsv11Q0Strategy``
    STICS v11 ``q0`` (cumulative soil evaporation limit of the first stage,
    mm). ``"default"`` uses the ModelsDictionary default. ``"computed"``
    derives it from the soil texture (``Soil.Sand`` and ``Soil.Clay``, %):

    - sand > 80: ``q0 = 5 + 0.15 × (100 − sand)``;
    - else clay > 50: ``q0 = 5 + 0.06 × (100 − clay)``;
    - else: ``q0 = 8 + 0.08 × clay``.

    A ``SoilParameterOverrides`` value for ``q0`` always wins over both
    strategies.

Soil parameters
---------------

Soil values come from two sources, combined by each converter with the same
rule:

- **measured properties**, read from the MasterInput ``Soil`` and
  ``SoilLayers`` tables (water contents, bulk density, carbon, texture,
  saturation...);
- **model parameters**, read from ModelsDictionary and replaced, for a given
  soil, by the ``SoilParameterOverrides`` table.

Model parameters
~~~~~~~~~~~~~~~~

For each parameter, the value is the ModelsDictionary default, replaced by an
override of the soil. ``SoilParameterOverrides.NumLayer`` sets the scope of
the override:

- ``0``: the whole profile;
- ``1`` to ``n``: one layer, numbered in ``SoilLayers`` order (a ``simple``
  soil has layer 1 only).

At layer *k*, a parameter takes the override of layer *k*, else the override
of the profile, else the default. Parameters that hold one value for the whole
soil (for example STICS ``q0`` or DSSAT ``dssat_soil_site`` parameters) accept
``NumLayer = 0`` only. An override on another layer, or on a layer the soil
does not have, raises an error.

Measured properties
~~~~~~~~~~~~~~~~~~~

A measured property is written as measured. Only an explicit override that
applies to the layer replaces it; a ModelsDictionary default never does. When
a property is not measured, the default is used.

Some measured properties have a dedicated fallback:

- ``Ssat`` (saturated water content, %): ``SoilLayers.Ssat``, else
  ``Soil.Ssat``, else derived from the field capacity.
- ``Ssks`` (saturated hydraulic conductivity, cm/h): ``SoilLayers.Ssks``,
  else ``Soil.Ssks``, else the model default. APSIM ``KS`` uses it in mm/day
  (1 cm/h = 240 mm/day).
- ``DrainFAO`` (FAO drainage class 1 to 7): DSSAT ``SLDR`` takes the ``sldr``
  override, else the value of the class (0.01, 0.05, 0.25, 0.40, 0.60, 0.75,
  0.85), else the ModelsDictionary default.
- ``Coordinates.tav`` and ``Coordinates.amp`` (°C): DSSAT weather files use
  them when filled, else the ModelsDictionary defaults.

The albedo stays a measured soil property, controlled only by
``albedoStrategy``.

Weather coverage
----------------

A simulation is run only if the weather of its ``idPoint`` covers its whole
period, from ``StartYear``/``StartDay`` to ``EndYear``/``EndDay``, without
missing days. The other simulations are skipped, kept in ``SimUnitList``, and
listed in ``skipped_simulations_<model>.csv`` in ``directorypath``. Daily
outputs are limited to the simulation period.

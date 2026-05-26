ModFileGen
==========

.. image:: https://readthedocs.org/projects/modfilegen/badge/?version=latest
   :target: https://modfilegen.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

ModFileGen is a Python package designed to **generate and run model input files for different models** based on a standardized shared database.  
It serves as a bridge between shared datasets and simulation-ready input files for crop models.

Overview
--------

The package enables:

- Generation of model-specific input files from a **unified MasterInput database**
- Support for multiple model types via configuration
- Automation of batch simulations using defined strategies
- Integration with standardized simulation units and metadata

ModFileGen simplifies model interoperability and fosters reproducibility by separating data, configuration, and execution layers.

Features
--------

- 📄 Input file generation for various crop/environmental models (DSSAT, STICS, Celsius, etc.)
- 🛠️ Flexible support for model templates (JSON, XML, INI, etc.)
- 🗃️ Based on a shared, versioned **SQLite database**
- 🚀 Batch simulation launcher (optional, model-dependent)
- 🔄 Version tracking and reproducible configurations
- ⚡ Parallel processing support for large-scale simulations

Database Structure
------------------

ModFileGen expects data to conform to the **MasterInput** schema (climate, soil, management, etc.).  
It may also use a **ModelsDictionary** schema for model metadata and input requirements.

**Important:** All database tables used in INNER JOIN queries must have matching records to avoid runtime errors. 
Key tables include:

- ``SimUnitList`` - Simulation units
- ``CropManagement`` - Management practices  
- ``SoilTillPolicy`` - Tillage operations
- ``OrganicFertilizationPolicy`` - Fertilization policies
- ``ListCultivars`` - Cultivar definitions
- ``Coordinates`` - Geographic locations

See the full database specification in the documentation (In progress):
`https://modfilegen.readthedocs.io <https://modfilegen.readthedocs.io/en/latest/>`_

Installation
------------

You can install the package using pip (once distributed via PyPI):

.. code-block:: bash

    pip install modfilegen

Or clone the repository and install locally:

.. code-block:: bash

    git clone https://github.com/CropModelingPlatform/ModFileGen.git
    cd modfilegen
    pip install -e .

Usage
-----

**STICS Example:**

.. code-block:: python

    from modfilegen.Converter.SticsConverter.sticsconverter import SticsConverter
    from modfilegen import GlobalVariables
    import os

    directory_path = "path/to/your/data"
    
    # Set GlobalVariables
    GlobalVariables["dbModelsDictionary"] = directory_path + "/ModelsDictionary.db"
    GlobalVariables["dbMasterInput"] = directory_path + "/MasterInput.db"
    
    c = SticsConverter()
    c.nthreads = 6
    c.DirectoryPath = directory_path
    c.pltfolder = os.path.join(directory_path, "cultivars", "stics")
    result = c.export()

**DSSAT Example:**

.. code-block:: python

    from modfilegen.Converter.DssatConverter import dssatconverter
    from modfilegen import GlobalVariables
    import os

    directory_path = "path/to/your/data"
    
    # Set all required GlobalVariables
    GlobalVariables["dbModelsDictionary"] = directory_path + "/ModelsDictionary.db"
    GlobalVariables["dbMasterInput"] = directory_path + "/MasterInput.db"
    GlobalVariables["directorypath"] = directory_path + "/output"
    GlobalVariables["pltfolder"] = os.path.join(directory_path, "cultivars", "dssat")
    GlobalVariables["nthreads"] = 4  # Number of parallel threads
    GlobalVariables["dt"] = 1  # Delete temporary files (0=keep, 1=delete)
    GlobalVariables["parts"] = 1  # Number of parts for chunking
    
    # Run converter
    dssatconverter.main()

**Celsius Example:**

.. code-block:: python

    from modfilegen.Converter.CelsiusConverter import celsiusconverter
    from modfilegen import GlobalVariables

    GlobalVariables["dbModelsDictionary"] = "path/to/ModelsDictionary.db"
    GlobalVariables["dbMasterInput"] = "path/to/MasterInput.db"
    GlobalVariables["dbCelsius"] = "path/to/Celsius.db"
    GlobalVariables["directorypath"] = "path/to/output"
    GlobalVariables["nthreads"] = 4
    GlobalVariables["dt"] = 1
    GlobalVariables["ori_MI"] = "path/to/original_MasterInput.db"
    GlobalVariables["parts"] = 1
    
    celsiusconverter.main()

Troubleshooting
---------------

**Common Issues:**

1. **Segmentation Fault during export:**
   
   - **Cause:** SQL queries returning empty results due to missing data in joined tables
   - **Solution:** Verify all required tables have matching records for your simulation units
   - **Check:** 
   
     .. code-block:: sql
     
        -- Verify SimUnitList has data
        SELECT COUNT(*) FROM SimUnitList;
        
        -- Check if related tables have matching records
        SELECT SimUnitList.idsim, CropManagement.idMangt 
        FROM SimUnitList 
        LEFT JOIN CropManagement ON SimUnitList.idMangt = CropManagement.idMangt
        WHERE CropManagement.idMangt IS NULL;
        
   - If this query returns rows, those SimUnitList entries are missing CropManagement data

2. **Database locked errors:**
   
   - **Cause:** Multiple threads trying to access SQLite simultaneously
   - **Solution:** Set ``GlobalVariables["nthreads"] = 1`` for single-threaded execution

3. **Memory issues with large simulations:**
   
   - Increase ``GlobalVariables["parts"]`` to split data into more chunks
   - Reduce ``GlobalVariables["nthreads"]`` if running out of memory

Documentation
-------------

Full documentation is available at:
`https://modfilegen.readthedocs.io <https://modfilegen.readthedocs.io/en/latest/>`_

Contributing
------------

Contributions are welcome! Please submit issues or pull requests via GitHub:
`https://github.com/CropModelingPlatform/ModFileGen <https://github.com/CropModelingPlatform/ModFileGen>`_

License
-------

ModFileGen is developed by the **LIMA Team** and distributed under the **MIT License**.  
See the `LICENSE <https://github.com/CropModelingPlatform/ModFileGen/blob/main/LICENSE>`_ file for details.

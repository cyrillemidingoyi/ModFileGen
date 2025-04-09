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

- 📄 Input file generation for various crop/environmental models
- 🛠️ Flexible support for model templates (JSON, XML, INI, etc.)
- 🗃️ Based on a shared, versioned SQLite or Access database
- 🚀 Batch simulation launcher (optional, model-dependent)
- 🔄 Version tracking and reproducible configurations

Database Structure
------------------

ModFileGen expects data to conform to the **MasterInput** schema (climate, soil, management, etc.).  
It may also use a **ModelsDictionary** schema for model metadata and input requirements.

See the full database specification in the documentation:
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

Basic usage:

.. code-block:: python

    from modfilegen.Converter.SticsConverter.sticsconverter import SticsConverter
	from modfilegen import GlobalVariables

	directory_path = "path/to/your/data"
	modeldictionnary_f = directory_path + "/ModelsDictionary.db"
	masterinput_f =  directory_path + "/MasterInput.db"
	GlobalVariables["dbModelsDictionary" ] = modeldictionnary_f     
	GlobalVariables["dbMasterInput" ] = masterinput_f 
	c = SticsConverter()
	c.nthreads = 6
	c.DirectoryPath = directory_path
	c.pltfolder = os.path.join(data,"cultivars","stics")
	result = c.export()

Documentation
-------------

Full documentation is available at:
`https://modfilegen.readthedocs.io <https://modfilegen.readthedocs.io/en/latest/>`_

Contributing
------------

Contributions are welcome! Please submit issues or pull requests via GitLab.

License
-------

ModFileGen is developed by the **LIMA Team** and distributed under the **MIT License**.  
See the `LICENSE <https://gitlab.com/your-username/modfilegen/-/blob/main/LICENSE>`_ file for details.

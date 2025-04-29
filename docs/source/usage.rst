Usage
=====

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
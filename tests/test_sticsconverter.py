from modfilegen.Converter.SticsConverter.sticsconverter import SticsConverter
from modfilegen import GlobalVariables
import sqlite3
from pathlib import Path
import os

data = os.path.join(Path(__file__).parent, "data")
modeldictionnary_f = os.path.join(data,"ModelsDictionaryArise.db")
masterinput_f =  os.path.join(data, "MasterInput.db")
directory_path = data

GlobalVariables["dbModelsDictionary" ] = modeldictionnary_f     
GlobalVariables["dbMasterInput" ] = masterinput_f 
GlobalVariables["dt"] = 0
 
def test_sticsconverter():
    c = SticsConverter()
    c.nthreads = 6
    c.DirectoryPath = directory_path
    c.pltfolder = os.path.join(data,"cultivars","stics")
    r = c.export()
    return r

if __name__ == "__main__":
    test_sticsconverter()
    print("test_sticsconverter")
    
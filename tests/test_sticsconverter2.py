from modfilegen.Converter.SticsConverter import sticsconverter
from modfilegen import GlobalVariables
import sqlite3
from pathlib import Path
import os

data = os.path.join(Path(__file__).parent, "data")
modeldictionnary_f = os.path.join(data,"ModelsDictionaryArise.db")
masterinput_f =  os.path.join(data, "MasterInput.db")


GlobalVariables["dbModelsDictionary" ] = modeldictionnary_f     
GlobalVariables["dbMasterInput" ] = masterinput_f 
GlobalVariables["directorypath"] = data   # contains the path of list of USM
GlobalVariables["pltfolder"] = os.path.join(data,"cultivars","stics") # path of cultivars
GlobalVariables["nthreads"] = 6

def test_sticsconverter():
    sticsconverter.main()
    return 0

if __name__ == "__main__":
    test_sticsconverter()
    print("test_sticsconverter")

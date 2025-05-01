from modfilegen.Converter.DssatConverter import dssatconverter
from modfilegen import GlobalVariables
import sqlite3
from pathlib import Path
import os

data = os.path.join(Path(__file__).parent, "data")
modeldictionnary_f = os.path.join(data,"ModelsDictionaryArise.db")
masterinput_f =  os.path.join(data, "MasterInput.db")
directorypath = os.path.join(data, "output")


GlobalVariables["dbModelsDictionary" ] = modeldictionnary_f     
GlobalVariables["dbMasterInput" ] = masterinput_f 
GlobalVariables["directorypath"] = directorypath   # contains the path of list of USM
GlobalVariables["pltfolder"] = os.path.join(data,"cultivars","dssat") # path of cultivars
GlobalVariables["nthreads"] = 4
GlobalVariables["dt"] = 0

def test_dssatconverter():
    dssatconverter.main()
    return 0

if __name__ == "__main__":
    test_dssatconverter()
    print("test_dssatconverter")

from modfilegen.Converter.CelsiusConverter import celsiusconverter
from modfilegen import GlobalVariables
import sqlite3
from pathlib import Path
import os

data = os.path.join(Path(__file__).parent, "data")
modeldictionnary_f = os.path.join(data,"ModelsDictionaryArise.db")
masterinput_f =  os.path.join(data, "MasterInput.db")
celsius_f = os.path.join(data, "CelsiusV3nov17_dataArise.db")
ori_mi = os.path.join(data, "ori_MasterInput.db")


GlobalVariables["dbModelsDictionary" ] = modeldictionnary_f     
GlobalVariables["dbMasterInput" ] = masterinput_f 
GlobalVariables["directorypath"] = data   # contains the path of list of USM
GlobalVariables["nthreads"] = 4
GlobalVariables["dt"] = 0
GlobalVariables["ori_MI"] = ori_mi
GlobalVariables["dbCelsius"] = celsius_f


def test_celsiusconverter():
    celsiusconverter.main()
    return 0

if __name__ == "__main__":
    test_celsiusconverter()
    print("test_celsiusconverter")

from modfilegen.Converter.SticsConverter.sticstempoparv6converter import SticsTempoparv6Converter
import sqlite3
from pathlib import Path
import os

data = os.path.join(Path(__file__).parent, "data")
print(data)
modeldictionnary_f = os.path.join(data,"ModelsDictionaryArise.db")
masterinput_f =  os.path.join(data, "MasterInput.db")
directory_path = data

masterinput = sqlite3.connect(masterinput_f)
modeldictionnary = sqlite3.connect(modeldictionnary_f)

def test_sticstempoparv6converter():
    stv6 = SticsTempoparv6Converter()
    r = stv6.export(directory_path, modeldictionnary)
    return r

if __name__ == "__main__":
    test_sticstempoparv6converter()
    print("test_tempoparv6")
    

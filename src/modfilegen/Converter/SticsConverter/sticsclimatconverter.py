from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsClimatConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        file_name = "climat.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)        
        Site = ST[-2]
        Year = ST[-1]
        T = "Select   Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table]= 'st_climat'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        fetchAllQuery = "select * from RaClimateD where idPoint='" + Site + "' And (Year=" + Year + " or Year=" + str(int(Year) + 1) + ");"
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            fileContent += row["idPoint"] + " "
            year = row["year"]
            fileContent += str(year) + " "
            mois = row["Nmonth"]
            fileContent += str(mois).rjust(3)
            jour = row["NdayM"]
            fileContent += str(jour).rjust(3)
            jjulien = row["DOY"]
            fileContent += str(jjulien).rjust(4)
            mintemp = row["tmin"]
            fileContent += format(mintemp, ".1f").rjust(8)
            maxtemp = row["tmax"]
            fileContent += format(maxtemp, ".1f").rjust(7)
            gradiation = row["srad"]
            fileContent += format(gradiation, ".1f").rjust(7)
            ppet = row["Etppm"]
            fileContent += format(ppet, ".1f").rjust(7)
            precipitation = row["rain"]
            fileContent += format(precipitation, ".1f").rjust(7)
            vent = row["wind"]
            fileContent += format(vent, ".1f").rjust(7)
            #surfpress = row["Surfpress"]
            rw = DT[DT["Champ"] == "vapeurp"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".1f").rjust(7)
            rw = DT[DT["Champ"] == "co2"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".1f").rjust(7)
            fileContent += "\n"
        try:
            # Export file to specified directory    
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        
        return fileContent

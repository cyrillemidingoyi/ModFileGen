from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsFicIniConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        fileName = "ficini.txt"
        fileContent = ""
        ST = directory_path.split(os.path.sep)
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table] = 'st_ficini'));"
        fetchAllQuery = """SELECT SimUnitList.idIni, Soil.IdSoil, Soil.SoilOption, Soil.Wwp, Soil.Wfc, Soil.bd, InitialConditions.WStockinit, InitialConditions.Ninit 
        FROM InitialConditions INNER JOIN (Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) ON InitialConditions.idIni = SimUnitList.idIni
        where idSim = '%s';"""%(ST[-3])
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            fileContent += ":nbplantes:" + "\n"
            rw = DT[DT["Champ"] == "nbplantes"]
            fileContent += rw["dv"].values[0] + "\n"
            fileContent += ":plante:" + "\n"
            rw = DT[DT["Champ"] == "stade0"]
            fileContent += rw["dv"].values[0] + "\n"
            rw = DT[DT["Champ"] == "lai0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            rw = DT[DT["Champ"] == "masec0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            rw = DT[DT["Champ"] == "zrac0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            rw = DT[DT["Champ"] == "magrain0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            rw = DT[DT["Champ"] == "qnplante0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            rw = DT[DT["Champ"] == "resperenne0"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + "\n"
            fileContent += ":densinitial:" + "\n"
            rw = DT[DT["Champ"] == "densinitial"]
            fileContent += format(float(rw["dv"].values[0]), ".1f") + " 0.0 0.0 0.0 0.0" + "\n"
            fileContent += ":plante:"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += "\n"
            fileContent += ":densinitial:" + "\n"
            fileContent += "     " + "\n"
            
            sql = "Select * From soillayers where Lower(idsoil)= '" + row["IdSoil"].lower() + "' Order by NumLayer"
            Adp = pd.read_sql_query(sql, master_input_connection)
            jeu = Adp.to_dict(orient='records')

            fileContent += ":hinit:" + "\n"
            if row["SoilOption"].lower() == "simple":
                fileContent += format((row["Wwp"] + row["WStockinit"] * (row["Wfc"] - row["Wwp"]) / 100) / row["bd"], ".4f")  + " 0.0 0.0 0.0 0.0" + "\n"
            else:
                for i in range(5):
                    if i < len(jeu):
                        row = jeu[i]
                        fileContent += format((jeu[i]["Wwp"] + row["WStockinit"] * (jeu[i]["Wfc"] - jeu[i]["Wwp"]) / 100) / jeu[i]["bd"], ".4f")
                    else:
                        fileContent += "0.0"
                fileContent += "\n"

            fileContent += ":NO3init:" + "\n"
            if row["SoilOption"].lower() == "simple":
                fileContent += format(row["Ninit"], ".1f") + " 0.0 0.0 0.0 0.0" + "\n"
            else:
                for i in range(5):
                    if i < len(jeu):
                        fileContent += format(row["Ninit"] / len(jeu), ".1f")
                    else:
                        fileContent += "0.0"
                fileContent += "\n"
                
            fileContent += ":NH4init:" + "\n"
            rw = DT[DT["Champ"] == "NH4initf"]
            fileContent += rw["dv"].values[0] + " 0.0 0.0 0.0 0.0" + "\n"
        fileContent += "\n"

        try:
            self.write_file(usmdir, fileName, fileContent)
        except Exception as e:
            traceback.print_exc()
            print("Error during writing file : " + str(e))


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
        file_lines = []
        fileContent = ""
        ST = directory_path.split(os.path.sep)
        id_sim = ST[-3]
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table] = 'st_ficini'));"
        fetchAllQuery = """SELECT SimUnitList.idIni, Soil.IdSoil, Soil.SoilOption, Soil.Wwp, Soil.Wfc, Soil.bd, InitialConditions.WStockinit, InitialConditions.Ninit 
        FROM InitialConditions INNER JOIN (Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) ON InitialConditions.idIni = SimUnitList.idIni
        where idSim = '%s';"""%(id_sim)
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        defaults = DT.set_index("Champ")["dv"].to_dict()
        
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            file_lines.append(":nbplantes:")
            file_lines.append(str(defaults["nbplantes"]))
            file_lines.append(":plante:")
            file_lines.append(str(defaults["stade0"]))
            file_lines.append(f"{float(defaults['lai0']):.1f}")
            file_lines.append(f"{float(defaults['masec0']):.1f}")
            file_lines.append(f"{float(defaults['zrac0']):.1f}")
            file_lines.append(f"{float(defaults['magrain0']):.1f}")
            file_lines.append(f"{float(defaults['qnplante0']):.1f}")
            file_lines.append(f"{float(defaults['resperenne0']):.1f}")
            file_lines.append(":densinitial:")
            file_lines.append(f"{float(defaults['densinitial']):.1f} 0.0 0.0 0.0 0.0")
            file_lines.append(":plante:")
            file_lines.extend([""] * 8)  # blank lines for :plante: section
            file_lines.append(":densinitial:")
            file_lines.append("     ")  # unclear content
            
            '''fileContent += ":nbplantes:" + "\n"
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
            fileContent += "     " + "\n"'''
            
            sql = "Select * From soillayers where Lower(idsoil)= '" + row["IdSoil"].lower() + "' Order by NumLayer"
            Adp = pd.read_sql_query(sql, master_input_connection)
            jeu = Adp.to_dict(orient='records')

            #fileContent += ":hinit:" + "\n"
            file_lines.append(":hinit:")
            if row["SoilOption"].lower() == "simple":
                hinit_val = (row["Wwp"] + row["WStockinit"] * (row["Wfc"] - row["Wwp"]) / 100) / row["bd"]
                #fileContent += format((row["Wwp"] + row["WStockinit"] * (row["Wfc"] - row["Wwp"]) / 100) / row["bd"], ".4f")  + " 0.0 0.0 0.0 0.0" + "\n"
                file_lines.append(f"{hinit_val:.4f} 0.0 0.0 0.0 0.0")
            else:
                hinit_vals = []
                for i in range(5):
                    if i < len(jeu):
                        lyr = jeu[i]
                        #fileContent += format((jeu[i]["Wwp"] + row["WStockinit"] * (jeu[i]["Wfc"] - jeu[i]["Wwp"]) / 100) / jeu[i]["bd"], ".4f")
                        hinit = (lyr["Wwp"] + row["WStockinit"] * (lyr["Wfc"] - lyr["Wwp"]) / 100) / lyr["bd"]
                        hinit_vals.append(f"{hinit:.4f}")
                    else:
                        #fileContent += "0.0"
                        hinit_vals.append("0.0")
                #fileContent += "\n"
                file_lines.append(" ".join(hinit_vals))

            #fileContent += ":NO3init:" + "\n"
            file_lines.append(":NO3init:")
            if row["SoilOption"].lower() == "simple":
                #fileContent += format(row["Ninit"], ".1f") + " 0.0 0.0 0.0 0.0" + "\n"
                file_lines.append(f"{row['Ninit']:.1f} 0.0 0.0 0.0 0.0")
            else:
                no3_vals = []
                for i in range(5):
                    if i < len(jeu):
                        #fileContent += format(row["Ninit"] / len(jeu), ".1f")
                        no3_vals.append(f"{row['Ninit'] / len(jeu):.1f}")
                    else:
                        #fileContent += "0.0"
                        no3_vals.append("0.0")
                #fileContent += "\n"
                file_lines.append(" ".join(no3_vals))

            file_lines.append(":NH4init:")
            #rw = DT[DT["Champ"] == "NH4initf"]
            file_lines.append(f"{defaults['NH4initf']} 0.0 0.0 0.0 0.0")
            #fileContent += rw["dv"].values[0] + " 0.0 0.0 0.0 0.0" + "\n"
        #fileContent += "\n"
        file_lines.append("")

        try:
            #self.write_file(usmdir, fileName, fileContent)
            self.write_file(usmdir, fileName, "\n".join(file_lines))
        except Exception as e:
            traceback.print_exc()
            print("Error during writing file : " + str(e))
        return "\n".join(file_lines) + "\n"   #fileContent 


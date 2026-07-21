from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsFicIniConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir, season_order=None):
        fileName = "ficini.txt"
        file_lines = []
        fileContent = ""
        ST = directory_path.split(os.path.sep)
        id_sim = ST[-3]
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'sticsv11') And ([Table] = 'ficini'));"
        fetchAllQuery = """SELECT SimUnitList.idIni, Soil.IdSoil, Soil.SoilOption, Soil.Wwp, Soil.Wfc, Soil.bd, InitialConditions.WStockinit, InitialConditions.Ninit 
        FROM InitialConditions INNER JOIN (Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) ON InitialConditions.idIni = SimUnitList.idIni
        where idSim = '%s';"""%(id_sim)
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        defaults = DT.set_index("Champ")["dv"].to_dict()
        
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            file_lines.append(":nbplantes:")

            sql = """SELECT Max(CropManagement.PlantOrder) AS MaxDePlantOrder FROM CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt WHERE SimUnitList.idsim = '%s'"""%(id_sim)
            if season_order is not None:
                sql += " AND CropManagement.SeasonOrder = %d" % int(season_order)
            DA2 = pd.read_sql_query(sql, master_input_connection)
            rows2 = DA2.to_dict(orient='records')
            
            if len(rows2) > 0:
                nbplt = rows2[0]["MaxDePlantOrder"]
                file_lines.append(str(nbplt))
            else:
                nbplt = 1
                file_lines.append("1")
    
            file_lines.append(":plante:")        
            file_lines.append(str(defaults["stade0"]))
            file_lines.append(f"{float(defaults['lai0']):.1f}")
            file_lines.append(f"{float(defaults['magrain0']):.1f}")
            file_lines.append(f"{float(defaults['zrac0']):.1f}")
            file_lines.append("code_acti_reserve")
            file_lines.append(str(defaults["code_acti_reserve"]))
            file_lines.append(f"{float(defaults['maperenne0']):.1f}")
            file_lines.append(f"{float(defaults['QNperenne0']):.1f}")
            file_lines.append(f"{float(defaults['masecnp0']):.1f}")
            file_lines.append(f"{float(defaults['QNplantenp0']):.1f}")
            file_lines.append(f"{float(defaults['masec0']):.1f}")
            file_lines.append(f"{float(defaults['QNplante0']):.1f}")
            file_lines.append(f"{float(defaults['restemp0']):.1f}")
            file_lines.append("densinitial")
            file_lines.append(f"{float(defaults['densinitial']):.1f} 0.0 0.0 0.0 0.0")
            file_lines.append(":plante:")
            if nbplt == 1:
                file_lines.extend([""] * 4)  
                file_lines.append("code_acti_reserve")
                file_lines.append("2")
                file_lines.append("0")
                file_lines.append("0")
                file_lines.append("0")
                file_lines.append("0") 
                file_lines.extend([""] * 3)           
                file_lines.append(":densinitial:")
                file_lines.append("     ")
            else:
                file_lines.append(str(defaults["stade0_2"]))
                file_lines.append(f"{float(defaults['lai0_2']):.1f}")
                file_lines.append(f"{float(defaults['masec0_2']):.1f}")
                file_lines.append(f"{float(defaults['zrac0_2']):.1f}")
                file_lines.append("code_acti_reserve")
                file_lines.append(str(defaults["code_acti_reserve_2"]))
                file_lines.append(f"{float(defaults['maperenne0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNperenne0_2']):.1f}")
                file_lines.append(f"{float(defaults['masecnp0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNplantenp0_2']):.1f}")
                file_lines.append(f"{float(defaults['masec0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNplante0_2']):.1f}")
                file_lines.append(f"{float(defaults['restemp0_2']):.1f}")
                file_lines.append(":densinitial:")
                file_lines.append(f"{float(defaults['densinitial_2']):.1f} 0.0 0.0 0.0 0.0")
                
            sql = "Select * From soillayers where Lower(idsoil)= '" + row["IdSoil"].lower() + "' Order by NumLayer"
            Adp = pd.read_sql_query(sql, master_input_connection)
            jeu = Adp.to_dict(orient='records')

            file_lines.append(":Hinitf:")
            if row["SoilOption"].lower() == "simple":
                hinit_val = (row["Wwp"] + row["WStockinit"] * (row["Wfc"] - row["Wwp"]) / 100) / row["bd"]
                file_lines.append(f"{hinit_val:.4f} 0.0 0.0 0.0 0.0")
            else:
                hinit_vals = []
                for i in range(5):
                    if i < len(jeu):
                        lyr = jeu[i]
                        hinit = format((lyr["Wwp"] + row["WStockinit"] * (lyr["Wfc"] - lyr["Wwp"]) / 100) / lyr["bd"], ".4f")
                        hinit_vals.append(hinit)
                    else:
                        hinit_vals.append("0.0")
                file_lines.append(" ".join(hinit_vals))

            file_lines.append(":NO3init:")
            if row["SoilOption"].lower() == "simple":
                file_lines.append(f"{row['Ninit']:.1f} 0.0 0.0 0.0 0.0")
            else:
                no3_vals = []
                for i in range(5):
                    if i < len(jeu):
                        no3_vals.append(f"{row['Ninit'] / len(jeu):.1f}")
                    else:
                        no3_vals.append("0.0")
                file_lines.append(" ".join(no3_vals))

            file_lines.append(":NH4initf:")
            has_row_nh4 = "NH4initf" in row and pd.notna(row["NH4initf"])
            default_nh4 = float(defaults["NH4initf"])
            if row["SoilOption"].lower() == "simple":
                nh4_value = float(row["NH4initf"]) if has_row_nh4 else default_nh4
                file_lines.append(f"{nh4_value:.1f} 0.0 0.0 0.0 0.0")
            else:
                nh4_vals = []
                for i in range(5):
                    if i < len(jeu):
                        if has_row_nh4 and len(jeu) > 0:
                            nh4_vals.append(f"{float(row['NH4initf']) / len(jeu):.1f}")
                        else:
                            nh4_vals.append(f"{default_nh4:.1f}")
                    else:
                        nh4_vals.append("0.0")
                file_lines.append(" ".join(nh4_vals))

            file_lines.append(":snow:")  # TO ADD
            file_lines.append("Sdepth0")  # TO ADD
            file_lines.append("0")
            file_lines.append("Sdry0")  # TO ADD
            file_lines.append("0")
            file_lines.append("Swet0")  # TO ADD
            file_lines.append("0")
            file_lines.append("ps0")  # TO ADD
            file_lines.append("0")
        file_lines.append("")

        try:
            self.write_file(usmdir, fileName, "\n".join(file_lines))
        except Exception as e:
            traceback.print_exc()
            print("Error during writing file : " + str(e))
        return "\n".join(file_lines) + "\n"   

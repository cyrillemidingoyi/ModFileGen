from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsParamSolConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        file_name = "param.sol"
        fileContent = ""
        ST = directory_path.split(os.sep)
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table] = 'st_param_sol'));"
        DT = pd.read_sql_query(T,ModelDictionary_Connection) 
        fetchAllQuery = """SELECT Soil.IdSoil,Soil.SoilOption, Soil.OrganicC,Soil.OrganicNStock, Soil.SoilRDepth, Soil.SoilTotalDepth, Soil.SoilTextureType, Soil.Wwp, Soil.Wfc, Soil.bd, Soil.albedo, Soil.Ph, Soil.cf, RunoffTypes.RunoffCoefBSoil, SoilTypes.Clay
        FROM SoilTypes INNER JOIN (RunoffTypes INNER JOIN (Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) ON RunoffTypes.RunoffType = Soil.RunoffType) ON Lower(SoilTypes.SoilTextureType) = Lower(Soil.SoilTextureType)
        where idSim='%s';"""%(ST[-3])
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            fileContent += "     1   "
            fileContent += "Sol "
            fileContent += format(row["Clay"], ".1f") + " "
            fileContent += format(row["OrganicNStock"], ".4f") + " "
            rw = DT[DT["Champ"] == "profhum"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "calc"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            fileContent += format(row["pH"], ".4f") + " "
            rw = DT[DT["Champ"] == "concseuil"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            fileContent += format(row["albedo"], ".4f") + " "
            rw = DT[DT["Champ"] == "q0"]    
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            fileContent += format(row["RunoffCoefBSoil"], ".4f") + " "
            fileContent += format(row["SoilRDepth"], ".4f") + " "
            rw = DT[DT["Champ"] == "pluiebat"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "mulchbat"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "zesx"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "cfes"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "z0solnu"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f").rjust(7) + " "
            fileContent += format(row["OrganicC"]/row["OrganicNStock"], ".4f") + " " ### csurNsol Initial C to N ratio of soil humus
            rw = DT[DT["Champ"] == "penterui"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            fileContent += "\n"      
             
            fileContent += "     1   "
            rw = DT[DT["Champ"] == "codecailloux"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "codemacropor"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "codefente"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "codrainage"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "coderemontcap"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "codenitrif"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "codedenit"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            fileContent += "\n"
            
            fileContent += "     1   "
            rw = DT[DT["Champ"] == "profimper"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "ecartdrain"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "ksol"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "profdrain"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "capiljour"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "humcapil"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            rw = DT[DT["Champ"] == "profdenit"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".0f") + " "
            rw = DT[DT["Champ"] == "vpotdenit"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".4f") + " "
            fileContent += "\n"
            
            sql = f"""Select * From soillayers where idsoil = "{row['IdSoil'].lower()}" Order by NumLayer"""
            DA2 = pd.read_sql_query(sql, master_input_connection)
            rows = DA2.to_dict(orient='records')
            for i in range(5):
                if row["SoilOption"] == "simple":
                    fileContent += "     1   "
                    if i == 0:
                        fileContent += format(row["SoilTotalDepth"], ".2f") + " "
                    else:
                        fileContent += "0.00 "    
                    fileContent += format(row["Wfc"]/row["bd"], ".2f") + " "
                    fileContent += format(row["Wwp"]/row["bd"], ".2f") + " "
                    fileContent += format(row["bd"], ".2f") + " "
                    fileContent += format(row["cf"], ".2f") + " "
                    rw = DT[DT["Champ"] == "typecailloux"]
                    Dv = rw["dv"].values[0]
                    fileContent += format(int(Dv)) + " "
                    rw = DT[DT["Champ"] == "infil"]
                    Dv = rw["dv"].values[0]
                    fileContent += format(int(Dv)) + " "
                    rw = DT[DT["Champ"] == "epd"]
                    Dv = rw["dv"].values[0]
                    fileContent += format(int(Dv)) + " "
                    fileContent += "\n"
                else:
                    if i < len(rows):
                        fileContent += "     1   "
                        fileContent += format(rows[i]["Ldown"] - rows[i]["LUp"], ".2f") + " "
                        fileContent += format(rows[i]["Wfc"]/rows[i]["bd"], ".2f") + " "
                        fileContent += format(rows[i]["Wwp"]/rows[i]["bd"], ".2f") + " "
                        fileContent += format(rows[i]["bd"], ".2f") + " "
                        fileContent += format(row["cf"], ".2f") + " "
                        rw = DT[DT["Champ"] == "typecailloux"]
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        rw = DT[DT["Champ"] == "infil"]
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        rw = DT[DT["Champ"] == "epd"]
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        fileContent += "\n"
                        
                    else:
                        fileContent += "     1   "
                        fileContent += "0.00 "
                        fileContent += "0.00 "
                        fileContent += "0.00 "
                        fileContent += "0.00 "
                        fileContent += "0.00 "
                        
                        rw = DT[DT["Champ"] == "typecailloux"]
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        rw = DT[DT["Champ"] == "infil"] 
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        rw = DT[DT["Champ"] == "epd"]
                        Dv = rw["dv"].values[0]
                        fileContent += format(int(Dv)) + " "
                        fileContent += "\n"
        try:
            self.write_file(usmdir, file_name, fileContent) 
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")
            


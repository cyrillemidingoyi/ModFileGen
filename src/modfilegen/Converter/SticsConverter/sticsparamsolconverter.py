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
        id_sim = ST[-3]
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table] = 'st_param_sol'));"
        DT = pd.read_sql_query(T,ModelDictionary_Connection)
        defaults = DT.set_index("Champ")["dv"].to_dict() 
        
        fetchAllQuery = """SELECT Soil.IdSoil,Soil.SoilOption, Soil.OrganicC,Soil.OrganicNStock as "OrganicNStock", Soil.SoilRDepth, Soil.SoilTotalDepth, Soil.SoilTextureType, Soil.Wwp, Soil.Wfc, Soil.bd, Soil.albedo, Soil.Ph as "pH", Soil.cf, RunoffTypes.RunoffCoefBSoil as "RunoffCoefBSoil", Soil.Clay as "Clay"
        FROM RunoffTypes INNER JOIN (Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) ON RunoffTypes.RunoffType = Soil.RunoffType
        where idSim='%s';"""%(id_sim)
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        
        file_lines = []
        for row in rows:
            line1 = [
                "     1  ","Sol", f"{row['Clay']:.1f}", f"{row['OrganicNStock']:.4f}",
                f"{float(defaults['profhum']):.4f}", f"{float(defaults['calc']):.4f}",
                f"{row['pH']:.4f}", f"{float(defaults['concseuil']):.4f}",
                f"{row['albedo']:.4f}", f"{float(defaults['q0']):.4f}",
                f"{row['RunoffCoefBSoil']:.4f}", f"{row['SoilRDepth']:.4f}",
                f"{float(defaults['pluiebat']):.4f}", f"{float(defaults['mulchbat']):.4f}",
                f"{float(defaults['zesx']):.4f}", f"{float(defaults['cfes']):.4f}",
                f"{float(defaults['z0solnu']):.4f}", f"{row['OrganicC']/row['OrganicNStock']:.4f}",
                f"{float(defaults['penterui']):.4f}"
            ]
            file_lines.append(" ".join(line1))
            '''fileContent += "     1   "
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
            fileContent += "\n"   '''

            codes = ["codecailloux", "codemacropor", "codefente", "codrainage", "coderemontcap", "codenitrif", "codedenit"]
            line2 = ["     1  "] + [f"{int(float(defaults[c])):.0f}" for c in codes]
            file_lines.append(" ".join(line2))             
            '''fileContent += "     1   "
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
            fileContent += "\n"'''
            
            line3 = [
                "     1  ",
                f"{float(defaults['profimper']):.4f}", f"{float(defaults['ecartdrain']):.4f}",
                f"{float(defaults['ksol']):.4f}", f"{float(defaults['profdrain']):.4f}",
                f"{float(defaults['capiljour']):.4f}", f"{float(defaults['humcapil']):.4f}",
                f"{int(float(defaults['profdenit'])):.0f}", f"{float(defaults['vpotdenit']):.4f}"
            ]
            file_lines.append(" ".join(line3))            
            '''fileContent += "     1   "
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
            fileContent += "\n"'''
            
            sql = f"""Select * From soillayers where idsoil = '{row['IdSoil'].lower()}' Order by NumLayer"""
            DA2 = pd.read_sql_query(sql, master_input_connection)
            rows = DA2.to_dict(orient='records')
            for i in range(5):
                if row["SoilOption"] == "simple":
                    #fileContent += "     1   "
                    file_lines.append("     1  ")
                    if i == 0:
                        #fileContent += format(row["SoilTotalDepth"], ".2f") + " "
                        depth = f"{row['SoilTotalDepth']:.2f} "
                    else:
                        #fileContent += "0.00 "  
                        depth = "0.00" 
                    values = [
                        depth,
                        f"{row['Wfc']/row['bd']:.2f}",
                        f"{row['Wwp']/row['bd']:.2f}",
                        f"{row['bd']:.2f}",
                        f"{row['cf']:.2f}",
                        f"{int(defaults['typecailloux'])}",
                        f"{int(defaults['infil'])}",
                        f"{int(defaults['epd'])}"
                    ]
                    file_lines[-1] += " " + " ".join(values)
                    '''fileContent += format(row["Wfc"]/row["bd"], ".2f") + " "
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
                    fileContent += "\n"'''
                else:
                    if i < len(rows):
                        values = ["     1  ",
                            f"{rows[i]['Ldown'] - rows[i]['LUp']:.2f}",
                            f"{rows[i]['Wfc']/rows[i]['bd']:.2f}",
                            f"{rows[i]['Wwp']/rows[i]['bd']:.2f}",
                            f"{rows[i]['bd']:.2f}",
                            f"{row['cf']:.2f}",
                            f"{int(defaults['typecailloux'])}",
                            f"{int(defaults['infil'])}",
                            f"{int(defaults['epd'])}"
                        ]
                                
                        '''fileContent += "     1   "
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
                        fileContent += "\n"'''
                        
                    else:
                        values = ["     1  ","0.00","0.00","0.00","0.00","0.00",f"{row['cf']:.2f}",
                            f"{int(defaults['typecailloux'])}",
                            f"{int(defaults['infil'])}",
                            f"{int(defaults['epd'])}"
                        ]
                        '''fileContent += "     1   "
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
                        fileContent += "\n"'''
                    file_lines[-1] += " " + " ".join(values)
        try:
            #self.write_file(usmdir, file_name, fileContent) 
            self.write_file(usmdir, file_name, "\n".join(file_lines) + "\n")
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")
        return   "\n".join(file_lines) + "\n"      #fileContent
            


from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class DssatSoilConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        fileContent = ""
        ST = directory_path.split(os.sep) 
        idSoil = ST[-4]       
        Mngt = ST[-1][:4]
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] like 'dssat_soil_%'));"
        DT = pd.read_sql_query(T,ModelDictionary_Connection) 

        fetchAllQuery = """SELECT DISTINCT Coordinates.*, RunoffTypes.CurveNumber, Soil.albedo
        From Coordinates INNER Join ((RunoffTypes INNER Join Soil On RunoffTypes.RunoffType = Soil.RunoffType)
        INNER Join SimUnitList On Soil.IdSoil = SimUnitList.idsoil) ON Coordinates.idPoint = SimUnitList.idPoint 
        where SimUnitList.IdSim='%s';"""%(ST[-5])

        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        
        for row in rows:
            rw = DT[DT["Champ"] == "filename"]
            Dv = rw["dv"].values[0]
            fileName = "XX.SOL"
            fileContent += f"*SOILS: DSSAT Soil Input File\n"
            fileContent += f"*XX{Mngt}0101\n"
            fileContent += f"@SITE        COUNTRY          LAT     LONG SCS FAMILY\n"
            fileContent += v_fmt["SITE"].format(f"XX{Mngt}0101")
            rw = DT[DT["Champ"] == "country"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["COUNTRY"].format(Dv)
            fileContent += v_fmt["LAT"].format(row['latitudeDD'])
            fileContent += v_fmt["LONG"].format(row['longitudeDD'])
            rw = DT[DT["Champ"] == "scs family"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SCS FAMILY"].format(Dv)+ "\n"
            fileContent += "@ SCOM  SALB  SLU1  SLDR  SLRO  SLNF  SLPF  SMHB  SMPX  SMKE\n"
            rw = DT[DT["Champ"] == "scom"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SCOM"].format(Dv)
            fileContent += v_fmt["SALB"].format(row["albedo"])
            rw = DT[DT["Champ"] == "slu1"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SLU1"].format(float(Dv))
            rw = DT[DT["Champ"] == "sldr"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SLDR"].format(float(Dv))
            fileContent += v_fmt["SLRO"].format(row["CurveNumber"])
            rw = DT[DT["Champ"] == "slnf"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SLNF"].format(float(Dv))
            rw = DT[DT["Champ"] == "slpf"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SLPF"].format(float(Dv))
            rw = DT[DT["Champ"] == "smhb"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SMHB"].format(Dv)
            rw = DT[DT["Champ"] == "smpx"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SMPX"].format(Dv)
            rw = DT[DT["Champ"] == "smke"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt["SMKE"].format(Dv)+ "\n"
            fileContent += "@  SLB  SLMH  SLLL  SDUL  SSAT  SRGF  SSKS  SBDM  SLOC  SLCL  SLSI  SLCF  SLNI  SLHW  SLHB  SCEC  SADC" +"\n"
            
            fetchAllQuery1 = """Select Soil.Wwp AS 'Soil.Wwp', Soil.Wfc AS 'Soil.Wfc', Soil.bd AS 'Soil.bd', Soil.OrganicC AS 'Soil.OrganicC', Soil.Cf AS 'Soil.Cf', Soil.pH AS 'Soil.pH', Soil.SoilOption AS 'SoilOption', Soil.OrganicNStock AS 'OrganicNStock', Soil.SoilTotalDepth AS 'SoilTotalDepth', 
                    SoilLayers.Wwp AS 'SoilLayers.Wwp', SoilLayers.Wfc AS 'SoilLayers.Wfc', SoilLayers.bd AS 'SoilLayers.bd', SoilLayers.OrganicC AS 'SoilLayers.OrganicC', SoilLayers.Clay AS 'SoilLayers.Clay', SoilLayers.Silt AS 'SoilLayers.Silt', SoilLayers.Cf AS 'SoilLayers.Cf', SoilLayers.pH AS 'SoilLayers.pH', SoilLayers.Ldown AS 'Ldown', SoilLayers.TotalN AS 'TotalN',
                    Soiltypes.Clay AS 'Soiltypes.Clay', SoilTypes.Silt AS 'SoilTypes.Silt' FROM SoilTypes INNER JOIN Soil On lower(SoilTypes.SoilTextureType) = Lower(Soil.SoilTextureType) LEFT JOIN SoilLayers On Lower(Soil.IdSoil) = lower(SoilLayers.idsoil) where Lower(Soil.idSoil) = "%s" ;"""%(idSoil.lower())
            DA1 = pd.read_sql_query(fetchAllQuery1, master_input_connection)
            rows1 = DA1.to_dict(orient='records')
            if rows1[0]["SoilOption"] == "simple":
                for i in range(0, 2):
                    if i == 0:
                        fileContent += v_fmt["SLB"].format(30)
                    else:
                        fileContent += v_fmt["SLB"].format(rows1[0]["SoilTotalDepth"])
                    rw = DT[DT["Champ"] == "slmh"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SLMH"].format(Dv)
                    fileContent += v_fmt["SLLL"].format(rows1[0]["Soil.Wwp"] / 100)
                    fileContent += v_fmt["SDUL"].format(rows1[0]["Soil.Wfc"] / 100)
                    rw = DT[DT["Champ"] == "ssat"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SSAT"].format(float(Dv))
                    rw = DT[DT["Champ"] == "srgf"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SRGF"].format(float(Dv))
                    rw = DT[DT["Champ"] == "ssks"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SSKS"].format(float(Dv))
                    fileContent += v_fmt["SBDM"].format(rows1[0]["Soil.bd"])
                    if i == 0:
                        fileContent += v_fmt["SLOC"].format(rows1[0]["Soil.OrganicC"])
                    else:
                        fileContent += v_fmt["SLOC"].format(0)
                    fileContent += v_fmt["SLCL"].format(rows1[0]["Soiltypes.Clay"])
                    fileContent += v_fmt["SLSI"].format(rows1[0]["SoilTypes.Silt"])
                    fileContent += v_fmt["SLCF"].format(rows1[0]["Soil.Cf"])
                    if i == 0:
                        fileContent += v_fmt["SLNI"].format(rows1[0]["OrganicNStock"])
                    else:
                        fileContent += v_fmt["SLNI"].format(0)
                    fileContent += v_fmt["SLHW"].format(rows1[0]["Soil.pH"])
                    rw = DT[DT["Champ"] == "slhb"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f")
                    else: fileContent += v_fmt["SLHB"].format(float(Dv))
                    rw = DT[DT["Champ"] == "scec"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f")
                    else: fileContent += v_fmt["SCEC"].format(float(Dv))
                    rw = DT[DT["Champ"] == "sadc"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f")+ "\n"
                    else: fileContent += v_fmt["SADC"].format(float(Dv))+ "\n"
            else:
                for row1 in rows1:
                    fileContent += v_fmt["SLB"].format(row1["Ldown"])
                    rw = DT[DT["Champ"] == "slmh"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SLMH"].format(Dv)
                    fileContent += v_fmt["SLLL"].format(row1["SoilLayers.Wwp"] / 100)
                    fileContent += v_fmt["SDUL"].format(row1["SoilLayers.Wfc"] / 100)
                    rw = DT[DT["Champ"] == "ssat"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SSAT"].format(float(Dv))
                    rw = DT[DT["Champ"] == "srgf"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SRGF"].format(float(Dv))
                    rw = DT[DT["Champ"] == "ssks"]
                    Dv = rw["dv"].values[0]
                    fileContent += v_fmt["SSKS"].format(float(Dv))
                    fileContent += v_fmt["SBDM"].format(row1["SoilLayers.bd"])
                    fileContent += v_fmt["SLOC"].format(row1["SoilLayers.OrganicC"])
                    fileContent += v_fmt["SLCL"].format(row1["SoilLayers.Clay"])
                    fileContent += v_fmt["SLSI"].format(row1["SoilLayers.Silt"])
                    fileContent += v_fmt["SLCF"].format(row1["SoilLayers.Cf"])
                    fileContent += v_fmt["SLNI"].format(row1["TotalN"])
                    fileContent += v_fmt["SLHW"].format(row1["SoilLayers.pH"])
                    rw = DT[DT["Champ"] == "slhb"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f")
                    else: fileContent += v_fmt["SLHB"].format(float(Dv))
                    rw = DT[DT["Champ"] == "scec"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f")
                    else: fileContent += v_fmt["SCEC"].format(float(Dv))
                    rw = DT[DT["Champ"] == "sadc"]
                    Dv = rw["dv"].values[0]
                    if int(Dv) == -99:
                        fileContent += format(-99, "6.0f") + '\n'
                    else: fileContent += v_fmt["SADC"].format(float(Dv))+ "\n"
            
            fileContent += "SLB  SLPX  SLPT  SLPO CACO3  SLAL  SLFE  SLMN  SLBS  SLPA  SLPB  SLKE  SLMG  SLNA  SLSU  SLEC  SLCA" +"\n"
            if rows1[0]["SoilOption"] == "simple":
                for i in range(0, 2):
                    if i == 0:
                        fileContent += v_fmt["SLB"].format(30)
                    else:
                        fileContent += v_fmt["SLB"].format(rows1[0]["SoilTotalDepth"])
                    fileContent += v_fmt["SLPX"].format(rows1[0]["Soil.extp"])  
                    fileContent += v_fmt["SLPT"].format(rows1[0]["Soil.totp"]) 
                    fileContent += v_fmt["SLPO"].format(-99)
                    fileContent += v_fmt["CACO3"].format(-99)
                    fileContent += v_fmt["SLAL"].format(-99)
                    fileContent += v_fmt["SLFE"].format(-99)
                    fileContent += v_fmt["SLMN"].format(-99)
                    fileContent += v_fmt["SLBS"].format(-99)
                    fileContent += v_fmt["SLPA"].format(-99)
                    fileContent += v_fmt["SLPB"].format(-99)
                    fileContent += v_fmt["SLKE"].format(-99)
                    fileContent += v_fmt["SLMG"].format(-99)
                    fileContent += v_fmt["SLNA"].format(-99)
                    fileContent += v_fmt["SLSU"].format(-99)
                    fileContent += v_fmt["SLEC"].format(-99)
                    fileContent += v_fmt["SLCA"].format(-99)+ "\n"         
            fileContent += "\n"    


        try:
            self.write_file(usmdir, fileName, fileContent) 
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")
        return fileContent
            


v_fmt = {
        "PEDON": "*{:<10}",
        "SOURCE": "  {:<11}",
        "TEXTURE": " {:<5}",
        "DEPTH": "{:6.0f}",
        "DESCRIPTION": " {:}",
        "SITE": " {:<12}",
        "COUNTRY": "{:<11}",
        "LAT": "{:9.3f}",
        "LONG": "{:9.3f}",
        "SCS FAMILY": " {:}",
        "SCOM": "{:>6}",
        "SALB": "{:6.2f}",
        "SLU1": "{:6.2f}",
        "SLDR": "{:6.2f}",
        "SLRO": "{:6.1f}",
        "SLNF": "{:6.2f}",
        "SLPF": "{:6.2f}",
        "SMHB": "{:>6}",
        "SMPX": "{:>6}",
        "SMKE": "{:>6}",
        # "SGRP": "{:>6}",
        "SLB": "{:6.0f}",
        "SLMH": "{:>6}",
        "SLLL": "{:6.3f}",
        "SDUL": "{:6.3f}",
        "SSAT": "{:6.3f}",
        "SRGF": "{:6.3f}",
        "SSKS": "{:6.3f}",
        "SBDM": "{:6.2f}",
        "SLOC": "{:6.3f}",
        "SLCL": "{:6.1f}",
        "SLSI": "{:6.1f}",
        "SLCF": "{:6.1f}",
        "SLNI": "{:6.2f}",
        "SLHW": "{:6.2f}",
        "SLHB": "{:6.2f}",
        "SCEC": "{:6.2f}",
        "SADC": "{:6f}",
        "SLPX": "{:6.2f}",
        "SLPT": "{:6.0f}",
        "SLPO": "{:6.0f}",
        "CACO3": "{:6.2f}",
        "SLAL": "{:6.2f}",
        "SLFE": "{:6f}",
        "SLMN": "{:6f}",
        "SLBS": "{:6f}",
        "SLPA": "{:6f}",
        "SLPB": "{:6f}",
        "SLKE": "{:6.2f}",
        "SLMG": "{:6.2f}",
        "SLNA": "{:6.2f}",
        "SLSU": "{:6f}",
        "SLEC": "{:6f}",
        "SLCA": "{:6.2f}",
        "ALFVG": "{:6.3f}",
        "MVG": "{:6.3f}",
        "NVG": "{:6.3f}",
        "WCRES": "{:6.3f}",
        "SCS Family": " {:}"
    }


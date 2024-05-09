from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsNewTravailConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        file_name = "new_travail.usm"
        fileContent = ""
        ST = directory_path.split(os.sep)
        fetchAllQuery = """SELECT SimUnitList.idsim, SimUnitList.idPoint as idPoint, SimUnitList.StartYear,SimUnitList.StartDay,SimUnitList.EndDay,SimUnitList.Endyear, SimUnitList.idsoil, SimUnitList.idMangt, SimUnitList.idIni, Coordinates.LatitudeDD, CropManagement.sowingdate,
        ListCultivars.SpeciesName FROM InitialConditions INNER JOIN ((ListCultivars INNER JOIN CropManagement ON ListCultivars.IdCultivar = CropManagement.Idcultivar) INNER JOIN (Coordinates INNER
        Join SimUnitList ON Coordinates.idPoint = SimUnitList.idPoint) ON CropManagement.idMangt = SimUnitList.idMangt) ON InitialConditions.idIni = SimUnitList.idIni Where idsim = '%s';"""%(ST[-3])
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'stics') And ([Table] = 'st_new_travail'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        rows = DA.to_dict(orient='records')

        fileContent += ":codesimul" + "\n"
        rw = DT[DT["Champ"] == "codesimul"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += ":codeoptim" + "\n"
        rw = DT[DT["Champ"] == "codeoptim"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += ":codesuite" + "\n"
        rw = DT[DT["Champ"] == "codesuite"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += ":nbplantes" + "\n"
        rw = DT[DT["Champ"] == "nbplantes"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += ":nom" + "\n"
        fileContent += rows[0]["SpeciesName"] + "\n"
        fileContent += ":datedebut" + "\n"
        fileContent += str(rows[0]["StartDay"]) + "\n"
        fileContent += ":datefin" + "\n"
        if rows[0]["StartYear"] % 4 == 0:
            Bissext = 1
        else:
            Bissext = 0
        if rows[0]["StartYear"] != rows[0]["EndYear"]:
            fileContent += str(rows[0]["EndDay"] + 365 + Bissext) + "\n"
        else:
            fileContent += str(rows[0]["EndDay"]) + "\n"

        fileContent += ":finit" + "\n"
        fileContent += "ficini.txt" + "\n"
        fileContent += ":numsol" + "\n"
        fileContent += "1" + "\n"
        fileContent += ":nomsol" + "\n"
        fileContent += "param.sol" + "\n"
        fileContent += ":fstation" + "\n"
        fileContent += "station.txt" + "\n"
        fileContent += ":fclim1" + "\n"
        fileContent += "cli" + rows[0]["idPoint"] + "j." + str(rows[0]["StartYear"]) + "\n"
        fileContent += ":fclim2" + "\n"
        fileContent += "cli" + rows[0]["idPoint"] + "j." + str(rows[0]["StartYear"] + 1) + "\n"
        fileContent += ":nbans" + "\n"
        
        if rows[0]["StartYear"] != rows[0]["EndYear"]:
            fileContent += "2" + "\n"
        else:
            fileContent += "1" + "\n"
        fileContent += ":culturean" + "\n"
        if rows[0]["StartYear"] != rows[0]["EndYear"]:
            fileContent += "2" + "\n"
        else:
            fileContent += "1" + "\n"
        fileContent += ":fplt1" + "\n"
        fileContent += "ficplt1.txt" + "\n"
        fileContent += ":ftec1" + "\n"
        fileContent += "fictec1.txt" + "\n"
        fileContent += ":flai1" + "\n"
        
        rw = DT[DT["Champ"] == "flai1"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        
        try:
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")


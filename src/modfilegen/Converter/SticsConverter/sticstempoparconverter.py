from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd

class SticsTempoparConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, master_input_connection, tempoparfix, usmdir):
        file_name = "tempopar.sti"
        fileContent = ""
        # split directory_path in ST
        ST = directory_path.split(os.sep)
        output_path = os.path.join(*ST[:-2])
        fetchAllQuery = """SELECT SimUnitList.idsim, SimulationOptions.StressW_YN, SimulationOptions.StressN_YN, SimulationOptions.StressP_YN, SimulationOptions.StressK_YN
         FROM SimUnitList INNER JOIN SimulationOptions ON SimUnitList.IdOption = SimulationOptions.IdOptions Where idsim ='%s';"""%(ST[-3])
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        row = rows[0]
        fileContent += "codeinnact\n"
        if row["StressN_YN"]:
            fileContent += "1\n"
        else:
            fileContent += "2\n"
        fileContent += "codeh2oact\n"
        if row["StressW_YN"]:
            fileContent += "1\n"
        else:
            fileContent += "2\n"
        fileContent += tempoparfix
        fileContent += "\n"
        try:
            # Exporter le fichier vers le répertoire spécifié
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print(f"Error during writing file : {e}")


from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback
import shutil

class SticsFicplt1Converter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, master_input_connection, pltfolder, usmdir):
        file_name = "ficplt1.txt"
        ST = directory_path.split(os.sep)
        sq = """SELECT SimUnitList.idsim as idsim, ListCultOption.FicPlt as fic 
        FROM (ListCultOption INNER JOIN (ListCultivars INNER JOIN CropManagement ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON ListCultOption.CodePSpecies = ListCultivars.CodePSpecies) INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt where idSim= '%s' ;"""%(ST[-3])   
        df_sim = pd.read_sql(sq, master_input_connection)
        rows = df_sim.to_dict('records')
        
        src_path = os.path.join(pltfolder, rows[0]["fic"])
        dest_path = os.path.join(usmdir, file_name)
        shutil.copyfile(src_path, dest_path)  






from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback
import shutil

class DssatCultivarConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, master_input_connection, pltfolder, usmdir, treat_id, culticache2, culticache):
        if treat_id in culticache2:
            crop = culticache2[treat_id]
        else:
            ST = directory_path.split(os.sep)   
            idMangt = ST[-1]
            sq = """SELECT CropManagement.idMangt as idMangt, ListCultOption.PRCROP as crop 
            FROM (ListCultOption INNER JOIN (ListCultivars INNER JOIN CropManagement ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON ListCultOption.CodePSpecies = ListCultivars.CodePSpecies) where idMangt= '%s' ;"""%(ST[-1])   
            df_sim = pd.read_sql(sq, master_input_connection)
                
            if df_sim.empty:
                raise ValueError(f"No crop found for idMangt: {idMangt}")
            crop = df_sim.iloc[0]["crop"]
            culticache2[treat_id] = crop
            
        src_files = {
            "CUL": os.path.join(pltfolder, f"{crop}CER047.CUL"),
            "ECO": os.path.join(pltfolder, f"{crop}CER047.ECO"),
            "SPE": os.path.join(pltfolder, f"{crop}CER047.SPE")
        }
        if not os.path.exists(usmdir):
            os.makedirs(usmdir)
            
        # copy src_path_eco to usmdir if it exists
        for file_type, src_path in src_files.items():
            if file_type == "ECO" and not os.path.exists(src_path):
                continue  # Copie ECO uniquement s'il existe
            fname = os.path.basename(src_path)
            if fname not in culticache:
                with open(src_path, "rb") as f:
                    culticache[fname] = f.read()
            dst_path = os.path.join(usmdir, os.path.basename(src_path))
            with open(dst_path, "wb") as f:
                f.write(culticache[fname])
        return crop
        
        """rows = df_sim.to_dict('records')
            
            src_path_cul = os.path.join(pltfolder, rows[0]["crop"]+"CER047.CUL")
            src_path_eco = os.path.join(pltfolder, rows[0]["crop"]+"CER047.ECO")
            src_path_spe = os.path.join(pltfolder, rows[0]["crop"]+"CER047.SPE")
        
        if not os.path.exists(usmdir):
            os.makedirs(usmdir)
        # copy src_path_eco to usmdir if it exists
        if os.path.exists(src_path_eco):
            shutil.copy(src_path_eco, usmdir)
        shutil.copy(src_path_cul, usmdir)
        shutil.copy(src_path_spe, usmdir)
        return rows[0]["crop"]
        #return src_path_cul, src_path_eco, src_path_spe"""






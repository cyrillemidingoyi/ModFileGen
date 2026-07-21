from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback
import re
import shutil


DSSAT_CULTIVAR_SUFFIXES = {
    "v47": "047",
    "v48": "048",
}


def cultivar_code_for_version(dscrop, dssat_version):
    """Return DSCROP with the suffix matching the requested DSSAT version."""
    try:
        suffix = DSSAT_CULTIVAR_SUFFIXES[dssat_version]
    except KeyError as exc:
        supported_versions = ", ".join(DSSAT_CULTIVAR_SUFFIXES)
        raise ValueError(
            f"Unsupported DSSAT version {dssat_version!r}; expected {supported_versions}"
        ) from exc

    base_dscrop = re.sub(r"(?:047|048)$", "", str(dscrop).strip())
    return base_dscrop + suffix

class DssatCultivarConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, master_input_connection, pltfolder, usmdir, dssat_version="v47"):
        ST = directory_path.split(os.sep)   
        sq = """SELECT CropManagement.idMangt as idMangt, ListCultOption.PRCROP as crop, ListCultOption.DSCROP as dscrop 
        FROM (ListCultOption INNER JOIN (ListCultivars INNER JOIN CropManagement ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON ListCultOption.CodePSpecies = ListCultivars.CodePSpecies) where idMangt= '%s' ;"""%(ST[-1])   
        df_sim = pd.read_sql(sq, master_input_connection)
        rows = df_sim.to_dict('records')

        cultivar_code = rows[0]["crop"] + cultivar_code_for_version(
            rows[0]["dscrop"], dssat_version
        )
        src_path_cul = os.path.join(pltfolder, cultivar_code + ".CUL")
        src_path_eco = os.path.join(pltfolder, cultivar_code + ".ECO")
        src_path_spe = os.path.join(pltfolder, cultivar_code + ".SPE")
        
        if not os.path.exists(usmdir):
            os.makedirs(usmdir)
        # copy src_path_eco to usmdir if it exists
        if os.path.exists(src_path_eco):
            shutil.copy(src_path_eco, usmdir)
        shutil.copy(src_path_cul, usmdir)
        shutil.copy(src_path_spe, usmdir)
        return rows[0]["crop"]
        #return src_path_cul, src_path_eco, src_path_spe






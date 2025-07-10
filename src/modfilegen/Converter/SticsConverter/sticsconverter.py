from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import sticstempoparv6converter, sticsficiniconverter, sticsnewtravailconverter, sticsparamsolconverter
from . import sticstempoparconverter, sticsclimatconverter, sticsfictec1converter
from . import sticsstationconverter, sticsficplt1converter
import subprocess
import re
import os
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
from time import time
import traceback
from joblib import Parallel, delayed, parallel_backend  
import concurrent.futures
import sys
from joblib import Memory
memory = Memory("./cachedir", verbose=0)


def get_coord(d):
    res = re.findall("([-]?\d+[.]?\d+)[_]", d)
    lat = float(res[0])
    lon = float(res[1])
    year = int(float(res[2]))
    return {'lon': lon, 'lat': lat, 'year': year}

def remove_comma(f):
    try:
        with open(f, "r") as fil:
            cod =fil.readlines()
        if cod[-1].endswith(";\n"):
            cod[-1] = cod[-1].replace(";\n", "\n")
        with open(f, "w") as fil:
            fil.writelines(cod)
    except Exception as e:
        print(f"Error removing comma in file {f}: {e}")
        raise
    
def create_df_summary(f):
    #d_name = os.path.dirname(f).split(os.path.sep)[-1]
    d_name = Path(f).stem[len("mod_rapport_"):]
    remove_comma(f)
    c = get_coord(d_name)
    df = pd.read_csv(f, sep=';', skipinitialspace=True)
    df = df.reset_index().rename(columns={"iplts": "Planting","ilevs":"Emergence","iflos":"Ant","imats":"Mat","masec(n)":"Biom_ma","mafruit":"Yield","chargefruit":'GNumber',"laimax":"MaxLai","Qles":"Nleac","QNapp":"SoilN","QNplante":"CroN_ma","ces":"CumE","cep":"Transp"})
    df.insert(0, "Model", "Stics")
    df.insert(1, "Idsim", d_name)
    df.insert(2, "Texte", "")
    df['time'] = df['ansemis'].astype(float).astype(int)
    df['lon'] = c['lon']
    df['lat'] = c['lat']
    return df



def common_rap():
    fileContent = ""
    fileContent += "1\n"
    fileContent += "1\n"
    fileContent += "2\n"
    fileContent += "1\n"
    fileContent += "rec\n"
    fileContent += "masec(n)\n"
    fileContent += "mafruit\n"
    fileContent += "chargefruit\n"
    fileContent += "iplts\n"
    fileContent += "ilevs\n"
    fileContent += "iflos\n"
    fileContent += "imats\n"
    fileContent += "irecs\n"
    fileContent += "laimax\n"
    fileContent += "QNplante\n"
    fileContent += "Qles\n"
    fileContent += "QNapp\n" #'    fileContent += "soilN\n"
    fileContent += "ces\n"
    fileContent += "cep\n"
    return fileContent
    
def common_prof():
    fileContent = ""
    fileContent += "2"
    fileContent += "tsol(iz)\n"
    fileContent += "10\n"
    fileContent += "01 01 2000\n"
    return fileContent

def common_var():
    fileContent = ""
    fileContent += "lai(n)\n"
    fileContent += "masec(n)\n"
    fileContent += "mafruit\n"
    fileContent += "HR(1)\n"
    fileContent += "HR(2)\n"
    fileContent += "HR(3)\n"
    fileContent += "HR(4)\n"
    fileContent += "HR(5)\n"
    fileContent += "resmes\n"
    fileContent += "drain\n"
    fileContent += "esol\n"
    fileContent += "et\n"
    fileContent += "zrac\n"
    fileContent += "tcult\n"
    fileContent += "AZnit(1)\n"
    fileContent += "AZnit(2)\n"
    fileContent += "AZnit(3)\n"
    fileContent += "AZnit(4)\n"
    fileContent += "AZnit(5)\n"
    fileContent += "Qles\n"
    fileContent += "QNplante\n"
    fileContent += "azomes\n"
    fileContent += "inn\n"
    fileContent += "chargefruit\n"
    fileContent += "AZamm(1)\n"
    fileContent += "AZamm(2)\n"
    fileContent += "AZamm(3)\n"
    fileContent += "AZamm(4)\n"
    fileContent += "AZamm(5)\n"
    #'fileContent += "leaching_from_plt\n"
    fileContent += "CNgrain\n"
    fileContent += "concNO3les\n"
    fileContent += "drat\n"
    fileContent += "fapar\n"
    fileContent += "hauteur\n"
    fileContent += "Hmax\n"
    fileContent += "humidite\n"
    fileContent += "LRACH(1)\n"
    fileContent += "LRACH(2)\n"
    fileContent += "LRACH(3)\n"
    fileContent += "LRACH(4)\n"
    fileContent += "LRACH(5)\n"
    fileContent += "mafrais\n"
    fileContent += "pdsfruitfrais\n"
    fileContent += "Qdrain\n"
    fileContent += "rnet\n"
    fileContent += "QNapp\n"
    #'fileContent += "soilN\n"
    fileContent += "ces\n"
    fileContent += "cep\n"
    #'fileContent += "QNplante\n"
    #'fileContent += "Qles\n"
    #'fileContent += "soilN\n"
    return fileContent
        

def common_tempoparv6(modelDictionary):
    fileContent = ""
    ModelDictionary_Connection = sqlite3.connect(modelDictionary)

    # Tempopar query
    T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='stics') AND ([Table]='st_tempoparv6'));"
    DT = pd.read_sql_query(T,ModelDictionary_Connection)

    # Ajouter les résultats à file_content
    fileContent += format_stics_data_v6(DT, "codetempfauche")
    fileContent += format_stics_data_v6(DT, "coefracoupe1", 1, 1)
    fileContent += format_stics_data_v6(DT, "coefracoupe2", 1, 1)
    fileContent += format_stics_data_v6(DT, "codepluiepoquet")
    fileContent += format_stics_data_v6(DT, "nbjoursrrversirrig")
    fileContent += format_stics_data_v6(DT, "swfacmin", 1)
    fileContent += format_stics_data_v6(DT, "codetranspitalle")
    fileContent += format_stics_data_v6(DT, "codedyntalle1", -1, 1)
    fileContent += format_stics_data_v6(DT, "SurfApex1", -1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilMorTalle1", 2, 1)
    fileContent += format_stics_data_v6(DT, "SigmaDisTalle1", 1, 1)
    fileContent += format_stics_data_v6(DT, "VitReconsPeupl1", -1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilReconsPeupl1", 1, 1)
    fileContent += format_stics_data_v6(DT, "MaxTalle1", 1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilLAIapex1", 1, 1)
    fileContent += format_stics_data_v6(DT, "tigefeuilcoupe1", 1, 1)
    fileContent += format_stics_data_v6(DT, "codedyntalle2", 1, 1)
    fileContent += format_stics_data_v6(DT, "SurfApex2", -1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilMorTalle2", 1, 1)
    fileContent += format_stics_data_v6(DT, "SigmaDisTalle2", 1, 1)
    fileContent += format_stics_data_v6(DT, "VitReconsPeupl2", -1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilReconsPeupl2", 1, 1)
    fileContent += format_stics_data_v6(DT, "MaxTalle2", 1, 1)
    fileContent += format_stics_data_v6(DT, "SeuilLAIapex2", 1, 1)
    fileContent += format_stics_data_v6(DT, "tigefeuilcoupe2", 1, 1)
    fileContent += format_stics_data_v6(DT, "resplmax1", 2, 1)
    fileContent += format_stics_data_v6(DT, "resplmax2", 2, 1)
    fileContent += format_stics_data_v6(DT, "codemontaison1", -1, 1)
    fileContent += format_stics_data_v6(DT, "codemontaison2", -1, 1)
    fileContent += format_stics_data_v6(DT, "nbj_pr_apres_semis")
    fileContent += format_stics_data_v6(DT, "eau_mini_decisemis")
    fileContent += format_stics_data_v6(DT, "humirac_decisemis", 2)
    fileContent += format_stics_data_v6(DT, "codecalferti")
    fileContent += format_stics_data_v6(DT, "ratiolN", 5)
    fileContent += format_stics_data_v6(DT, "dosimxN", 5)
    fileContent += format_stics_data_v6(DT, "codetesthumN")
    fileContent += format_stics_data_v6(DT, "codeNmindec")
    fileContent += format_stics_data_v6(DT, "rapNmindec", 5)
    fileContent += format_stics_data_v6(DT, "fNmindecmin", 5)
    fileContent += format_stics_data_v6(DT, "codetrosee")
    fileContent += format_stics_data_v6(DT, "codeSWDRH")
    fileContent += format_stics_data_v6(DT, "P_codedate_irrigauto")
    fileContent += format_stics_data_v6(DT, "datedeb_irrigauto")
    fileContent += format_stics_data_v6(DT, "datefin_irrigauto")
    fileContent += format_stics_data_v6(DT, "stage_start_irrigauto")
    fileContent += format_stics_data_v6(DT, "stage_end_irrigauto")
    fileContent += format_stics_data_v6(DT, "codemortalracine")
    fileContent += format_stics_data_v6(DT, "option_thinning")
    fileContent += format_stics_data_v6(DT, "option_engrais_multiple")
    fileContent += format_stics_data_v6(DT, "option_pature")
    fileContent += format_stics_data_v6(DT, "coderes_pature")
    fileContent += format_stics_data_v6(DT, "pertes_restit_ext")
    fileContent += format_stics_data_v6(DT, "Crespc_pature")
    fileContent += format_stics_data_v6(DT, "Nminres_pature")
    fileContent += format_stics_data_v6(DT, "eaures_pature")
    fileContent += format_stics_data_v6(DT, "coef_calcul_qres")
    fileContent += format_stics_data_v6(DT, "engrais_pature")
    fileContent += format_stics_data_v6(DT, "coef_calcul_doseN")
    fileContent += format_stics_data_v6(DT, "codemineralOM")
    fileContent += format_stics_data_v6(DT, "GMIN1")
    fileContent += format_stics_data_v6(DT, "GMIN2")
    fileContent += format_stics_data_v6(DT, "GMIN3")
    fileContent += format_stics_data_v6(DT, "GMIN4")
    fileContent += format_stics_data_v6(DT, "GMIN5")
    fileContent += format_stics_data_v6(DT, "GMIN6")
    fileContent += format_stics_data_v6(DT, "GMIN7")
    fileContent += "\n"
    ModelDictionary_Connection.close()
    return fileContent

def format_stics_data_v6(row, champ, precision=5, field_it=0):
    res = ""
    type_data = ""
    data = None
    field_name = champ
    file_content = ""

    # For repeated fields, build field name
    if field_it != 0:
        field_name = field_name[:-1] + "(" + field_name[-1] + ")"
        # champ = champ + str(field_it)

    # Fetch data
    rw = row[row['Champ'] == champ]
    if len(rw) == 0:
        pass
    else:
        data = rw["dv"].values[0]
        res = ""

        # If type is string or int
        if isinstance(data, str) or isinstance(data, int):
            res = str(data)

        # If type is real
        if isinstance(data, float):
            tmp = float(data)
            if 0 < precision < 7:
                res = "{:.{}f}".format(tmp, precision)
            else:
                res = "{:0.3e}".format(tmp)
        # If cell is null
        if data is None:
            res = ""
        # Print data in file
        file_content += field_name + "\n"
        file_content += res + "\n"
    return file_content
    
def common_tempopar(ModelDictionary):
    ModelDictionary_Connection = sqlite3.connect(ModelDictionary)
    T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='stics') AND ([Table]='st_tempopar')) OR ((model='stics') AND ([Table]='st_tempopar_2')) OR ((model='stics') AND ([Table]='st_tempopar_3'));"
    DT = pd.read_sql_query(T,ModelDictionary_Connection)

    # Ajouter les résultats à file_content
    fileContent = ""
    fileContent += format_stics_data(DT, "codeminopt")
    fileContent += format_stics_data(DT, "iniprofil")
    fileContent += format_stics_data(DT, "codeprofmes")
    fileContent += format_stics_data(DT, "codeinitprec")
    fileContent += format_stics_data(DT, "codemsfinal")
    fileContent += format_stics_data(DT, "codeactimulch")
    fileContent += format_stics_data(DT, "codefrmur")
    fileContent += format_stics_data(DT, "codemicheur")
    fileContent += format_stics_data(DT, "codeoutscient")
    fileContent += format_stics_data(DT, "codeseprapport")
    fileContent += format_stics_data(DT, "separateurrapport")
    fileContent += format_stics_data(DT, "codesensibilite")
    fileContent += format_stics_data(DT, "codesnow")
    fileContent += format_stics_data(DT, "flagecriture")
    fileContent += format_stics_data(DT, "parsurrg")
    fileContent += format_stics_data(DT, "coefb")
    fileContent += format_stics_data(DT, "proprac")
    fileContent += format_stics_data(DT, "y0msrac")
    fileContent += format_stics_data(DT, "khaut")
    fileContent += format_stics_data(DT, "dacohes")
    fileContent += format_stics_data(DT, "daseuilbas")
    fileContent += format_stics_data(DT, "daseuilhaut")
    fileContent += format_stics_data(DT, "beta")
    fileContent += format_stics_data(DT, "lvopt")
    fileContent += format_stics_data(DT, "rayon")
    fileContent += format_stics_data(DT, "difN")
    fileContent += format_stics_data(DT, "concrr")
    fileContent += format_stics_data(DT, "plNmin")
    fileContent += format_stics_data(DT, "irrlev")
    fileContent += format_stics_data(DT, "QNpltminINN")
    fileContent += format_stics_data(DT, "codesymbiose")
    fileContent += format_stics_data(DT, "codefxn")
    fileContent += format_stics_data(DT, "FTEMh")
    fileContent += format_stics_data(DT, "FTEMha")
    fileContent += format_stics_data(DT, "TREFh")
    fileContent += format_stics_data(DT, "FTEMr")
    fileContent += format_stics_data(DT, "FTEMra")
    fileContent += format_stics_data(DT, "TREFr")
    fileContent += format_stics_data(DT, "FINERT")
    fileContent += format_stics_data(DT, "FMIN1")
    fileContent += format_stics_data(DT, "FMIN2")
    fileContent += format_stics_data(DT, "FMIN3")
    fileContent += format_stics_data(DT, "Wh")
    fileContent += format_stics_data(DT, "pHminvol")
    fileContent += format_stics_data(DT, "pHmaxvol")
    fileContent += format_stics_data(DT, "Vabs2")
    fileContent += format_stics_data(DT, "Xorgmax")
    fileContent += format_stics_data(DT, "hminm")
    fileContent += format_stics_data(DT, "hoptm")
    fileContent += format_stics_data(DT, "alphaph")
    fileContent += format_stics_data(DT, "dphvolmax")
    fileContent += format_stics_data(DT, "phvols")
    fileContent += format_stics_data(DT, "fhminsat")
    fileContent += format_stics_data(DT, "fredkN")
    fileContent += format_stics_data(DT, "fredlN")
    fileContent += format_stics_data(DT, "fNCbiomin")
    fileContent += format_stics_data(DT, "fredNsup")
    fileContent += format_stics_data(DT, "Primingmax")
    fileContent += format_stics_data(DT, "hminn")
    fileContent += format_stics_data(DT, "hoptn")
    fileContent += format_stics_data(DT, "pHminnit")
    fileContent += format_stics_data(DT, "pHmaxnit")
    fileContent += format_stics_data(DT, "nh4_min")
    fileContent += format_stics_data(DT, "pHminden")
    fileContent += format_stics_data(DT, "pHmaxden")
    fileContent += format_stics_data(DT, "wfpsc")
    fileContent += format_stics_data(DT, "tdenitopt_gauss")
    fileContent += format_stics_data(DT, "scale_tdenitopt")
    fileContent += format_stics_data(DT, "Kd")
    fileContent += format_stics_data(DT, "k_desat")
    fileContent += format_stics_data(DT, "code_vnit")
    fileContent += format_stics_data(DT, "fnx")
    fileContent += format_stics_data(DT, "vnitmax")
    fileContent += format_stics_data(DT, "Kamm")
    fileContent += format_stics_data(DT, "code_tnit")
    fileContent += format_stics_data(DT, "tnitmin")
    fileContent += format_stics_data(DT, "tnitopt")
    fileContent += format_stics_data(DT, "tnitopt2")
    fileContent += format_stics_data(DT, "tnitmax")
    fileContent += format_stics_data(DT, "tnitopt_gauss")
    fileContent += format_stics_data(DT, "scale_tnitopt")
    fileContent += format_stics_data(DT, "code_rationit")
    fileContent += format_stics_data(DT, "rationit", 6)
    fileContent += format_stics_data(DT, "code_hourly_wfps_nit")
    fileContent += format_stics_data(DT, "code_pdenit")
    fileContent += format_stics_data(DT, "cmin_pdenit")
    fileContent += format_stics_data(DT, "cmax_pdenit")
    fileContent += format_stics_data(DT, "min_pdenit")
    fileContent += format_stics_data(DT, "max_pdenit")
    fileContent += format_stics_data(DT, "code_ratiodenit")
    fileContent += format_stics_data(DT, "ratiodenit")
    fileContent += format_stics_data(DT, "code_hourly_wfps_denit")
    fileContent += format_stics_data(DT, "pminruis")
    fileContent += format_stics_data(DT, "diftherm")
    fileContent += format_stics_data(DT, "bformnappe")
    fileContent += format_stics_data(DT, "rdrain")
    fileContent += format_stics_data(DT, "psihumin")
    fileContent += format_stics_data(DT, "psihucc")
    fileContent += format_stics_data(DT, "prophumtasssem")
    fileContent += format_stics_data(DT, "prophumtassrec")
    fileContent += format_stics_data(DT, "codhnappe")
    fileContent += format_stics_data(DT, "distdrain", 2, 0)
    fileContent += format_stics_data(DT, "proflabour")
    fileContent += format_stics_data(DT, "proftravmin")
    fileContent += format_stics_data(DT, "codetycailloux")
    fileContent += format_stics_data(DT, "masvolcx", 5, 1)
    fileContent += format_stics_data(DT, "hcccx", 5, 1)
    fileContent += format_stics_data(DT, "masvolcx", 5, 2)
    fileContent += format_stics_data(DT, "hcccx", 5, 2)
    fileContent += format_stics_data(DT, "masvolcx", 5, 3)
    fileContent += format_stics_data(DT, "hcccx", 5, 3)
    fileContent += format_stics_data(DT, "masvolcx", 5, 4)
    fileContent += format_stics_data(DT, "hcccx", 5, 4)
    fileContent += format_stics_data(DT, "masvolcx", 5, 5)
    fileContent += format_stics_data(DT, "hcccx", 5, 5)
    fileContent += format_stics_data(DT, "masvolcx", 5, 6)
    fileContent += format_stics_data(DT, "hcccx", 5, 6)
    fileContent += format_stics_data(DT, "masvolcx", 5, 7)
    fileContent += format_stics_data(DT, "hcccx", 5, 7)
    fileContent += format_stics_data(DT, "masvolcx", 5, 8)
    fileContent += format_stics_data(DT, "hcccx", 5, 8)
    fileContent += format_stics_data(DT, "masvolcx", 5, 9)
    fileContent += format_stics_data(DT, "hcccx", 5, 9)
    fileContent += format_stics_data(DT, "masvolcx", 5, 10)
    fileContent += format_stics_data(DT, "hcccx", 5, 10)
    fileContent += format_stics_data(DT, "codetypeng")
    fileContent += format_stics_data(DT, "engamm", 5, 1)
    fileContent += format_stics_data(DT, "orgeng", 5, 1)
    fileContent += format_stics_data(DT, "deneng", 5, 1)
    fileContent += format_stics_data(DT, "voleng", 5, 1)
    fileContent += format_stics_data(DT, "engamm", 5, 2)
    fileContent += format_stics_data(DT, "orgeng", 5, 2)
    fileContent += format_stics_data(DT, "deneng", 5, 2)
    fileContent += format_stics_data(DT, "voleng", 5, 2)
    fileContent += format_stics_data(DT, "engamm", 5, 3)
    fileContent += format_stics_data(DT, "orgeng", 5, 3)
    fileContent += format_stics_data(DT, "deneng", 5, 3)
    fileContent += format_stics_data(DT, "voleng", 5, 3)
    fileContent += format_stics_data(DT, "engamm", 5, 4)
    fileContent += format_stics_data(DT, "orgeng", 5, 4)
    fileContent += format_stics_data(DT, "deneng", 5, 4)
    fileContent += format_stics_data(DT, "voleng", 5, 4)
    fileContent += format_stics_data(DT, "engamm", 5, 5)
    fileContent += format_stics_data(DT, "orgeng", 5, 5)
    fileContent += format_stics_data(DT, "deneng", 5, 5)
    fileContent += format_stics_data(DT, "voleng", 5, 5)
    fileContent += format_stics_data(DT, "engamm", 5, 6)
    fileContent += format_stics_data(DT, "orgeng", 5, 6)
    fileContent += format_stics_data(DT, "deneng", 5, 6)
    fileContent += format_stics_data(DT, "voleng", 5, 6)
    fileContent += format_stics_data(DT, "engamm", 5, 7)
    fileContent += format_stics_data(DT, "orgeng", 5, 7)
    fileContent += format_stics_data(DT, "deneng", 5, 7)
    fileContent += format_stics_data(DT, "voleng", 5, 7)
    fileContent += format_stics_data(DT, "engamm", 5, 8)
    fileContent += format_stics_data(DT, "orgeng", 5, 8)
    fileContent += format_stics_data(DT, "deneng", 5, 8)
    fileContent += format_stics_data(DT, "voleng", 5, 8)
    fileContent += format_stics_data(DT, "codetypres")

    fileContent += format_stics_data(DT, "CroCo", 5, 1)
    fileContent += format_stics_data(DT, "akres", 5, 1)
    fileContent += format_stics_data(DT, "bkres", 5, 1)
    fileContent += format_stics_data(DT, "awb", 5, 1)
    fileContent += format_stics_data(DT, "bwb", 5, 1)
    fileContent += format_stics_data(DT, "cwb", 5, 1)
    fileContent += format_stics_data(DT, "ahres", 5, 1)
    fileContent += format_stics_data(DT, "bhres", 5, 1)
    fileContent += format_stics_data(DT, "kbio", 5, 1)
    fileContent += format_stics_data(DT, "yres", 5, 1)
    fileContent += format_stics_data(DT, "CNresmin", 5, 1)
    fileContent += format_stics_data(DT, "CNresmax", 5, 1)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 1)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 1)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 1)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 1)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 1)

    fileContent += format_stics_data(DT, "CroCo", 5, 2)
    fileContent += format_stics_data(DT, "akres", 5, 2)
    fileContent += format_stics_data(DT, "bkres", 5, 2)
    fileContent += format_stics_data(DT, "awb", 5, 2)
    fileContent += format_stics_data(DT, "bwb", 5, 2)
    fileContent += format_stics_data(DT, "cwb", 5, 2)
    fileContent += format_stics_data(DT, "ahres", 5, 2)
    fileContent += format_stics_data(DT, "bhres", 5, 2)
    fileContent += format_stics_data(DT, "kbio", 5, 2)
    fileContent += format_stics_data(DT, "yres", 5, 2)
    fileContent += format_stics_data(DT, "CNresmin", 5, 2)
    fileContent += format_stics_data(DT, "CNresmax", 5, 2)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 2)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 2)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 2)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 2)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 2)

    fileContent += format_stics_data(DT, "CroCo", 5, 3)
    fileContent += format_stics_data(DT, "akres", 5, 3)
    fileContent += format_stics_data(DT, "bkres", 5, 3)
    fileContent += format_stics_data(DT, "awb", 5, 3)
    fileContent += format_stics_data(DT, "bwb", 5, 3)
    fileContent += format_stics_data(DT, "cwb", 5, 3)
    fileContent += format_stics_data(DT, "ahres", 5, 3)
    fileContent += format_stics_data(DT, "bhres", 5, 3)
    fileContent += format_stics_data(DT, "kbio", 5, 3)
    fileContent += format_stics_data(DT, "yres", 5, 3)
    fileContent += format_stics_data(DT, "CNresmin", 5, 3)
    fileContent += format_stics_data(DT, "CNresmax", 5, 3)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 3)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 3)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 3)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 3)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 3)

    fileContent += format_stics_data(DT, "CroCo", 5, 4)
    fileContent += format_stics_data(DT, "akres", 5, 4)
    fileContent += format_stics_data(DT, "bkres", 5, 4)
    fileContent += format_stics_data(DT, "awb", 5, 4)
    fileContent += format_stics_data(DT, "bwb", 5, 4)
    fileContent += format_stics_data(DT, "cwb", 5, 4)
    fileContent += format_stics_data(DT, "ahres", 5, 4)
    fileContent += format_stics_data(DT, "bhres", 5, 4)
    fileContent += format_stics_data(DT, "kbio", 5, 4)
    fileContent += format_stics_data(DT, "yres", 5, 4)
    fileContent += format_stics_data(DT, "CNresmin", 5, 4)
    fileContent += format_stics_data(DT, "CNresmax", 5, 4)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 4)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 4)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 4)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 4)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 4)

    fileContent += format_stics_data(DT, "CroCo", 5, 5)
    fileContent += format_stics_data(DT, "akres", 5, 5)
    fileContent += format_stics_data(DT, "bkres", 5, 5)
    fileContent += format_stics_data(DT, "awb", 5, 5)
    fileContent += format_stics_data(DT, "bwb", 5, 5)
    fileContent += format_stics_data(DT, "cwb", 5, 5)
    fileContent += format_stics_data(DT, "ahres", 5, 5)
    fileContent += format_stics_data(DT, "bhres", 5, 5)
    fileContent += format_stics_data(DT, "kbio", 5, 5)
    fileContent += format_stics_data(DT, "yres", 5, 5)
    fileContent += format_stics_data(DT, "CNresmin", 5, 5)
    fileContent += format_stics_data(DT, "CNresmax", 5, 5)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 5)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 5)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 5)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 5)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 5)

    fileContent += format_stics_data(DT, "CroCo", 5, 6)
    fileContent += format_stics_data(DT, "akres", 5, 6)
    fileContent += format_stics_data(DT, "bkres", 5, 6)
    fileContent += format_stics_data(DT, "awb", 5, 6)
    fileContent += format_stics_data(DT, "bwb", 5, 6)
    fileContent += format_stics_data(DT, "cwb", 5, 6)
    fileContent += format_stics_data(DT, "ahres", 5, 6)
    fileContent += format_stics_data(DT, "bhres", 5, 6)
    fileContent += format_stics_data(DT, "kbio", 5, 6)
    fileContent += format_stics_data(DT, "yres", 5, 6)
    fileContent += format_stics_data(DT, "CNresmin", 5, 6)
    fileContent += format_stics_data(DT, "CNresmax", 5, 6)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 6)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 6)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 6)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 6)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 6)

    fileContent += format_stics_data(DT, "CroCo", 5, 7)
    fileContent += format_stics_data(DT, "akres", 5, 7)
    fileContent += format_stics_data(DT, "bkres", 5, 7)
    fileContent += format_stics_data(DT, "awb", 5, 7)
    fileContent += format_stics_data(DT, "bwb", 5, 7)
    fileContent += format_stics_data(DT, "cwb", 5, 7)
    fileContent += format_stics_data(DT, "ahres", 5, 7)
    fileContent += format_stics_data(DT, "bhres", 5, 7)
    fileContent += format_stics_data(DT, "kbio", 5, 7)

    fileContent += format_stics_data(DT, "yres", 5, 7)
    fileContent += format_stics_data(DT, "CNresmin", 5, 7)
    fileContent += format_stics_data(DT, "CNresmax", 5, 7)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 7)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 7)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 7)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 7)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 7)

    fileContent += format_stics_data(DT, "CroCo", 5, 8)
    fileContent += format_stics_data(DT, "akres", 5, 8)
    fileContent += format_stics_data(DT, "bkres", 5, 8)
    fileContent += format_stics_data(DT, "awb", 5, 8)
    fileContent += format_stics_data(DT, "bwb", 5, 8)
    fileContent += format_stics_data(DT, "cwb", 5, 8)
    fileContent += format_stics_data(DT, "ahres", 5, 8)
    fileContent += format_stics_data(DT, "bhres", 5, 8)
    fileContent += format_stics_data(DT, "kbio", 5, 8)
    fileContent += format_stics_data(DT, "yres", 5, 8)
    fileContent += format_stics_data(DT, "CNresmin", 5, 8)
    fileContent += format_stics_data(DT, "CNresmax", 5, 8)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 8)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 8)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 8)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 8)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 8)

    fileContent += format_stics_data(DT, "CroCo", 5, 9)
    fileContent += format_stics_data(DT, "akres", 5, 9)
    fileContent += format_stics_data(DT, "bkres", 5, 9)
    fileContent += format_stics_data(DT, "awb", 5, 9)
    fileContent += format_stics_data(DT, "bwb", 5, 9)
    fileContent += format_stics_data(DT, "cwb", 5, 9)
    fileContent += format_stics_data(DT, "ahres", 5, 9)
    fileContent += format_stics_data(DT, "bhres", 5, 9)
    fileContent += format_stics_data(DT, "kbio", 5, 9)
    fileContent += format_stics_data(DT, "yres", 5, 9)
    fileContent += format_stics_data(DT, "CNresmin", 5, 9)
    fileContent += format_stics_data(DT, "CNresmax", 5, 9)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 9)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 9)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 9)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 9)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 9)

    fileContent += format_stics_data(DT, "CroCo", 5, 10)
    fileContent += format_stics_data(DT, "akres", 5, 10)
    fileContent += format_stics_data(DT, "bkres", 5, 10)
    fileContent += format_stics_data(DT, "awb", 5, 10)
    fileContent += format_stics_data(DT, "bwb", 5, 10)
    fileContent += format_stics_data(DT, "cwb", 5, 10)
    fileContent += format_stics_data(DT, "ahres", 5, 10)
    fileContent += format_stics_data(DT, "bhres", 5, 10)
    fileContent += format_stics_data(DT, "kbio", 5, 10)
    fileContent += format_stics_data(DT, "yres", 5, 10)
    fileContent += format_stics_data(DT, "CNresmin", 5, 10)
    fileContent += format_stics_data(DT, "CNresmax", 5, 10)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 10)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 10)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 10)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 10)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 10)

    fileContent += format_stics_data(DT, "CroCo", 5, 11)
    fileContent += format_stics_data(DT, "akres", 5, 11)
    fileContent += format_stics_data(DT, "bkres", 5, 11)
    fileContent += format_stics_data(DT, "awb", 5, 11)
    fileContent += format_stics_data(DT, "bwb", 5, 11)
    fileContent += format_stics_data(DT, "cwb", 5, 11)
    fileContent += format_stics_data(DT, "ahres", 5, 11)
    fileContent += format_stics_data(DT, "bhres", 5, 11)
    fileContent += format_stics_data(DT, "kbio", 5, 11)
    fileContent += format_stics_data(DT, "yres", 5, 11)
    fileContent += format_stics_data(DT, "CNresmin", 5, 11)
    fileContent += format_stics_data(DT, "CNresmax", 5, 11)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 11)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 11)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 11)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 11)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 11)

    fileContent += format_stics_data(DT, "CroCo", 5, 12)
    fileContent += format_stics_data(DT, "akres", 5, 12)
    fileContent += format_stics_data(DT, "bkres", 5, 12)
    fileContent += format_stics_data(DT, "awb", 5, 12)
    fileContent += format_stics_data(DT, "bwb", 5, 12)
    fileContent += format_stics_data(DT, "cwb", 5, 12)
    fileContent += format_stics_data(DT, "ahres", 5, 12)
    fileContent += format_stics_data(DT, "bhres", 5, 12)
    fileContent += format_stics_data(DT, "kbio", 5, 12)
    fileContent += format_stics_data(DT, "yres", 5, 12)
    fileContent += format_stics_data(DT, "CNresmin", 5, 12)
    fileContent += format_stics_data(DT, "CNresmax", 5, 12)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 12)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 12)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 12)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 12)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 12)

    fileContent += format_stics_data(DT, "CroCo", 5, 13)
    fileContent += format_stics_data(DT, "akres", 5, 13)
    fileContent += format_stics_data(DT, "bkres", 5, 13)
    fileContent += format_stics_data(DT, "awb", 5, 13)
    fileContent += format_stics_data(DT, "bwb", 5, 13)
    fileContent += format_stics_data(DT, "cwb", 5, 13)
    fileContent += format_stics_data(DT, "ahres", 5, 13)
    fileContent += format_stics_data(DT, "bhres", 5, 13)
    fileContent += format_stics_data(DT, "kbio", 5, 13)
    fileContent += format_stics_data(DT, "yres", 5, 13)
    fileContent += format_stics_data(DT, "CNresmin", 5, 13)
    fileContent += format_stics_data(DT, "CNresmax", 5, 13)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 13)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 13)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 13)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 13)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 13)

    fileContent += format_stics_data(DT, "CroCo", 5, 14)
    fileContent += format_stics_data(DT, "akres", 5, 14)
    fileContent += format_stics_data(DT, "bkres", 5, 14)
    fileContent += format_stics_data(DT, "awb", 5, 14)
    fileContent += format_stics_data(DT, "bwb", 5, 14)
    fileContent += format_stics_data(DT, "cwb", 5, 14)
    fileContent += format_stics_data(DT, "ahres", 5, 14)
    fileContent += format_stics_data(DT, "bhres", 5, 14)
    fileContent += format_stics_data(DT, "kbio", 5, 14)
    fileContent += format_stics_data(DT, "yres", 5, 14)
    fileContent += format_stics_data(DT, "CNresmin", 5, 14)
    fileContent += format_stics_data(DT, "CNresmax", 5, 14)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 14)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 14)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 14)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 14)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 14)

    fileContent += format_stics_data(DT, "CroCo", 5, 15)
    fileContent += format_stics_data(DT, "akres", 5, 15)
    fileContent += format_stics_data(DT, "bkres", 5, 15)
    fileContent += format_stics_data(DT, "awb", 5, 15)
    fileContent += format_stics_data(DT, "bwb", 5, 15)
    fileContent += format_stics_data(DT, "cwb", 5, 15)
    fileContent += format_stics_data(DT, "ahres", 5, 15)
    fileContent += format_stics_data(DT, "bhres", 5, 15)
    fileContent += format_stics_data(DT, "kbio", 5, 15)
    fileContent += format_stics_data(DT, "yres", 5, 15)
    fileContent += format_stics_data(DT, "CNresmin", 5, 15)
    fileContent += format_stics_data(DT, "CNresmax", 5, 15)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 15)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 15)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 15)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 15)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 15)

    fileContent += format_stics_data(DT, "CroCo", 5, 16)
    fileContent += format_stics_data(DT, "akres", 5, 16)
    fileContent += format_stics_data(DT, "bkres", 5, 16)
    fileContent += format_stics_data(DT, "awb", 5, 16)
    fileContent += format_stics_data(DT, "bwb", 5, 16)
    fileContent += format_stics_data(DT, "cwb", 5, 16)
    fileContent += format_stics_data(DT, "ahres", 5, 16)
    fileContent += format_stics_data(DT, "bhres", 5, 16)
    fileContent += format_stics_data(DT, "kbio", 5, 16)
    fileContent += format_stics_data(DT, "yres", 5, 16)
    fileContent += format_stics_data(DT, "CNresmin", 5, 16)
    fileContent += format_stics_data(DT, "CNresmax", 5, 16)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 16)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 16)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 16)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 16)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 16)

    fileContent += format_stics_data(DT, "CroCo", 5, 17)
    fileContent += format_stics_data(DT, "akres", 5, 17)
    fileContent += format_stics_data(DT, "bkres", 5, 17)
    fileContent += format_stics_data(DT, "awb", 5, 17)
    fileContent += format_stics_data(DT, "bwb", 5, 17)
    fileContent += format_stics_data(DT, "cwb", 5, 17)
    fileContent += format_stics_data(DT, "ahres", 5, 17)
    fileContent += format_stics_data(DT, "bhres", 5, 17)
    fileContent += format_stics_data(DT, "kbio", 5, 17)
    fileContent += format_stics_data(DT, "yres", 5, 17)
    fileContent += format_stics_data(DT, "CNresmin", 5, 17)
    fileContent += format_stics_data(DT, "CNresmax", 5, 17)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 17)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 17)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 17)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 17)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 17)

    fileContent += format_stics_data(DT, "CroCo", 5, 18)
    fileContent += format_stics_data(DT, "akres", 5, 18)
    fileContent += format_stics_data(DT, "bkres", 5, 18)
    fileContent += format_stics_data(DT, "awb", 5, 18)
    fileContent += format_stics_data(DT, "bwb", 5, 18)
    fileContent += format_stics_data(DT, "cwb", 5, 18)
    fileContent += format_stics_data(DT, "ahres", 5, 18)
    fileContent += format_stics_data(DT, "bhres", 5, 18)
    fileContent += format_stics_data(DT, "kbio", 5, 18)
    fileContent += format_stics_data(DT, "yres", 5, 18)
    fileContent += format_stics_data(DT, "CNresmin", 5, 18)
    fileContent += format_stics_data(DT, "CNresmax", 5, 18)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 18)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 18)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 18)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 18)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 18)

    fileContent += format_stics_data(DT, "CroCo", 5, 19)
    fileContent += format_stics_data(DT, "akres", 5, 19)
    fileContent += format_stics_data(DT, "bkres", 5, 19)
    fileContent += format_stics_data(DT, "awb", 5, 19)
    fileContent += format_stics_data(DT, "bwb", 5, 19)
    fileContent += format_stics_data(DT, "cwb", 5, 19)
    fileContent += format_stics_data(DT, "ahres", 5, 19)
    fileContent += format_stics_data(DT, "bhres", 5, 19)
    fileContent += format_stics_data(DT, "kbio", 5, 19)
    fileContent += format_stics_data(DT, "yres", 5, 19)
    fileContent += format_stics_data(DT, "CNresmin", 5, 19)
    fileContent += format_stics_data(DT, "CNresmax", 5, 19)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 19)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 19)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 19)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 19)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 19)

    fileContent += format_stics_data(DT, "CroCo", 5, 20)
    fileContent += format_stics_data(DT, "akres", 5, 20)
    fileContent += format_stics_data(DT, "bkres", 5, 20)
    fileContent += format_stics_data(DT, "awb", 5, 20)
    fileContent += format_stics_data(DT, "bwb", 5, 20)
    fileContent += format_stics_data(DT, "cwb", 5, 20)
    fileContent += format_stics_data(DT, "ahres", 5, 20)
    fileContent += format_stics_data(DT, "bhres", 5, 20)
    fileContent += format_stics_data(DT, "kbio", 5, 20)
    fileContent += format_stics_data(DT, "yres", 5, 20)
    fileContent += format_stics_data(DT, "CNresmin", 5, 20)
    fileContent += format_stics_data(DT, "CNresmax", 5, 20)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 20)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 20)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 20)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 20)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 20)

    fileContent += format_stics_data(DT, "CroCo", 5, 21)
    fileContent += format_stics_data(DT, "akres", 5, 21)
    fileContent += format_stics_data(DT, "bkres", 5, 21)
    fileContent += format_stics_data(DT, "awb", 5, 21)
    fileContent += format_stics_data(DT, "bwb", 5, 21)
    fileContent += format_stics_data(DT, "cwb", 5, 21)
    fileContent += format_stics_data(DT, "ahres", 5, 21)
    fileContent += format_stics_data(DT, "bhres", 5, 21)
    fileContent += format_stics_data(DT, "kbio", 5, 21)
    fileContent += format_stics_data(DT, "yres", 5, 21)
    fileContent += format_stics_data(DT, "CNresmin", 5, 21)
    fileContent += format_stics_data(DT, "CNresmax", 5, 21)
    fileContent += format_stics_data(DT, "qmulchruis0", 5, 21)
    fileContent += format_stics_data(DT, "mouillabilmulch", 5, 21)
    fileContent += format_stics_data(DT, "kcouvmlch", 5, 21)
    fileContent += format_stics_data(DT, "albedomulchresidus", 5, 21)
    fileContent += format_stics_data(DT, "Qmulchdec", 5, 21)

    fileContent += "\n"
    ModelDictionary_Connection.close()
    return fileContent


def format_stics_data(row, champ, precision=5, field_it=0):
    res = ""
    type_data = ""
    data = None
    file_content = ""
    field_name = champ

    # For repeated fields, build field name
    if field_it != 0:
        champ = champ + str(field_it) 

    # Fetch data
    rw = row[row['Champ'] == champ]
    data = rw["dv"].values[0]
    res = ""

    # If type is string or int
    if isinstance(data, str) or isinstance(data, int):
        res = str(data)

    # If type is real
    if isinstance(data, float):
        tmp = float(data)
        if 0 < precision < 7:
            res = "{:.{}f}".format(tmp, precision)
        else:
            res = "{:0.3e}".format(tmp)
    # If cell is null
    if data is None:
        res = ""
    # Print data in file
    file_content += field_name + "\n"
    file_content += res + "\n"
    return file_content

def write_file(directory, filename, content):
    try:
        with open(os.path.join(directory, filename), "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing file {filename} in {directory}: {e}")
        
def process_chunk(*args):
    chunk, mi, md, tpv6,tppar, directoryPath,pltfolder, rap, var, prof, dt = args
    dataframes = []
    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}
    tempopar = {}
    tectable = {}
    initable = {}

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
        
    for i, row in enumerate(chunk):
        print(f"Iteration {i}", flush=True)
        # Création du chemin du fichier
        simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]))
        usmdir = os.path.join(directoryPath, str(row["idsim"]))
            
        try:
            # Tempoparv6
            Path(usmdir).mkdir(parents=True, exist_ok=True)
            write_file(usmdir, "tempoparv6.sti", tpv6)

            # Tempopar
            tempoparid =  row["idOption"]
            if tempoparid not in tempopar:            
                tempoparConverter = sticstempoparconverter.SticsTempoparConverter()
                r = tempoparConverter.export(simPath, MasterInput_Connection, tppar, usmdir)
                tempopar[tempoparid] = r
            else:
                write_file(usmdir, "tempopar.sti", tempopar[tempoparid])

            # Soil Station
            soilid =  row["idsoil"]
            if soilid not in soiltable:
                paramsolconverter = sticsparamsolconverter.SticsParamSolConverter()
                r1 = paramsolconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)           
                stationconverter = sticsstationconverter.SticsStationConverter()
                r2 = stationconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, rap, var, prof, usmdir)         
                soiltable[soilid] = [r1, r2]
            else:
                write_file(usmdir, "param.sol", soiltable[soilid][0])
                write_file(usmdir, "station.txt", soiltable[soilid][1][0])
                write_file(usmdir, "snow_variables.txt",  soiltable[soilid][1][1])
                write_file(usmdir, "prof.mod",  prof)
                write_file(usmdir, "rap.mod",  rap)
                write_file(usmdir, "var.mod",  var)
            
            # NewTravail
            newtravailconverter = sticsnewtravailconverter.SticsNewTravailConverter()
            newtravailconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
            
            # Init  
            iniid =  ".".join([str(row["idsoil"]), str(row["idIni"])])    
            if iniid not in initable:            
                ficiniconverter = sticsficiniconverter.SticsFicIniConverter()
                r = ficiniconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                initable[iniid] = r
            else:
                write_file(usmdir, "ficini.txt", initable[iniid])
            
            # Climat
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            if climid not in weathertable:
                climatconverter = sticsclimatconverter.SticsClimatConverter()
                r = climatconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                weathertable[climid] = r
            else:
                write_file(usmdir, "climat.txt", weathertable[climid])
            
            # Fictec1
            tecid =  ".".join([str(row["idMangt"]), str(row["idsoil"])]) 
            if tecid not in tectable:  
                fictec1converter = sticsfictec1converter.SticsFictec1Converter()
                r = fictec1converter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                tectable[tecid] = r
            else:
                write_file(usmdir, "fictec1.txt", tectable[tecid])
            
            # Ficplt1   
            ficplt1converter = sticsficplt1converter.SticsFicplt1Converter()
            ficplt1converter.export(simPath, MasterInput_Connection, pltfolder, usmdir)

            # run stics
            bs = os.path.join(Path(__file__).parent, "sticsrun.sh")
            try:
                result = subprocess.run(["bash", bs, usmdir, directoryPath, str(dt)],capture_output=True, check=True, text=True, timeout=180)
            except subprocess.TimeoutExpired as e:
                print(f"⏰ STICS run timed out for {usmdir}. Killing...")
                # Forcefully terminate the process if it hangs
                #result.kill()  # Python 3.9+
                raise e

            except subprocess.CalledProcessError as e:
                print(f"❌ STICS run failed for {usmdir} with return code {e.returncode}")
                print("STDOUT:\n", e.stdout)
                print("STDERR:\n", e.stderr)
                #result.kill()  # Python 3.9+
                raise e  # skip to next simulation
            except Exception as e:
                print(f"⚠️ Unexpected error for {usmdir}: {str(e)}")
                #result.kill()  # Python 3.9+
                raise e
            finally:
                # Cleanup: Close any open files or resources here
                pass  # Add cleanup logic if needed

            # get the file "mod_rapport.sti" in the usmdir directory
            mod_r = os.path.join(directoryPath, f"mod_rapport_{str(row['idsim'])}.sti") 
            if not os.path.exists(mod_r):
                print(f"Warning: {mod_r} does not exist")
                continue
            df = create_df_summary(mod_r)
            dataframes.append(df)
            if dt==1: os.remove(mod_r)
            
        except Exception as ex:
            print("Error during Running STICS  :", ex)
            traceback.print_exc()
            raise
    if not dataframes:
        print("No dataframes to concatenate.")
        ModelDictionary_Connection.close()
        MasterInput_Connection.close()
        return pd.DataFrame()

    # close connections
    ModelDictionary_Connection.close()
    MasterInput_Connection.close()
    return pd.concat(dataframes, ignore_index=True)
            
def export(MasterInput, ModelDictionary):
    MasterInput_Connection = sqlite3.connect(MasterInput)
    ModelDictionary_Connection = sqlite3.connect(ModelDictionary)
    try:
        print(f"dbMasterInput: {MasterInput}")
        print(f"dbModelsDictionary: {ModelDictionary}")
        # Set PRAGMA synchronous to OFF
        MasterInput_Connection.execute("PRAGMA synchronous = OFF")
        ModelDictionary_Connection.execute("PRAGMA synchronous = OFF")
        # Set PRAGMA journal_mode to WAL
        MasterInput_Connection.execute("PRAGMA journal_mode = WAL")
        ModelDictionary_Connection.execute("PRAGMA journal_mode = WAL")
        # Run full checkpoint
        MasterInput_Connection.execute("PRAGMA wal_checkpoint(FULL)")
    except Exception as ex:
        print(f"Connection Error: {ex}")
    

    try:
        cursor = MasterInput_Connection.cursor()
        cursor2 = ModelDictionary_Connection.cursor()
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsim ON SimUnitList (idsim);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idCoord ON Coordinates (idPoint);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idMangt ON CropManagement (idMangt);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoil ON Soil (IdSoil);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoill ON Soil (Lower(IdSoil));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoiltl ON SoilTypes (Lower(SoilTextureType));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idoption ON SimulationOptions (idOptions);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultivars ON ListCultivars (idCultivar);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultopt ON ListCultivars (CodePSpecies);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultoptspec ON ListCultOption (CodePSpecies);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orga ON OrganicFOperations (idFertOrga);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orga_res ON OrganicFOperations (TypeResidues);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_res ON ListResidues (TypeResidues);")
        cursor2.execute("CREATE INDEX IF NOT EXISTS idx_model_table ON Variables (model, [Table]);")
        MasterInput_Connection.commit()
        ModelDictionary_Connection.commit()
        print("Indexes created successfully!")

    except sqlite3.Error as e:
        print(f"Error creating indexes: {e}")
    MasterInput_Connection.close()
    ModelDictionary_Connection.close()
    
    # convert this code from vb to python:
def fetch_data_from_sqlite(masterInput):
    conn = sqlite3.connect(masterInput)
    df = pd.read_sql_query(f"SELECT * FROM SimUnitList", conn)
    rows = df.to_dict(orient='records')
    conn.close()
    return rows
    
    
def chunk_data(data, chunk_size):    # values, num_sublists 
    #sublist_size = max(len(data) // chunk_size, 3)
    #return [data[i:i + sublist_size] for i in range(0, len(data), sublist_size)]
    k, m = divmod(len(data), chunk_size)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(chunk_size)]
    return sublists

def main():
    mi= GlobalVariables["dbMasterInput"]
    md = GlobalVariables["dbModelsDictionary"]
    directoryPath = GlobalVariables["directorypath"]
    pltfolder = GlobalVariables["pltfolder"]
    nthreads = GlobalVariables["nthreads"]
    dt = GlobalVariables["dt"]
    export(mi, md)
    tppar = common_tempopar(md)
    tpv6 = common_tempoparv6(md)
    rap = common_rap()
    var = common_var()
    prof = common_prof()
    data = fetch_data_from_sqlite(mi)
    # Split data into chunks
    chunks = chunk_data(data, chunk_size=nthreads)
    # Create a Pool of worker processes
    import uuid
    args_list = [(chunk,mi, md, tpv6,tppar,directoryPath,pltfolder, rap, var, prof, dt) for chunk in chunks]
    # create a random name
    result_name = str(uuid.uuid4()) + "_stics"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_stics"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")
    try:
        start = time()
        processed_data_chunks = []
        """with concurrent.futures.ProcessPoolExecutor(max_workers=nthreads) as executor:
            processed_data_chunks = list(executor.map(process_chunk,args_list))"""
        
        with parallel_backend("threading", n_jobs=nthreads):
            processed_data_chunks = Parallel()(
                delayed(process_chunk)(*args) for args in args_list
            )
        processed_data = pd.concat(processed_data_chunks, ignore_index=True)
        processed_data.to_csv(os.path.join(directoryPath, f"{result_name}.csv"), index=False)
        print(f"STICS total time, {time()-start}")
    except Exception as ex:  
        print("Error during processing:", ex)
        traceback.print_exc() 
        sys.exit(1)       
    
if __name__ == "__main__":
    main()
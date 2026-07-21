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
from concurrent.futures import ThreadPoolExecutor
import sys
import gc



SUMMARY_COLS = ["Model","Idsim","Texte","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","Nleac","SoilN","CroN_ma","CumE","Transp"]
DAILY_OUTPUT_TABLE = "SticsDailyOutput"
PROFILE_OUTPUT_TABLE = "SticsProfile"

def get_coord(d):
    res = re.findall(r"([-]?\d+[.]?\d+)[_]", d)
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

def create_df_summary(f, dt, idsim, plant_role=""):
    #d_name = os.path.dirname(f).split(os.path.sep)[-1]
    #d_name = Path(f).stem[len("mod_rapport_"):]
    remove_comma(f)
    if dt == 1: c = get_coord(idsim)
    df = pd.read_csv(f, sep=';', skipinitialspace=True)
    # STICS may leave repeated report headers when a working directory is reused.
    # Keep only actual result rows so rerunning the same idsim remains idempotent.
    numeric_ansemis = pd.to_numeric(df["ansemis"], errors="coerce")
    df = df.loc[numeric_ansemis.notna()].copy()
    df["ansemis"] = numeric_ansemis.loc[numeric_ansemis.notna()]
    df = df.reset_index().rename(columns={"iplts": "Planting","ilevs":"Emergence","iflos":"Ant","imats":"Mat","masec(n)":"Biom_ma","mafruit":"Yield","chargefruit":'GNumber',"laimax":"MaxLai","Qles":"Nleac","QNapp":"SoilN","QNplante":"CroN_ma","ces":"CumE","cep":"Transp", "cep2": "Transp"})
    df.insert(0, "Model", "Stics")
    df.insert(1, "Idsim", idsim)
    df.insert(2, "Texte", plant_role)
    df['time'] = df['ansemis'].astype(float).astype(int)
    if dt == 1:
        df['lon'] = c['lon']
        df['lat'] = c['lat']
    return df


def create_df_daily(daily_file, idsim, season_order=1, plant_role=""):
    """Read one STICS v11 mod_s file and identify its season and plant."""
    daily = pd.read_csv(daily_file, sep=";", skipinitialspace=True)
    daily.columns = [column.strip() for column in daily.columns]
    daily = daily.dropna(axis=1, how="all")
    daily["jul"] = pd.to_numeric(daily["jul"], errors="raise").astype(int)
    daily.insert(0, "Texte", plant_role)
    daily.insert(0, "SeasonOrder", int(season_order))
    daily.insert(0, "Idsim", str(idsim))
    daily.insert(0, "Model", "Stics")
    return daily


def collect_daily_outputs(directory_path, key, idsim, season_order, is_mixed_crop):
    """Read and remove every copied mod_s file for one STICS execution."""
    daily_frames = []
    pattern = f"mod_s_{key}__mod_s*.sti"
    for daily_path in sorted(Path(directory_path).glob(pattern)):
        source_name = daily_path.stem.split("__", 1)[-1]
        plant_role = ""
        if is_mixed_crop:
            suffix = source_name[len("mod_s"):].lower()
            if suffix.startswith("a"):
                plant_role = "A"
            elif suffix.startswith("p"):
                plant_role = "P"
        daily_frames.append(
            create_df_daily(daily_path, idsim, season_order, plant_role)
        )
        daily_path.unlink()
    return daily_frames


def create_df_profile(profile_file, idsim, season_order=1, plant_role=""):
    """Convert one STICS v11 profile matrix to normalized long form."""
    with open(profile_file, "r") as profile_stream:
        variable = profile_stream.readline().strip()
        header = profile_stream.readline().split()
    if len(header) < 2 or header[0].lower() != "cm":
        raise ValueError(f"Invalid STICS profile header in {profile_file}")

    julian_days = [int(value) for value in header[1:]]
    profile = pd.read_csv(
        profile_file,
        sep=r"\s+",
        skiprows=2,
        header=None,
        names=["depth_cm", *julian_days],
    )
    profile = profile.melt(
        id_vars="depth_cm", var_name="jul", value_name="value"
    )
    profile["depth_cm"] = pd.to_numeric(profile["depth_cm"], errors="raise")
    profile["jul"] = pd.to_numeric(profile["jul"], errors="raise").astype(int)
    profile.insert(0, "variable", variable)
    profile.insert(0, "Texte", plant_role)
    profile.insert(0, "SeasonOrder", int(season_order))
    profile.insert(0, "Idsim", str(idsim))
    profile.insert(0, "Model", "Stics")
    return profile


def collect_profile_outputs(directory_path, key, idsim, season_order, is_mixed_crop):
    """Read and remove every copied profile file for one STICS execution."""
    profile_frames = []
    pattern = f"mod_profil_{key}__mod_profil*.sti"
    for profile_path in sorted(Path(directory_path).glob(pattern)):
        source_name = profile_path.stem.split("__", 1)[-1]
        plant_role = ""
        if is_mixed_crop:
            suffix = source_name[len("mod_profil"):].lower()
            if suffix.startswith("a"):
                plant_role = "A"
            elif suffix.startswith("p"):
                plant_role = "P"
        profile_frames.append(
            create_df_profile(profile_path, idsim, season_order, plant_role)
        )
        profile_path.unlink()
    return profile_frames



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
    fileContent += "2\n"
    fileContent += "tsol\n"
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
    fileContent += "Chumt\n"
    fileContent += "Cb\n"
    fileContent += "Cr\n"
    fileContent += "Cmulch\n"
    fileContent += "Cbmulch\n"
    #'fileContent += "QNplante\n"
    #'fileContent += "Qles\n"
    #'fileContent += "soilN\n"
    return fileContent
        

def common_tempoparv6(modelDictionary):
    fileContent = ""
    ModelDictionary_Connection = sqlite3.connect(modelDictionary)

    # Tempopar query
    T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='sticsv11') AND ([Table]='st_tempoparv6'));"
    DT = pd.read_sql_query(T,ModelDictionary_Connection)

    fileContent += format_stics_data_v6(DT, "codepluiepoquet")
    fileContent += format_stics_data_v6(DT, "nbjoursrrversirrig")
    fileContent += format_stics_data_v6(DT, "codecalferti")
    fileContent += format_stics_data_v6(DT, "ratiolN", 5)
    fileContent += format_stics_data_v6(DT, "dosimxN", 5)
    fileContent += format_stics_data_v6(DT, "codetesthumN")
    fileContent += format_stics_data_v6(DT, "codeNmindec")
    fileContent += format_stics_data_v6(DT, "rapNmindec", 5)
    fileContent += format_stics_data_v6(DT, "fNmindecmin", 5)
    fileContent += format_stics_data_v6(DT, "codetrosee")
    fileContent += format_stics_data_v6(DT, "codeSWDRH")
    fileContent += format_stics_data_v6(DT, "option_pature")
    fileContent += format_stics_data_v6(DT, "coderes_pature")
    fileContent += format_stics_data_v6(DT, "pertes_restit_ext")
    fileContent += format_stics_data_v6(DT, "Crespc_pature")
    fileContent += format_stics_data_v6(DT, "Nminres_pature")
    fileContent += format_stics_data_v6(DT, "eaures_pature")
    fileContent += format_stics_data_v6(DT, "coef_calcul_qres")
    fileContent += format_stics_data_v6(DT, "engrais_pature")
    fileContent += format_stics_data_v6(DT, "coef_calcul_doseN")
    fileContent += format_stics_data_v6(DT, "code_CsurNsol_dynamic")
    fileContent += format_stics_data_v6(DT, "humirac")
    fileContent += format_stics_data_v6(DT, "code_ISOP")
    fileContent += format_stics_data_v6(DT, "code_pct_legume")
    fileContent += format_stics_data_v6(DT, "pct_legum")
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
    T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='sticsv11') AND ([Table]='st_tempopar')) OR ((model='sticsv11') AND ([Table]='st_tempopar_2')) OR ((model='sticsv11') AND ([Table]='st_tempopar_3'));"
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
    fileContent += format_stics_data(DT, "hauteur_threshold")
    fileContent += format_stics_data(DT, "parsurrg")
    fileContent += format_stics_data(DT, "par_to_net")
    fileContent += format_stics_data(DT, "coefb")
    fileContent += format_stics_data(DT, "proprac")
    fileContent += format_stics_data(DT, "y0msrac")
    fileContent += format_stics_data(DT, "dacohes")
    fileContent += format_stics_data(DT, "daseuilbas")
    fileContent += format_stics_data(DT, "daseuilhaut")
    fileContent += format_stics_data(DT, "beta")
    fileContent += format_stics_data(DT, "lvopt")
    fileContent += format_stics_data(DT, "difN")
    fileContent += format_stics_data(DT, "plNmin")
    fileContent += format_stics_data(DT, "irrlev")
    fileContent += format_stics_data(DT, "QNpltminINN")
    fileContent += format_stics_data(DT, "codesymbiose")
    fileContent += format_stics_data(DT, "codefxn")
    fileContent += format_stics_data(DT, "tmin_mineralisation")  # Added in VB InterCrop v11
    fileContent += format_stics_data(DT, "FTEMh")
    fileContent += format_stics_data(DT, "FTEMha")
    fileContent += format_stics_data(DT, "TREFh")
    fileContent += format_stics_data(DT, "FTEMr")
    fileContent += format_stics_data(DT, "FTEMra")
    fileContent += format_stics_data(DT, "TREFr")

    fileContent += format_stics_data(DT, "GMIN1")  # Added in VB InterCrop v11, replaces FINERT/FMIN
    fileContent += format_stics_data(DT, "GMIN2")
    fileContent += format_stics_data(DT, "GMIN3")
    fileContent += format_stics_data(DT, "GMIN4")
    fileContent += format_stics_data(DT, "GMIN5")
    fileContent += format_stics_data(DT, "GMIN6")
    fileContent += format_stics_data(DT, "GMIN7")
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
    fileContent += format_stics_data(DT, "kdesat")  # Renamed from k_desat in VB InterCrop v11
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
    chunk, mi, md, tpv6,tppar, directoryPath,pltfolder, rap, var, prof, dt, tempDir, *options = args
    dailyoutput = int(options[0]) if options else 0
    dataframes = []
    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}
    tempopar = {}
    tectable = {}
    initable = {}
    
    # Clear caches periodically to prevent memory buildup
    CACHE_CLEAR_INTERVAL = 50000

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
        
    for i, row in enumerate(chunk):
        # Periodically clear caches to free memory
        if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
            print(f" Clearing caches at row {i} to free memory", flush=True)
            weathertable.clear()
            soiltable.clear()
            tempopar.clear()
            tectable.clear()
            initable.clear()
            # Also trigger garbage collection
            import gc
            gc.collect()
        print(f"Iteration {i}", flush=True)
        # Création du chemin du fichier
        idsim = str(row["idsim"])
        simPath = os.path.join(directoryPath, idsim, str(row["idPoint"]), str(row["StartYear"]))
        usmdir = os.path.join(tempDir, idsim)
            
        try:
            # Tempoparv6
            Path(usmdir).mkdir(parents=True, exist_ok=True)
            for report_name in (
                "mod_rapport.sti", "mod_rapportA.sti", "mod_rapportP.sti"
            ):
                report_file = Path(usmdir) / report_name
                if report_file.exists():
                    report_file.unlink()
            if dailyoutput == 1:
                for daily_file in Path(usmdir).glob("mod_s*.sti"):
                    daily_file.unlink()
                for profile_file in Path(usmdir).glob("mod_profil*.sti"):
                    profile_file.unlink()
            write_file(usmdir, "tempoparv6.sti", tpv6)

            # Tempopar
            tempoparid =  row["idOption"]
            if tempoparid not in tempopar:            
                tempoparConverter = sticstempoparconverter.SticsTempoparConverter()
                r = tempoparConverter.export(simPath, MasterInput_Connection, tppar, usmdir)
                tempopar[tempoparid] = r
                del tempoparConverter  # Free converter object
            else:
                write_file(usmdir, "tempopar.sti", tempopar[tempoparid])

            # Soil Station
            is_mixed_crop = bool(row["is_mixed_crop"])
            soilid =  (row["idsoil"], is_mixed_crop)
            if soilid not in soiltable:
                paramsolconverter = sticsparamsolconverter.SticsParamSolConverter()
                r1 = paramsolconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                del paramsolconverter  # Free converter
                stationconverter = sticsstationconverter.SticsStationConverter()
                r2 = stationconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, rap, var, prof, usmdir)         
                soiltable[soilid] = [r1, r2]
                del stationconverter  # Free converter
            else:
                write_file(usmdir, "param.sol", soiltable[soilid][0])
                write_file(usmdir, "station.txt", soiltable[soilid][1])
                write_file(usmdir, "prof.mod",  prof)
                write_file(usmdir, "rap.mod",  rap)
                write_file(usmdir, "var.mod",  var)
            
            # NewTravail
            newtravailconverter = sticsnewtravailconverter.SticsNewTravailConverter()
            newtravailconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
            del newtravailconverter  # Free converter
            
            # Init  
            iniid =  ".".join([str(row["idsoil"]), str(row["idIni"])])    
            if iniid not in initable:            
                ficiniconverter = sticsficiniconverter.SticsFicIniConverter()
                r = ficiniconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                initable[iniid] = r
                del ficiniconverter  # Free converter
            else:
                write_file(usmdir, "ficini.txt", initable[iniid])
            
            # Climat
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            if climid not in weathertable:
                climatconverter = sticsclimatconverter.SticsClimatConverter()
                r = climatconverter.export(
                    simPath,
                    ModelDictionary_Connection,
                    MasterInput_Connection,
                    usmdir,
                    start_year=row["StartYear"],
                    end_year=row["EndYear"],
                )
                weathertable[climid] = r
                del climatconverter  # Free converter
            else:
                write_file(usmdir, "climat.txt", weathertable[climid])
            
            # Fictec1
            tecid =  ".".join([str(row["idMangt"]), str(row["idsoil"])]) 
            if tecid not in tectable:  
                fictec1converter = sticsfictec1converter.SticsFictec1Converter()
                r = fictec1converter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                tectable[tecid] = r
                del fictec1converter  # Free converter
            else:
                if isinstance(tectable[tecid], list) and len(tectable[tecid]) == 2:
                    write_file(usmdir, "fictec1.txt", tectable[tecid][0])
                    write_file(usmdir, "fictec2.txt", tectable[tecid][1])
                else: write_file(usmdir, "fictec1.txt", tectable[tecid])
            
            # Ficplt1   
            ficplt1converter = sticsficplt1converter.SticsFicplt1Converter()
            ficplt1converter.export(simPath, MasterInput_Connection, pltfolder, usmdir)
            del ficplt1converter  # Free converter

            # run stics
            bs = os.path.join(Path(__file__).parent, "sticsrun.sh")
            try:
                result = subprocess.run(
                    ["bash", bs, usmdir, directoryPath, str(dt), str(dailyoutput)],
                    capture_output=True, check=True, text=True, timeout=180,
                )
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
            if is_mixed_crop:
                reports = [
                    ("A", os.path.join(directoryPath, f"mod_rapportA_{idsim}.sti")),
                    ("P", os.path.join(directoryPath, f"mod_rapportP_{idsim}.sti")),
                ]
            else:
                reports = [
                    ("", os.path.join(directoryPath, f"mod_rapport_{idsim}.sti")),
                ]
            for plant_role, report_path in reports:
                if not os.path.exists(report_path):
                    print(f"Warning: {report_path} does not exist")
                    continue

                plant_df = create_df_summary(
                    report_path,
                    dt,
                    idsim,
                    plant_role,
                )
                dataframes.append(plant_df)
                os.remove(report_path)

                del plant_df  # Free df after appending

        except Exception as ex:
            print("Error during Running STICS  :", ex)
            traceback.print_exc()
            raise
    if not dataframes:
        print("No dataframes to concatenate.")
        ModelDictionary_Connection.close()
        MasterInput_Connection.close()
        # Clear all caches
        weathertable.clear()
        soiltable.clear()
        tempopar.clear()
        tectable.clear()
        initable.clear()
        return pd.DataFrame()

    # close connections
    ModelDictionary_Connection.close()
    MasterInput_Connection.close()
    
    # Clear all caches before concatenation
    weathertable.clear()
    soiltable.clear()
    tempopar.clear()
    tectable.clear()
    initable.clear()
    
    # Concatenate in batches to reduce memory usage
    batch_size = 60000
    if len(dataframes) <= batch_size:
        result = pd.concat(dataframes, ignore_index=True)
        del dataframes  # Free the list
        return result
    
    result = pd.DataFrame()
    for i in range(0, len(dataframes), batch_size):
        batch = dataframes[i:i+batch_size]
        batch_concat = pd.concat(batch, ignore_index=True)
        result = pd.concat([result, batch_concat], ignore_index=True)
        # Clear the batch to free memory
        del batch
        del batch_concat
    
    del dataframes  # Free the list
    return result
            
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
    query = """
        SELECT
            SimUnitList.*,
            CASE
                WHEN (
                    SELECT COUNT(DISTINCT cm.PlantOrder)
                    FROM CropManagement AS cm
                    WHERE cm.idMangt = SimUnitList.idMangt
                ) > 1
                THEN 1
                ELSE 0
            END AS is_mixed_crop
        FROM SimUnitList
    """
    df = pd.read_sql_query(query, conn)
    rows = df.to_dict(orient='records')
    conn.close()
    return rows
    
    
def chunk_data(data, parts, chunk_size):    # values, num_sublists 
    #sublist_size = max(len(data) // chunk_size, 3)
    #return [data[i:i + sublist_size] for i in range(0, len(data), sublist_size)]
    k, m = divmod(len(data), parts * chunk_size)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(parts * chunk_size)]
    return sublists

def process_chunk_safe(idx, args):
    try:
        chunk_df = process_chunk(*args)
        return idx, chunk_df, None
    except Exception:
        return idx, None, traceback.format_exc()
    
    
def _main_standard(simulations=None):
    mi = GlobalVariables.get("dbMasterInput")
    md = GlobalVariables.get("dbModelsDictionary")
    directoryPath = GlobalVariables.get("directorypath", os.getcwd())
    pltfolder = GlobalVariables.get("pltfolder")
    nthreads = GlobalVariables.get("nthreads", 4)
    dt = GlobalVariables.get("dt", 1)
    parts = GlobalVariables.get("parts", 1)
    tempDir = GlobalVariables.get("tempDir")
    package = GlobalVariables.get("package")

    if not mi or not md:
        raise ValueError("dbMasterInput and dbModelsDictionary must be set in GlobalVariables")

    os.makedirs(directoryPath, exist_ok=True)
    os.makedirs(tempDir, exist_ok=True)
    
    stics_params = os.path.join(package, "data", "stics_params")
    if not os.path.exists(stics_params):
        rap = common_rap()
        var = common_var()
        prof = common_prof()
    else:
        rapfile = os.path.join(stics_params, "rap.mod")
        with open(rapfile, "r") as f:
            rap = f.read()
        varfile = os.path.join(stics_params, "var.mod")
        with open(varfile, "r") as f:
            var = f.read()
        proffile = os.path.join(stics_params, "prof.mod")
        with open(proffile, "r") as f:
            prof = f.read()
    export(mi, md)

    tppar = common_tempopar(md)
    tpv6 = common_tempoparv6(md)

    data = fetch_data_from_sqlite(mi) if simulations is None else simulations
    # Split data into chunks
    chunks = chunk_data(data, parts, chunk_size=nthreads)
    n_simulations = len(data)
    print(f"📊 Total simulations to process: {len(data)}", flush=True)
    del data  # Free original data list after chunking
    # Create a Pool of worker processes
    import uuid
    args_list = [(chunk,mi, md, tpv6,tppar,directoryPath,pltfolder, rap, var, prof, dt, tempDir) for chunk in chunks]
    del chunks  # Free chunks list after creating args_list
    # create a random name
    result_name = str(uuid.uuid4()) + "_stics"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_stics"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")
    try:
        start = time()
        
        # Use joblib Parallel with loky backend, write results directly to final file
        print(f"Processing {len(args_list)} chunks...", flush=True)
        
        write_header = True
        total_chunks_written = 0
        MAX_SIMULATIONS_IN_MEMORY = 100000  # Adjust this threshold based on available memory and typical simulation size
        
        if n_simulations <= MAX_SIMULATIONS_IN_MEMORY:
            print(f"Using in-memory concatenation for {n_simulations} simulations", flush=True)
            processed_data_chunks = Parallel(n_jobs=nthreads, backend="loky")(
                delayed(process_chunk)(*args) for args in args_list
            )

            processed_data_chunks = [
                df for df in processed_data_chunks
                if df is not None and not df.empty
            ]

            if processed_data_chunks:
                processed_data = pd.concat(processed_data_chunks, ignore_index=True)
                processed_data.to_csv(result_path, index=False)

                print(
                    f"✅ Written {len(processed_data)} rows to {result_path}",
                    flush=True
                )

                del processed_data
            else:
                print("No data to process.")
                return

            del processed_data_chunks
            gc.collect()
        
        else:
            print(f"Using chunked processing for {n_simulations} simulations", flush=True)            
            results = Parallel(
                n_jobs=nthreads,
                backend="loky",
                return_as="generator_unordered",
                batch_size="auto"
            )(
                delayed(process_chunk_safe)(i, args)
                for i, args in enumerate(args_list)
            )        

            for idx, chunk_df, error in results:
                if error is not None:
                    print(f"❌ Chunk {idx + 1}/{len(args_list)} failed:\n{error}", flush=True)
                    continue

                if chunk_df is not None and not chunk_df.empty:
                    chunk_df.to_csv(
                        result_path,
                        mode="a",
                        header=write_header,
                        index=False
                    )
                    write_header = False
                    total_chunks_written += 1

                    print(
                        f"✅ Chunk {idx + 1}/{len(args_list)}: "
                        f"{len(chunk_df)} rows written",
                        flush=True
                    )

                del chunk_df
                gc.collect()
            
            if total_chunks_written == 0:
                print("No data to process.")
                return
        
        print(f"✅ Results saved to {result_path}")
        print(f"STICS total time: {time()-start:.2f}s", flush=True)

        return result_path

    except Exception as ex:  
        print("Error during processing:", ex)
        traceback.print_exc() 
        raise


def partition_simulations(master_input, simulations):
    """Split SimUnitList rows into standard and successive managements."""
    connection = sqlite3.connect(master_input)
    try:
        crop_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(CropManagement)")
        }
        if "SeasonOrder" not in crop_columns:
            return simulations, []
        season_counts = {
            str(id_mangt): int(count)
            for id_mangt, count in connection.execute(
                """
                SELECT idMangt, COUNT(DISTINCT SeasonOrder)
                FROM CropManagement
                GROUP BY idMangt
                """
            )
        }
    finally:
        connection.close()

    standard = []
    successive = []
    for simulation in simulations:
        count = season_counts.get(str(simulation["idMangt"]), 0)
        if count == 0:
            raise ValueError(
                f"No CropManagement rows for idMangt={simulation['idMangt']}"
            )
        (successive if count > 1 else standard).append(simulation)
    return standard, successive


def get_simulation_weights(master_input, simulations):
    """Return the number of seasons used as scheduling weight for each idsim."""
    connection = sqlite3.connect(master_input)
    try:
        season_counts = {
            str(id_mangt): max(1, int(count))
            for id_mangt, count in connection.execute(
                """
                SELECT idMangt, COUNT(DISTINCT SeasonOrder)
                FROM CropManagement
                GROUP BY idMangt
                """
            )
        }
    finally:
        connection.close()
    return {
        str(simulation["idsim"]): season_counts.get(str(simulation["idMangt"]), 1)
        for simulation in simulations
    }


def build_balanced_simulation_chunks(simulations, weights, number_of_chunks):
    """Distribute simulations by decreasing season count over balanced chunks."""
    if not simulations:
        return []
    chunk_count = min(max(1, int(number_of_chunks)), len(simulations))
    chunks = [[] for _ in range(chunk_count)]
    loads = [0] * chunk_count
    ordered = sorted(
        simulations,
        key=lambda simulation: weights[str(simulation["idsim"])],
        reverse=True,
    )
    for simulation in ordered:
        index = min(range(chunk_count), key=lambda item: (loads[item], item))
        chunks[index].append(simulation)
        loads[index] += weights[str(simulation["idsim"])]
    return chunks


def process_routed_chunk(
    chunk_index, total_chunks, chunk, weights, successive_ids,
    mi, md, tpv6, tppar, directory_path, pltfolder,
    rap, var, prof, dt, temp_dir, package, dailyoutput,
):
    """Process one balanced chunk containing standard and successive idsim rows."""
    from . import sticssuccessiveconverter

    standard_rows = [
        simulation for simulation in chunk
        if str(simulation["idsim"]) not in successive_ids
    ]
    frames = []
    daily_frames = []
    profile_frames = []
    if standard_rows:
        standard_frame = process_chunk(
            standard_rows, mi, md, tpv6, tppar, directory_path, pltfolder,
            rap, var, prof, dt, temp_dir, dailyoutput,
        )
        if standard_frame is not None and not standard_frame.empty:
            frames.append(standard_frame)
        if int(dailyoutput) == 1:
            for simulation in standard_rows:
                daily_frames.extend(
                    collect_daily_outputs(
                        directory_path,
                        str(simulation["idsim"]),
                        simulation["idsim"],
                        1,
                        bool(simulation.get("is_mixed_crop", 0)),
                    )
                )
                profile_frames.extend(
                    collect_profile_outputs(
                        directory_path,
                        str(simulation["idsim"]),
                        simulation["idsim"],
                        1,
                        bool(simulation.get("is_mixed_crop", 0)),
                    )
                )
    for simulation in chunk:
        if str(simulation["idsim"]) in successive_ids:
            frame, daily, profile = sticssuccessiveconverter.process_simulation(
                simulation, mi, md, directory_path, temp_dir, pltfolder, package,
                dt, dailyoutput,
            )
            if frame is not None and not frame.empty:
                frames.append(frame)
            if daily is not None and not daily.empty:
                daily_frames.append(daily)
            if profile is not None and not profile.empty:
                profile_frames.append(profile)
    summary = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    daily = (
        pd.concat(daily_frames, ignore_index=True, sort=False)
        if daily_frames else pd.DataFrame()
    )
    profile = (
        pd.concat(profile_frames, ignore_index=True, sort=False)
        if profile_frames else pd.DataFrame()
    )
    return summary, daily, profile


def save_summary_output(result_path, master_input):
    """Replace all STICS rows in SummaryOutput with the current result batch."""
    summary_cols = [
        "Model", "Idsim", "Texte", "SeasonOrder", "Planting", "Emergence", "Ant", "Mat",
        "Biom_ma", "Yield", "GNumber", "MaxLai", "Nleac", "SoilN",
        "CroN_ma", "CumE", "Transp",
    ]
    dataframe = pd.read_csv(result_path, usecols=lambda column: column in summary_cols)
    for column in summary_cols:
        if column not in dataframe.columns:
            dataframe[column] = 1 if column == "SeasonOrder" else None
    dataframe["SeasonOrder"] = (
        pd.to_numeric(dataframe["SeasonOrder"], errors="coerce")
        .fillna(1)
        .astype(int)
    )
    dataframe = dataframe[summary_cols]

    connection = sqlite3.connect(master_input)
    try:
        summary_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(SummaryOutput)")
        }
        if "SeasonOrder" not in summary_columns:
            connection.execute(
                "ALTER TABLE SummaryOutput ADD COLUMN SeasonOrder INTEGER"
            )
        connection.execute("DELETE FROM SummaryOutput WHERE Model = 'Stics'")
        dataframe.to_sql("SummaryOutput", connection, if_exists="append", index=False)
        connection.commit()
    finally:
        connection.close()
    print(f"✅ {len(dataframe)} rows inserted into SummaryOutput.", flush=True)


def save_daily_output(dataframe, master_input):
    """Replace all STICS daily rows with the current result batch."""
    if dataframe is None or dataframe.empty:
        print("Warning: no daily STICS results were produced.", flush=True)
        return
    connection = sqlite3.connect(master_input)
    try:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (DAILY_OUTPUT_TABLE,),
        ).fetchone() is not None
        if table_exists:
            existing_columns = {
                row[1] for row in connection.execute(
                    f'PRAGMA table_info("{DAILY_OUTPUT_TABLE}")'
                )
            }
            for column, column_type in (("SeasonOrder", "INTEGER"), ("Texte", "TEXT")):
                if column not in existing_columns:
                    connection.execute(
                        f'ALTER TABLE "{DAILY_OUTPUT_TABLE}" '
                        f'ADD COLUMN "{column}" {column_type}'
                    )
            connection.execute(
                f'DELETE FROM "{DAILY_OUTPUT_TABLE}" WHERE Model = \'Stics\''
            )
            dataframe.to_sql(
                DAILY_OUTPUT_TABLE, connection, if_exists="append", index=False
            )
        else:
            dataframe.to_sql(
                DAILY_OUTPUT_TABLE, connection, if_exists="replace", index=False
            )
        connection.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{DAILY_OUTPUT_TABLE}_idsim_season_jul" '
            f'ON "{DAILY_OUTPUT_TABLE}" ("Idsim", "SeasonOrder", "jul")'
        )
        connection.commit()
    finally:
        connection.close()
    print(
        f"✅ {len(dataframe)} rows inserted into {DAILY_OUTPUT_TABLE}.",
        flush=True,
    )


def save_profile_output(dataframe, master_input):
    """Replace all STICS profile rows with the current result batch."""
    if dataframe is None or dataframe.empty:
        print("Warning: no STICS profile results were produced.", flush=True)
        return
    connection = sqlite3.connect(master_input)
    try:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (PROFILE_OUTPUT_TABLE,),
        ).fetchone() is not None
        if table_exists:
            existing_columns = {
                row[1] for row in connection.execute(
                    f'PRAGMA table_info("{PROFILE_OUTPUT_TABLE}")'
                )
            }
            for column, column_type in (("SeasonOrder", "INTEGER"), ("Texte", "TEXT")):
                if column not in existing_columns:
                    connection.execute(
                        f'ALTER TABLE "{PROFILE_OUTPUT_TABLE}" '
                        f'ADD COLUMN "{column}" {column_type}'
                    )
            connection.execute(
                f'DELETE FROM "{PROFILE_OUTPUT_TABLE}" WHERE Model = \'Stics\''
            )
            dataframe.to_sql(
                PROFILE_OUTPUT_TABLE, connection, if_exists="append", index=False
            )
        else:
            dataframe.to_sql(
                PROFILE_OUTPUT_TABLE, connection, if_exists="replace", index=False
            )
        connection.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{PROFILE_OUTPUT_TABLE}_lookup" '
            f'ON "{PROFILE_OUTPUT_TABLE}" '
            f'("Idsim", "SeasonOrder", "Texte", "jul", "depth_cm", "variable")'
        )
        connection.commit()
    finally:
        connection.close()
    print(
        f"✅ {len(dataframe)} rows inserted into {PROFILE_OUTPUT_TABLE}.",
        flush=True,
    )


def main():
    """Single STICS entry point for standard, mixed and successive simulations."""
    mi = GlobalVariables.get("dbMasterInput")
    if not mi:
        raise ValueError("dbMasterInput must be set in GlobalVariables")

    simulations = fetch_data_from_sqlite(mi)
    target_idsim = GlobalVariables.get("sticsIdsim")
    if target_idsim is not None:
        simulations = [
            simulation for simulation in simulations
            if str(simulation["idsim"]) == str(target_idsim)
        ]
        if not simulations:
            raise ValueError(f"STICS simulation {target_idsim!r} was not found")

    standard, successive = partition_simulations(mi, simulations)
    print(
        f"STICS routing: {len(standard)} standard, "
        f"{len(successive)} successive simulation(s)",
        flush=True,
    )

    md = GlobalVariables.get("dbModelsDictionary")
    directory_path = GlobalVariables.get("directorypath", os.getcwd())
    pltfolder = GlobalVariables.get("pltfolder")
    temp_dir = GlobalVariables.get("tempDir")
    package = GlobalVariables.get("package")
    nthreads = max(1, int(GlobalVariables.get("nthreads", 4)))
    parts = max(1, int(GlobalVariables.get("parts", 1)))
    dt = int(GlobalVariables.get("dt", 1))
    dailyoutput = int(GlobalVariables.get("dailyoutput", 0))
    if not md or not pltfolder or not temp_dir or not package:
        raise ValueError(
            "dbModelsDictionary, pltfolder, tempDir and package must be configured"
        )
    os.makedirs(directory_path, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    export(mi, md)

    stics_params = os.path.join(package, "data", "stics_params")
    if os.path.exists(stics_params):
        with open(os.path.join(stics_params, "rap.mod")) as stream:
            rap = stream.read()
        with open(os.path.join(stics_params, "var.mod")) as stream:
            var = stream.read()
        with open(os.path.join(stics_params, "prof.mod")) as stream:
            prof = stream.read()
    else:
        rap, var, prof = common_rap(), common_var(), common_prof()
    tppar = common_tempopar(md)
    tpv6 = common_tempoparv6(md)

    weights = get_simulation_weights(mi, simulations)
    chunks = build_balanced_simulation_chunks(
        simulations, weights, nthreads * parts
    )
    successive_ids = {str(simulation["idsim"]) for simulation in successive}
    loads = [sum(weights[str(row["idsim"])] for row in chunk) for chunk in chunks]
    standard_count = len(standard)
    successive_season_count = sum(
        weights[str(simulation["idsim"])] for simulation in successive
    )
    individual_simulation_count = standard_count + successive_season_count
    print(
        f"📊 Total individual STICS simulations to process: "
        f"{individual_simulation_count} "
        f"({standard_count} standard + {successive_season_count} successive seasons "
        f"from {len(successive)} rotation(s))",
        flush=True,
    )
    print(
        f"SimUnitList rows selected: {len(simulations)}",
        flush=True,
    )
    print(
        f"Balanced into {len(chunks)} sub-batches "
        f"(estimated season loads: {loads})",
        flush=True,
    )
    processed_chunks = Parallel(n_jobs=nthreads, backend="loky")(
        delayed(process_routed_chunk)(
            chunk_index, len(chunks), chunk, weights, successive_ids,
            mi, md, tpv6, tppar, directory_path,
            pltfolder, rap, var, prof, dt, temp_dir, package, dailyoutput,
        )
        for chunk_index, chunk in enumerate(chunks)
    )
    frames = [
        result[0] for result in processed_chunks
        if result is not None and result[0] is not None and not result[0].empty
    ]
    daily_frames = [
        result[1] for result in processed_chunks
        if result is not None and result[1] is not None and not result[1].empty
    ]
    profile_frames = [
        result[2] for result in processed_chunks
        if result is not None and result[2] is not None and not result[2].empty
    ]
    if not frames:
        print("No STICS reports produced.", flush=True)
        return None

    import uuid

    result_path = os.path.join(directory_path, f"{uuid.uuid4()}_stics.csv")
    pd.concat(frames, ignore_index=True, sort=False).to_csv(result_path, index=False)
    print(f"✅ Results saved to {result_path}", flush=True)

    if int(GlobalVariables.get("dt", 1)) == 0:
        save_summary_output(result_path, mi)
    if dailyoutput == 1:
        daily_result = (
            pd.concat(daily_frames, ignore_index=True, sort=False)
            if daily_frames else pd.DataFrame()
        )
        save_daily_output(daily_result, mi)
        profile_result = (
            pd.concat(profile_frames, ignore_index=True, sort=False)
            if profile_frames else pd.DataFrame()
        )
        save_profile_output(profile_result, mi)
    return result_path
    
if __name__ == "__main__":
    main()

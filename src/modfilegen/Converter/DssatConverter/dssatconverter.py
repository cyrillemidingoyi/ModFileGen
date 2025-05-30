from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import dssatweatherconverter, dssatcultivarconverter, dssatsoilconverter, dssatxconverter
import sys, subprocess, shutil
import concurrent.futures
import numpy as np
import os
import datetime
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
import time
import traceback
from joblib import Parallel, delayed, parallel_backend   
import re 


def get_coord(d):
    res = re.findall("([-]?\d+[.]?\d+)[_]", d)
    lat = float(res[0])
    lon = float(res[1])
    year = int(float(res[2]))
    return {'lon': lon, 'lat': lat, 'year': year}


def transform(fil):
    with open(fil, "r") as fil_:
        FILE = fil_.readlines()
    #d_name = os.path.dirname(fil).split(os.path.sep)[-1]
    d_name = Path(fil).stem[len("Summary_"):]
    c = get_coord(d_name)
    outData = FILE[4:]
    varId = FILE[3]					# Read the raw variables
    varId = list(map(str, str.split(varId[1:])[13:]))		# Only get the useful variables
    nYear = np.size(outData)
    dataArr = [list(map(float, str.split(outData[i])[13:]))
		                   for i in range(nYear)][0]   
    df = pd.DataFrame({varId[i]: [dataArr[i]] for i in range(len(varId))})
    df = df.reset_index().rename(columns={"PDAT": "Planting","EDAT":"Emergence","ADAT":"Ant","MDAT":"Mat","CWAM":"Biom_ma","HWAM":"Yield","H#AM":'GNumber',"LAIX":"MaxLai","NLCM":"Nleac","NIAM":"SoilN","CNAM":"CroN_ma","ESCP":"CumE","EPCP":"Transp"})
    df.insert(0, "Model", "Dssat")
    df.insert(1, "Idsim", d_name)
    df.insert(2, "Texte", "")

    df['lon'] = c['lon']
    df['lat'] = c['lat']
    df['time'] = int(c['year'])

    return df


def general(DT):
    fileContentg = "*GENERAL\n"
    fileContentg += "@PEOPLE\n"
    rw = DT[DT["Champ"] == "PEOPLE"]
    Dv = rw["dv"].values[0]
    fileContentg += Dv + "\n"
    fileContentg += "@ADDRESS\n"
    rw = DT[DT["Champ"] == "ADDRESS"]
    Dv = rw["dv"].values[0]
    fileContentg += Dv + "\n"
    fileContentg += "@SITE\n"
    rw = DT[DT["Champ"] == "SITE"]
    Dv = rw["dv"].values[0]
    fileContentg += Dv + "\n"
    site_columns_header = "@ PAREA  PRNO  PLEN  PLDR  PLSP  PLAY HAREA  HRNO  HLEN  HARM........."
    fileContentg += site_columns_header + "\n"
    rw = DT[DT["Champ"] == "PAREA"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PAREA"].format(float(Dv))
    rw = DT[DT["Champ"] == "PRNO"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PRNO"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLEN"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PLEN"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLDR"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PLDR"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLSP"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PLSP"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLAY"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["PLAY"].format(float(Dv))
    rw = DT[DT["Champ"] == "HAREA"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["HAREA"].format(float(Dv))
    rw = DT[DT["Champ"] == "HRNO"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["HRNO"].format(float(Dv))
    rw = DT[DT["Champ"] == "HLEN"]
    Dv = rw["dv"].values[0]
    fileContentg += v_fmt_general["HLEN"].format(float(Dv))
    rw = DT[DT["Champ"] == "HARM"]
    Dv = rw["dv"].values[0] 
    fileContentg += v_fmt_general["HARM"].format(Dv) + "\n"
    fileContentg += "@NOTES\n"
    rw = DT[DT["Champ"] == "NOTES"]
    Dv = rw["dv"].values[0]
    fileContentg += Dv + "\n\n"
    return fileContentg

    
v_fmt_soil = {"A" : "{:2.0f}", "SADAT" : "{:>6}", "SMHB" : "{:>6}", "SMPX" : "{:>6}", "SMKE" : "{:>6}",
               "SANAME" : "  {:<}", "SABL" : "{:6.0f}", "SADM" : "{:6.1f}", "SAOC" : "{:6.2f}",
               "SANI" : "{:6.2f}", "SAPHW" : "{:6.1f}", "SAPHB" : "{:6.0f}", "SAPX" : "{:6.1f}",
               "SAKE" : "{:6.1f}", "SASC" : "{:6.3f}"}


def writeBlockSoilAnalysis(dssat_tableName, dssat_tableId, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = ["@A", "SADAT", " SMHB", " SMPX", " SMKE", " SANAME"]
    fileContent += "\n"
    fileContent += "*SOIL ANALYSIS\n"
    fileContent += " ".join(siteColumnsHeader) + "\n"
    rw = DT[DT["Champ"] == "LNSA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["A"].format(float(Dv))
    rw = DT[DT["Champ"] == "SADAT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SADAT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMHB"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMHB"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMPX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMPX"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMKE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMKE"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SANAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SANAME"].format(Dv.strip()) + "\n"
    dssat_tableName = "dssat_x_soil_analysis_data"
    fileContent += writeBlockSoilAnalysisData(dssat_tableName, modelDictionary_Connection)
    return fileContent
    

def writeBlockSoilAnalysisData(dssat_tableName, Connection):
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, Connection)
    siteColumnsHeader = "@A  SABL  SADM  SAOC  SANI SAPHW SAPHB  SAPX  SAKE  SASC"
    fileContent = ""
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSA"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["A"].format(float(Dv))
    rw = DT[DT["Champ"] == "SABL"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SABL"].format(float(Dv))
    rw = DT[DT["Champ"] == "SADM"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SADM"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAOC"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAOC"].format(float(Dv))
    rw = DT[DT["Champ"] == "SANI"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SANI"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPHW"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPHW"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPHB"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPHB"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPX"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPX"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAKE"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAKE"].format(float(Dv))
    rw = DT[DT["Champ"] == "SASC"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f") + '\n'
    else: fileContent += v_fmt_soil["SASC"].format(float(Dv)) + "\n"
    return fileContent

def write_file(directory, filename, content):
    try:
        with open(os.path.join(directory, filename), "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing file {filename}: {e}")    
        
def process_chunk(*args):
    chunk, mi, md, directoryPath,pltfolder, dt = args
    idpoints = tuple([row["idPoint"] for row in chunk])
    if len(idpoints) == 1:
        idpoints = f"({idpoints[0]})"
    dataframes = []
    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}
    tectable = {}
    culticache = {}
    culticache2={}
    cache_treat = {}
    cache_tcult = {}
    cache_tfield = {}
    cache_option = {}
    cache_practice_pl = {}
    cache_practice_ferti = {}

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
    T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table]= 'dssat_weather_site'));"
    DT = pd.read_sql_query(T, ModelDictionary_Connection)
    climate_df = pd.read_sql_query(f"Select * From RaClimateD where idPoint in {idpoints}", MasterInput_Connection)
    coord_df = pd.read_sql_query(f"select * from Coordinates where idPoint in {idpoints}", MasterInput_Connection)      

    tg = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],[Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] like 'dssat_x_%'));"
    dtg = pd.read_sql_query(tg, ModelDictionary_Connection)
    treatg = general(dtg)    
    soilanalysis = writeBlockSoilAnalysis("dssat_x_soil_analysis", "dssat_x_exp_id", ModelDictionary_Connection)   
    for i, row in enumerate(chunk):
        print(f"Iteration {i}", flush=True)
        # Création du chemin du fichier
        try:
            #starti = time.perf_counter()
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            treatid =  row["idMangt"] 
            # cultivar 
            #culttart = time.perf_counter()
            cultivarconverter = dssatcultivarconverter.DssatCultivarConverter()
            crop = cultivarconverter.export(simPath, MasterInput_Connection, pltfolder, usmdir, treatid, culticache2, culticache)
            #print(f"cultivar export completed for {i} in {(time.perf_counter() - culttart)*1000:.2f} mseconds")
            
            # weather
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            #climstart = time.perf_counter()
            if climid not in weathertable:
                weatherconverter = dssatweatherconverter.DssatweatherConverter()
                r = weatherconverter.export(simPath, usmdir, DT, climate_df, coord_df)
                weathertable[climid] = r
            else:
                r = weathertable[climid]
                keys = list(r.keys())
                values = list(r.values())
                write_file(usmdir, keys[0], values[0])
                write_file(usmdir, keys[1], values[1])
            #print(f"climat export completed for {i} in {(time.perf_counter() - climstart)*1000:.2f} mseconds")
            
            # soil
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idsoil"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            soilid =  row["idsoil"] + "." + row["idMangt"]
            soilstart = time.perf_counter()
            if soilid not in soiltable:
                soilconverter = dssatsoilconverter.DssatSoilConverter()
                r = soilconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                soiltable[soilid] = r
            else:
                write_file(usmdir, "XX.SOL", soiltable[soilid])
            #print(f"soil export completed for {i} in {(time.perf_counter() - soilstart)*1000:.2f} mseconds")
            
            
            # xfile
            #xstart = time.perf_counter()
            #tecid =  row["idsoil"] + "." + row["idMangt"]  + row["idOption"]
            #if tecid not in tectable:
            
            optionid = f'{row["idOption"]} + "." + {row["StartYear"]}+ "." + {row["StartDay"]}+ "." + {row["EndYear"]}+ "." + {row["EndDay"]}'
            practice_id = f'{row["idMangt"]} + "." + {row["StartYear"]}+ "." + {row["StartDay"]}+ "." + {row["EndYear"]}+ "." + {row["EndDay"]}' 
            simPath = os.path.join(directoryPath, str(row["idsim"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            xconverter = dssatxconverter.DssatXConverter()
            xconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir, crop, treatg, treatid, cache_treat, cache_tcult, cache_tfield, soilanalysis, optionid, cache_option, practice_id, cache_practice_pl, cache_practice_ferti)     
            ##print(f"xstart export completed for {i} in {(time.perf_counter() - xstart)*1000:.2f} mseconds")

            # run dssat
            bs = os.path.join(Path(__file__).parent, "dssatrun.sh")
            try:
                #from time import time
                #exect = time()
                result = subprocess.run([bs, usmdir, directoryPath, str(dt)],executable=bs, capture_output=True, check=True, text=True, timeout=60)
                #print(f"Script stdout: {result.stdout}")
                #print(f"duration of dssat execution is  {time() - exect:.2f} seconds")
            except subprocess.CalledProcessError as e:
                print(f"❌ DSSAT run failed for {usmdir} with return code {e.returncode}", flush=True)
                print("STDOUT:\n", e.stdout)
                print("STDERR:\n", e.stderr)
                continue 
            except Exception as e:
                print(f"Error running dssat: {e}", flush=True)
                traceback.print_exc()
                continue
            summary = os.path.join(directoryPath, f"Summary_{str(row['idsim'])}.OUT")
            # if summary exists, process it
            if not os.path.exists(summary):
                print(f"Summary file {summary} not found.")
                continue
            df = transform(summary)
            dataframes.append(df)
            if dt==1: os.remove(summary)            
        except Exception as ex:
            print("Error during Running Dssat  :", ex)
            traceback.print_exc()
            continue
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
        traceback.print_exc()
        return

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

    
def chunk_data(data, chunk_size):    
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

    import uuid
    # create a random name
    result_name = str(uuid.uuid4()) + "_dssat"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_dssat"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")

    data = fetch_data_from_sqlite(mi)
    # Split data into chunks
    chunks = chunk_data(data, chunk_size=nthreads)
    args_list = [(chunk, mi, md, directoryPath, pltfolder, dt) for chunk in chunks]
    # Create a Pool of worker processes
    try:
        start = time.time()        
        processed_data_chunks = []
        """with concurrent.futures.ProcessPoolExecutor(max_workers=nthreads) as executor:
            processed_data_chunks = list(executor.map(process_chunk, args_list)) """

        with parallel_backend("loky", n_jobs=nthreads):
            processed_data_chunks = Parallel()(
                delayed(process_chunk)(*args) for args in args_list
            )
        if not processed_data_chunks:
            print("No data to process.")
            return
        processed_data = pd.concat(processed_data_chunks, ignore_index=True)
        processed_data.to_csv(os.path.join(directoryPath, f"{result_name}.csv"), index=False)
        print(f"DSSAT total time execution, {time.time()-start}")
    except Exception as ex:      
        print("Export not completed successfully!")
        traceback.print_exc()
        sys.exit(1)


v_fmt_general = {
    "PEOPLE": "{:<}",
    "ADDRESS": "{:<}",
    "SITE": "{:<}",
    "PAREA": "{:7.0f}",
    "PRNO": "{:6.0f}",
    "PLEN": "{:6.0f}",
    "PLDR": "{:6.0f}",
    "PLSP": "{:6.0f}",
    "PLAY": "{:6.0f}",
    "HAREA": "{:6.0f}",
    "HRNO": "{:6.0f}",
    "HLEN": "{:6.0f}",
    "HARM": "  {:<13s}",
    "NOTES": "{:<}"
}
if __name__ == "__main__":
    main()

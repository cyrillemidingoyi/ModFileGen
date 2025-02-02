from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import dssatweatherconverter, dssatcultivarconverter, dssatsoilconverter, dssatxconverter
import sys, subprocess, shutil
import concurrent.futures

import os
import datetime
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
from time import time
import traceback
from joblib import Parallel, delayed    

def write_file(directory, filename, content):
    with open(os.path.join(directory, filename), "w") as f:
        f.write(content)
        
def process_chunk(chunk, mi, md, directoryPath,pltfolder, dt):

    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
        
    for i, row in enumerate(chunk):
        print(f"Iteration {i}")
        # Création du chemin du fichier
        try:
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
             
            # cultivar 
            cultivarconverter = dssatcultivarconverter.DssatCultivarConverter()
            crop = cultivarconverter.export(simPath, MasterInput_Connection, pltfolder, usmdir)

            # weather
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            if climid not in weathertable:
                weatherconverter = dssatweatherconverter.DssatweatherConverter()
                r = weatherconverter.export(simPath,  ModelDictionary_Connection,MasterInput_Connection, usmdir)
                weathertable[climid] = r
            else:

                r = weathertable[climid]
                keys = list(r.keys())
                values = list(r.values())
                write_file(usmdir, keys[0], values[0])
                write_file(usmdir, keys[1], values[1])
            
            # soil
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idsoil"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            soilid =  row["idsoil"] + "." + row["idMangt"]
            if soilid not in soiltable:
                soilconverter = dssatsoilconverter.DssatSoilConverter()
                r = soilconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                soiltable[soilid] = r
            else:
                write_file(usmdir, "XX.SOL", soiltable[soilid])
            
            # xfile
            simPath = os.path.join(directoryPath, str(row["idsim"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            xconverter = dssatxconverter.DssatXConverter()
            xconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir, crop)

            # run dssat
            bs = os.path.join(Path(__file__).parent, "dssatrun.sh")
            subprocess.run(["bash", bs, usmdir, directoryPath, str(dt)])
            
        except Exception as ex:
            print("Error during Running Dssat  :", ex)
            traceback.print_exc()
            sys.exit(1)
            
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
    
    # Split data into chunks
    #def chunk_data(data, chunk_size):
        #return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    
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

    data = fetch_data_from_sqlite(mi)
    # Split data into chunks
    chunks = chunk_data(data, chunk_size=nthreads)
    # Create a Pool of worker processes
    try:
        start = time()
        with Pool(processes=nthreads) as pool:
            # Apply the processing function to each chunk in parallel
            processed_data_chunks = pool.starmap(process_chunk,[(chunk, mi, md, directoryPath, pltfolder, dt) for chunk in chunks])  
            #Parallel(n_jobs=nthreads)(delayed(process_chunk)(chunk, mi, md, directoryPath, pltfolder) for chunk in chunks)

        #with concurrent.futures.ThreadPoolExecutor(nthreads) as executor: # to test
            #executor.map(process_chunk, [(chunk, mi, md, directoryPath, pltfolder, dt) for chunk in chunks])        
        
        print(f"total time, {time()-start}")
    except Exception as ex:      
        print("Export completed successfully!")

if __name__ == "__main__":
    main()
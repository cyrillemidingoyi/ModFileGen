"""
DSSAT Converter - Parallel processing with memory optimization

MEMORY MANAGEMENT:
- To reduce OOM errors, reduce 'nthreads' (fewer parallel workers)
- Increase 'parts' to create smaller chunks per worker
- Worker caches are auto-cleared every 50 rows
- Results are written directly to disk (not held in memory)

CONFIGURATION (in GlobalVariables):
- nthreads: Number of parallel worker processes
- parts: Number of chunks per thread (total chunks = nthreads * parts)
"""

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
from time import time
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
    

def write_file(directory, filename, content):
    try:
        with open(os.path.join(directory, filename), "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing file {filename}: {e}")    
        
def process_chunk(*args):
    chunk, mi, md, directoryPath,pltfolder, dt = args
    dataframes = []
    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}
    
    # Clear caches periodically to prevent memory buildup
    CACHE_CLEAR_INTERVAL = 50000

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
        
    for i, row in enumerate(chunk):
        # Periodically clear caches to free memory
        if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
            print(f"🗑️ Clearing caches at row {i} to free memory", flush=True)
            weathertable.clear()
            soiltable.clear()
            # Also trigger garbage collection
            import gc
            gc.collect()
        print(f"Iteration {i}", flush=True)
        # Création du chemin du fichier
        try:
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
             
            # cultivar 
            cultivarconverter = dssatcultivarconverter.DssatCultivarConverter()
            crop = cultivarconverter.export(simPath, MasterInput_Connection, pltfolder, usmdir)
            del cultivarconverter  # Free converter

            # weather
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            if climid not in weathertable:
                weatherconverter = dssatweatherconverter.DssatweatherConverter()
                r = weatherconverter.export(simPath,  ModelDictionary_Connection,MasterInput_Connection, usmdir)
                weathertable[climid] = r
                del weatherconverter  # Free converter
            else:
                ST = simPath.split(os.sep)
                Mngt = ST[-1][:4]
                Year = ST[-2]
                
                r = weathertable[climid]
                keys = list(r.keys())
                values = list(r.values())
                write_file(usmdir, Mngt.upper() + Year[2:4] + "01" + ".WTH", values[0])
                write_file(usmdir, Mngt.upper() + str(int(Year)+1)[2:4] + "01" + ".WTH", values[1])
                #write_file(usmdir, keys[1], values[1])
            
            # soil
            simPath = os.path.join(directoryPath, str(row["idsim"]), str(row["idsoil"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            soilid =  row["idsoil"] + "." + row["idMangt"]
            if soilid not in soiltable:
                soilconverter = dssatsoilconverter.DssatSoilConverter()
                r = soilconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                soiltable[soilid] = r
                del soilconverter  # Free converter
            else:
                write_file(usmdir, "XX.SOL", soiltable[soilid])
            
            # xfile
            simPath = os.path.join(directoryPath, str(row["idsim"]),str(row["idMangt"]))
            usmdir = os.path.join(directoryPath, str(row["idsim"])) 
            xconverter = dssatxconverter.DssatXConverter()
            xconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir, crop, dt)
            del xconverter  # Free converter

            # run dssat
            bs = os.path.join(Path(__file__).parent, "dssatrun.sh")
            try:
                result = subprocess.run(["bash", bs, usmdir, directoryPath, str(dt)],
                                        #capture_output=True,
                                        stdout=subprocess.DEVNULL,
                                        stderr=sys.stderr, 
                                        check=True, 
                                        text=True, 
                                        timeout=300)
            except subprocess.TimeoutExpired as e:
                print(f"⏰ DSSAT run timed out for {usmdir}. Killing... {e}", file=sys.stderr, flush=True)
                # Forcefully terminate the process if it hangs
                #result.kill()  # Python 3.9+
                raise 
            except subprocess.CalledProcessError as e:
                print(f"❌ DSSAT run failed for {usmdir} with return code {e.returncode}", file=sys.stderr, flush=True)
                #print("STDOUT:\n", e.stdout, file=sys.stdout, flush=True)
                #if e.stderr: print("STDERR:\n", e.stderr, file=sys.stderr, flush=True)
                raise 
            except Exception as e:
                print(f"Error running dssat: {e}", file=sys.stderr, flush=True)
                traceback.print_exc()
                raise
            summary = os.path.join(directoryPath, f"Summary_{str(row['idsim'])}.OUT")
            # if summary exists, process it
            if not os.path.exists(summary):
                print(f"Summary file {summary} not found.")
                continue
            df = transform(summary)
            dataframes.append(df)
            if dt==1: os.remove(summary)
            del df  # Free df after appending
        except Exception as ex:
            print("Error during Running Dssat  :", ex, file=sys.stderr, flush=True)
            traceback.print_exc()
            continue
    if not dataframes:
        print("No dataframes to concatenate.")
        ModelDictionary_Connection.close()
        MasterInput_Connection.close()
        # Clear all caches
        weathertable.clear()
        soiltable.clear()
        return pd.DataFrame()
    # close connections
    ModelDictionary_Connection.close()
    MasterInput_Connection.close()
    
    # Clear all caches before concatenation
    weathertable.clear()
    soiltable.clear()
    
    batch_size = 1000
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

    
def chunk_data(data, parts, chunk_size):    
    k, m = divmod(len(data), parts * chunk_size)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(chunk_size * parts)]
    return sublists

def main():
    mi= GlobalVariables["dbMasterInput"]
    md = GlobalVariables["dbModelsDictionary"]
    directoryPath = GlobalVariables["directorypath"]
    pltfolder = GlobalVariables["pltfolder"]
    nthreads = GlobalVariables["nthreads"]
    dt = GlobalVariables["dt"]
    parts = GlobalVariables["parts"]
    export(mi, md)

    import uuid
    # create a random name
    result_name = str(uuid.uuid4()) + "_dssat"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_dssat"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")

    data = fetch_data_from_sqlite(mi)
    print(f"📊 Total simulations to process: {len(data)}", flush=True)
    
    # Split data into chunks
    chunks = chunk_data(data, parts, chunk_size=nthreads)
    del data  # Free original data list after chunking
    
    args_list = [(chunk, mi, md, directoryPath, pltfolder, dt) for chunk in chunks]
    del chunks  # Free chunks list after creating args_list
    
    # Create a Pool of worker processes
    try:
        start = time()
        # Use joblib Parallel with loky backend, write results directly to final file
        print(f"Processing {len(args_list)} chunks...", flush=True)
        
        write_header = True
        total_chunks_written = 0
        
        with parallel_backend('loky', n_jobs=nthreads):
            # Process in small batches to avoid holding all results in memory
            batch_size = max(1, nthreads)  # Process nthreads chunks at a time
            
            for batch_idx in range(0, len(args_list), batch_size):
                batch_args = args_list[batch_idx:batch_idx + batch_size]
                
                # Process this batch
                batch_results = Parallel()(
                    delayed(process_chunk)(*args) for args in batch_args
                )
                
                # Write each result directly to final file
                for i, chunk_df in enumerate(batch_results):
                    if not chunk_df.empty:
                        # Append to result file (write header only once)
                        chunk_df.to_csv(result_path, mode='a', header=write_header, index=False)
                        write_header = False  # Only write header for first chunk
                        total_chunks_written += 1
                        print(f"✅ Chunk {batch_idx + i + 1}/{len(args_list)}: {len(chunk_df)} rows written", flush=True)
                    
                    # Free memory immediately
                    del chunk_df
                
                # Free batch results
                del batch_results
        
        if total_chunks_written == 0:
            print("No data to process.", flush=True)
            return
        print(f"✅ Results saved to {result_path}", flush=True)
        print(f"DSSAT total time, {time()-start}", flush=True)
    except Exception as ex:      
        print("Export not completed successfully!")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

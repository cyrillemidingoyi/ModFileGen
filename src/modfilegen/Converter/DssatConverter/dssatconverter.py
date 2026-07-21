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
import gc
import calendar

DSSAT_DAILY_OUTPUT_TABLE = "DssatDailyOutput"
DSSAT_DAILY_FILES = {
    "ET": "ET.OUT",
    "PlantGro": "PlantGro.OUT",
    "PlantN": "PlantN.OUT",
    "SoilOrg": "SoilOrg.OUT",
    "SoilWat": "SoilWat.OUT",
    "Weather": "Weather.OUT",
}


class _Stop99Error(RuntimeError):
    """Raised when DSSAT reports a STOP99 error."""
    pass


def extract_corrected_doy(date_col, ys):
    year = (date_col // 1000).astype('float')
    doy = (date_col % 1000).astype('float')
    correction = np.where(
        year > ys,
        np.where([calendar.isleap(int(y)) if not np.isnan(y) else False for y in year],
                 366, 365),
        0
    )
    return doy + correction


def get_coord(d):
    res = re.findall("([-]?\d+[.]?\d+)[_]", d)
    lat = float(res[0])
    lon = float(res[1])
    year = int(float(res[2]))
    return {'lon': lon, 'lat': lat, 'year': year}


def transform(fil, dt):
    with open(fil, "r") as fil_:
        FILE = fil_.readlines()
    #d_name = os.path.dirname(fil).split(os.path.sep)[-1]
    d_name = Path(fil).stem[len("Summary_"):]
    if dt == 1:
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

    if dt == 1:
        df['lon'] = c['lon']
        df['lat'] = c['lat']
        df['time'] = int(c['year'])

    return df


def read_dssat_daily_file(file_path, source):
    """Read one DSSAT daily output and prefix non-date columns by source."""
    with open(file_path, "r") as output_stream:
        lines = output_stream.readlines()

    header_index = next(
        (index for index, line in enumerate(lines) if line.lstrip().startswith("@YEAR")),
        None,
    )
    if header_index is None:
        raise ValueError(f"No @YEAR header found in {file_path}")

    columns = lines[header_index].lstrip()[1:].split()
    data = pd.read_csv(
        file_path,
        sep=r"\s+",
        skiprows=header_index + 1,
        names=columns,
        header=None,
        comment="*",
    )
    data["YEAR"] = pd.to_numeric(data["YEAR"], errors="coerce")
    data["DOY"] = pd.to_numeric(data["DOY"], errors="coerce")
    data = data.dropna(subset=["YEAR", "DOY"])
    data["YEAR"] = data["YEAR"].astype(int)
    data["DOY"] = data["DOY"].astype(int)
    data = data.replace(-99, np.nan).replace(-99.0, np.nan)
    data = data.rename(
        columns={
            column: f"{source}_{column}"
            for column in data.columns
            if column not in ("YEAR", "DOY")
        }
    )
    return data


def create_df_daily(output_files, idsim):
    """Merge DSSAT daily modules on calendar year and day of year."""
    daily = None
    for source, file_path in output_files.items():
        module_data = read_dssat_daily_file(file_path, source)
        daily = (
            module_data
            if daily is None
            else daily.merge(module_data, on=["YEAR", "DOY"], how="outer")
        )
    if daily is None:
        return pd.DataFrame()
    daily = daily.sort_values(["YEAR", "DOY"]).reset_index(drop=True)
    daily.insert(0, "Idsim", str(idsim))
    daily.insert(0, "Model", "Dssat")
    return daily


def append_dataframe_csv(dataframe, csv_path):
    """Append while keeping a valid CSV when DSSAT modules change columns."""
    if not os.path.exists(csv_path):
        dataframe.to_csv(csv_path, index=False)
        return

    existing_columns = pd.read_csv(csv_path, nrows=0).columns.tolist()
    new_columns = [column for column in dataframe.columns if column not in existing_columns]
    all_columns = existing_columns + new_columns

    if new_columns:
        existing_data = pd.read_csv(csv_path)
        existing_data = existing_data.reindex(columns=all_columns)
        existing_data.to_csv(csv_path, index=False)
        del existing_data

    dataframe.reindex(columns=all_columns).to_csv(
        csv_path,
        mode="a",
        header=False,
        index=False,
    )


def align_sqlite_table(connection, table_name, dataframe):
    """Add newly encountered DSSAT columns and align a frame to the table."""
    table_info = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    existing_columns = [row[1] for row in table_info]

    for column in dataframe.columns:
        if column in existing_columns:
            continue
        sql_type = "REAL" if pd.api.types.is_numeric_dtype(dataframe[column]) else "TEXT"
        escaped_column = column.replace('"', '""')
        connection.execute(
            f'ALTER TABLE "{table_name}" ADD COLUMN "{escaped_column}" {sql_type}'
        )
        existing_columns.append(column)

    return dataframe.reindex(columns=existing_columns)
    

def write_file(directory, filename, content):
    try:
        with open(os.path.join(directory, filename), "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing file {filename}: {e}")    
        
def process_chunk(*args):
    import psutil
    import gc
    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss / 1024**2  # MB
    chunk, mi, md, directoryPath,pltfolder, dt, thirdyear, tempDir, idx, dailyoutput, dssat_version = args
    tmp_csv = os.path.join(directoryPath, f"chunk_{idx}.csv")
    tmp_daily_csv = os.path.join(directoryPath, f"chunk_{idx}_dssat_daily.csv")
    for stale_file in (tmp_csv, tmp_daily_csv):
        if os.path.exists(stale_file):
            os.remove(stale_file)
    #dataframes = []
    # Apply series of functions to each row in the chunk
    weathertable = {}
    soiltable = {}
    
    # Clear caches periodically to prevent memory buildup
    CACHE_CLEAR_INTERVAL = 50000

    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
        
    for i, row in enumerate(chunk):
        write_header = not os.path.exists(tmp_csv)
        # Periodically clear caches to free memory
        if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
            print(f" Clearing caches at row {i} to free memory", flush=True)
            weathertable.clear()
            soiltable.clear()
            # Also trigger garbage collection
            import gc
            gc.collect()
        print(f"Iteration {i}", flush=True)
        # Création du chemin du fichier
        try:
            simPath = os.path.join(tempDir, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            usmdir = os.path.join(tempDir, str(row["idsim"])) 
             
            # cultivar 
            cultivarconverter = dssatcultivarconverter.DssatCultivarConverter()
            crop = cultivarconverter.export(simPath, MasterInput_Connection, pltfolder, usmdir, dssat_version)
            del cultivarconverter  # Free converter

            # weather
            climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
            if climid not in weathertable:
                weatherconverter = dssatweatherconverter.DssatweatherConverter()
                r = weatherconverter.export(simPath,  ModelDictionary_Connection,MasterInput_Connection, usmdir, thirdyear)
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
                if thirdyear == 1:
                    write_file(usmdir, Mngt.upper() + str(int(Year)+2)[2:4] + "01" + ".WTH", values[2])
                #write_file(usmdir, keys[1], values[1])
            
            # soil
            simPath = os.path.join(tempDir, str(row["idsim"]), str(row["idsoil"]), str(row["idPoint"]), str(row["StartYear"]),str(row["idMangt"]))
            soilid =  row["idsoil"] + "." + row["idMangt"]
            if soilid not in soiltable:
                soilconverter = dssatsoilconverter.DssatSoilConverter()
                r = soilconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                soiltable[soilid] = r
                del soilconverter  # Free converter
            else:
                write_file(usmdir, "XX.SOL", soiltable[soilid])
            
            # xfile
            simPath = os.path.join(tempDir, str(row["idsim"]),str(row["idMangt"])) 
            xconverter = dssatxconverter.DssatXConverter()
            xconverter.export(
                simPath,
                ModelDictionary_Connection,
                MasterInput_Connection,
                usmdir,
                crop,
                dt,
                dssat_version,
            )
            del xconverter  # Free converter

            # run dssat
            bs = os.path.join(Path(__file__).parent, "dssatrun.sh")
            try:
                result = subprocess.run(["bash", bs, usmdir, tempDir, str(dt), str(dailyoutput), dssat_version],
                                        stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE,   # capture to detect STOP99
                                        check=False,              # manual check below
                                        text=True,
                                        timeout=300)
                # Always forward stderr to cluster error file
                if result.stderr:
                    print(result.stderr, file=sys.stderr, end="", flush=True)
                # Detect STOP99 by text in stderr or return code 99
                if "STOP 99" in (result.stderr or "").upper() or result.returncode == 99:
                    print(f"🚨 STOP99 | idsim={row['idsim']} | rc={result.returncode}",
                          file=sys.stderr, flush=True)
                    raise _Stop99Error(f"STOP99 in simulation {row['idsim']}")
                if result.returncode != 0:
                    raise subprocess.CalledProcessError(
                        result.returncode, result.args, None, result.stderr)
            except subprocess.TimeoutExpired as e:
                print(f"⏰ DSSAT run timed out for {usmdir}. Killing... {e}", file=sys.stderr, flush=True)
                raise
            except _Stop99Error:
                raise
            except subprocess.CalledProcessError as e:
                print(f"❌ DSSAT run failed for {usmdir} with return code {e.returncode}",
                      file=sys.stderr, flush=True)
                raise
            except Exception as e:
                print(f"Error running dssat: {e}", file=sys.stderr, flush=True)
                traceback.print_exc()
                raise
            summary = os.path.join(tempDir, f"Summary_{str(row['idsim'])}.OUT")
            # if summary exists, process it
            if not os.path.exists(summary):
                print(f"Summary file {summary} not found.")
                continue
            df = transform(summary, dt)
            df.to_csv(tmp_csv, mode='a', header=write_header, index=False)
            if dailyoutput == 1:
                expected_output_files = {
                    source: os.path.join(tempDir, f"{source}_{row['idsim']}.out")
                    for source in DSSAT_DAILY_FILES
                }
                missing = [
                    path for path in expected_output_files.values()
                    if not os.path.exists(path)
                ]
                if missing:
                    print(
                        f"Warning: DSSAT daily outputs missing for {row['idsim']}: "
                        + ", ".join(os.path.basename(path) for path in missing),
                        flush=True,
                    )
                output_files = {
                    source: path
                    for source, path in expected_output_files.items()
                    if os.path.exists(path)
                }
                if output_files:
                    daily_df = create_df_daily(output_files, row["idsim"])
                    append_dataframe_csv(daily_df, tmp_daily_csv)
                    del daily_df
                    for output_file in output_files.values():
                        os.remove(output_file)
            #dataframes.append(df)
            os.remove(summary)
            del df  # Free df after appending
            gc.collect()  # Force garbage collection after each iteration
        except _Stop99Error:
            raise  # Stop chunk processing — let it propagate up
        except Exception as ex:
            print(f"Error during Running Dssat [{row.get('idsim', '?')}]: {ex}",
                  file=sys.stderr, flush=True)
            traceback.print_exc()
            continue
    if not os.path.exists(tmp_csv):
        print("No dataframes to concatenate.")
        ModelDictionary_Connection.close()
        MasterInput_Connection.close()
        # Clear all caches
        weathertable.clear()
        soiltable.clear()
        return None
    # close connections
    ModelDictionary_Connection.close()
    MasterInput_Connection.close()
    
    # Clear all caches before concatenation
    weathertable.clear()
    soiltable.clear()
    gc.collect()
    
    '''batch_size = 1000
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
    
    del dataframes  # Free the list'''
    mem_after = proc.memory_info().rss / 1024**2
    print(f"Worker {os.getpid()} - mémoire: {mem_before:.0f}MB → {mem_after:.0f}MB (delta: {mem_after-mem_before:.0f}MB)", flush=True)
    return tmp_csv, (tmp_daily_csv if os.path.exists(tmp_daily_csv) else None)
            
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

def process_chunk_safe(idx, args):
    try:
        chunk_df = process_chunk(*args)
        return idx, chunk_df, None
    except Exception:
        return idx, None, traceback.format_exc()

def main():
    mi= GlobalVariables["dbMasterInput"]
    md = GlobalVariables["dbModelsDictionary"]
    directoryPath = GlobalVariables["directorypath"] # correspond to inter
    pltfolder = GlobalVariables["pltfolder"]
    nthreads = GlobalVariables["nthreads"]
    dt = GlobalVariables["dt"]
    parts = GlobalVariables["parts"]
    thirdyear = int(GlobalVariables["thirdyear"])
    tempDir = GlobalVariables.get("tempDir")
    dailyoutput = int(GlobalVariables.get("dailyoutput", 0))
    dssat_version = GlobalVariables.get("dssat", "v47")
    if dssat_version not in dssatcultivarconverter.DSSAT_CULTIVAR_SUFFIXES:
        raise ValueError(
            f"Unsupported DSSAT version {dssat_version!r}; expected v47 or v48"
        )
    export(mi, md)

    import uuid
    os.makedirs(directoryPath, exist_ok=True)
    os.makedirs(tempDir, exist_ok=True)
    # create a random name
    result_name = str(uuid.uuid4()) + "_dssat"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_dssat"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")

    data = fetch_data_from_sqlite(mi)
    n_simulations = len(data)
    print(f"📊 Total simulations to process: {n_simulations}", flush=True)
    
    # Split data into chunks
    chunks = chunk_data(data, parts, chunk_size=nthreads)
    del data  # Free original data list after chunking
    
    args_list = [(chunk, mi, md, directoryPath, pltfolder, dt, thirdyear, tempDir, idx, dailyoutput,dssat_version) for idx, chunk in enumerate(chunks)]
    del chunks  # Free chunks list after creating args_list
    
    try:
        start = time()
        print("dssat version: ", dssat_version)
        print(f"Processing {len(args_list)} chunks...", flush=True)
        
        write_header = True
        total_chunks_written = 0
        daily_table_initialized = False
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

        for idx, chunk_paths, error in results:
            if error is not None:
                print(f"❌ Chunk {idx + 1}/{len(args_list)} failed:\n{error}", flush=True)
                continue

            if chunk_paths is None:
                print(f"❌ Chunk {idx + 1}/{len(args_list)} failed:\n{error}", flush=True)
                continue

            tmp_path, tmp_daily_path = chunk_paths
            if not os.path.exists(tmp_path):
                print(f"❌ Summary output is missing for chunk {idx + 1}", flush=True)
                continue
                
            if os.path.exists(tmp_path):
                df = pd.read_csv(tmp_path)
                df.to_csv(result_path, mode="a", header=write_header, index=False)
                write_header = False
                total_chunks_written += 1

                print(
                        f"✅ Chunk {idx + 1}/{len(args_list)}: "
                        f"{len(df)} rows written",
                        flush=True
                )

                del df
                gc.collect()

            if dailyoutput == 1 and tmp_daily_path and os.path.exists(tmp_daily_path):
                with sqlite3.connect(mi) as daily_conn:
                    for daily_chunk in pd.read_csv(tmp_daily_path, chunksize=50000):
                        if daily_table_initialized:
                            daily_chunk = align_sqlite_table(
                                daily_conn,
                                DSSAT_DAILY_OUTPUT_TABLE,
                                daily_chunk,
                            )
                            daily_chunk.to_sql(
                                DSSAT_DAILY_OUTPUT_TABLE,
                                daily_conn,
                                if_exists="append",
                                index=False,
                            )
                        else:
                            daily_chunk.to_sql(
                                DSSAT_DAILY_OUTPUT_TABLE,
                                daily_conn,
                                if_exists="replace",
                                index=False,
                            )
                            daily_table_initialized = True
                os.remove(tmp_daily_path)
            
            if total_chunks_written == 0:
                print("No data to process.")
                return

        print(f"✅ Results saved to {result_path}", flush=True)
        if dailyoutput == 1:
            if daily_table_initialized:
                with sqlite3.connect(mi) as daily_conn:
                    daily_conn.execute(
                        f'CREATE INDEX IF NOT EXISTS "idx_{DSSAT_DAILY_OUTPUT_TABLE}_idsim_date" '
                        f'ON "{DSSAT_DAILY_OUTPUT_TABLE}" ("Idsim", "YEAR", "DOY")'
                    )
                print(
                    f"✅ Daily results inserted into {DSSAT_DAILY_OUTPUT_TABLE}.",
                    flush=True,
                )
            else:
                print("Warning: no DSSAT daily results were imported.", flush=True)
        print(f"DSSAT total time: {time()-start:.2f}s", flush=True)

        if dt == 0:
            summary_cols = ["Model", "Idsim", "Texte", "Planting", "Emergence", "Ant", "Mat",
                            "Biom_ma", "Yield", "GNumber", "MaxLai", "Nleac", "SoilN",
                            "CroN_ma", "CumE", "Transp"]
            df_result = pd.read_csv(result_path, usecols=lambda c: c in summary_cols + ["ys"] or c == "Idsim")
            for col in summary_cols:
                if col not in df_result.columns:
                    df_result[col] = None
            df_result["ys"] = (df_result["Planting"].astype(str).str[:4]).astype(int)
            df_result = df_result.replace(-99, np.nan)
            for col in ["Planting", "Emergence", "Ant", "Mat"]:
                df_result[col] = extract_corrected_doy(df_result[col], df_result["ys"])
            for col in ["Yield", "Biom_ma"]:
                df_result[col] = df_result[col] / 1000
            cols_to_clean = ["Planting", "Emergence", "Ant", "Mat", "Biom_ma", "Yield", "GNumber",
                             "MaxLai", "Nleac", "SoilN", "CroN_ma", "CumE", "Transp"]
            df_result[cols_to_clean] = df_result[cols_to_clean].mask(df_result[cols_to_clean] < 0, np.nan)
            df_result = df_result[summary_cols]
            _conn = sqlite3.connect(mi)
            _conn.execute("DELETE FROM SummaryOutput WHERE Model = 'Dssat'")
            _conn.commit()
            df_result.to_sql("SummaryOutput", _conn, if_exists="append", index=False)
            _conn.commit()
            _conn.close()
            print(f"✅ {len(df_result)} rows inserted into SummaryOutput.", flush=True)
            del df_result

    except Exception as ex:      
        print("Export not completed successfully!")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

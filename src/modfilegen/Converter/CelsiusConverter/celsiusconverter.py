"""
Celsius Converter - Parallel processing with memory optimization

MEMORY MANAGEMENT:
- To reduce OOM errors, reduce 'nthreads' (fewer parallel workers)
- Increase 'parts' to create smaller chunks per worker
- Results are written directly to database (not held in memory)

CONFIGURATION (in GlobalVariables):
- nthreads: Number of parallel worker processes
- parts: Number of chunks per thread (total chunks = nthreads * parts)
"""

import os
import sqlite3
import pandas as pd
import shutil
from pathlib import Path
from time import time
import subprocess
from modfilegen import GlobalVariables
from modfilegen.converter import Converter
import uuid
import sys
import traceback
import concurrent.futures
from joblib import Parallel, delayed



def create_idJourClim(df):
    return df['IdDClim'].astype(str) + '.' + df['annee'].astype(str) + '.' + df['jda'].astype(str)

def process_chunk(*args):
    
    chunk, masterInput, DB_MD, DB_Celsius, directoryPath, dt, ori_mi = args
    
    try:

        quoted = ", ".join(f"'{row['idsim']}'" for row in chunk)
        idsims = f"({quoted})"
        print(f"Number of idsims", len(idsims), flush=True)
        print(f"creating new directory for process", flush=True)
        tmp_base = "/dev/shm" if os.path.isdir("/dev/shm") else directoryPath
        new_dir = os.path.join(tmp_base, f"proc_{str(uuid.uuid4())}")
        while os.path.exists(new_dir):
            new_dir = os.path.join(tmp_base, f"proc_{str(uuid.uuid4())}")
        Path(new_dir).mkdir(parents=True, exist_ok=True)
        new_db_cel = os.path.join(new_dir, "CelsiusV3nov17_dataArise.db")
        new_db_mi = os.path.join(new_dir, "MasterInput.db")
        shutil.copy(DB_Celsius, new_db_cel)
        shutil.copy(ori_mi, new_db_mi)
        
        # connect to the masterInput database
        with sqlite3.connect(masterInput) as conn, sqlite3.connect(new_db_mi) as new_conn, sqlite3.connect(new_db_cel) as new_conn_cel:
            cursor = conn.cursor()
            cursor_dst = new_conn.cursor()
            print("Copy SimulationList", flush=True)            
            query_sim = f"SELECT * FROM SimUnitList WHERE idsim IN {idsims}"
            sim_df = pd.read_sql(query_sim, conn)
            sim_df.to_sql('SimUnitList', new_conn, if_exists='replace', index=False)
            print("SimulationList copied", flush=True)
            
            
            print( "Start transfert of climate data from MI to Cel", flush=True)
            idPoints = tuple(sim_df["idPoint"].unique())
            '''if len(idPoints) == 1:
                idPoints = f"({idPoints[0]})"   '''
            if len(idPoints) == 0:
                raise ValueError("No idPoints found in sim_df['idPoint'].")            
            print( "Start transfert of climate data from MI to Cel", flush=True)
            print(f"Number of idPoints", len(idPoints), flush=True)
            placeholders = ",".join("?" * len(idPoints))
            query = f"""
                    SELECT idPoint, year, DOY, Nmonth, NdayM, srad, tmax, tmin, tmoy, rain, Etppm 
                    FROM RAclimateD 
                    WHERE idPoint IN ({placeholders})
                """
            first = True
            for dfc in pd.read_sql(query, conn, params=idPoints, chunksize=100_000):  
                #df_clim_MI = pd.read_sql(query, conn)
                dfc = dfc.rename(columns={"idPoint":"IdDClim", "year":"annee", "DOY":"jda", "Nmonth":"mois", "NdayM":"jour", "srad":"rg", "rain":"plu", "Etppm":"Etp"})
                dfc['idjourclim'] = create_idJourClim(dfc)
                #df_sorted = df.sort_values(by='idjourclim')
                dfc = dfc[['IdDClim', 'idjourclim', 'annee',"jda","mois","jour","tmax","tmin","tmoy","rg","plu",'Etp' ]]
                dfc.to_sql('Dweather', new_conn_cel, if_exists='replace' if first else 'append', index=False)
                first = False
            create_index_query_idDclim = "CREATE INDEX IF NOT EXISTS idx_idDclim ON Dweather (IdDClim, annee);"
            cursor_cel = new_conn_cel.cursor()
            cursor_cel.execute(create_index_query_idDclim)
            new_conn_cel.commit()
            print( "transfert of climate data from MI to Cel done")    
                
            # copier les autres tables CropManagement, soil et SoilLayers
            print("copy CropManagement, Soil and SoilLayers", flush=True)
            tables_to_copy = ["CropManagement", "Soil", "SoilLayers", "Coordinates", "InitialConditions"]
            for table in tables_to_copy:
                # remove the content of the table in the new database
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                create_table_sql = cursor.fetchone()[0]
                cursor_dst.execute(f"DROP TABLE IF EXISTS {table}")
                cursor_dst.execute(create_table_sql)  # Recrée la structure exacte de la table
                cursor.execute(f"SELECT * FROM {table}")
                columns = [desc[0] for desc in cursor.description]
                col_names = ", ".join(columns)
                placeholders = ",".join("?" * len(columns))               
                query2 = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                batch_size = 1000
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    cursor_dst.executemany(query2, [tuple(row) for row in rows])
                new_conn.commit()

        # use subprocess to run the celsius model with the command "celsius convert -m celsius -t ${THREADS} -dbMasterInput ${new_db_mi} -dbModelsDictionary ${DB_MD} -dbCelsius ${new_db_cel}"
        try:
            print("convert celsius", flush=True)
            result = subprocess.run(["datamill", "convert", "-m", "celsius", "-dbMasterInput", new_db_mi, "-dbModelsDictionary", DB_MD, "-dbCelsius", new_db_cel],
                            check=True,
                            text=True)
            print("✅ Celsius conversion completed successfully!", flush=True)

            print("run celsius")
            subprocess.run(["celsius", "convert", "-m", "celsius", "-dbCelsius", new_db_cel], check=True,
                                text=True)
            print("✅ Celsius run completed successfully!", flush=True)
            # Get in a dataframe the table "OutputSyn" from the new_db_cel database
            new_conn_cel = sqlite3.connect(new_db_cel)
            df = pd.read_sql_query("SELECT * FROM OutputSynt", new_conn_cel)
            new_conn_cel.close()
            # if df is empty return empty dataframe
            if df.empty:
                if dt == 1: shutil.rmtree(new_dir)
                return pd.DataFrame()
            if dt == 1: shutil.rmtree(new_dir)
            return df
        except subprocess.CalledProcessError as e:
            print("❌ Error during Celsius run:", flush=True)
            print(f"Exception type: {type(e).__name__}", flush=True)
            print(f"Exception message: {str(e)}", flush=True)
            print(f"STDOUT:\n{e.stdout}", flush=True)
            print(f"STDERR:\n{e.stderr}", flush=True)
            traceback.print_exc()
        except Exception as e:
            print(f"Error running celsius: {e}", flush=True)
            traceback.print_exc()
    except Exception as e:
        print("❌ Error in process_chunk:", flush=True)
        print(f"Exception type: {type(e).__name__}", flush=True)
        print(f"Exception message: {str(e)}", flush=True)
        traceback.print_exc()
        raise RuntimeError(f"process_chunk failed:\n{traceback.format_exc()}") from e

def fetch_data_from_sqlite(masterInput):
    conn = sqlite3.connect(masterInput)
    df = pd.read_sql_query(f"SELECT * FROM SimUnitList", conn)
    rows = df.to_dict(orient='records')
    conn.close()
    return rows
    
    
def chunk_data(data, split, chunk_size):    # values, num_sublists
    n = split * chunk_size
    k, m = divmod(len(data), n)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]
    return [s for s in sublists if s]  # drop empty chunks

def main():
    mi= GlobalVariables["dbMasterInput"]
    md = GlobalVariables["dbModelsDictionary"]
    celsius = GlobalVariables["dbCelsius"]
    directoryPath = GlobalVariables["directorypath"]
    nthreads = GlobalVariables["nthreads"]
    dt = GlobalVariables["dt"]
    ori_mi = GlobalVariables["ori_MI"]
    split = GlobalVariables["parts"]
    dailyoutput = GlobalVariables.get("dailyoutput", 0)
    
    data = fetch_data_from_sqlite(mi)
    print(f"📊 Total simulations to process: {len(data)}", flush=True)
    
    # Split data into chunks
    chunks = chunk_data(data, split, chunk_size=nthreads)
    del data  # Free original data list after chunking
    
    args_list = [(chunk, mi, md, celsius, directoryPath, dt, ori_mi) for chunk in chunks]
    del chunks  # Free chunks list after creating args_list
    
    # Use joblib Parallel with loky backend, write results directly to database
    try:
        start = time()
        print(f"Processing {len(args_list)} chunks...", flush=True)
        
        # Clear OutputSynt table once at the beginning
        with sqlite3.connect(celsius) as conn:
            conn.execute("DELETE FROM OutputSynt")
            conn.commit()
        
        # Clear SummaryOutput for Celsius if dailyoutput is enabled
        if dailyoutput == 1:
            with sqlite3.connect(mi) as conn:
                conn.execute("DELETE FROM SummaryOutput WHERE Model = 'Celsius'")
                conn.commit()
        
        total_rows = 0
        total_chunks = len(args_list)

        # Stream results as they complete — workers stay busy the whole time
        results = Parallel(n_jobs=nthreads, backend="loky", return_as="generator_unordered")(
            delayed(process_chunk)(*args) for args in args_list
        )

        for chunk_idx, chunk_df in enumerate(results):
            if chunk_df is not None and not chunk_df.empty:
                with sqlite3.connect(celsius) as conn:
                    chunk_df.to_sql("OutputSynt", conn, if_exists='append', index=False)
                    conn.commit()
                
                if dailyoutput == 1:
                    # Map OutputSynt columns to SummaryOutput columns
                    column_mapping = {
                        "idsim": "Idsim",
                        "iplt": "Planting",
                        "JulPheno1_1": "Emergence",
                        "JulPheno1_4": "Ant",
                        "JulPheno1_6": "Mat",
                        "Biom(nrec)": "Biom_ma",
                        "Grain(nrec)": "Yield",
                        "LAI": "MaxLai",
                        "SigmaSimEsol": "CumE",
                        "Ngrain": "GNumber",
                        "stockNsol": "SoilN",
                        "SigmaCultEsol": "Transp"
                    }
                    
                    summary_df = chunk_df.rename(columns=column_mapping)
                    summary_df["Model"] = "Celsius"
                    summary_df["Texte"] = ""
                    
                    summary_cols = ["Model", "Idsim", "Texte", "Planting", "Emergence", "Ant", "Mat",
                                    "Biom_ma", "Yield", "GNumber", "MaxLai", "SoilN", "CumE", "Transp"]
                    
                    # Keep only the columns that exist
                    available_cols = [col for col in summary_cols if col in summary_df.columns]
                    summary_df = summary_df[available_cols]
                    
                    # Add missing columns as None
                    for col in summary_cols:
                        if col not in summary_df.columns:
                            summary_df[col] = None
                    
                    summary_df = summary_df[summary_cols]
                    
                    with sqlite3.connect(mi) as conn:
                        summary_df.to_sql("SummaryOutput", conn, if_exists='append', index=False)
                        conn.commit()
                
                total_rows += len(chunk_df)
                print(f"✅ Chunk {chunk_idx + 1}/{total_chunks}: {len(chunk_df)} rows written to database", flush=True)
            del chunk_df
        
        if total_rows == 0:
            print("No data to process.", flush=True)
            return
        
        print(f"✅ Total rows in OutputSynt: {total_rows}", flush=True)
        print(f"Celsius total time: {time()-start:.2f}s", flush=True)
    except Exception as ex:
        print("❌ Error during parallel processing:", flush=True)
        print(f"Exception type: {type(ex).__name__}", flush=True)
        print(f"Exception message: {str(ex)}", flush=True)
        traceback.print_exc()
        sys.exit(1) 

if __name__ == "__main__":
    main()

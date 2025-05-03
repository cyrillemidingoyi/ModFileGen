import os
import sqlite3
import pandas as pd
import shutil
from pathlib import Path
from time import time
from multiprocessing import Pool
import subprocess
from joblib import Parallel, delayed
from modfilegen import GlobalVariables
from modfilegen.converter import Converter
import uuid
import sys
import traceback
import concurrent.futures


def create_idJourClim(row):
    return row['IdDClim'] + '.' + str(row['annee']) + '.' + str(row['jda'])

def process_chunk(chunk
                  , masterInput
                  , DB_MD
                  , DB_Celsius
                  , directoryPath
                  , dt
                  , ori_mi):
    
    try:

        idsims = tuple([row["idsim"] for row in chunk])
        if len(idsims) == 1:
            idsims = f"({idsims[0]})"
        print(f"Number of idsims", len(idsims), flush=True)
        
        # Create a new folder and copy the DB_Celsius database for each process
        print(f"creating new directory for process", flush=True)
        new_dir = os.path.join(directoryPath, f"proc_{str(uuid.uuid4())}")
        Path(new_dir).mkdir(parents=True, exist_ok=True) 
        new_db_cel = os.path.join(new_dir, "CelsiusV3nov17_dataArise.db")
        new_db_mi = os.path.join(new_dir, "MasterInput.db")
        shutil.copy(DB_Celsius, new_db_cel)
        shutil.copy(ori_mi, new_db_mi)
        
        # connect to the masterInput database
        with sqlite3.connect(masterInput) as conn, sqlite3.connect(new_db_mi) as new_conn, sqlite3.connect(new_db_cel) as new_conn_cel:
            cursor = conn.cursor()
            cursor_dst = new_conn.cursor()
            cursor.execute('PRAGMA journal_mode = WAL')  # Enable Write-Ahead Logging
            cursor.execute('PRAGMA synchronous = NORMAL') 
        
            # Copier SimulationList filtré
            try:
                print("Copy SimulationList", flush=True)
                sim_chunks = pd.read_sql(
                    "SELECT * FROM SimUnitList", 
                    conn,
                    chunksize=500
                )
                filtered_dfs = [
                    chunk[chunk['idsim'].isin(idsims)] 
                    for chunk in sim_chunks
                ]
                sim_df = pd.concat(filtered_dfs)
                cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='SimUnitList'")
                create_table_sql = cursor.fetchone()[0]
                cursor_dst.execute("DROP TABLE IF EXISTS SimUnitList")
                cursor_dst.execute(create_table_sql)  # Recrée la structure exacte de la table

                sim_df.to_sql(
                    'SimUnitList',
                    new_conn,
                    if_exists='append',
                    index=False,
                    chunksize=500
                )
                print("SimulationList copied", flush=True)
            except Exception as e:
                print("❌ Error during SimulationList copy:", flush=True)
                print(f"Exception type: {type(e).__name__}", flush=True)
                print(f"Exception message: {str(e)}", flush=True) 
                traceback.print_exc()
                shutil.rmtree(new_dir)
                raise
                

            # Récupérer les idPoints utilisés
            try:
                idPoints = pd.read_sql(
                        f"SELECT DISTINCT idPoint FROM SimUnitList WHERE idsim IN {idsims}",
                        conn
                    )['idPoint'].tolist()                
                print( "Start transfert of climate data from MI to Cel", flush=True)
                idPoints_tuple = tuple(idPoints)
                if len(idPoints_tuple) == 1:
                    idPoints_tuple = f"({idPoints_tuple[0]})"
                print(f"Number of idPoints", len(idPoints_tuple), flush=True)

                query = """
                    SELECT idPoint, year, DOY, Nmonth, NdayM, srad, tmax, tmin, tmoy, rain, Etppm 
                    FROM RAclimateD 
                    WHERE idPoint IN {}
                """.format(idPoints_tuple)

                df_clim_MI = pd.read_sql(query, conn)
                df = df_clim_MI.rename(columns={"idPoint":"IdDClim", "year":"annee", "DOY":"jda", "Nmonth":"mois", "NdayM":"jour", "srad":"rg", "rain":"plu", "Etppm":"Etp"})
                df['idjourclim'] = df.apply(create_idJourClim, axis=1)
                #df_sorted = df.sort_values(by='idjourclim')
                df = df[['IdDClim', 'idjourclim', 'annee',"jda","mois","jour","tmax","tmin","tmoy","rg","plu",'Etp' ]]
                df.to_sql('Dweather', new_conn_cel, if_exists='replace', index=False)

                create_index_query_idDclim = "CREATE INDEX IF NOT EXISTS idx_idDclim ON Dweather (IdDClim, annee);"
                cursor_cel = new_conn_cel.cursor()
                cursor_cel.execute(create_index_query_idDclim)
                new_conn_cel.commit()
                print( "transfert of climate data from MI to Cel done")    

            except Exception as e:
                print("❌ Error during RAclimateD copy:", flush=True)
                print(f"Exception type: {type(e).__name__}", flush=True)
                print(f"Exception message: {str(e)}", flush=True)
                traceback.print_exc()
                shutil.rmtree(new_dir)
                raise
                
            # copier les autres tables CropManagement, soil et SoilLayers
            print("copy CropManagement, Soil and SoilLayers", flush=True)
            try:
                tables_to_copy = ["CropManagement", "Soil", "SoilLayers", "Coordinates", "InitialConditions"]
                for table in tables_to_copy:
                    # remove the content of the table in the new database
                    cursor_dst.execute(f"DELETE FROM {table}")
                    rows = cursor.execute(f"SELECT * FROM {table}").fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    col_names = ", ".join(columns)
                    placeholders = ",".join("?" * len(columns))

                    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                    create_table_sql = cursor.fetchone()[0]
                    cursor_dst.execute(f"DROP TABLE IF EXISTS {table}")
                    cursor_dst.execute(create_table_sql)  # Recrée la structure exacte de la table
                                
                    query2 = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                    cursor_dst.executemany(query2, [tuple(row) for row in rows])
            except Exception as e:
                print(f"❌ Error during {table} copy:", flush=True)
                print(f"Exception type: {type(e).__name__}", flush=True)
                print(f"Exception message: {str(e)}", flush=True)
                shutil.rmtree(new_dir)
                traceback.print_exc()
                raise


            # Commit changes 
            new_conn.commit()
        # use subprocess to run the celsius model with the command "celsius convert -m celsius -t ${THREADS} -dbMasterInput ${new_db_mi} -dbModelsDictionary ${DB_MD} -dbCelsius ${new_db_cel}"
        try:
            print("convert celsius", flush=True)
            result = subprocess.run(["datamill", "convert", "-m", "celsius", "-dbMasterInput", new_db_mi, "-dbModelsDictionary", DB_MD, "-dbCelsius", new_db_cel],
                            check=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True)
            print("✅ Celsius conversion completed successfully!", flush=True)
        except subprocess.CalledProcessError as e:
            print("❌ Error during Celsius conversion:", flush=True)
            print(f"Exit code: {e.returncode}", flush=True)
            print(f"STDOUT:\n{e.stdout}", flush=True)
            print(f"STDERR:\n{e.stderr}", flush=True)
            shutil.rmtree(new_dir)
            traceback.print_exc()
            raise 
            
        try:
            print("run celsius")
            subprocess.run(["celsius", "convert", "-m", "celsius", "-dbCelsius", new_db_cel],
                                check=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True
                            )
        except subprocess.CalledProcessError as e:
            print("❌ Error during Celsius run!", flush=True)
            print(f"Exit code: {e.returncode}", flush=True)
            print(f"STDOUT:\n{e.stdout}", flush=True)
            print(f"STDERR:\n{e.stderr}", flush=True)
                # close connection
            shutil.rmtree(new_dir)
            traceback.print_exc()
            raise
                #raise RuntimeError(f"Celsius run failed:\n{e.stderr}") from e

        # Get in a dataframe the table "OutputSyn" from the new_db_cel database
        new_conn_cel = sqlite3.connect(new_db_cel)
        df = pd.read_sql_query("SELECT * FROM OutputSynt", new_conn_cel)
        new_conn_cel.close()
        if dt == 1: shutil.rmtree(new_dir)
        return df
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
    
    
def chunk_data(data, chunk_size):    # values, num_sublists 
    k, m = divmod(len(data), chunk_size)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(chunk_size)]
    return sublists

def main():
    mi= GlobalVariables["dbMasterInput"]
    md = GlobalVariables["dbModelsDictionary"]
    celsius = GlobalVariables["dbCelsius"]
    directoryPath = GlobalVariables["directorypath"]
    nthreads = GlobalVariables["nthreads"]
    dt = GlobalVariables["dt"]
    ori_mi = GlobalVariables["ori_MI"]



    data = fetch_data_from_sqlite(mi)
    # Split data into chunks
    chunks = chunk_data(data, chunk_size=nthreads)
    # Create a Pool of worker processes
    try:
        start = time()
        processed_data_chunks = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=nthreads) as executor:
            futures = [
                executor.submit(
                    process_chunk, chunk, mi, md, celsius, directoryPath, dt, ori_mi
                )
                for chunk in chunks
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    processed_data_chunks.append(result)
                except Exception as e:
                    print("❌ Error in one of the processes:", flush=True)
                    print(f"Exception type: {type(e).__name__}", flush=True)
                    print(f"Exception message: {str(e)}", flush=True)
                    traceback.print_exc()
                    #executor.shutdown(wait=False, cancel_futures=True)
                    sys.exit(1)  # Interrompt tout
        if not processed_data_chunks:
            print("No data to process.")
            return
        df = pd.concat(processed_data_chunks, ignore_index=True)
        print(f"Number of rows in OutputSynt", len(df), flush=True)
        with sqlite3.connect(celsius) as conn:
            conn.execute("DELETE FROM OutputSynt")
            df.to_sql("OutputSynt", conn, if_exists='append', index=False)
            conn.commit()
        print(f"total time, {time()-start}")
    except Exception as ex:
        print("❌ Error during parallel processing:", flush=True)
        print(f"Exception type: {type(ex).__name__}", flush=True)
        print(f"Exception message: {str(ex)}", flush=True)
        traceback.print_exc()
        sys.exit(1) 

if __name__ == "__main__":
    main()

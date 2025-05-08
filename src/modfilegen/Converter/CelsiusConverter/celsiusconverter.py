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


def create_idJourClim(row):
    return row['IdDClim'] + '.' + str(row['annee']) + '.' + str(row['jda'])

def process_chunk(args):
    
    chunk, masterInput, DB_MD, DB_Celsius, directoryPath, dt, ori_mi = args
    
    try:

        idsims = tuple([row["idsim"] for row in chunk])
        if len(idsims) == 1:
            idsims = f"({idsims[0]})"
        print(f"Number of idsims", len(idsims), flush=True)
        print(f"creating new directory for process", flush=True)
        new_dir = os.path.join(directoryPath, f"proc_{str(uuid.uuid4())}")
        while os.path.exists(new_dir):
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
            print("Copy SimulationList", flush=True)            
            query_sim = f"SELECT * FROM SimUnitList WHERE idsim IN {idsims}"
            sim_df = pd.read_sql(query_sim, conn)
            sim_df.to_sql('SimUnitList', new_conn, if_exists='replace', index=False)
            print("SimulationList copied", flush=True)
            
            
            print( "Start transfert of climate data from MI to Cel", flush=True)
            idPoints = tuple(sim_df["idPoint"].unique())
            if len(idPoints) == 1:
                idPoints = f"({idPoints[0]})"               
            print( "Start transfert of climate data from MI to Cel", flush=True)
            print(f"Number of idPoints", len(idPoints), flush=True)
            query = """
                    SELECT idPoint, year, DOY, Nmonth, NdayM, srad, tmax, tmin, tmoy, rain, Etppm 
                    FROM RAclimateD 
                    WHERE idPoint IN {}
                """.format(idPoints)

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
    args_list = [(chunk, mi, md, celsius, directoryPath, dt, ori_mi) for chunk in chunks]
    # Create a Pool of worker processes
    try:
        start = time()
        processed_data_chunks = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=nthreads) as executor:
            processed_data_chunks = list(executor.map(process_chunk,args_list))
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

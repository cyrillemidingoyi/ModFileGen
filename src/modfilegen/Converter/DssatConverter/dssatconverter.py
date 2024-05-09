from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import dssatweatherconverter
import sys, subprocess


import os
import datetime
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
from time import time
import traceback


class SticsConverter(Converter):
    def __init__(self):
        super().__init__()
        self.MasterInput= GlobalVariables["dbMasterInput"]
        self.ModelDictionary = GlobalVariables["dbModelsDictionary"]
        self.DirectoryPath = ""
        self.pltfolder = ""


        
    def process_chunk(self, chunk):
        # Apply series of functions to each row in the chunk
        for i, row in enumerate(chunk):
            ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)
            MasterInput_Connection = sqlite3.connect(self.MasterInput)
            print(f"Iteration {i}")
            # Création du chemin du fichier
            simPath = os.path.join(self.DirectoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]), str(row["IdMangt"]))
            usmdir = os.path.join(self.DirectoryPath, str(row["idsim"]))
            try:
                # wth files
                weatherConverter = dssatweatherconverter.DssatweatherConverter()
                weatherConverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection)
                # run stics
                subprocess.run(["bash", "sticsrun.sh", usmdir, self.DirectoryPath])
                # 
            except Exception as ex:
                print("Error during Export DSSAT :", ex)
                traceback.print_exc()
                sys.exit(1)
                  
            MasterInput_Connection.close()
            ModelDictionary_Connection.close()
                    
    def export(self):
        MasterInput_Connection = sqlite3.connect(self.MasterInput)
        ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)
        try:
            print(f"dbMasterInput: {self.MasterInput}")
            print(f"dbModelsDictionary: {self.ModelDictionary}")
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
        if not os.path.exists(self.DirectoryPath):
            os.makedirs(self, self.DirectoryPath)

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
        def fetch_data_from_sqlite():
            conn = sqlite3.connect(self.MasterInput)
            df = pd.read_sql_query(f"SELECT * FROM SimUnitList", conn)
            rows = df.to_dict(orient='records')
            conn.close()
            return rows
        
        # Split data into chunks
        def chunk_data(data, chunk_size):
            return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

        data = fetch_data_from_sqlite()
        # Split data into chunks
        chunks = chunk_data(data, chunk_size=self.nthreads)
        # Create a Pool of worker processes
        
        try:
            start = time()
            with Pool(processes=self.nthreads) as pool:
                # Apply the processing function to each chunk in parallel
                processed_data_chunks = pool.map(self.process_chunk, chunks)  
            print(f"total time, {time()-start}")
        except Exception as ex:          
            print("Export completed successfully!")

    
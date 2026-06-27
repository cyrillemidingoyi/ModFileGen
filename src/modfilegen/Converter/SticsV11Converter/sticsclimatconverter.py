from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsClimatConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        file_name = "climat.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)        
        Site = ST[-2]
        Year = ST[-1]
        T = "Select   Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'sticsv11') And ([Table]= 'climat'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        fetchAllQuery = "select * from RaClimateD where idPoint='" + Site + "' And (Year=" + Year + " or Year=" + str(int(Year) + 1) + ") ORDER BY w_date;"
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        
        # Pre-cache default values
        vapeurp_dv = float(DT[DT["Champ"] == "vapeurp"]["dv"].values[0])
        co2_dv = float(DT[DT["Champ"] == "co2"]["dv"].values[0])
        
        # Process data in bulk
        DA['srad'] = DA['srad'].fillna(-999.9)
        DA['wind'] = DA['wind'].fillna(-999.9)
        
        # Format all lines at once
        lines = (
            DA['idPoint'] + ' ' + 
            DA['year'].astype(str) + ' ' +
            DA['Nmonth'].astype(str).str.rjust(3) +
            DA['NdayM'].astype(str).str.rjust(3) +
            DA['DOY'].astype(str).str.rjust(4) +
            DA['tmin'].apply(lambda x: format(x, ".1f")).str.rjust(8) +
            DA['tmax'].apply(lambda x: format(x, ".1f")).str.rjust(7) +
            DA['srad'].apply(lambda x: format(x, ".1f")).str.rjust(7) +
            DA['Etppm'].apply(lambda x: format(x, ".1f")).str.rjust(7) +
            DA['rain'].apply(lambda x: format(x, ".1f")).str.rjust(7) +
            DA['wind'].apply(lambda x: format(x, ".1f")).str.rjust(7) +
            str(format(vapeurp_dv, ".1f")).rjust(7) +
            str(format(co2_dv, ".1f")).rjust(7) + '\n'
        )
        
        fileContent = ''.join(lines.tolist())        
        try:
            # Export file to specified directory    
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        
        return fileContent

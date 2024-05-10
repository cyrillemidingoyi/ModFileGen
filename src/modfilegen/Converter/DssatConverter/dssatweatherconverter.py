from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class DssatweatherConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection):
        file_name = "climat.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)        
        Site = ST[-3]
        Year = ST[-2]
        # get the four first elements of ST[-1] to get the IdMngt
        Mngt = ST[-1][:4]
        # Create the output path from ST without the two last elements
        output_path = os.path.join(*ST[:-3])
        T = "Select   Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table]= 'dssat_weather_site'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        fetchAllQuery = "select * from RaClimateD where idPoint='" + Site + "' And (Year=" + Year + " or Year=" + str(int(Year) + 1) + ");"
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        
        v_fmt_general = {
                'INSI': "{:>6}",
                'LAT': "{:9.3f}",
                'LONG': "{:9.3f}",
                'ELEV': "{:6.0f}",
                'TAV': "{:6.1f}",
                'AMP': "{:6.1f}",
                'REFHT': "{:6.1f}",
                'WNDHT': "{:6.1f}",
                'CO2': "{:6f}"
            }
        
        v_fmt = {
            'DATE': "{:>7}",
            'SRAD': "{:6.1f}",
            'TMAX': "{:6.1f}",
            'TMIN': "{:6.1f}",
            'RAIN': "{:6.1f}",
            'WIND': "{:6.0f}",
            'RHUM': "{:6.1f}",
            'DEWP': "{:6.1f}",
            'PAR': "{:6.1f}",
            'EVAP': "{:6.1f}",
            'VAPR': "{:6.2f}",
            'SUNH': "{:6.1f}"
        }
        
        fileContent += f"*WEATHER DATA : {Site}\n"
        fileContent += "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n"
        fileContent += f"  ACNM   {v_fmt_general.format(row['LAT'])} 
                                  {v_fmt.format(row['LONG'])}  
                                  {v_fmt.format(row['ELEV'])}  
                                  {v_fmt.format(row['TAV'])}  
                                  {v_fmt.format(row['AMP'])} 
                                  {v_fmt.format(row['REFHT'])} 
                                  {v_fmt.format(row['WNDHT'])}\n"
                                  
        fileContent += "@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  EVAP  RHUM\n"
        for row in rows:
            fileContent += f"{v_fmt.format(row['DATE'])} 
                             {v_fmt.format(row['TMAX'])} 
                             {v_fmt.format(row['TMIN'])}   
                             {v_fmt.format(row['RAIN'])}  
                             {v_fmt.format(row['DEWP']) if pd.notna(v_fmt.format(row['DEWP'])) else ''}  
                             {v_fmt.format(row['WIND']) if pd.notna(v_fmt.format(row['WIND'])) else ''}   
                             {v_fmt.format(row['PAR']) if pd.notna(v_fmt.format(row['PAR'])) else ''}  
                             {v_fmt.format(row['EVAP']) if pd.notna(v_fmt.format(row['EVAP'])) else ''}  
                             {v_fmt.format(row['RHUM']) if pd.notna(v_fmt.format(row['RHUM'])) else ''}\n"
        try:
            # Export file to specified directory    
            self.write_file(output_path, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()

from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class DssatweatherConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection,usmdir):
        res = {}
        try:
            #print("Exporting Dssat Weather")
            fileContent = ""
            ST = directory_path.split(os.sep)        
            Site = ST[-3]
            Year = ST[-2]
            # get the four first elements of ST[-1] to get the IdMngt
            Mngt = ST[-1][:4]
            # Create the output path from ST without the two last elements
            T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table]= 'dssat_weather_site'));"
            DT = pd.read_sql_query(T, ModelDictionary_Connection)
            rw = DT[DT["Champ"] == "tav"]
            tav = rw["dv"].values[0]
            rw = DT[DT["Champ"] == "amp"]
            amp = rw["dv"].values[0]
            rw = DT[DT["Champ"] == "refht"]
            refht = rw["dv"].values[0] 
            rw = DT[DT["Champ"] == "wndht"]
            wndht = rw["dv"].values[0] 
    
            fetchAllQuery1 = "select * from Coordinates where idPoint='" + Site + "';"
            DA1 = pd.read_sql_query(fetchAllQuery1, master_input_connection)
            rows1 = DA1.to_dict(orient='records')
    
            fileNameArray = [None]*4
            fileNameArray[0] = ""
            fileNameArray[1] = "00"
            fileNameArray[2] = "01"
            fileNameArray[3] = ".WTH"
            
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
                'DATE': "{:>5}",
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
            for i in range(0, 2):
                Year = int(Year) + i
                file_name = ""
                for row in rows1:
                    fileContent =  ""
                    fileNameArray[0] = Mngt.upper() 
                    fileNameArray[1] = str(Year)[2:4]
                    fileContent += f"*WEATHER DATA : {Site} , {str(Year)}\n\n"
                    fileContent += "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n"
                    fileContent += v_fmt_general['INSI'].format(Site[0:4])
                    fileContent += v_fmt_general['LAT'].format(row['latitudeDD'])
                    fileContent += v_fmt_general["LONG"].format(row['longitudeDD'])
                    fileContent += v_fmt_general["ELEV"].format(row['altitude']) 
                    fileContent += v_fmt_general["TAV"].format(float(tav)) 
                    fileContent += v_fmt_general["AMP"].format(float(amp))
                    fileContent += v_fmt_general["REFHT"].format(float(refht))
                    fileContent += v_fmt_general["WNDHT"].format(float(wndht)) + "\n"
                                            
                    Year = str(Year)
                    fetchAllQuery = "select * from RaClimateD where idPoint='" + Site + "' and year='" + Year + "' ORDER BY w_date ;"  
                    DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
                    rows = DA.to_dict(orient='records')
                    fileNameArray[2] = "01" 
                                        
                    fileContent += "@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  EVAP  RHUM\n"
                    for row in rows:
                        fileContent += v_fmt["DATE"].format(str(row['year'])[2:4]+str(row['DOY']).rjust(3,"0"))
                        fileContent += v_fmt["SRAD"].format(row['srad'])  
                        fileContent += v_fmt['TMAX'].format(row['tmax'])
                        fileContent += v_fmt['TMIN'].format(row['tmin'])  
                        fileContent += v_fmt['RAIN'].format(row['rain'])
                        if 'dewp' in row and row['dewp']:
                            fileContent += v_fmt['DEWP'].format(row['dewp'])
                        else:
                            if ('Tdewmin' in row and row['Tdewmin']) and ('Tdewmax' in row and  row['Tdewmax']):
                                fileContent += v_fmt['DEWP'].format(float(row['Tdewmin']) + float(row['Tdewmax']) / 2.0)
                            else:
                                fileContent += ' '*6
                        fileContent += v_fmt['WIND'].format(row['wind']*86.4) if 'wind' in row and row['wind'] is not None else ' '*6 
                        fileContent += v_fmt['PAR'].format(row['par']) if 'par' in row and row['par'] is not None else  ' '*6
                        fileContent += v_fmt['EVAP'].format(row['evap']) if 'evap' in row and row['evap'] is not None else ' '*6
                        fileContent += v_fmt['RHUM'].format(float(row['rhum'])) +"\n" if 'rhum' in row and  row['rhum'] is not None else ' '*6 +"\n"
                        
                file_name = fileNameArray[0] + fileNameArray[1] + fileNameArray[2] + fileNameArray[3]
                self.write_file(usmdir, file_name, fileContent)
                res[file_name] = fileContent
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        return res

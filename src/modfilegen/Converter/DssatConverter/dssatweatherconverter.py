from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback


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
            
class DssatweatherConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, usmdir, DT, climate_df, coord_df):
        res = {}
        try:
            #print("Exporting Dssat Weather")
            #fileContent = ""
            ST = directory_path.split(os.sep)        
            Site, Year, Mngt = ST[-3], ST[-2], ST[-1][:4].upper()
            
            params = DT.set_index("Champ")['dv'].to_dict()
            tav, amp, refht, wndht = (float(params.get(key, 0)) for key in ('tav', 'amp', 'refht', 'wndht'))

            subset_coord_df = coord_df.loc[coord_df["idPoint"] == Site]
            coord_dict = subset_coord_df.iloc[0].to_dict()      
         
            for year_offset in range(2):
                fileContent =  []
                current_year = int(Year) + year_offset
                file_name = f"{Mngt.upper()}{str(current_year)[2:]}01.WTH"
                header = [
                    f"*WEATHER DATA : {Site} , {str(current_year)}\n\n",
                    "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n",
                    f"{v_fmt_general['INSI'].format(Site[:4])}",
                    f"{v_fmt_general['LAT'].format(coord_dict['latitudeDD'])}",
                    f"{v_fmt_general['LONG'].format(coord_dict['longitudeDD'])}",
                    f"{v_fmt_general['ELEV'].format(coord_dict['altitude'])}",
                    f"{v_fmt_general['TAV'].format(tav)}",
                    f"{v_fmt_general['AMP'].format(amp)}",
                    f"{v_fmt_general['REFHT'].format(refht)}",
                    f"{v_fmt_general['WNDHT'].format(wndht)}\n",
                    "@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  EVAP  RHUM\n"
                ] 
                
                fileContent = header
                
                year_df = climate_df[(climate_df["idPoint"] == Site) & (climate_df["year"] == current_year)]
                for row in year_df.itertuples(index=False):
                    DEWP = getattr(row, 'dewp', None)
                    if DEWP is None and hasattr(row, 'Tdewmin') and hasattr(row, 'Tdewmax'):
                        DEWP = (getattr(row, 'Tdewmin', 0) + getattr(row, 'Tdewmax', 0)) / 2

                    fields = {
                        'DATE': f"{str(row.year)[2:4]}{str(row.DOY).zfill(3)}",
                        'SRAD': f"{row.srad:6.1f}",
                        'TMAX': f"{row.tmax:6.1f}",
                        'TMIN': f"{row.tmin:6.1f}",
                        'RAIN': f"{row.rain:6.1f}",
                        'DEWP': f"{DEWP:6.1f}" if DEWP is not None else ' '*6,
                        'WIND': f"{row.wind * 86.4:6.0f}" if getattr(row, 'wind', None) is not None else ' '*6,
                        'PAR': f"{row.par:6.1f}" if getattr(row, 'par', None) is not None else ' '*6,
                        'EVAP': f"{row.evap:6.1f}" if getattr(row, 'evap', None) is not None else ' '*6,
                        'RHUM': f"{row.rhum:6.1f}" if getattr(row, 'rhum', None) is not None else ' '*6,
                    }
                    line = "{DATE:>5}{SRAD}{TMAX}{TMIN}{RAIN}{DEWP}{WIND}{PAR}{EVAP}{RHUM}\n".format(**fields)
                    fileContent.append(line)

                file_content_str = "".join(fileContent)
                self.write_file(usmdir, file_name, file_content_str)
                res[file_name] = file_content_str                    
            
        except Exception as e:
            print(f"Error during export: {str(e)}")
            traceback.print_exc()
    
        return res

            
        """rows1 = subset_coord_df.to_dict(orient='records')
    
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
                    fileNameArray[0] = Mngt 
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
                    subset_df1 = climate_df.loc[
                        (climate_df["idPoint"] == Site) & 
                        (climate_df["year"]==int(Year))  # Using set for faster lookups
                    ] 
                    rows = subset_df1.to_dict(orient='records')
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
                        fileContent += v_fmt['RHUM'].format(float(row['rhum'])) if 'rhum' in row and  row['rhum'] is not None else ' '*6 +"\n"
                        
                file_name = fileNameArray[0] + fileNameArray[1] + fileNameArray[2] + fileNameArray[3]
                self.write_file(usmdir, file_name, fileContent)
                res[file_name] = fileContent
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        return res"""


def calc_dewp(row):
    if 'dewp' in row and pd.notna(row['dewp']):
        return row['dewp']
    elif all(pd.notna(row.get(k)) for k in ['Tdewmin', 'Tdewmax']):
        return (float(row['Tdewmin']) + float(row['Tdewmax'])) / 2.0
    return None
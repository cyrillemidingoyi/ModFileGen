from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsClimatConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(
        self,
        directory_path,
        ModelDictionary_Connection,
        master_input_connection,
        usmdir,
        start_year=None,
        end_year=None,
    ):
        file_name = "climat.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)        
        Site = ST[-2]
        idsim = ST[-3]
        Site = ST[-2]
        if start_year is None or end_year is None:
            simulation = master_input_connection.execute(
                "SELECT StartYear, EndYear FROM SimUnitList WHERE idsim = ?",
                (idsim,),
            ).fetchone()
            if simulation is None:
                raise ValueError(f"Simulation {idsim!r} was not found in SimUnitList")
            start_year = simulation[0] if start_year is None else start_year
            end_year = simulation[1] if end_year is None else end_year
        start_year = int(start_year)
        end_year = int(end_year)
        if end_year < start_year:
            raise ValueError(
                f"EndYear ({end_year}) must be greater than or equal to "
                f"StartYear ({start_year}) for {idsim}"
            )
        T = "Select   Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'sticsv11') And ([Table]= 'climat'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        fetchAllQuery = """
            SELECT *
            FROM RaClimateD
            WHERE idPoint = ? AND Year BETWEEN ? AND ?
            ORDER BY w_date
        """
        DA = pd.read_sql_query(
            fetchAllQuery,
            master_input_connection,
            params=(Site, start_year, end_year),
        )
        if DA.empty:
            raise ValueError(
                f"No climate data for idPoint={Site}, years "
                f"{start_year}-{end_year}"
            )
        
        # Pre-cache default values
        vapeurp_dv = float(DT[DT["Champ"] == "vapeurp"]["dv"].values[0])
        co2_dv = float(DT[DT["Champ"] == "co2"]["dv"].values[0])
        
        # Process data in bulk
        DA['srad'] = DA['srad'].fillna(-999.9)
        DA['wind'] = DA['wind'].fillna(-999.9)
        DA['Etppm'] = pd.to_numeric(DA['Etppm'], errors='coerce').fillna(-999.9)
        if 'vapeurp' in DA.columns:
            vapeurp_values = pd.to_numeric(DA['vapeurp'], errors='coerce').fillna(vapeurp_dv)
        else:
            vapeurp_values = pd.Series(vapeurp_dv, index=DA.index)
        
        # same with co2, if it's not present in the DataFrame, create a Series with default value
        if 'co2' in DA.columns:
            co2_values = pd.to_numeric(DA['co2'], errors='coerce').fillna(co2_dv)
        else:
            co2_values = pd.Series(co2_dv, index=DA.index)
        
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
            vapeurp_values.apply(lambda x: format(x, ".1f")).str.rjust(7) +
            co2_values.apply(lambda x: format(x, ".1f")).str.rjust(7) + '\n'
        )
        
        fileContent = ''.join(lines.tolist())        
        try:
            # Export file to specified directory    
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        
        return fileContent

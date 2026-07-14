from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback


def is_leap_year(year):
    year = int(year)
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def rollover_weather_rows(rows, weather_year):
    copied_rows = []
    target_year = int(weather_year)
    for row in rows:
        doy = int(row["DOY"])
        if doy == 366 and not is_leap_year(target_year):
            continue
        copied_row = dict(row)
        copied_row["year"] = target_year
        copied_rows.append(copied_row)
    return copied_rows


def parse_weather_directory(directory_path):
    parts = directory_path.split(os.sep)
    site = parts[-3]
    year = int(parts[-2])
    management = parts[-1][:4].upper()
    return site, year, management


def weather_rows_for_year(site, weather_year, master_input_connection, previous_rows=None):
    fetch_all_query = (
        "select * from RaClimateD where idPoint='"
        + site
        + "' and year='"
        + str(weather_year)
        + "' ORDER BY w_date ;"
    )
    dataframe = pd.read_sql_query(fetch_all_query, master_input_connection)
    rows = dataframe.to_dict(orient="records")
    if rows:
        return rows
    if previous_rows is None:
        raise ValueError(f"No weather data found in RaClimateD for idPoint={site}, year={weather_year}")
    print(
        f"Warning: no weather data found in RaClimateD for idPoint={site}, year={weather_year}; "
        f"reusing {weather_year - 1} values as DSSAT rollover weather."
    )
    return rollover_weather_rows(previous_rows, weather_year)


def normalize_export_years(start_year, years=None, thirdyear=None):
    if isinstance(years, int) and thirdyear is None and years in (0, 1):
        thirdyear = years
        years = None

    if years is None:
        span = 3 if int(thirdyear or 0) == 1 else 2
        years = [start_year + offset for offset in range(span)]
    elif isinstance(years, int):
        years = [years]

    return sorted({int(year) for year in years})


class DssatweatherConverter(Converter):
    def __init__(self):
        super().__init__()

    def _write_header(self, site, site_row, tav, amp, refht, wndht, title):
        content = ""
        content += f"*WEATHER DATA : {site} , {title}\n\n"
        content += "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n"
        content += f"{site[0:4]:>6}"
        content += f"{site_row['latitudeDD']:9.3f}"
        content += f"{site_row['longitudeDD']:9.3f}"
        content += f"{site_row['altitude']:6.0f}"
        content += f"{float(tav):6.1f}"
        content += f"{float(amp):6.1f}"
        content += f"{float(refht):6.1f}"
        content += f"{float(wndht):6.1f}\n"
        content += "@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  EVAP  RHUM\n"
        return content

    def _append_weather_rows(self, content, rows):
        for row in rows:
            content += f"{str(row['year'])[2:4]}{str(row['DOY']).rjust(3, '0'):>5}"[-5:]
            content += f"{row['srad']:6.1f}"
            content += f"{row['tmax']:6.1f}"
            content += f"{row['tmin']:6.1f}"
            content += f"{row['rain']:6.1f}"
            if 'dewp' in row and row['dewp']:
                content += f"{row['dewp']:6.1f}"
            elif ('Tdewmin' in row and row['Tdewmin']) and ('Tdewmax' in row and row['Tdewmax']):
                content += f"{(float(row['Tdewmin']) + float(row['Tdewmax']) / 2.0):6.1f}"
            else:
                content += ' ' * 6
            content += f"{row['wind'] * 86.4:6.0f}" if 'wind' in row and row['wind'] is not None else ' ' * 6
            content += f"{row['par']:6.1f}" if 'par' in row and row['par'] is not None else ' ' * 6
            content += f"{row['evap']:6.1f}" if 'evap' in row and row['evap'] is not None else ' ' * 6
            content += f"{float(row['rhum']):6.1f}\n" if 'rhum' in row and row['rhum'] is not None else ' ' * 6 + "\n"
        return content

    def export(
        self,
        directory_path,
        ModelDictionary_Connection,
        master_input_connection,
        usmdir,
        years=None,
        thirdyear=None,
        single_file=False,
        file_name=None,
    ):
        """Export DSSAT weather files with legacy and explicit-year arguments."""
        _, start_year, _ = parse_weather_directory(directory_path)
        years = normalize_export_years(start_year, years=years, thirdyear=thirdyear)
        return self.export_years(
            directory_path,
            ModelDictionary_Connection,
            master_input_connection,
            usmdir,
            years=years,
            single_file=single_file,
            file_name=file_name,
        )

    def export_years(
        self,
        directory_path,
        ModelDictionary_Connection,
        master_input_connection,
        usmdir,
        years=None,
        single_file=False,
        file_name=None,
    ):
        """Export weather files for explicit years."""
        res = {}
        try:
            site, start_year, management = parse_weather_directory(directory_path)
            query = (
                "Select Champ, Default_Value_Datamill, defaultValueOtherSource, "
                "IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv "
                "From Variables Where ((model = 'dssat') And ([Table]= 'dssat_weather_site'));"
            )
            dt = pd.read_sql_query(query, ModelDictionary_Connection)
            tav = dt[dt["Champ"] == "tav"]["dv"].values[0]
            amp = dt[dt["Champ"] == "amp"]["dv"].values[0]
            refht = dt[dt["Champ"] == "refht"]["dv"].values[0]
            wndht = dt[dt["Champ"] == "wndht"]["dv"].values[0]

            coordinates_query = "select * from Coordinates where idPoint='" + site + "';"
            coordinates = pd.read_sql_query(coordinates_query, master_input_connection).to_dict(orient='records')
            if not coordinates:
                raise ValueError(f"No coordinates found for idPoint={site}")
            site_row = coordinates[0]

            years = normalize_export_years(start_year, years=years)

            weather_by_year = {}
            previous_rows = None
            for weather_year in years:
                rows = weather_rows_for_year(site, weather_year, master_input_connection, previous_rows)
                weather_by_year[weather_year] = rows
                previous_rows = rows

            if single_file:
                title = str(years[0]) if len(years) == 1 else f"{years[0]}-{years[-1]}"
                file_content = self._write_header(site, site_row, tav, amp, refht, wndht, title)
                all_rows = []
                for weather_year in years:
                    all_rows.extend(weather_by_year[weather_year])
                file_content = self._append_weather_rows(file_content, all_rows)
                target_name = file_name or f"{management}.WTH"
                self.write_file(usmdir, target_name, file_content)
                res[target_name] = file_content
                return res

            for weather_year in years:
                file_content = self._write_header(site, site_row, tav, amp, refht, wndht, str(weather_year))
                file_content = self._append_weather_rows(file_content, weather_by_year[weather_year])
                target_name = f"{management}{str(weather_year)[2:4]}01.WTH"
                self.write_file(usmdir, target_name, file_content)
                res[target_name] = file_content
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        return res

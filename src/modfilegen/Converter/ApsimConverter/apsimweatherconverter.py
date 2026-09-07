"""
APSIM Weather Converter
This module converts weather data from the database to APSIM .met weather file format.

APSIM weather file format:
- Header with metadata (latitude, longitude, tav, amp)
- Column names: year, day, radn, maxt, mint, rain, vp, wind, code
- Units line: () () (MJ/m^2) (oC) (oC) (mm) (hPa) (m/s) ()
- Data rows with space-separated values (aligned columns)

Variables:
- year: Year (YYYY)
- day: Day of year (1-365/366)
- radn: Solar radiation (MJ/m²/day)
- maxt: Maximum temperature (°C)
- mint: Minimum temperature (°C)
- rain: Rainfall (mm)
- vp: Vapor pressure (hPa)
- wind: Wind speed (m/s)
- code: Quality code

Metadata:
- latitude: Site latitude (decimal degrees)
- longitude: Site longitude (decimal degrees)
- tav: Annual average ambient temperature (°C)
- amp: Annual amplitude in mean monthly temperature (°C)
"""

from modfilegen.converter import Converter
import os
import pandas as pd
import traceback


class ApsimWeatherConverter(Converter):
    """
    Converter class for generating APSIM weather files (.met format)
    """
    
    def __init__(self):
        super().__init__()
        self.file_extension = ".met"
    
    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        """
        Generate weather data content in APSIM .met format (without writing to file).
        
        This method returns the weather file content as a string. Use this method when you need
        the content for caching or manual file writing (e.g., in parallel processing workflows).
        
        For direct file creation, use export_to_file() instead.
        
        Args:
            directory_path (str): Path containing Site and Year information
            ModelDictionary_Connection: Connection to model dictionary database
            master_input_connection: Connection to master input database
            usmdir (str): Output directory (not used, kept for compatibility)
            
        Returns:
            str: The generated weather file content (or empty string if error)
        """
        file_name = "weather.met"
        file_content = ""
        
        # Parse site and year from directory path
        ST = directory_path.split(os.sep)
        Site = ST[-2]
        Year = ST[-1]
        
        # Get default values from model dictionary
        T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, " \
            "IFNULL([defaultValueOtherSource], [Default_Value_Datamill]) As dv " \
            "From Variables Where ((model = 'apsim') And ([Table]= 'weather'));"
        
        try:
            DT = pd.read_sql_query(T, ModelDictionary_Connection)
        except Exception as e:
            print(f"Warning: Could not load default values from model dictionary: {e}")
            # Create default values if table doesn't exist
            DT = pd.DataFrame({
                'Champ': ['pan', 'vp', 'code'],
                'dv': [2.0, 20.0, '222222']
            })
        
        # Fetch weather data from master input database
        fetchAllQuery = f"SELECT * FROM RaClimateD WHERE idPoint='{Site}' " \
                       f"AND (Year={Year} OR Year={int(Year) + 1}) ORDER BY w_date;"
        
        try:
            DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        except Exception as e:
            print(f"Error fetching weather data: {e}")
            traceback.print_exc()
            return ""
        
        if DA.empty:
            print(f"Warning: No weather data found for site {Site} and year {Year}")
            return ""
        
        # Detect which optional columns are present in the data
        optional_cols = []
        if 'vp' in DA.columns:
            optional_cols.append('vp')
        if 'pan' in DA.columns:
            optional_cols.append('pan')
        if 'wind' in DA.columns:
            optional_cols.append('wind')
        
        # Get default values for optional columns
        defaults = {
            'vp': self._get_default_value(DT, 'vp', 20.0),
            'pan': self._get_default_value(DT, 'pan', 2.0),
            'wind': self._get_default_value(DT, 'wind', 2.5),
            'code': self._get_default_value(DT, 'code', '999999')
        }
        
        # Get metadata if available
        latitude = self._get_default_value(DT, 'latitude', None)
        longitude = self._get_default_value(DT, 'longitude', None)
        tav = self._get_default_value(DT, 'tav', None)
        amp = self._get_default_value(DT, 'amp', None)
        
        # Build file header
        file_content = self._build_header(Site, Year, optional_cols, latitude, longitude, tav, amp)
        
        # Process weather data
        file_content += self._build_data_rows(DA, optional_cols, defaults)
        
        # Return content only - file will be created by caller (process_chunk)
        return file_content
    
    def export_to_file(self, directory_path, ModelDictionary_Connection, master_input_connection, output_file):
        """
        Generate weather data and write directly to file.
        
        This method is a convenience wrapper that generates weather content and writes it to a file
        in one step. Use this when you want to create the weather file directly without caching.
        
        Args:
            directory_path (str): Path containing Site and Year information
            ModelDictionary_Connection: Connection to model dictionary database
            master_input_connection: Connection to master input database
            output_file (str): Full path to the output .met file
            
        Returns:
            str: Path to the created weather file (or None if error)
            
        Example:
            >>> converter = ApsimWeatherConverter()
            >>> weather_file = converter.export_to_file(
            ...     directory_path="/path/to/Site1/2020",
            ...     ModelDictionary_Connection=md_conn,
            ...     master_input_connection=mi_conn,
            ...     output_file="/output/weather.met"
            ... )
            >>> print(f"Weather file created: {weather_file}")
        """
        # Generate weather content
        content = self.export(
            directory_path=directory_path,
            ModelDictionary_Connection=ModelDictionary_Connection,
            master_input_connection=master_input_connection,
            usmdir=None  # Not used
        )
        
        if not content:
            print(f"Error: No weather content generated for {directory_path}")
            return None
        
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # Write content to file
            with open(output_file, 'w') as f:
                f.write(content)
            
            print(f"✓ Weather file created: {output_file}")
            return output_file
            
        except Exception as e:
            print(f"Error writing weather file {output_file}: {e}")
            traceback.print_exc()
            return None
    
    def _get_default_value(self, df, field_name, default):
        """
        Get default value from dataframe or return fallback default.
        
        Args:
            df (DataFrame): DataFrame containing default values
            field_name (str): Name of the field
            default: Fallback default value
            
        Returns:
            The default value for the field
        """
        try:
            row = df[df["Champ"] == field_name]
            if not row.empty:
                return row["dv"].values[0]
        except:
            pass
        return default
    
    def _build_header(self, site, year, optional_cols, latitude=None, longitude=None, tav=None, amp=None):
        """
        Build the APSIM weather file header.
        
        Args:
            site (str): Site identifier
            year (str): Year of the data
            optional_cols (list): List of optional columns present (e.g., ['vp', 'wind', 'pan'])
            latitude (float): Latitude of the site (optional)
            longitude (float): Longitude of the site (optional)
            tav (float): Annual average ambient temperature (optional)
            amp (float): Annual amplitude in mean monthly temperature (optional)
            
        Returns:
            str: Header content
        """
        header = "[weather.met.weather]\n"
        
        # Add metadata if provided
        if latitude is not None:
            header += f"latitude = {latitude}\n"
        if longitude is not None:
            header += f"longitude = {longitude}\n"
        if tav is not None:
            header += f"tav = {tav:.2f} (oC) ! Annual average ambient temperature\n"
        if amp is not None:
            header += f"amp = {amp:.2f} (oC) ! Annual amplitude in mean monthly temperature\n"
        
        # Add station description
        header += f"Station: {site}\n"
        
        # Build column headers dynamically based on available columns
        # Required columns
        col_headers = "year  day  radn  maxt  mint  rain"
        col_units = " ()   ()  (MJ/m^2) (oC) (oC)  (mm)"
        
        # Add optional columns in order: vp, pan, wind
        if 'vp' in optional_cols:
            col_headers += "   vp"
            col_units += " (hPa)"
        if 'pan' in optional_cols:
            col_headers += "  pan"
            col_units += "  (mm)"
        if 'wind' in optional_cols:
            col_headers += "  wind"
            col_units += "  (m/s)"
        
        # Always add code at the end
        col_headers += "   code\n"
        col_units += "    ()\n"
        
        header += col_headers
        header += col_units
        
        return header
    
    def _build_data_rows(self, data, optional_cols, defaults):
        """
        Build data rows for APSIM weather file with space-aligned columns.
        
        Args:
            data (DataFrame): Weather data from database
            optional_cols (list): List of optional columns present (e.g., ['vp', 'wind', 'pan'])
            defaults (dict): Default values for optional columns
            
        Returns:
            str: Formatted data rows
        """
        lines = []
        
        for _, row in data.iterrows():
            # Get year - try different possible column names
            year = row.get('year')
            if pd.isna(year) or year is None:
                year = row.get('Year', '')
            
            # Get day of year - try different possible column names
            doy = row.get('day')  # APSIM uses 'day' for DOY
            if pd.isna(doy) or doy is None:
                doy = row.get('DOY', '')
            
            # Solar radiation (srad in database -> radn in APSIM)
            radn = row.get('radn')  # Try 'radn' first (APSIM convention)
            if pd.isna(radn) or radn is None:
                radn = row.get('srad', -999.9)  # Try 'srad' (database convention)
            if pd.isna(radn):
                radn = -999.9
            
            # Temperature
            maxt = row.get('maxt')  # Try 'maxt' first (APSIM convention)
            if pd.isna(maxt) or maxt is None:
                maxt = row.get('tmax', -999.9)  # Try 'tmax' (database convention)
            if pd.isna(maxt):
                maxt = -999.9
            
            mint = row.get('mint')  # Try 'mint' first (APSIM convention)
            if pd.isna(mint) or mint is None:
                mint = row.get('tmin', -999.9)  # Try 'tmin' (database convention)
            if pd.isna(mint):
                mint = -999.9
            
            # Rainfall
            rain = row.get('rain', 0.0)
            if pd.isna(rain):
                rain = 0.0
            
            # Format the line with required columns
            line = f"{int(year):4d} {int(doy):3d} {radn:6.1f} {maxt:5.1f} {mint:5.1f} {rain:5.1f}"
            
            # Add optional columns in order: vp, pan, wind
            if 'vp' in optional_cols:
                vp = row.get('vp', defaults.get('vp', 20.0))
                if pd.isna(vp):
                    vp = defaults.get('vp', 20.0)
                line += f" {vp:5.1f}"
            
            if 'pan' in optional_cols:
                pan = row.get('pan', defaults.get('pan', 2.0))
                if pd.isna(pan):
                    pan = defaults.get('pan', 2.0)
                line += f" {pan:5.1f}"
            
            if 'wind' in optional_cols:
                wind = row.get('wind', defaults.get('wind', 2.5))
                if pd.isna(wind):
                    wind = defaults.get('wind', 2.5)
                line += f" {wind:5.1f}"
            
            # Always add code at the end
            code = row.get('code', defaults.get('code', '999999'))
            if pd.isna(code):
                code = defaults.get('code', '999999')
            line += f" {code}\n"
            
            lines.append(line)
        
        return ''.join(lines)
    
    def export_simple(self, output_path, weather_data_df, site_name="Unknown", year="2000",
                     latitude=None, longitude=None, tav=None, amp=None):
        """
        Export weather data from a pandas DataFrame to APSIM .met format.
        This is a simplified export method that doesn't require database connections.
        
        Args:
            output_path (str): Full path to output file
            weather_data_df (DataFrame): DataFrame with columns: year, day (DOY), radn, maxt, mint, rain
                                         Optional: vp, wind, code
            site_name (str): Name of the site/station
            year (str): Year of the data
            latitude (float): Latitude of the site (optional)
            longitude (float): Longitude of the site (optional)
            tav (float): Annual average ambient temperature (optional)
            amp (float): Annual amplitude in mean monthly temperature (optional)
            
        Example:
            >>> import pandas as pd
            >>> converter = ApsimWeatherConverter()
            >>> df = pd.DataFrame({
            ...     'year': [1999]*10,
            ...     'day': range(1, 11),
            ...     'radn': [8.0, 8.0, 13.0, 26.0, 25.0, 27.0, 27.0, 30.0, 26.0, 21.0],
            ...     'maxt': [23.0, 23.5, 27.5, 30.5, 30.0, 30.0, 30.5, 32.5, 33.5, 31.0],
            ...     'mint': [17.5, 18.0, 18.5, 19.0, 18.0, 17.0, 16.5, 16.5, 18.5, 20.0],
            ...     'rain': [4.9, 20.2, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.6]
            ... })
            >>> converter.export_simple('/path/to/output/weather.met', df, 'MySite', '1999',
            ...                         latitude=1.0, longitude=39.4, tav=28.7, amp=4.1)
        """
        # Detect which optional columns are present in the data
        optional_cols = []
        if 'vp' in weather_data_df.columns:
            optional_cols.append('vp')
        if 'pan' in weather_data_df.columns:
            optional_cols.append('pan')
        if 'wind' in weather_data_df.columns:
            optional_cols.append('wind')
        
        # Set default code if not present
        if 'code' not in weather_data_df.columns:
            weather_data_df['code'] = '999999'
        
        # Default values for optional columns
        defaults = {
            'vp': 20.0,
            'pan': 2.0,
            'wind': 2.5,
            'code': '999999'
        }
        
        # Build header
        file_content = self._build_header(site_name, year, optional_cols, latitude, longitude, tav, amp)
        
        # Build data rows
        file_content += self._build_data_rows(weather_data_df, optional_cols, defaults)
        
        try:
            # Ensure directory exists
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # Write file
            with open(output_path, 'w') as f:
                f.write(file_content)
            
            print(f"Successfully created weather file: {output_path}")
            return file_content
        except Exception as e:
            print(f"Error writing weather file: {e}")
            traceback.print_exc()
            return ""


if __name__ == "__main__":
    """Example usage"""
    import pandas as pd
    
    # Create sample weather data
    sample_data = pd.DataFrame({
        'year': [1999] * 15,
        'day': range(1, 16),
        'radn': [8.0, 8.0, 13.0, 26.0, 25.0, 27.0, 27.0, 30.0, 26.0, 21.0, 
                 27.0, 14.0, 27.0, 28.0, 26.0],
        'maxt': [23.0, 23.5, 27.5, 30.5, 30.0, 30.0, 30.5, 32.5, 33.5, 31.0,
                 34.5, 33.0, 33.5, 33.5, 32.0],
        'mint': [17.5, 18.0, 18.5, 19.0, 18.0, 17.0, 16.5, 16.5, 18.5, 20.0,
                 18.0, 21.0, 21.0, 20.0, 20.0],
        'rain': [4.9, 20.2, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.6,
                 5.8, 0.0, 13.4, 0.0, 0.0],
        'vp': [21.0, 23.0, 22.0, 23.0, 20.0, 18.0, 20.0, 19.0, 23.0, 25.0,
               24.0, 29.0, 25.0, 25.0, 22.0],
        'wind': [2.0, 1.8, 4.0, 7.0, 6.8, 9.2, 8.2, 9.0, 7.6, 6.0,
                 6.8, 6.0, 8.0, 10.0, 10.2],
        'code': ['999999'] * 15
    })
    
    # Create converter and export
    converter = ApsimWeatherConverter()
    converter.export_simple(
        'example_weather.met', 
        sample_data, 
        'TestSite', 
        '1999',
        latitude=-23.8,
        longitude=151.3,
        tav=22.5,
        amp=8.2
    )
    print("Example weather file created!")

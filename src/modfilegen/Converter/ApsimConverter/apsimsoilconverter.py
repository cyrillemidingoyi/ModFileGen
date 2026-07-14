"""
APSIM Soil Converter
This module converts soil data to APSIM .apsimx soil JSON format.

The soil structure in APSIM includes:
- Physical properties (BD, AirDry, LL15, DUL, SAT, KS)
- WaterBalance (SWCON, drainage parameters)
- Organic matter (Carbon, FOM, FBiom, FInert)
- Chemical properties (PH, EC, ESP, CEC)
- Initial water content
- Soil temperature
- Nutrients
"""

from modfilegen.converter import Converter
import os
import pandas as pd
import json
import traceback


class ApsimSoilConverter(Converter):
    """
    Converter class for generating APSIM soil definitions in .apsimx format
    """
    
    def __init__(self):
        super().__init__()
    
    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, output_apsimx):
        """
        Export soil data to APSIM .apsimx format.
        
        Args:
            directory_path (str): Path containing Site information
            ModelDictionary_Connection: Connection to model dictionary database
            master_input_connection: Connection to master input database
            output_apsimx (str): Path to output .apsimx file
            
        Returns:
            dict: The generated soil JSON structure
        """
        # Parse site from directory path
        ST = directory_path.split(os.sep)
        Site = ST[-2] if len(ST) >= 2 else ST[-1]
        
        # Fetch soil data from database
        fetchSoilQuery = f"SELECT * FROM RaSoilProfile WHERE idPoint='{Site}' ORDER BY layer_number;"
        
        try:
            soil_data = pd.read_sql_query(fetchSoilQuery, master_input_connection)
        except Exception as e:
            print(f"Error fetching soil data: {e}")
            traceback.print_exc()
            return None
        
        if soil_data.empty:
            print(f"Warning: No soil data found for site {Site}")
            return None
        
        # Build soil JSON structure
        soil_json = self._build_soil_structure(soil_data, Site)
        
        try:
            # If output_apsimx exists, update it; otherwise create new
            if os.path.exists(output_apsimx):
                with open(output_apsimx, 'r') as f:
                    apsimx_data = json.load(f)
                # Find and replace soil in the structure
                self._update_soil_in_apsimx(apsimx_data, soil_json)
            else:
                # Create minimal apsimx structure with soil
                apsimx_data = self._create_minimal_apsimx(soil_json)
            
            # Write updated/new file
            with open(output_apsimx, 'w') as f:
                json.dump(apsimx_data, f, indent=2)
            
            print(f"Successfully created/updated APSIM soil in: {output_apsimx}")
            return soil_json
            
        except Exception as e:
            print(f"Error writing APSIM file: {e}")
            traceback.print_exc()
            return None
    
    def export_simple(self, soil_data_df, output_apsimx, site_name="Unknown", 
                     latitude=None, longitude=None, soil_type="Unknown"):
        """
        Export soil data from a pandas DataFrame to APSIM .apsimx format.
        
        Args:
            soil_data_df (DataFrame): DataFrame with soil layers data
                Required columns: thickness, bd, ll15, dul, sat
                Optional: airdry, ks, swcon, carbon, ph, no3, nh4
            output_apsimx (str): Path to output .apsimx file
            site_name (str): Name of the site
            latitude (float): Site latitude
            longitude (float): Site longitude
            soil_type (str): Soil type description
            
        Example:
            >>> import pandas as pd
            >>> converter = ApsimSoilConverter()
            >>> soil = pd.DataFrame({
            ...     'thickness': [150, 150, 300, 300, 300],
            ...     'bd': [1.02, 1.03, 1.05, 1.10, 1.15],
            ...     'airdry': [0.10, 0.15, 0.20, 0.22, 0.24],
            ...     'll15': [0.23, 0.23, 0.24, 0.25, 0.26],
            ...     'dul': [0.46, 0.46, 0.47, 0.48, 0.49],
            ...     'sat': [0.55, 0.55, 0.54, 0.53, 0.52],
            ...     'ks': [20, 20, 20, 20, 20],
            ...     'carbon': [1.2, 0.8, 0.5, 0.3, 0.2],
            ...     'ph': [7.5, 7.8, 8.0, 8.0, 8.0]
            ... })
            >>> converter.export_simple(soil, 'output.apsimx', 'MySite', -27.5, 151.3)
        """
        # Build soil JSON structure
        soil_json = self._build_soil_from_dataframe(
            soil_data_df, site_name, latitude, longitude, soil_type
        )
        
        try:
            # If output_apsimx exists, update it; otherwise create new
            if os.path.exists(output_apsimx):
                with open(output_apsimx, 'r') as f:
                    apsimx_data = json.load(f)
                # Find and replace soil in the structure
                self._update_soil_in_apsimx(apsimx_data, soil_json)
            else:
                # Create minimal apsimx structure with soil
                apsimx_data = self._create_minimal_apsimx(soil_json)
            
            # Write updated/new file
            with open(output_apsimx, 'w') as f:
                json.dump(apsimx_data, f, indent=2)
            
            print(f"Successfully created/updated APSIM soil in: {output_apsimx}")
            return soil_json
            
        except Exception as e:
            print(f"Error writing APSIM file: {e}")
            traceback.print_exc()
            return None
    
    def _build_soil_structure(self, soil_data, site_name):
        """Build APSIM soil JSON structure from database data"""
        
        n_layers = len(soil_data)
        
        # Extract arrays for each property
        thickness = soil_data['thickness'].tolist() if 'thickness' in soil_data.columns else [150] * n_layers
        bd = soil_data['bd'].tolist() if 'bd' in soil_data.columns else [1.0] * n_layers
        airdry = soil_data['airdry'].tolist() if 'airdry' in soil_data.columns else [0.1] * n_layers
        ll15 = soil_data['ll15'].tolist() if 'll15' in soil_data.columns else [0.2] * n_layers
        dul = soil_data['dul'].tolist() if 'dul' in soil_data.columns else [0.4] * n_layers
        sat = soil_data['sat'].tolist() if 'sat' in soil_data.columns else [0.5] * n_layers
        ks = soil_data['ks'].tolist() if 'ks' in soil_data.columns else [20.0] * n_layers
        
        return self._build_soil_json(
            site_name=site_name,
            thickness=thickness,
            bd=bd,
            airdry=airdry,
            ll15=ll15,
            dul=dul,
            sat=sat,
            ks=ks,
            soil_data=soil_data
        )
    
    def _build_soil_from_dataframe(self, df, site_name, latitude, longitude, soil_type):
        """Build APSIM soil JSON structure from DataFrame"""
        
        # Required fields
        thickness = df['thickness'].tolist()
        bd = df['bd'].tolist()
        ll15 = df['ll15'].tolist()
        dul = df['dul'].tolist()
        sat = df['sat'].tolist()
        
        # Optional fields with defaults
        airdry = df['airdry'].tolist() if 'airdry' in df.columns else [ll * 0.5 for ll in ll15]
        ks = df['ks'].tolist() if 'ks' in df.columns else [20.0] * len(thickness)
        
        return self._build_soil_json(
            site_name=site_name,
            thickness=thickness,
            bd=bd,
            airdry=airdry,
            ll15=ll15,
            dul=dul,
            sat=sat,
            ks=ks,
            latitude=latitude,
            longitude=longitude,
            soil_type=soil_type,
            soil_data=df
        )
    
    def _build_soil_json(self, site_name, thickness, bd, airdry, ll15, dul, sat, ks,
                        latitude=None, longitude=None, soil_type=None, soil_data=None):
        """Build the complete APSIM soil JSON structure"""
        
        n_layers = len(thickness)
        
        # Extract optional properties from soil_data if available
        swcon = None
        carbon = None
        ph = None
        
        if soil_data is not None:
            if 'swcon' in soil_data.columns:
                swcon = soil_data['swcon'].tolist()
            if 'carbon' in soil_data.columns:
                carbon = soil_data['carbon'].tolist()
            elif 'oc' in soil_data.columns:
                carbon = soil_data['oc'].tolist()
            if 'ph' in soil_data.columns:
                ph = soil_data['ph'].tolist()
        
        # Set defaults if not provided
        if swcon is None:
            swcon = [0.3] * n_layers
        if carbon is None:
            # Default carbon profile (decreasing with depth)
            carbon = [1.2 * (0.8 ** i) for i in range(n_layers)]
        if ph is None:
            ph = [8.0] * n_layers
        
        # Calculate FOM (Fresh Organic Matter) - exponential decay with depth
        cumulative_depth = 0
        fom = []
        for t in thickness:
            cumulative_depth += t
            fom_value = 347.13 * (0.78 ** (cumulative_depth / 1000.0))
            fom.append(fom_value)
        
        soil_json = {
            "$type": "Models.Soils.Soil, Models",
            "RecordNumber": 0,
            "ASCOrder": "",
            "ASCSubOrder": "",
            "SoilType": soil_type or "Unknown",
            "LocalName": None,
            "Site": site_name,
            "NearestTown": site_name,
            "Region": "",
            "State": "",
            "Country": "",
            "NaturalVegetation": "",
            "ApsoilNumber": "",
            "Latitude": latitude or 0.0,
            "Longitude": longitude or 0.0,
            "LocationAccuracy": "",
            "YearOfSampling": "",
            "DataSource": "Generated by ModFileGen ApsimSoilConverter",
            "Comments": "",
            "Name": "Soil",
            "ResourceName": None,
            "Children": [
                {
                    "$type": "Models.Soils.Physical, Models",
                    "Thickness": thickness,
                    "ParticleSizeSand": None,
                    "ParticleSizeSilt": None,
                    "ParticleSizeClay": None,
                    "Rocks": None,
                    "Texture": None,
                    "BD": bd,
                    "AirDry": airdry,
                    "LL15": ll15,
                    "DUL": dul,
                    "SAT": sat,
                    "KS": ks,
                    "BDMetadata": None,
                    "AirDryMetadata": None,
                    "LL15Metadata": None,
                    "DULMetadata": None,
                    "SATMetadata": None,
                    "KSMetadata": None,
                    "RocksMetadata": None,
                    "TextureMetadata": None,
                    "ParticleSizeSandMetadata": None,
                    "ParticleSizeSiltMetadata": None,
                    "ParticleSizeClayMetadata": None,
                    "Name": "Physical",
                    "ResourceName": None,
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.WaterModel.WaterBalance, Models",
                    "SummerDate": "1-Nov",
                    "SummerU": 5.0,
                    "SummerCona": 5.0,
                    "WinterDate": "1-Apr",
                    "WinterU": 5.0,
                    "WinterCona": 5.0,
                    "DiffusConst": 40.0,
                    "DiffusSlope": 16.0,
                    "Salb": 0.12,
                    "CN2Bare": 73.0,
                    "CNRed": 20.0,
                    "CNCov": 0.8,
                    "DischargeWidth": "NaN",
                    "CatchmentArea": "NaN",
                    "PSIDul": -100.0,
                    "Thickness": thickness,
                    "SWCON": swcon,
                    "KLAT": None,
                    "Name": "SoilWater",
                    "ResourceName": "WaterBalance",
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.Soils.Organic, Models",
                    "FOMCNRatio": 40.0,
                    "Thickness": thickness,
                    "Carbon": carbon,
                    "CarbonUnits": 0,
                    "SoilCNRatio": [12.0] * n_layers,
                    "FBiom": [0.04 * (0.5 ** i) for i in range(n_layers)],
                    "FInert": [min(0.4 + 0.15 * i, 1.0) for i in range(n_layers)],
                    "FOM": fom,
                    "CarbonMetadata": None,
                    "FOMMetadata": None,
                    "Name": "Organic",
                    "ResourceName": None,
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.Soils.Chemical, Models",
                    "Thickness": thickness,
                    "PH": ph,
                    "PHUnits": 0,
                    "EC": None,
                    "ESP": None,
                    "CEC": None,
                    "ECMetadata": None,
                    "CLMetadata": None,
                    "ESPMetadata": None,
                    "PHMetadata": None,
                    "Name": "Chemical",
                    "ResourceName": None,
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.Soils.Water, Models",
                    "Thickness": thickness,
                    "InitialValues": dul,  # Start at field capacity
                    "InitialPAWmm": sum((d - l) * t for d, l, t in zip(dul, ll15, thickness)),
                    "RelativeTo": "LL15",
                    "FilledFromTop": False,
                    "Name": "Water",
                    "ResourceName": None,
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.Soils.CERESSoilTemperature, Models",
                    "Name": "Temperature",
                    "ResourceName": None,
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                },
                {
                    "$type": "Models.Soils.Nutrients.Nutrient, Models",
                    "Name": "Nutrient",
                    "ResourceName": "Nutrient",
                    "Children": [],
                    "Enabled": True,
                    "ReadOnly": False
                }
            ],
            "Enabled": True,
            "ReadOnly": False
        }
        
        return soil_json
    
    def _update_soil_in_apsimx(self, apsimx_data, soil_json):
        """Recursively find and replace soil in APSIM structure"""
        if isinstance(apsimx_data, dict):
            if apsimx_data.get('$type') == 'Models.Soils.Soil, Models':
                # Found soil, replace it
                apsimx_data.clear()
                apsimx_data.update(soil_json)
                return True
            elif 'Children' in apsimx_data:
                for i, child in enumerate(apsimx_data['Children']):
                    if isinstance(child, dict) and child.get('$type') == 'Models.Soils.Soil, Models':
                        apsimx_data['Children'][i] = soil_json
                        return True
                    elif self._update_soil_in_apsimx(child, soil_json):
                        return True
        return False
    
    def _create_minimal_apsimx(self, soil_json):
        """Create a minimal APSIM simulation structure with soil"""
        return {
            "$type": "Models.Core.Simulations, Models",
            "Version": 180,
            "Name": "Simulations",
            "ResourceName": None,
            "Children": [
                {
                    "$type": "Models.Core.Simulation, Models",
                    "Descriptors": None,
                    "Name": "Simulation",
                    "ResourceName": None,
                    "Children": [
                        {
                            "$type": "Models.Core.Zone, Models",
                            "Area": 1.0,
                            "Slope": 0.0,
                            "Name": "Field",
                            "ResourceName": None,
                            "Children": [
                                soil_json
                            ],
                            "Enabled": True,
                            "ReadOnly": False
                        }
                    ],
                    "Enabled": True,
                    "ReadOnly": False
                }
            ],
            "Enabled": True,
            "ReadOnly": False
        }


if __name__ == "__main__":
    """Example usage"""
    import pandas as pd
    
    # Create sample soil profile data (5 layers)
    sample_soil = pd.DataFrame({
        'thickness': [150, 150, 300, 300, 300],
        'bd': [1.02, 1.07, 1.09, 1.16, 1.17],
        'airdry': [0.13, 0.20, 0.28, 0.28, 0.28],
        'll15': [0.26, 0.25, 0.28, 0.28, 0.28],
        'dul': [0.52, 0.50, 0.49, 0.48, 0.47],
        'sat': [0.59, 0.57, 0.56, 0.53, 0.53],
        'ks': [20.0, 20.0, 20.0, 20.0, 20.0],
        'swcon': [0.3, 0.3, 0.3, 0.3, 0.3],
        'carbon': [1.2, 0.96, 0.6, 0.3, 0.18],
        'ph': [8.0, 8.0, 8.0, 8.0, 8.0]
    })
    
    # Create converter and export
    converter = ApsimSoilConverter()
    converter.export_simple(
        sample_soil,
        'Maize.apsimx',
        site_name='TestSite',
        latitude=-27.58,
        longitude=151.32,
        soil_type='Clay'
    )
    print("Example APSIM soil file created!")

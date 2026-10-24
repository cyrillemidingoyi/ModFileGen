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
    
    PARAMETER_TABLES = ("Physical", "SoilWater", "Organic", "Chemical")

    def export(self, parameter_resolver, soil_repository, output_apsimx, id_soil):
        """Generate an APSIM soil file from MasterInput data and return its path.

        Args:
            parameter_resolver (ParameterResolver): Required resolver already
                prefetched for model apsim, PARAMETER_TABLES and id_soil.
                Supplies model defaults and explicit soil overrides; Organic.FOMCNRatio
                must have a configured value.
            soil_repository (SoilDataRepository): Required repository already
                prefetched for id_soil. Supplies Soil and SoilLayers records.
            output_apsimx (str or os.PathLike): Destination .apsimx file path.
                An existing soil node is replaced if the file already exists;
                otherwise a minimal document containing the soil is created.
                The parent directory must already exist.
            id_soil (str): Required MasterInput Soil.IdSoil identifier,
                supplied by the caller from the simulation row.

        The caller owns prefetching and database connections. This method
        neither queries databases nor constructs repositories or resolvers.

        Returns:
            str: Path of the generated or updated APSIM .apsimx file.

        Raises:
            ValueError: Layer thickness is not positive,
                required soil properties, saturation or C/N parameters are missing,
                or an existing destination has no soil node to replace.
            KeyError: The requested soil is not present in the repository.
            RuntimeError: A supplied resolver was not prefetched as required.


        Physical input: depths in cm, Wwp/Wfc/Ssat in percent; APSIM uses
        mm and volumetric fractions. Layer values precede soil values.
        Model defaults and explicit overrides use APSIM child names as
        TargetTable (Physical, SoilWater, Organic, Chemical), in APSIM units.
        """
        soil = {k.casefold(): v for k, v in
                soil_repository.get_soil(id_soil).items()}
        layers = soil_repository.get_layers(id_soil)
        if not layers:
            layers = [{"Lup": 0, "Ldown": soil["soiltotaldepth"]}]

        def present(value):
            return (value is not None and pd.notna(value)
                    and str(value).strip().casefold() not in {"", "null", "-99", "-99.0"})

        # Both concentrations describe Soil, as in STICS v11 param.sol.
        # Do not mix a layer's OrganicC with a soil-wide nitrogen value.
        organic_parameters = parameter_resolver.resolve("apsim", "Organic", id_soil)
        fom_cn_ratio = organic_parameters.get('fomcnratio')
        if not present(fom_cn_ratio):
            raise ValueError(
                f"Missing Organic.FOMCNRatio for soil {id_soil}: configure "
                "the APSIM parameter in ModelsDictionary or SoilParameterOverrides")
        soil_carbon = soil.get('organicc')
        soil_nitrogen = soil.get('organicnstock')
        if parameter_resolver.has_override('apsim', 'Organic', 'SoilCNRatio', id_soil):
            soil_cn_ratio = organic_parameters.get('soilcnratio')
        elif (present(soil_carbon) and present(soil_nitrogen)
              and float(soil_nitrogen) > 0):
            soil_cn_ratio = float(soil_carbon) / float(soil_nitrogen)
        else:
            soil_cn_ratio = organic_parameters.get('soilcnratio')
        if not present(soil_cn_ratio) or float(soil_cn_ratio) <= 0:
            raise ValueError(
                f"Cannot determine SoilCNRatio for soil {id_soil}: provide "
                "Soil.OrganicC and positive Soil.OrganicNStock or configure "
                "Organic.SoilCNRatio")

        records = []
        # One APSIM layer per SoilLayers row (or one for a soil without rows).
        physical_layers = parameter_resolver.resolve_layers(
            "apsim", "Physical", id_soil, len(layers))
        for number, layer in enumerate(layers, start=1):
            layer = {k.casefold(): v for k, v in layer.items()}

            def value(name):
                result = layer.get(name)
                return result if present(result) else soil.get(name)

            thickness = (float(layer['ldown']) - float(layer['lup'])) * 10
            if thickness <= 0:
                raise ValueError(f"Non-positive layer thickness for {id_soil}")
            bd, ll, dul = (value(k) for k in ('bd', 'wwp', 'wfc'))
            if not all(present(v) for v in (bd, ll, dul)):
                raise ValueError(f"Missing BD/Wwp/Wfc for soil {id_soil}")
            sat = value('ssat')
            if parameter_resolver.has_override(
                    'apsim', 'Physical', 'SAT', id_soil, layer=number):
                sat = physical_layers[number - 1].get('sat')
            elif present(sat):
                sat = float(sat) / 100
            else:
                sat = physical_layers[number - 1].get('sat')
            if not present(sat):
                raise ValueError(
                    f"Missing Ssat for soil {id_soil}, layer "
                    f"{layer.get('numlayer', 1)}: configure Physical.SAT "
                    "or provide Soil/SoilLayers.Ssat")
            sat = float(sat)
            # Measured Ssks is in cm/h (DSSAT unit); APSIM KS is in mm/day.
            ssks = value('ssks')
            records.append(dict(ks=float(ssks) * 240 if present(ssks) else None,
                                thickness=thickness, bd=float(bd),
                                ll15=float(ll)/100, dul=float(dul)/100,
                                sat=sat, carbon=value('organicc'),
                                ph=value('ph'),
                                sand=value('sand') if present(value('sand')) else None,
                                silt=value('silt') if present(value('silt')) else None,
                                clay=value('clay') if present(value('clay')) else None,
                                rocks=value('cf') if present(value('cf')) else None,
                                texture=value('soiltexturetype') if present(value('soiltexturetype')) else None))
        soil_json = self._build_soil_json(
            site_name=str(id_soil),
            thickness=[r['thickness'] for r in records],
            bd=[r['bd'] for r in records],
            airdry=[r['ll15'] * 0.5 for r in records],
            ll15=[r['ll15'] for r in records],
            dul=[r['dul'] for r in records],
            sat=[r['sat'] for r in records],
            ks=[20.0 if r['ks'] is None else r['ks'] for r in records],
            swcon=[0.3] * len(records),
            carbon=[r['carbon'] for r in records],
            soil_cn_ratio=[float(soil_cn_ratio)] * len(records),
            fom_cn_ratio=float(fom_cn_ratio),
            ph=[r['ph'] for r in records],
            sand=[r['sand'] for r in records],
            silt=[r['silt'] for r in records],
            clay=[r['clay'] for r in records],
            rocks=[r['rocks'] for r in records],
            texture=[r['texture'] for r in records],
            soil_type=soil.get('soiltexturetype'),
        )
        # Shared measured properties win over model defaults; only an explicit
        # override (of the layer or of the whole profile) replaces them.
        measured = {'Thickness', 'BD', 'LL15', 'DUL', 'Carbon', 'PH', 'SAT',
                    'ParticleSizeSand', 'ParticleSizeSilt', 'ParticleSizeClay',
                    'Rocks', 'Texture', 'SoilCNRatio'}
        for child in soil_json['Children']:
            table = child['Name']
            if table not in self.PARAMETER_TABLES:
                continue
            parameters = parameter_resolver.resolve('apsim', table, id_soil)
            # List fields hold one value per layer; the others are whole-soil.
            per_layer = parameter_resolver.resolve_layers(
                'apsim', table, id_soil, len(records),
                [field for field, original in child.items() if isinstance(original, list)])
            for field, original in list(child.items()):
                key = field.casefold()
                if isinstance(original, list):
                    configured = [values[key] for values in per_layer]
                    if all(value is None for value in configured):
                        continue
                    layered = []
                    for number, value in enumerate(configured, start=1):
                        current = original[number - 1] if number <= len(original) else None
                        is_measured = field in measured or (
                            field == 'KS' and records[number - 1]['ks'] is not None)
                        if value is None or (is_measured and not parameter_resolver.has_override(
                                'apsim', table, field, id_soil, layer=number)):
                            layered.append(current)
                        else:
                            layered.append(str(value) if field == 'Texture' else float(value))
                    child[field] = layered
                    continue
                configured = parameters.get(key)
                if configured is None:
                    continue
                if field in measured and not parameter_resolver.has_override(
                        'apsim', table, field, id_soil):
                    continue
                child[field] = configured
        # Keep initial water consistent with possibly overridden physical data.
        physical = soil_json['Children'][0]
        water = next(c for c in soil_json['Children'] if c['Name'] == 'Water')
        water['InitialValues'] = physical['DUL'][:]
        water['InitialPAWmm'] = sum((d-l)*t for d, l, t in zip(
            physical['DUL'], physical['LL15'], physical['Thickness']))
        output_apsimx = os.fspath(output_apsimx)
        if os.path.exists(output_apsimx):
            with open(output_apsimx) as stream:
                document = json.load(stream)
            if not self._update_soil_in_apsimx(document, soil_json):
                raise ValueError(f"No soil node in {output_apsimx}")
        else:
            document = self._create_minimal_apsimx(soil_json)
        with open(output_apsimx, 'w') as stream:
            json.dump(document, stream, indent=2, allow_nan=False)
        return output_apsimx

    def export_simple(self, soil_data_df, output_apsimx, site_name="Unknown", 
                     latitude=None, longitude=None, soil_type="Unknown"):
        """
        Export soil data from a pandas DataFrame to APSIM .apsimx format.
        
        Args:
            soil_data_df (DataFrame): DataFrame with soil layers data
                Required columns: thickness, bd, ll15, dul, sat
                Optional: airdry, ks, swcon, carbon, soil_cn_ratio, ph,
                sand, silt, clay, cf, soilTextureType, no3, nh4
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
    
    def _build_soil_from_dataframe(self, df, site_name, latitude, longitude, soil_type):
        """Adapt external DataFrame columns and legacy defaults to layer lists.

        This adapter is used only by export_simple, not by MasterInput export.
        """
        
        # Required fields
        thickness = df['thickness'].tolist()
        bd = df['bd'].tolist()
        ll15 = df['ll15'].tolist()
        dul = df['dul'].tolist()
        sat = df['sat'].tolist()
        
        # Optional fields with defaults
        airdry = df['airdry'].tolist() if 'airdry' in df.columns else [ll * 0.5 for ll in ll15]
        ks = df['ks'].tolist() if 'ks' in df.columns else [20.0] * len(thickness)
        swcon = df['swcon'].tolist() if 'swcon' in df.columns else [0.3] * len(thickness)
        if 'carbon' in df.columns:
            carbon = df['carbon'].tolist()
        elif 'oc' in df.columns:
            carbon = df['oc'].tolist()
        else:
            carbon = [1.2 * (0.8 ** i) for i in range(len(thickness))]
        ph = df['ph'].tolist() if 'ph' in df.columns else [8.0] * len(thickness)
        
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
            swcon=swcon,
            carbon=carbon,
            soil_cn_ratio=(df['soil_cn_ratio'].tolist() if 'soil_cn_ratio' in df.columns
                           else [12.0] * len(thickness)),
            fom_cn_ratio=40.0,
            ph=ph,
            sand=df['sand'].tolist() if 'sand' in df.columns else None,
            silt=df['silt'].tolist() if 'silt' in df.columns else None,
            clay=df['clay'].tolist() if 'clay' in df.columns else None,
            rocks=df['cf'].tolist() if 'cf' in df.columns else None,
            texture=df['soilTextureType'].tolist() if 'soilTextureType' in df.columns else None,
        )
    
    def _build_soil_json(self, site_name, thickness, bd, airdry, ll15, dul, sat, ks,
                        *, swcon, carbon, ph, soil_cn_ratio, fom_cn_ratio,
                        latitude=None, longitude=None, soil_type=None,
                        sand=None, silt=None, clay=None, rocks=None, texture=None):
        """Build APSIM nodes from explicit layer lists, without reading columns.

        carbon and ph contain the already prepared values for each layer;
        swcon contains the water-balance drainage coefficients; soil_cn_ratio
        contains the prepared carbon-to-nitrogen ratios. fom_cn_ratio is the
        scalar C/N ratio of the fresh organic matter.
        sand, silt and clay contain particle-size percentages, if available.
        rocks contains cf percentages and texture contains SoilTextureType labels.
        """
        n_layers = len(thickness)

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
                    "ParticleSizeSand": sand,
                    "ParticleSizeSilt": silt,
                    "ParticleSizeClay": clay,
                    "Rocks": rocks,
                    "Texture": texture,
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
                    "FOMCNRatio": fom_cn_ratio,
                    "Thickness": thickness,
                    "Carbon": carbon,
                    "CarbonUnits": 0,
                    "SoilCNRatio": soil_cn_ratio,
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

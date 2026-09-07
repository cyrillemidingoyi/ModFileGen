"""
APSIM Initialization Converter
This module converts initial conditions data to APSIM .apsimx initialization format.

The initialization structure in APSIM includes:
- Initial water content by layer (Models.Soils.Water)
- Initial nitrogen levels (NO3, NH4) in Models.Soils.Chemical
- Initial organic matter (FOM) in Models.Soils.Organic
- Initial surface residue in Models.Surface.SurfaceOrganicMatter
- Initial crop state if present (LAI, biomass, root depth, etc.)

Author: ModFileGen Team
Date: 2024-2026
"""

from modfilegen.converter import Converter
import os
import pandas as pd
import json
import traceback


class ApsimInitConverter(Converter):
    """
    Converter class for generating APSIM initialization conditions in .apsimx format
    """
    
    def __init__(self):
        super().__init__()
    
    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, 
               output_apsimx, **kwargs):
        """
        Export initialization data to APSIM .apsimx format.
        
        Args:
            directory_path (str): Path containing simulation information
            ModelDictionary_Connection: Connection to model dictionary database
            master_input_connection: Connection to master input database
            output_apsimx (str): Path to output .apsimx file
            **kwargs: Additional parameters for initialization:
                - initial_water: Initial water content (default from database)
                - initial_no3: Initial NO3 content (default from database)
                - initial_nh4: Initial NH4 content (default from database)
                - initial_residue_mass: Initial surface residue mass kg/ha
                - initial_residue_type: Type of residue (wheat, maize, etc.)
                - initial_residue_cnr: C:N ratio of residue
                - crop_initial_state: Dict with crop initial conditions
                
        Returns:
            str: JSON content for the initialization
        """
        # Parse simulation info from directory path
        ST = directory_path.split(os.sep)
        id_sim = ST[-3] if len(ST) >= 3 else ST[-1]
        id_point = ST[-2] if len(ST) >= 2 else None
        
        try:
            # Fetch default values from ModelDictionary
            default_query = """
            SELECT Champ, Default_Value_Datamill, defaultValueOtherSource, 
                   IFNULL(defaultValueOtherSource, Default_Value_Datamill) As dv 
            FROM Variables 
            WHERE model = 'apsim' AND [Table] = 'initialization';
            """
            
            # Try to get defaults, if table doesn't exist, use hardcoded defaults
            try:
                DT = pd.read_sql_query(default_query, ModelDictionary_Connection)
                defaults = DT.set_index("Champ")["dv"].to_dict() if not DT.empty else {}
            except:
                defaults = {}
            
            # Fetch initialization data from MasterInput
            init_query = f"""
            SELECT SimUnitList.idIni, SimUnitList.idsoil, SimUnitList.idPoint,
                   Soil.IdSoil, Soil.SoilOption, Soil.Wwp, Soil.Wfc, Soil.bd,
                   InitialConditions.WStockinit, InitialConditions.Ninit,
                   InitialConditions.NH4init, InitialConditions.residue_mass,
                   InitialConditions.residue_type, InitialConditions.residue_cnr
            FROM InitialConditions 
            INNER JOIN (Soil INNER JOIN SimUnitList ON LOWER(Soil.IdSoil) = LOWER(SimUnitList.idsoil)) 
                ON InitialConditions.idIni = SimUnitList.idIni
            WHERE SimUnitList.idSim = '{id_sim}';
            """
            
            try:
                DA = pd.read_sql_query(init_query, master_input_connection)
                if DA.empty:
                    print(f"Warning: No initialization data found for simulation {id_sim}")
                    return self._generate_default_init(**kwargs)
                
                row = DA.to_dict(orient='records')[0]
            except Exception as e:
                print(f"Warning: Could not fetch initialization data: {e}")
                print("Using provided parameters or defaults")
                row = None
            
            # Fetch soil layers for layer-by-layer initialization
            soil_layers = []
            if row and row.get('IdSoil'):
                layer_query = f"""
                SELECT * FROM soillayers 
                WHERE LOWER(idsoil) = '{row['IdSoil'].lower()}' 
                ORDER BY NumLayer;
                """
                try:
                    soil_df = pd.read_sql_query(layer_query, master_input_connection)
                    soil_layers = soil_df.to_dict(orient='records')
                except:
                    pass
            
            # Build initialization JSON structure
            init_structure = self._build_init_structure(row, soil_layers, defaults, **kwargs)
            
            # Save to file
            with open(output_apsimx, 'w') as f:
                json.dump(init_structure, f, indent=2)
            
            print(f"✓ Created initialization file: {output_apsimx}")
            return json.dumps(init_structure, indent=2)
            
        except Exception as e:
            print(f"Error in ApsimInitConverter: {e}")
            traceback.print_exc()
            return self._generate_default_init(**kwargs)
    
    def _build_init_structure(self, row, soil_layers, defaults, **kwargs):
        """
        Build the APSIM initialization JSON structure.
        
        Args:
            row: Database row with initialization data
            soil_layers: List of soil layer dictionaries
            defaults: Default values from ModelDictionary
            **kwargs: Override parameters
            
        Returns:
            dict: APSIM initialization structure
        """
        # Initialize structure
        init_json = {
            "$type": "Models.Core.Folder, Models",
            "Name": "Initialization",
            "Children": []
        }
        
        # 1. INITIAL WATER CONDITIONS
        water_init = self._create_water_init(row, soil_layers, defaults, **kwargs)
        if water_init:
            init_json["Children"].append(water_init)
        
        # 2. INITIAL NITROGEN CONDITIONS
        nitrogen_init = self._create_nitrogen_init(row, soil_layers, defaults, **kwargs)
        if nitrogen_init:
            init_json["Children"].extend(nitrogen_init)
        
        # 3. INITIAL ORGANIC MATTER
        organic_init = self._create_organic_init(row, soil_layers, defaults, **kwargs)
        if organic_init:
            init_json["Children"].append(organic_init)
        
        # 4. INITIAL SURFACE RESIDUE
        residue_init = self._create_residue_init(row, defaults, **kwargs)
        if residue_init:
            init_json["Children"].append(residue_init)
        
        # 5. INITIAL CROP STATE (if applicable)
        crop_init = self._create_crop_init(row, defaults, **kwargs)
        if crop_init:
            init_json["Children"].extend(crop_init)
        
        return init_json
    
    def _create_water_init(self, row, soil_layers, defaults, **kwargs):
        """
        Create initial water conditions (Models.Soils.Water).
        
        Returns water content by layer at simulation start.
        """
        if 'initial_water' in kwargs:
            initial_values = kwargs['initial_water']
            thickness = kwargs.get('soil_thickness', [150, 150, 300, 300, 300])
        elif row and soil_layers:
            # Calculate from database
            thickness = []
            initial_values = []
            
            if row.get('SoilOption', '').lower() == 'simple':
                # Simple option: uniform water across profile
                water_content = (row['Wwp'] + row['WStockinit'] * (row['Wfc'] - row['Wwp']) / 100) / row['bd']
                thickness = [150, 150, 300, 300, 300]
                initial_values = [water_content] * len(thickness)
            else:
                # Layer-by-layer from database
                for layer in soil_layers[:5]:  # Max 5 layers in APSIM
                    thickness.append(int(layer.get('Thickness', 150)))
                    water = (layer['Wwp'] + row['WStockinit'] * (layer['Wfc'] - layer['Wwp']) / 100) / layer['bd']
                    initial_values.append(round(water, 4))
        else:
            # Default: at field capacity
            thickness = [150, 150, 300, 300, 300]
            initial_values = [0.35, 0.35, 0.35, 0.35, 0.35]
        
        water_node = {
            "$type": "Models.Soils.Water, Models",
            "Thickness": thickness,
            "InitialValues": initial_values,
            "InitialPAWmm": kwargs.get('initial_paw_mm', None),
            "RelativeTo": kwargs.get('water_relative_to', 'LL15'),
            "FilledFromTop": kwargs.get('filled_from_top', True),
            "Name": "Water",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
        
        return water_node
    
    def _create_nitrogen_init(self, row, soil_layers, defaults, **kwargs):
        """
        Create initial nitrogen conditions (NO3 and NH4).
        
        Returns list of chemical initialization nodes.
        """
        nodes = []
        
        # Get or calculate NO3 by layer
        if 'initial_no3' in kwargs:
            no3_values = kwargs['initial_no3']
            thickness = kwargs.get('soil_thickness', [150, 150, 300, 300, 300])
        elif row and soil_layers:
            thickness = []
            no3_values = []
            
            if row.get('SoilOption', '').lower() == 'simple':
                # Uniform distribution
                thickness = [150, 150, 300, 300, 300]
                no3_per_layer = row.get('Ninit', 50.0)
                no3_values = [round(no3_per_layer, 1)] * len(thickness)
            else:
                # Layer-by-layer
                total_ninit = row.get('Ninit', 50.0)
                num_layers = len(soil_layers)
                for layer in soil_layers[:5]:
                    thickness.append(int(layer.get('Thickness', 150)))
                    no3_values.append(round(total_ninit / num_layers, 1))
        else:
            thickness = [150, 150, 300, 300, 300]
            no3_values = [10.0, 10.0, 10.0, 10.0, 10.0]
        
        # Get or calculate NH4 by layer
        if 'initial_nh4' in kwargs:
            nh4_values = kwargs['initial_nh4']
        elif row:
            nh4_per_layer = row.get('NH4init', defaults.get('NH4init', 0.5))
            nh4_values = [float(nh4_per_layer)] * len(thickness)
        else:
            nh4_values = [0.5] * len(thickness)
        
        # Create Chemical node with NO3 and NH4
        chemical_node = {
            "$type": "Models.Soils.Chemical, Models",
            "Thickness": thickness,
            "NO3": no3_values,
            "NH4": nh4_values,
            "NO3Units": 0,  # 0 = kg/ha, 1 = ppm
            "NH4Units": 0,
            "PH": kwargs.get('initial_ph', [7.0] * len(thickness)),
            "EC": None,
            "ESP": None,
            "PHUnits": 0,  # 0 = Water, 1 = CaCl2
            "Name": "Chemical",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
        
        nodes.append(chemical_node)
        return nodes
    
    def _create_organic_init(self, row, soil_layers, defaults, **kwargs):
        """
        Create initial organic matter conditions (FOM - Fresh Organic Matter).
        
        Returns organic matter initialization node.
        """
        if 'initial_fom' in kwargs:
            fom_values = kwargs['initial_fom']
            thickness = kwargs.get('soil_thickness', [150, 150, 300, 300, 300])
        else:
            # Default FOM distribution (decreases with depth)
            thickness = [150, 150, 300, 300, 300]
            fom_values = [
                float(defaults.get('FOM_0', 300.0)),
                float(defaults.get('FOM_1', 250.0)),
                float(defaults.get('FOM_2', 200.0)),
                float(defaults.get('FOM_3', 150.0)),
                float(defaults.get('FOM_4', 100.0))
            ]
        
        organic_node = {
            "$type": "Models.Soils.Organic, Models",
            "FOMCNRatio": kwargs.get('fom_cn_ratio', 40.0),
            "Thickness": thickness,
            "FOM": fom_values,
            "Name": "OrganicInitial",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
        
        return organic_node
    
    def _create_residue_init(self, row, defaults, **kwargs):
        """
        Create initial surface residue conditions (SurfaceOrganicMatter).
        
        Returns surface organic matter initialization node.
        """
        # Get residue parameters
        if row:
            residue_mass = row.get('residue_mass', defaults.get('InitialResidueMass', 500.0))
            residue_type = row.get('residue_type', defaults.get('InitialResidueType', 'wheat'))
            residue_cnr = row.get('residue_cnr', defaults.get('InitialCNR', 80.0))
        else:
            residue_mass = kwargs.get('initial_residue_mass', defaults.get('InitialResidueMass', 500.0))
            residue_type = kwargs.get('initial_residue_type', defaults.get('InitialResidueType', 'wheat'))
            residue_cnr = kwargs.get('initial_residue_cnr', defaults.get('InitialCNR', 80.0))
        
        residue_node = {
            "$type": "Models.Surface.SurfaceOrganicMatter, Models",
            "InitialResidueName": f"{residue_type}_stubble",
            "InitialResidueType": str(residue_type),
            "InitialResidueMass": float(residue_mass),
            "InitialStandingFraction": kwargs.get('standing_fraction', 0.0),
            "InitialCPR": kwargs.get('initial_cpr', 0.0),
            "InitialCNR": float(residue_cnr),
            "Name": "SurfaceOrganicMatter",
            "ResourceName": "SurfaceOrganicMatter",
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
        
        return residue_node
    
    def _create_crop_init(self, row, defaults, **kwargs):
        """
        Create initial crop state if crop is already established.
        
        Returns list of manager scripts to initialize crop.
        """
        crop_state = kwargs.get('crop_initial_state', None)
        if not crop_state:
            return []
        
        # Extract crop initial values
        initial_lai = crop_state.get('lai', 0.0)
        initial_biomass = crop_state.get('biomass', 0.0)
        initial_root_depth = crop_state.get('root_depth', 0.0)
        initial_stage = crop_state.get('stage', 0.0)
        initial_n_uptake = crop_state.get('n_uptake', 0.0)
        
        # Create manager script to initialize crop at start
        # This uses APSIM's SetSowingData method to establish initial crop state
        manager_code = [
            "using Models.PMF;",
            "using Models.PMF.Organs;",
            "using Models.PMF.Struct;",
            "using Models.Core;",
            "using System;",
            "using System.Linq;",
            "",
            "namespace Models",
            "{",
            "    [Serializable]",
            "    public class Script : Model",
            "    {",
            "        [Link] private Plant Crop;",
            "        [Link] private Summary Summary;",
            "        ",
            "        [EventSubscribe(\"StartOfSimulation\")]",
            "        private void OnStartOfSimulation(object sender, EventArgs e)",
            "        {",
            "            if (Crop != null)",
            "            {",
            f"                // Set initial crop state: LAI={initial_lai}, Biomass={initial_biomass} kg/ha, RootDepth={initial_root_depth} mm",
            "                ",
        ]
        
        # Add LAI initialization if provided
        if initial_lai > 0:
            manager_code.extend([
                "                // Initialize Leaf Area Index",
                "                var leaf = Crop.FindChild<Leaf>();",
                "                if (leaf != null)",
                "                {",
                f"                    leaf.LAI = {initial_lai};",
                "                }",
                "                ",
            ])
        
        # Add biomass initialization if provided
        if initial_biomass > 0:
            manager_code.extend([
                "                // Initialize above-ground biomass",
                "                var structure = Crop.FindChild<Structure>();",
                "                if (structure != null)",
                "                {",
                f"                    // Distribute biomass across organs (simplified approach)",
                f"                    double totalBiomass = {initial_biomass};",
                "                    var leaf = Crop.FindChild<Leaf>();",
                "                    var stem = Crop.FindChild<GenericOrgan>(\"Stem\");",
                "                    ",
                "                    if (leaf != null)",
                "                    {",
                "                        // Allocate 40% to leaves",
                "                        leaf.Live.StructuralWt = totalBiomass * 0.4;",
                "                    }",
                "                    if (stem != null)",
                "                    {",
                "                        // Allocate 60% to stem",
                "                        stem.Live.StructuralWt = totalBiomass * 0.6;",
                "                    }",
                "                }",
                "                ",
            ])
        
        # Add root depth initialization if provided
        if initial_root_depth > 0:
            manager_code.extend([
                "                // Initialize root depth",
                "                var root = Crop.FindChild<Root>();",
                "                if (root != null)",
                "                {",
                f"                    root.RootDepth = {initial_root_depth};",
                "                }",
                "                ",
            ])
        
        # Add phenological stage initialization if provided
        if initial_stage > 0:
            manager_code.extend([
                "                // Initialize phenological stage",
                "                var phenology = Crop.FindChild<Phenology>();",
                "                if (phenology != null)",
                "                {",
                f"                    phenology.CurrentStage = {initial_stage};",
                "                }",
                "                ",
            ])
        
        # Add nitrogen uptake initialization if provided
        if initial_n_uptake > 0:
            manager_code.extend([
                "                // Initialize nitrogen uptake",
                "                var leaf = Crop.FindChild<Leaf>();",
                "                if (leaf != null)",
                "                {",
                f"                    leaf.Live.StructuralN = {initial_n_uptake} * 0.4;",
                "                }",
                "                var stem = Crop.FindChild<GenericOrgan>(\"Stem\");",
                "                if (stem != null)",
                "                {",
                f"                    stem.Live.StructuralN = {initial_n_uptake} * 0.6;",
                "                }",
                "                ",
            ])
        
        # Close the method and class
        manager_code.extend([
            "                Summary.WriteMessage(this, $\"Crop initialized with custom initial conditions\", MessageType.Information);",
            "            }",
            "        }",
            "    }",
            "}"
        ])
        
        manager_node = {
            "$type": "Models.Manager, Models",
            "CodeArray": manager_code,
            "Parameters": [
                {"Key": "LAI", "Value": str(initial_lai)},
                {"Key": "Biomass", "Value": str(initial_biomass)},
                {"Key": "RootDepth", "Value": str(initial_root_depth)},
                {"Key": "Stage", "Value": str(initial_stage)},
                {"Key": "NUptake", "Value": str(initial_n_uptake)}
            ],
            "Name": "CropInitialization",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
        
        return [manager_node]
    
    def _generate_default_init(self, **kwargs):
        """
        Generate default initialization when database is unavailable.
        """
        defaults = {}
        return json.dumps(self._build_init_structure(None, [], defaults, **kwargs), indent=2)
    
    def export_to_file(self, output_apsimx, **kwargs):
        """
        Simplified export that uses only keyword arguments (no database).
        
        Args:
            output_apsimx: Output file path
            **kwargs: All initialization parameters as keyword arguments
            
        Example:
            converter = ApsimInitConverter()
            converter.export_to_file(
                "init.apsimx",
                initial_water=[0.35, 0.35, 0.35, 0.35, 0.35],
                initial_no3=[10.0, 10.0, 10.0, 10.0, 10.0],
                initial_nh4=[0.5, 0.5, 0.5, 0.5, 0.5],
                initial_residue_mass=500.0,
                initial_residue_type="wheat"
            )
        """
        init_structure = self._build_init_structure(None, [], {}, **kwargs)
        
        with open(output_apsimx, 'w') as f:
            json.dump(init_structure, f, indent=2)
        
        print(f"✓ Created initialization file: {output_apsimx}")
        return json.dumps(init_structure, indent=2)

"""
APSIM Management Converter

This module converts management data (sowing, fertilization, irrigation, harvest)
to APSIM Next Generation management operations format.

Management operations can be:
1. Embedded directly in simulation .apsimx files
2. Stored in separate toolbox .apsimx files for reuse across multiple simulations

Author: Generated for ModFileGen project
"""

import pandas as pd
import sqlite3
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

# Handle imports for both package and standalone execution
try:
    from modfilegen.converter import Converter
except ModuleNotFoundError:
    # Add parent directories to path for standalone execution
    script_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    if package_dir not in sys.path:
        sys.path.insert(0, package_dir)
    try:
        from modfilegen.converter import Converter
    except ModuleNotFoundError:
        print("Warning: Could not import Converter base class. Using fallback.")
        # Fallback base class
        class Converter:
            def __init__(self):
                pass


class ApsimManagementConverter(Converter):
    """
    Convert management operations from database to APSIM format.
    
    Supports:
    - Sowing operations (date-based and rule-based)
    - Fertilization (at sowing, fixed dates, split applications)
    - Irrigation (automatic and scheduled)
    - Harvest operations
    - Tillage operations
    - Residue management
    """
    
    def __init__(self):
        """Initialize the APSIM management converter."""
        super().__init__()
        self.output_format = "apsimx"
    
    # ==================== Main Export Methods ====================
    
    def export(self, 
               directory_path: str,
               ModelDictionary_Connection: sqlite3.Connection,
               master_input_connection: sqlite3.Connection,
               output_apsimx: str = None) -> str:
        """
        Export management operations from database to APSIM format.
        
        Args:
            directory_path: Output directory for management files (contains simulation ID)
            ModelDictionary_Connection: Connection to model dictionary database
            master_input_connection: Connection to master input database
            output_apsimx: Optional path to .apsimx file to update/create
        
        Returns:
            str: Path to the created/updated management file
        """
        # Query management data from database using CropManagement table
        # Automatically extracts all available operations
        management_data = self._query_management_data(
            master_input_connection,
            directory_path
        )
        
        if management_data.empty:
            print("No management data found in database")
            return None
        
        # Determine output file
        if output_apsimx is None:
            output_apsimx = os.path.join(directory_path, "management.apsimx")
        
        # Export using the simple method
        return self.export_simple(
            management_data,
            output_apsimx
        )
    
    def export_simple(self,
                     management_df: pd.DataFrame,
                     output_apsimx: str,
                     toolbox_name: str = "Management Operations") -> str:
        """
        Export management operations from DataFrame to APSIM format.
        
        Args:
            management_df: DataFrame with management operations
                          Required columns: operation_type, date, crop (optional: amount, depth, etc.)
            output_apsimx: Path to output .apsimx file
            toolbox_name: Name for the management toolbox/folder
        
        Returns:
            str: Path to the created/updated file
        """
        # Build management operations
        operations = self._build_management_operations(management_df)
        
        # Check if file exists
        if os.path.exists(output_apsimx):
            # Update existing file
            with open(output_apsimx, 'r') as f:
                apsimx_data = json.load(f)
            
            # Add or update management folder
            apsimx_data = self._add_management_to_apsimx(
                apsimx_data,
                operations,
                toolbox_name
            )
        else:
            # Create new toolbox file
            apsimx_data = self._create_management_toolbox(
                operations,
                toolbox_name
            )
        
        # Write to file
        os.makedirs(os.path.dirname(os.path.abspath(output_apsimx)), exist_ok=True)
        with open(output_apsimx, 'w') as f:
            json.dump(apsimx_data, f, indent=2)
        
        print(f"Successfully created/updated APSIM management in: {output_apsimx}")
        return output_apsimx
    
    # ==================== Management Operation Builders ====================
    
    def _build_management_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Build APSIM Manager objects from management DataFrame.
        
        Args:
            df: DataFrame with management operations
        
        Returns:
            List of Manager objects (dictionaries)
        """
        operations = []
        
        # Group by operation type
        if 'operation_type' not in df.columns:
            print("Warning: 'operation_type' column not found. Attempting to infer...")
            df = self._infer_operation_types(df)
        
        for op_type in df['operation_type'].unique():
            op_df = df[df['operation_type'] == op_type]
            
            if op_type.lower() in ['sow', 'sowing', 'planting']:
                operations.extend(self._build_sowing_operations(op_df))
            elif op_type.lower() in ['fertilize', 'fertilization', 'fertiliser']:
                operations.extend(self._build_fertilization_operations(op_df))
            elif op_type.lower() in ['irrigate', 'irrigation']:
                operations.extend(self._build_irrigation_operations(op_df))
            elif op_type.lower() in ['harvest', 'harvesting']:
                operations.extend(self._build_harvest_operations(op_df))
            elif op_type.lower() in ['tillage', 'till']:
                operations.extend(self._build_tillage_operations(op_df))
            else:
                print(f"Warning: Unknown operation type '{op_type}', skipping...")
        
        return operations
    
    def _build_sowing_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Build sowing manager operations."""
        operations = []
        
        for idx, row in df.iterrows():
            # Check if it's a rule-based or date-based sowing
            if 'sowing_rule' in df.columns and pd.notna(row.get('sowing_rule')):
                # Rule-based sowing
                operation = self._create_sowing_rule(row)
            else:
                # Date-based sowing
                operation = self._create_sowing_on_date(row)
            
            operations.append(operation)
        
        return operations
    
    def _create_sowing_rule(self, row: pd.Series) -> Dict[str, Any]:
        """Create a rule-based sowing manager."""
        crop = row.get('crop', 'Crop')
        cultivar = row.get('cultivar', row.get('variety', 'Default'))
        population = row.get('population', row.get('density', 10.0))
        depth = row.get('depth', row.get('sowing_depth', 30.0))
        row_spacing = row.get('row_spacing', 500.0)
        
        # Sowing window
        start_date = row.get('start_date', '1-oct')
        end_date = row.get('end_date', '31-dec')
        
        # Sowing criteria
        min_esw = row.get('min_esw', 100.0)  # mm
        min_rain = row.get('min_rain', 25.0)  # mm
        rain_days = int(row.get('rain_days', 7))  # days
        
        code = f"""using Models.Interfaces;
using APSIM.Shared.Utilities;
using Models.Utilities;
using Models.Soils;
using Models.PMF;
using Models.Core;
using System;
using System.Linq;
using Models.Climate;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] private Clock Clock;
        [Link] private Fertiliser Fertiliser;
        [Link] private Summary Summary;
        [Link] private Soil Soil;
        private Accumulator accumulatedRain;
        [Link]
        private ISoilWater waterBalance;
        
        [Description("Crop")]
        public IPlant Crop {{ get; set; }}

        [Description("Start of sowing window (d-mmm)")]
        public string StartDate {{ get; set; }}

        [Description("End of sowing window (d-mmm)")]
        public string EndDate {{ get; set; }}

        [Description("Minimum extractable soil water for sowing (mm)")]
        public double MinESW {{ get; set; }}

        [Description("Accumulated rainfall required for sowing (mm)")]
        public double MinRain {{ get; set; }}

        [Description("Duration of rainfall accumulation (d)")]
        public int RainDays {{ get; set; }}

        [Display(Type = DisplayType.CultivarName)]
        [Description("Cultivar to be sown")]
        public string CultivarName {{ get; set; }}

        [Description("Sowing depth (mm)")]
        public double SowingDepth {{ get; set; }}

        [Description("Row spacing (mm)")]
        public double RowSpacing {{ get; set; }}

        [Description("Plant population (/m2)")]
        public double Population {{ get; set; }}
        
        
        [EventSubscribe("StartOfSimulation")]
        private void OnSimulationCommencing(object sender, EventArgs e)
        {{
            accumulatedRain = new Accumulator(this, "[Weather].Rain", RainDays);
        }}

        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            accumulatedRain.Update();
            
            if (DateUtilities.WithinDates(StartDate, Clock.Today, EndDate) &&
                !Crop.IsAlive &&
                MathUtilities.Sum(waterBalance.ESW) > MinESW &&
                accumulatedRain.Sum > MinRain)
            {{
                Crop.Sow(population: Population, cultivar: CultivarName, depth: SowingDepth, rowSpacing: RowSpacing);    
            }}
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "Crop", "Value": crop},
                {"Key": "StartDate", "Value": start_date},
                {"Key": "EndDate", "Value": end_date},
                {"Key": "MinESW", "Value": str(min_esw)},
                {"Key": "MinRain", "Value": str(min_rain)},
                {"Key": "RainDays", "Value": str(rain_days)},
                {"Key": "CultivarName", "Value": cultivar},
                {"Key": "SowingDepth", "Value": str(depth)},
                {"Key": "RowSpacing", "Value": str(row_spacing)},
                {"Key": "Population", "Value": str(population)}
            ],
            "Name": f"Sow {crop} using a rule",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _create_sowing_on_date(self, row: pd.Series) -> Dict[str, Any]:
        """Create a date-based sowing manager."""
        crop = row.get('crop', 'Crop')
        cultivar = row.get('cultivar', row.get('variety', 'Default'))
        population = row.get('population', row.get('density', 10.0))
        depth = row.get('depth', row.get('sowing_depth', 30.0))
        row_spacing = row.get('row_spacing', 500.0)
        sowing_date = row.get('date', row.get('sowing_date', '15-nov'))
        
        code = f"""using Models.Soils;
using System;
using Models.Core;
using Models.PMF;
using APSIM.Shared.Utilities;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        
        [Description("Crop to sow")]
        public IPlant Crop {{ get; set; }}
        
        [Description("Sowing date (d-mmm)")]
        public string SowingDate {{ get; set; }}
        
        [Display(Type = DisplayType.CultivarName)]
        [Description("Cultivar to be sown")]
        public string CultivarName {{ get; set; }}

        [Description("Sowing depth (mm)")]
        public double SowingDepth {{ get; set; }}

        [Description("Row spacing (mm)")]
        public double RowSpacing {{ get; set; }}

        [Description("Plant population (/m2)")]
        public double Population {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (DateUtilities.DatesEqual(SowingDate, Clock.Today) && !Crop.IsAlive)
            {{
                Crop.Sow(population: Population, cultivar: CultivarName, depth: SowingDepth, rowSpacing: RowSpacing);
            }}
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "Crop", "Value": crop},
                {"Key": "SowingDate", "Value": sowing_date},
                {"Key": "CultivarName", "Value": cultivar},
                {"Key": "SowingDepth", "Value": str(depth)},
                {"Key": "RowSpacing", "Value": str(row_spacing)},
                {"Key": "Population", "Value": str(population)}
            ],
            "Name": f"Sow {crop} on a fixed date",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _build_fertilization_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Build fertilization manager operations."""
        operations = []
        
        for idx, row in df.iterrows():
            # Check fertilization type
            fert_timing = row.get('timing', 'at_sowing')
            
            # Handle NaN values
            if pd.isna(fert_timing):
                fert_timing = 'at_sowing'
            
            if str(fert_timing).lower() in ['at_sowing', 'atsowing', 'sowing']:
                operation = self._create_fertilize_at_sowing(row)
            else:
                operation = self._create_fertilize_on_date(row)
            
            operations.append(operation)
        
        return operations
    
    def _create_fertilize_at_sowing(self, row: pd.Series) -> Dict[str, Any]:
        """Create fertilization at sowing manager."""
        crop = row.get('crop', 'Crop')
        fert_type = row.get('fertilizer_type', row.get('type', 'NO3N'))
        amount = row.get('amount', 100.0)
        
        code = f"""using Models.Soils;
using System;
using System.Linq;
using Models.Core;
using Models.PMF;
namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        [Link] Fertiliser Fertiliser;
        
        [Description("Crop to be fertilised")]
        public IPlant Crop {{ get; set; }}

        [Description("Type of fertiliser to apply? ")] 
        public Fertiliser.Types FertiliserType {{ get; set; }}
    
        [Description("Amount of fertiliser to be applied (kg/ha)")]
        public double Amount {{ get; set; }}
        
        [EventSubscribe("Sowing")]
        private void OnSowing(object sender, EventArgs e)
        {{
            Model crop = sender as Model;
            if (Crop != null && crop.Name.ToLower() == (Crop as IModel).Name.ToLower())
                Fertiliser.Apply(Amount: Amount, Type: FertiliserType);
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "Crop", "Value": crop},
                {"Key": "FertiliserType", "Value": fert_type},
                {"Key": "Amount", "Value": str(amount)}
            ],
            "Name": f"Fertilise {crop} at sowing",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _create_fertilize_on_date(self, row: pd.Series) -> Dict[str, Any]:
        """Create fertilization on specific date manager."""
        fert_type = row.get('fertilizer_type', row.get('type', 'NO3N'))
        amount = row.get('amount', 100.0)
        fert_date = row.get('date', '15-nov')
        
        code = f"""using Models.Soils;
using System;
using Models.Core;
using APSIM.Shared.Utilities;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        [Link] Fertiliser Fertiliser;
        
        [Description("Fertilisation date (d-mmm)")]
        public string FertDate {{ get; set; }}

        [Description("Type of fertiliser to apply")] 
        public Fertiliser.Types FertiliserType {{ get; set; }}
    
        [Description("Amount of fertiliser to be applied (kg/ha)")]
        public double Amount {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (DateUtilities.DatesEqual(FertDate, Clock.Today))
                Fertiliser.Apply(Amount: Amount, Type: FertiliserType);
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "FertDate", "Value": fert_date},
                {"Key": "FertiliserType", "Value": fert_type},
                {"Key": "Amount", "Value": str(amount)}
            ],
            "Name": f"Fertilise on {fert_date}",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _build_irrigation_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Build irrigation manager operations."""
        operations = []
        
        for idx, row in df.iterrows():
            operation = self._create_irrigation(row)
            operations.append(operation)
        
        return operations
    
    def _create_irrigation(self, row: pd.Series) -> Dict[str, Any]:
        """Create irrigation manager."""
        crop = row.get('crop', 'Crop')
        amount = row.get('amount', 25.0)
        
        # Check if it's automatic or scheduled
        if 'automatic' in row and row['automatic']:
            # Auto irrigation based on soil water deficit
            threshold = row.get('threshold', 0.5)  # Fraction of available water
            
            code = f"""using Models.Soils;
using Models.PMF;
using Models.Core;
using System;
using System.Linq;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        [Link] Irrigation Irrigation;
        [Link] ISoilWater WaterBalance;
        
        [Description("Crop")]
        public IPlant Crop {{ get; set; }}
        
        [Description("Irrigation amount (mm)")]
        public double Amount {{ get; set; }}
        
        [Description("Critical fraction of available water")]
        public double CriticalFraction {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (Crop.IsAlive)
            {{
                double[] esw = WaterBalance.ESW;
                double[] paw = WaterBalance.PAW;
                double totalESW = esw.Sum();
                double totalPAW = paw.Sum();
                
                if (totalPAW > 0 && totalESW / totalPAW < CriticalFraction)
                {{
                    Irrigation.Apply(Amount);
                }}
            }}
        }}
    }}
}}"""
            
            return {
                "$type": "Models.Manager, Models",
                "CodeArray": code.split('\n'),
                "Parameters": [
                    {"Key": "Crop", "Value": crop},
                    {"Key": "Amount", "Value": str(amount)},
                    {"Key": "CriticalFraction", "Value": str(threshold)}
                ],
                "Name": f"Auto irrigation for {crop}",
                "ResourceName": None,
                "Children": [],
                "Enabled": True,
                "ReadOnly": False
            }
        else:
            # Scheduled irrigation
            irrig_date = row.get('date', '15-dec')
            
            code = f"""using Models.Core;
using System;
using APSIM.Shared.Utilities;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        [Link] Irrigation Irrigation;
        
        [Description("Irrigation date (d-mmm)")]
        public string IrrigDate {{ get; set; }}
        
        [Description("Irrigation amount (mm)")]
        public double Amount {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (DateUtilities.DatesEqual(IrrigDate, Clock.Today))
                Irrigation.Apply(Amount);
        }}
    }}
}}"""
            
            return {
                "$type": "Models.Manager, Models",
                "CodeArray": code.split('\n'),
                "Parameters": [
                    {"Key": "IrrigDate", "Value": irrig_date},
                    {"Key": "Amount", "Value": str(amount)}
                ],
                "Name": f"Irrigate on {irrig_date}",
                "ResourceName": None,
                "Children": [],
                "Enabled": True,
                "ReadOnly": False
            }
    
    def _build_harvest_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Build harvest manager operations."""
        operations = []
        
        for idx, row in df.iterrows():
            operation = self._create_harvest(row)
            operations.append(operation)
        
        return operations
    
    def _create_harvest(self, row: pd.Series) -> Dict[str, Any]:
        """Create harvest manager."""
        crop = row.get('crop', 'Crop')
        
        code = f"""using APSIM.Shared.Utilities;
using Models.Utilities;
using Models.Soils;
using Models.PMF;
using Models.Core;
using System;
using System.Linq;

namespace Models
{{
    [Serializable] 
    public class Script : Model
    {{
        [Description("Crop")]
        public IPlant Crop {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (Crop.IsReadyForHarvesting)
            {{
                Crop.Harvest();
                Crop.EndCrop();
            }}
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "Crop", "Value": crop}
            ],
            "Name": f"Harvest {crop}",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _build_tillage_operations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Build tillage manager operations."""
        operations = []
        
        for idx, row in df.iterrows():
            operation = self._create_tillage(row)
            operations.append(operation)
        
        return operations
    
    def _create_tillage(self, row: pd.Series) -> Dict[str, Any]:
        """Create tillage manager."""
        till_date = row.get('date', '1-oct')
        till_type = row.get('tillage_type', 'disc')
        
        code = f"""using Models.Soils;
using Models.Core;
using System;
using APSIM.Shared.Utilities;

namespace Models
{{
    [Serializable]
    public class Script : Model
    {{
        [Link] Clock Clock;
        [Link] Tillage Tillage;
        
        [Description("Tillage date (d-mmm)")]
        public string TillageDate {{ get; set; }}
        
        [Description("Tillage type")]
        public string TillageType {{ get; set; }}
        
        [EventSubscribe("DoManagement")]
        private void OnDoManagement(object sender, EventArgs e)
        {{
            if (DateUtilities.DatesEqual(TillageDate, Clock.Today))
                Tillage.Apply(TillageType);
        }}
    }}
}}"""
        
        return {
            "$type": "Models.Manager, Models",
            "CodeArray": code.split('\n'),
            "Parameters": [
                {"Key": "TillageDate", "Value": till_date},
                {"Key": "TillageType", "Value": till_type}
            ],
            "Name": f"Tillage on {till_date}",
            "ResourceName": None,
            "Children": [],
            "Enabled": True,
            "ReadOnly": False
        }
    
    # ==================== APSIMX File Management ====================
    
    def _create_management_toolbox(self, 
                                   operations: List[Dict[str, Any]],
                                   toolbox_name: str) -> Dict[str, Any]:
        """Create a new .apsimx file as a management toolbox."""
        return {
            "$type": "Models.Core.Simulations, Models",
            "Version": 174,
            "Name": toolbox_name,
            "ResourceName": None,
            "Children": [
                {
                    "$type": "Models.Core.Folder, Models",
                    "ShowInDocs": False,
                    "GraphsPerPage": 6,
                    "Name": "Management",
                    "ResourceName": None,
                    "Children": operations,
                    "Enabled": True,
                    "ReadOnly": False
                }
            ],
            "Enabled": True,
            "ReadOnly": False
        }
    
    def _add_management_to_apsimx(self,
                                  apsimx_data: Dict[str, Any],
                                  operations: List[Dict[str, Any]],
                                  folder_name: str) -> Dict[str, Any]:
        """Add management operations to existing .apsimx file."""
        # Find or create Management folder
        children = apsimx_data.get('Children', [])
        
        # Look for existing Management folder
        mgmt_folder = None
        for child in children:
            if child.get('$type') == 'Models.Core.Folder, Models' and \
               child.get('Name', '').lower() == folder_name.lower():
                mgmt_folder = child
                break
        
        if mgmt_folder is None:
            # Create new Management folder
            mgmt_folder = {
                "$type": "Models.Core.Folder, Models",
                "ShowInDocs": False,
                "GraphsPerPage": 6,
                "Name": folder_name,
                "ResourceName": None,
                "Children": [],
                "Enabled": True,
                "ReadOnly": False
            }
            children.append(mgmt_folder)
        
        # Add operations to the folder
        mgmt_folder['Children'].extend(operations)
        
        return apsimx_data
    
    # ==================== Database Query Methods ====================
    
    def _query_management_data(self,
                              connection: sqlite3.Connection,
                              directory_path: str) -> pd.DataFrame:
        """
        Query management data from CropManagement table.
        Automatically extracts all available operations (sowing, fertilization, tillage, harvest).
        
        Args:
            connection: Connection to master input database
            directory_path: Path containing site/simulation identifier
        
        Returns:
            DataFrame with management operations
        """
        # Extract simulation ID from directory path
        ST = directory_path.split(os.sep)
        sim_id = ST[-3] if len(ST) >= 3 else ST[-1]
        
        # Query basic crop management data
        base_query = """
        SELECT 
            SimUnitList.idsim, 
            SimUnitList.idMangt, 
            ListCultivars.idcultivarStics as cultivar,
            ListCultivars.SpeciesName as crop,
            CropManagement.sdens as population,
            CropManagement.sowingdate,
            CropManagement.SoilTillPolicyCode,
            CropManagement.OFertiPolicyCode,
            CropManagement.InoFertiPolicyCode
        FROM ListCultivars 
        INNER JOIN (CropManagement 
        INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt)
        ON ListCultivars.IdCultivar = CropManagement.Idcultivar
        WHERE idSim = ?
        """
        
        base_data = pd.read_sql_query(base_query, connection, params=[sim_id])
        
        if base_data.empty:
            print(f"Warning: No management data found for simulation {sim_id}")
            return pd.DataFrame()
        
        # Build comprehensive management operations DataFrame
        operations = []
        
        # Add sowing operation
        for _, row in base_data.iterrows():
            operations.append({
                'operation_type': 'sowing',
                'date': self._format_apsim_date(row['sowingdate']),
                'crop': row['crop'],
                'cultivar': row['cultivar'],
                'population': row['population'],
                'depth': 30.0,  # Default, can be customized
                'row_spacing': 500.0  # Default, can be customized
            })
        
        # Query organic fertilization operations
        # Skip if OFertiPolicyCode is 0 or '0' (no organic fertilization policy)
        ofert_code = base_data.iloc[0]['OFertiPolicyCode']
        if not base_data.empty and pd.notna(ofert_code) and str(ofert_code) != '0':
            organic_query = """
            SELECT 
                CropManagement.sowingdate, 
                OrganicFOperations.Dferti, 
                OrganicFOperations.OFNumber, 
                OrganicFOperations.CNferti, 
                OrganicFOperations.NFerti, 
                OrganicFOperations.Qmanure, 
                OrganicFOperations.TypeResidues
            FROM OrganicFertilizationPolicy 
            INNER JOIN (CropManagement 
            INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
            ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode
            INNER JOIN OrganicFOperations ON OrganicFertilizationPolicy.OFertiPolicyCode = OrganicFOperations.OFertiPolicyCode
            WHERE idSim = ?
            ORDER BY OFNumber
            """
            
            try:
                organic_data = pd.read_sql_query(organic_query, connection, params=[sim_id])
                for _, row in organic_data.iterrows():
                    if pd.notna(row['Qmanure']):
                        # Convert organic fertilization to APSIM format
                        operations.append({
                            'operation_type': 'fertilization',
                            'date': self._format_apsim_date(row['sowingdate'] + row['Dferti']),
                            'fertilizer_type': 'UreaN',  # Map from organic type
                            'amount': float(row['NFerti']) if pd.notna(row['NFerti']) else 0.0,
                            'crop': base_data.iloc[0]['crop']
                        })
            except Exception as e:
                print(f"Warning: Could not query organic fertilization: {e}")
        
        # Query inorganic fertilization operations
        # Skip if InoFertiPolicyCode is 0 or '0' (no inorganic fertilization policy)
        inofert_code = base_data.iloc[0]['InoFertiPolicyCode']
        if not base_data.empty and pd.notna(inofert_code) and str(inofert_code) != '0':
            inorganic_query = """
            SELECT 
                SimUnitList.idsim, 
                InorganicFOperations.N, 
                CropManagement.sowingdate, 
                InorganicFOperations.Dferti, 
                InorganicFertilizationPolicy.NumInorganicFerti
            FROM (InorganicFertilizationPolicy 
            INNER JOIN InorganicFOperations ON InorganicFertilizationPolicy.InorgFertiPolicyCode = InorganicFOperations.InorgFertiPolicyCode)
            INNER JOIN (CropManagement 
            INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
            ON InorganicFertilizationPolicy.InorgFertiPolicyCode = CropManagement.InoFertiPolicyCode
            WHERE idSim = ?
            """
            
            try:
                inorganic_data = pd.read_sql_query(inorganic_query, connection, params=[sim_id])
                for _, row in inorganic_data.iterrows():
                    operations.append({
                        'operation_type': 'fertilization',
                        'date': self._format_apsim_date(row['sowingdate'] + row['Dferti']),
                        'fertilizer_type': 'NO3N',
                        'amount': float(row['N']) if pd.notna(row['N']) else 0.0,
                        'crop': base_data.iloc[0]['crop']
                    })
            except Exception as e:
                print(f"Warning: Could not query inorganic fertilization: {e}")
        
        # Query tillage operations
        if not base_data.empty and pd.notna(base_data.iloc[0]['SoilTillPolicyCode']):
            tillage_query = """
            SELECT 
                SoilTillPolicy.SoilTillPolicyCode, 
                SoilTillageOperations.STNumber, 
                SoilTillPolicy.NumTillOperations, 
                SoilTillageOperations.DepthResUp, 
                SoilTillageOperations.DepthResLow, 
                SoilTillageOperations.DSTill
            FROM SoilTillPolicy 
            INNER JOIN SoilTillageOperations ON SoilTillPolicy.SoilTillPolicyCode = SoilTillageOperations.SoilTillPolicyCode
            WHERE SoilTillPolicy.SoilTillPolicyCode = ?
            """
            
            try:
                tillage_data = pd.read_sql_query(tillage_query, connection, 
                                                 params=[base_data.iloc[0]['SoilTillPolicyCode']])
                for _, row in tillage_data.iterrows():
                    operations.append({
                        'operation_type': 'tillage',
                        'date': self._format_apsim_date(base_data.iloc[0]['sowingdate'] + row['DSTill']),
                        'tillage_type': 'disc'  # Default type
                    })
            except Exception as e:
                print(f"Warning: Could not query tillage operations: {e}")
        
        # Add automatic harvest
        if not base_data.empty:
            operations.append({
                'operation_type': 'harvest',
                'crop': base_data.iloc[0]['crop']
            })
        
        return pd.DataFrame(operations)
    
    def _format_apsim_date(self, day_of_year: int, year: int = None) -> str:
        """
        Convert day of year to APSIM date format (d-mmm).
        
        Args:
            day_of_year: Day of year (1-365)
            year: Optional year (for leap years)
        
        Returns:
            Date string in APSIM format (e.g., '15-nov')
        """
        from datetime import datetime, timedelta
        
        try:
            if year is None:
                year = 2000  # Use a leap year as default
            
            # Convert day of year to date
            base_date = datetime(year, 1, 1)
            target_date = base_date + timedelta(days=int(day_of_year) - 1)
            
            # Format as d-mmm (e.g., 15-nov)
            return target_date.strftime('%-d-%b').lower()
        except:
            return '1-jan'  # Fallback
    
    def _infer_operation_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Infer operation types from column names if not explicitly provided."""
        # Simple inference logic
        df = df.copy()
        df['operation_type'] = 'unknown'
        
        if 'sowing_date' in df.columns or 'cultivar' in df.columns:
            df.loc[df['sowing_date'].notna() | df['cultivar'].notna(), 'operation_type'] = 'sowing'
        
        if 'fertilizer_type' in df.columns or 'fertiliser_type' in df.columns:
            df.loc[df['fertilizer_type'].notna() | df['fertiliser_type'].notna(), 'operation_type'] = 'fertilization'
        
        if 'irrigation_date' in df.columns or 'irrigation_amount' in df.columns:
            df.loc[df['irrigation_date'].notna() | df['irrigation_amount'].notna(), 'operation_type'] = 'irrigation'
        
        return df


# ==================== Example Usage ====================

if __name__ == "__main__":
    """Example demonstrating management converter usage."""
    
    print("=" * 70)
    print("APSIM MANAGEMENT CONVERTER - EXAMPLE USAGE")
    print("=" * 70)
    
    # Create sample management data
    management_data = pd.DataFrame([
        {
            'operation_type': 'sowing',
            'crop': 'Maize',
            'cultivar': 'Pioneer_3394',
            'start_date': '15-oct',
            'end_date': '15-dec',
            'population': 8.0,
            'depth': 40.0,
            'row_spacing': 750.0,
            'min_esw': 100.0,
            'min_rain': 25.0,
            'rain_days': 7,
            'sowing_rule': True
        },
        {
            'operation_type': 'fertilization',
            'crop': 'Maize',
            'timing': 'at_sowing',
            'fertilizer_type': 'NO3N',
            'amount': 150.0
        },
        {
            'operation_type': 'fertilization',
            'date': '1-jan',
            'fertilizer_type': 'UreaN',
            'amount': 50.0
        },
        {
            'operation_type': 'irrigation',
            'crop': 'Maize',
            'automatic': True,
            'threshold': 0.5,
            'amount': 30.0
        },
        {
            'operation_type': 'harvest',
            'crop': 'Maize'
        }
    ])
    
    print("\nManagement operations to convert:")
    print(management_data[['operation_type', 'crop']].to_string())
    
    # Create converter
    converter = ApsimManagementConverter()
    
    # Export to management toolbox with absolute path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "example_management.apsimx")
    
    print(f"\nCreating output file at: {output_file}")
    
    try:
        result_file = converter.export_simple(
            management_data,
            output_file,
            toolbox_name="Maize Management"
        )
        
        # Verify file was created
        if os.path.exists(result_file):
            file_size = os.path.getsize(result_file)
            print(f"\n✓ Management toolbox successfully created!")
            print(f"  - Location: {result_file}")
            print(f"  - File size: {file_size:,} bytes")
            print(f"  - {len(management_data)} operations included")
            print(f"  - Operations: {', '.join(management_data['operation_type'].unique())}")
            print("\nThis toolbox can now be referenced from multiple simulation .apsimx files")
        else:
            print(f"\n✗ Error: File was not created at {result_file}")
    
    except Exception as e:
        print(f"\n✗ Error during export: {e}")
        import traceback
        traceback.print_exc()

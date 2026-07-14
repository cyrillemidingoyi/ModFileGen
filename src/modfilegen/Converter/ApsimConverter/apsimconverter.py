"""
APSIM Converter - Main orchestrator for APSIM Next Generation conversions

This module manages the complete workflow for APSIM simulations:
- Weather file generation (.met)
- Soil profile conversion (.apsimx format)
- Management operations (sowing, fertilization, tillage, harvest)
- APSIM execution and output processing

Author: ModFileGen Team
Date: 2024-2026
"""

from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import apsimweatherconverter, apsimsoilconverter, apsimmanagementconverter, apsiminitconverter
import sys
import subprocess
import shutil
import concurrent.futures
import numpy as np
import os
import datetime
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
from time import time
import traceback

# Try to import joblib for parallel processing
try:
    from joblib import Parallel, delayed, parallel_backend
    JOBLIB_AVAILABLE = True
except ImportError:
    print("Warning: joblib not available, falling back to multiprocessing")
    JOBLIB_AVAILABLE = False

# Optional imports
try:
    from joblib import Parallel, delayed, parallel_backend
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False
    print("Warning: joblib not available, parallel processing may be limited")

import re
import json


def get_coord(d):
    """Extract coordinates and year from directory name."""
    res = re.findall(r"([-]?\d+[.]?\d+)[_]", d)
    lat = float(res[0])
    lon = float(res[1])
    year = int(float(res[2]))
    return {'lon': lon, 'lat': lat, 'year': year}


def transform_output(fil):
    """
    Transform APSIM output to standardized format.
    
    Args:
        fil: Path to APSIM output database (.db file)
        
    Returns:
        DataFrame with standardized output format
    """
    try:
        # APSIM Next Gen stores outputs in SQLite database
        conn = sqlite3.connect(fil)
        
        # Query the Report table for outputs
        query = """
        SELECT * FROM Report
        WHERE SimulationName IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame()
        
        # Extract simulation ID from file path
        d_name = Path(fil).parent.name
        c = get_coord(d_name)
        
        # Standardize column names to match other models
        # APSIM uses different names, map them to common format
        column_mapping = {
            'Clock.Today': 'Date',
            'Wheat.Phenology.Stage': 'Stage',
            'Wheat.AboveGround.Wt': 'Biom_ma',
            'Wheat.Grain.Wt': 'Yield',
            'Wheat.Grain.Number': 'GNumber',
            'Wheat.Leaf.LAI': 'LAI',
            'Wheat.Leaf.MaximumLAI': 'MaxLai',
            'Wheat.Nitrogen.NLeached': 'Nleac',
            'Wheat.Nitrogen.SoilN': 'SoilN',
            'Wheat.Nitrogen.TotalN': 'CroN_ma',
            'Wheat.Water.Transpiration': 'Transp',
            'Wheat.Water.Es': 'CumE'
        }
        
        # Rename columns if they exist
        df = df.rename(columns=column_mapping)
        
        # Add model identification
        df.insert(0, "Model", "APSIM")
        df.insert(1, "Idsim", d_name)
        df.insert(2, "Texte", "")
        
        # Add coordinates
        df['lon'] = c['lon']
        df['lat'] = c['lat']
        df['time'] = c['year']
        
        return df
        
    except Exception as e:
        print(f"Error transforming APSIM output {fil}: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def write_file(directory, filename, content):
    """Write content to file."""
    try:
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, filename), "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Error writing file {filename}: {e}")


def process_chunk(*args):
    """
    Process a chunk of simulations in parallel.
    
    Args:
        chunk: DataFrame chunk with simulation rows
        mi: Path to MasterInput database
        md: Path to ModelDictionary database
        directoryPath: Base output directory
        apsim_path: Path to APSIM executable
        dt: Delete temporary files flag (0=keep, 1=delete)
        template_apsimx: Path to existing .apsimx template file (optional)
        
    Returns:
        DataFrame with simulation results
    """
    chunk, mi, md, directoryPath, apsim_path, dt, template_apsimx = args
    dataframes = []
    
    # Cache for weather and soil data to avoid regeneration
    weathertable = {}
    soiltable = {}
    
    # Clear caches periodically to prevent memory buildup
    CACHE_CLEAR_INTERVAL = 50
    
    ModelDictionary_Connection = sqlite3.connect(md)
    MasterInput_Connection = sqlite3.connect(mi)
    
    for i, row in enumerate(chunk):
        # Periodically clear caches to free memory
        if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
            print(f"🗑️ Clearing caches at row {i} to free memory", flush=True)
            weathertable.clear()
            soiltable.clear()
            import gc
            gc.collect()
        
        print(f"Processing simulation {i+1}/{len(chunk)}: {row['idsim']}", flush=True)
        
        try:
            # Create simulation directory structure
            # Format: /directoryPath/idsim/work/output
            simPath = os.path.join(
                directoryPath, 
                str(row["idsim"]), 
                str(row["idPoint"]), 
                str(row["StartYear"])
            )
            usmdir = os.path.join(directoryPath, str(row["idsim"]))
            os.makedirs(simPath, exist_ok=True)
            os.makedirs(usmdir, exist_ok=True)
            
            # 1. WEATHER CONVERSION
            climid = f"{row['idPoint']}.{row['StartYear']}"
            if climid not in weathertable:
                print(f"  → Converting weather data for {climid}", flush=True)
                weatherconverter = apsimweatherconverter.ApsimWeatherConverter()
                weather_content = weatherconverter.export(
                    simPath,
                    ModelDictionary_Connection,
                    MasterInput_Connection,
                    usmdir
                )
                weathertable[climid] = weather_content
                del weatherconverter
            else:
                print(f"  → Reusing cached weather data for {climid}", flush=True)
                weather_content = weathertable[climid]
            
            # Write weather file from content
            weather_file = os.path.join(usmdir, "weather.met")
            if weather_content:
                with open(weather_file, 'w') as f:
                    f.write(weather_content)
                print(f"Successfully created weather file: {weather_file}")
            else:
                weather_file = None
            
            # 2. SOIL CONVERSION
            soilid = f"{row['idsoil']}.{row['idMangt']}"
            if soilid not in soiltable:
                print(f"  → Converting soil profile for {soilid}", flush=True)
                soilconverter = apsimsoilconverter.ApsimSoilConverter()
                soil_file = soilconverter.export(
                    simPath,
                    ModelDictionary_Connection,
                    MasterInput_Connection,
                    usmdir
                )
                soiltable[soilid] = soil_file
                del soilconverter
            else:
                print(f"  → Reusing cached soil data for {soilid}", flush=True)
                soil_file = soiltable[soilid]
                if soil_file and os.path.exists(soil_file):
                    shutil.copy(soil_file, usmdir)
            
            # 3. MANAGEMENT CONVERSION
            print(f"  → Converting management operations", flush=True)
            managementconverter = apsimmanagementconverter.ApsimManagementConverter()
            
            # Management converter extracts all operations automatically
            management_file = managementconverter.export(
                directory_path=simPath,
                ModelDictionary_Connection=ModelDictionary_Connection,
                master_input_connection=MasterInput_Connection,
                output_apsimx=os.path.join(usmdir, "Management.apsimx")
            )
            del managementconverter
            
            # 4. INITIALIZATION CONVERSION
            print(f"  → Converting initial conditions", flush=True)
            initconverter = apsiminitconverter.ApsimInitConverter()
            
            # Initialize soil water, nitrogen, and organic matter
            init_file = initconverter.export(
                directory_path=simPath,
                ModelDictionary_Connection=ModelDictionary_Connection,
                master_input_connection=MasterInput_Connection,
                output_apsimx=os.path.join(usmdir, "Initialization.apsimx")
            )
            del initconverter
            
            # 5. CREATE OR USE TEMPLATE SIMULATION FILE
            if template_apsimx and os.path.exists(template_apsimx):
                print(f"  → Using template .apsimx file: {template_apsimx}", flush=True)
                # Copy template and update with simulation-specific data
                sim_file = os.path.join(usmdir, "Simulation.apsimx")
                shutil.copy(template_apsimx, sim_file)
                
                # Update template with simulation-specific parameters
                update_apsim_template(
                    sim_file,
                    row,
                    weather_file,
                    soil_file,
                    management_file,
                    init_file
                )
            else:
                print(f"  → Creating APSIM simulation file from scratch", flush=True)
                create_apsim_simulation(
                    usmdir,
                    row,
                    weather_file,
                    soil_file,
                    management_file,
                    init_file
                )
            
            # 6. RUN APSIM
            if apsim_path:
                print(f"  → Running APSIM simulation", flush=True)
                run_apsim(usmdir, apsim_path, row['idsim'])
                
                # 6. PROCESS OUTPUT
                output_db = os.path.join(usmdir, "Simulation.db")
                if os.path.exists(output_db):
                    df = transform_output(output_db)
                    if not df.empty:
                        dataframes.append(df)
                    
                    # Clean up if requested
                    if dt == 1:
                        os.remove(output_db)
                else:
                    print(f"  ⚠ Output database not found: {output_db}")
            else:
                print(f"  ℹ APSIM execution skipped (no executable path provided)")
                
        except Exception as ex:
            print(f"❌ Error processing simulation {row['idsim']}: {ex}", file=sys.stderr, flush=True)
            traceback.print_exc()
            continue
    
    # Close connections
    ModelDictionary_Connection.close()
    MasterInput_Connection.close()
    
    # Clear all caches
    weathertable.clear()
    soiltable.clear()
    
    if not dataframes:
        print("No dataframes to concatenate.")
        return pd.DataFrame()
    
    # Concatenate results in batches to manage memory
    batch_size = 1000
    if len(dataframes) <= batch_size:
        result = pd.concat(dataframes, ignore_index=True)
        del dataframes
        return result
    
    result = pd.DataFrame()
    for i in range(0, len(dataframes), batch_size):
        batch = dataframes[i:i+batch_size]
        batch_concat = pd.concat(batch, ignore_index=True)
        result = pd.concat([result, batch_concat], ignore_index=True)
        del batch
        del batch_concat
    
    del dataframes
    return result


def update_apsim_template(sim_file, row, weather_file, soil_file, management_file, init_file=None):
    """
    Update an existing APSIM template with simulation-specific parameters.
    
    Args:
        sim_file: Path to the .apsimx simulation file to update
        row: Simulation metadata row
        weather_file: Path to weather .met file
        soil_file: Path to soil .apsimx file
        management_file: Path to management .apsimx file
        init_file: Path to initialization .apsimx file
    """
    try:
        # Load existing simulation file
        with open(sim_file, 'r') as f:
            simulation = json.load(f)
        
        # Find and update Clock dates
        def update_clock(node):
            if isinstance(node, dict):
                if node.get('$type') == 'Models.Clock, Models':
                    node['Start'] = f"{row['StartYear']}-01-01T00:00:00"
                    node['End'] = f"{row['EndYear']}-12-31T00:00:00"
                    print(f"    ✓ Updated simulation dates: {row['StartYear']}-{row['EndYear']}")
                    return True
                if 'Children' in node:
                    for child in node['Children']:
                        if update_clock(child):
                            return True
            return False
        
        # Find and update Weather file path
        def update_weather(node):
            if isinstance(node, dict):
                if node.get('$type') == 'Models.Climate.Weather, Models':
                    if weather_file:
                        node['FileName'] = os.path.basename(weather_file)
                        print(f"    ✓ Updated weather file reference")
                    return True
                if 'Children' in node:
                    for child in node['Children']:
                        if update_weather(child):
                            return True
            return False
        
        # Update simulation name
        if 'Name' in simulation:
            simulation['Name'] = f"Simulation_{row['idsim']}"
        
        # Apply updates
        update_clock(simulation)
        update_weather(simulation)
        
        # Update soil data if provided
        if soil_file and os.path.exists(soil_file):
            try:
                with open(soil_file, 'r') as f:
                    soil_data = json.load(f)
                
                # Extract soil component
                soil_component = None
                if 'Children' in soil_data:
                    for child in soil_data['Children']:
                        if child.get('$type') == 'Models.Soils.Soil, Models':
                            soil_component = child
                            break
                
                if soil_component:
                    # Find and replace soil in template
                    def replace_soil(node):
                        if isinstance(node, dict) and 'Children' in node:
                            # Find existing soil and replace it
                            for i, child in enumerate(node['Children']):
                                if isinstance(child, dict) and child.get('$type') == 'Models.Soils.Soil, Models':
                                    node['Children'][i] = soil_component
                                    print(f"    ✓ Updated soil profile in template")
                                    return True
                            # If no soil found, search deeper
                            for child in node['Children']:
                                if replace_soil(child):
                                    return True
                        return False
                    
                    if not replace_soil(simulation):
                        # If no existing soil found, add it to the first simulation
                        def add_soil_to_simulation(node):
                            if isinstance(node, dict):
                                if node.get('$type') == 'Models.Core.Simulation, Models':
                                    if 'Children' not in node:
                                        node['Children'] = []
                                    node['Children'].append(soil_component)
                                    print(f"    ✓ Added soil profile to template")
                                    return True
                                if 'Children' in node:
                                    for child in node['Children']:
                                        if add_soil_to_simulation(child):
                                            return True
                            return False
                        add_soil_to_simulation(simulation)
                        
            except Exception as e:
                print(f"    ⚠ Warning: Could not update soil in template: {e}")
        
        # Update management operations if provided
        if management_file and os.path.exists(management_file):
            try:
                with open(management_file, 'r') as f:
                    mgmt_data = json.load(f)
                
                # Extract management operations
                mgmt_operations = []
                if 'Children' in mgmt_data:
                    for child in mgmt_data['Children']:
                        if child.get('$type') == 'Models.Core.Folder, Models':
                            mgmt_operations.extend(child.get('Children', []))
                        elif child.get('$type') == 'Models.Manager, Models':
                            mgmt_operations.append(child)
                
                if mgmt_operations:
                    # Find simulation node and add/replace management operations
                    def update_management(node):
                        if isinstance(node, dict):
                            if node.get('$type') == 'Models.Core.Simulation, Models':
                                if 'Children' not in node:
                                    node['Children'] = []
                                
                                # Remove existing manager scripts (optional - could keep some)
                                # node['Children'] = [c for c in node['Children'] 
                                #                     if c.get('$type') != 'Models.Manager, Models']
                                
                                # Add new management operations
                                node['Children'].extend(mgmt_operations)
                                print(f"    ✓ Added {len(mgmt_operations)} management operation(s) to template")
                                return True
                            
                            if 'Children' in node:
                                for child in node['Children']:
                                    if update_management(child):
                                        return True
                        return False
                    
                    update_management(simulation)
                    
            except Exception as e:
                print(f"    ⚠ Warning: Could not update management in template: {e}")
        
        # Update initialization if provided
        if init_file and os.path.exists(init_file):
            try:
                with open(init_file, 'r') as f:
                    init_data = json.load(f)
                
                # Extract initialization components
                init_components = []
                if 'Children' in init_data:
                    init_components = init_data['Children']
                
                if init_components:
                    # Find simulation node and update/add initialization
                    def update_initialization(node):
                        if isinstance(node, dict):
                            if node.get('$type') == 'Models.Core.Simulation, Models':
                                if 'Children' not in node:
                                    node['Children'] = []
                                
                                # Remove old water/chemical/organic initialization if present
                                node['Children'] = [
                                    c for c in node['Children']
                                    if c.get('$type') not in [
                                        'Models.Soils.Water, Models',
                                        'Models.Soils.Chemical, Models',
                                        'Models.Soils.Organic, Models'
                                    ] or c.get('Name') != 'OrganicInitial'
                                ]
                                
                                # Add new initialization components
                                node['Children'].extend(init_components)
                                print(f"    ✓ Updated {len(init_components)} initialization component(s) in template")
                                return True
                            
                            if 'Children' in node:
                                for child in node['Children']:
                                    if update_initialization(child):
                                        return True
                        return False
                    
                    update_initialization(simulation)
                    
            except Exception as e:
                print(f"    ⚠ Warning: Could not update initialization in template: {e}")
        
        # Save updated simulation
        with open(sim_file, 'w') as f:
            json.dump(simulation, f, indent=2)
        
        print(f"  ✓ Updated template simulation file: {sim_file}")
        
    except Exception as e:
        print(f"  ⚠ Warning: Could not update template file: {e}")
        print(f"  Template will be used as-is")


def create_apsim_simulation(usmdir, row, weather_file, soil_file, management_file, init_file=None):
    """
    Create main APSIM .apsimx simulation file from scratch.
    
    Args:
        usmdir: Simulation directory
        row: Simulation metadata row
        weather_file: Path to weather .met file  
        soil_file: Path to soil .apsimx file  
        management_file: Path to management .apsimx file
        init_file: Path to initialization .apsimx file
    """
    # Basic APSIM simulation structure
    simulation = {
        "$type": "Models.Core.Simulations, Models",
        "Version": 174,
        "Name": f"Simulation_{row['idsim']}",
        "Children": [
            {
                "$type": "Models.Core.Simulation, Models",
                "Name": "Simulation",
                "Children": [
                    {
                        "$type": "Models.Clock, Models",
                        "Start": f"{row['StartYear']}-01-01T00:00:00",
                        "End": f"{row['EndYear']}-12-31T00:00:00",
                        "Name": "Clock"
                    },
                    {
                        "$type": "Models.Summary, Models",
                        "Name": "Summary"
                    },
                    {
                        "$type": "Models.Climate.Weather, Models",
                        "FileName": os.path.basename(weather_file) if weather_file else "weather.met",
                        "Name": "Weather"
                    }
                ],
                "Name": "Simulation"
            }
        ]
    }
    
    # Add soil if available
    if soil_file and os.path.exists(soil_file):
        try:
            with open(soil_file, 'r') as f:
                soil_data = json.load(f)
            # Extract soil from file and add to simulation
            if 'Children' in soil_data:
                for child in soil_data['Children']:
                    if child.get('$type') == 'Models.Soils.Soil, Models':
                        simulation['Children'][0]['Children'].append(child)
                        break
        except Exception as e:
            print(f"Warning: Could not load soil file: {e}")
    
    # Add management if available
    if management_file and os.path.exists(management_file):
        try:
            with open(management_file, 'r') as f:
                mgmt_data = json.load(f)
            # Extract management operations
            if 'Children' in mgmt_data:
                for child in mgmt_data['Children']:
                    if child.get('$type') == 'Models.Core.Folder, Models':
                        # Add all manager scripts
                        for mgr in child.get('Children', []):
                            simulation['Children'][0]['Children'].append(mgr)
        except Exception as e:
            print(f"Warning: Could not load management file: {e}")
    
    # Add initialization if available
    if init_file and os.path.exists(init_file):
        try:
            with open(init_file, 'r') as f:
                init_data = json.load(f)
            # Extract initialization components
            if 'Children' in init_data:
                for child in init_data['Children']:
                    # Add water, chemical, organic matter initialization
                    if child.get('$type') in [
                        'Models.Soils.Water, Models',
                        'Models.Soils.Chemical, Models',
                        'Models.Soils.Organic, Models',
                        'Models.Surface.SurfaceOrganicMatter, Models'
                    ]:
                        simulation['Children'][0]['Children'].append(child)
        except Exception as e:
            print(f"Warning: Could not load initialization file: {e}")
    
    # Add report for outputs
    report = {
        "$type": "Models.Report, Models",
        "VariableNames": [
            "[Clock].Today",
            "[Wheat].Phenology.Stage",
            "[Wheat].AboveGround.Wt",
            "[Wheat].Grain.Wt",
            "[Wheat].Grain.Number",
            "[Wheat].Leaf.LAI",
            "[Wheat].Nitrogen.TotalN",
            "[Wheat].Water.Transpiration"
        ],
        "EventNames": ["[Clock].DoReport"],
        "Name": "Report"
    }
    simulation['Children'][0]['Children'].append(report)
    
    # Write simulation file
    sim_file = os.path.join(usmdir, "Simulation.apsimx")
    with open(sim_file, 'w') as f:
        json.dump(simulation, f, indent=2)
    
    print(f"  ✓ Created simulation file: {sim_file}")


def run_apsim(usmdir, apsim_path, sim_id):
    """
    Execute APSIM simulation.
    
    Args:
        usmdir: Simulation directory
        apsim_path: Path to APSIM Models executable
        sim_id: Simulation identifier
    """
    sim_file = os.path.join(usmdir, "Simulation.apsimx")
    
    if not os.path.exists(sim_file):
        print(f"  ⚠ Simulation file not found: {sim_file}")
        return
    
    try:
        # Run APSIM Models
        # Typical command: Models.exe Simulation.apsimx
        result = subprocess.run(
            [apsim_path, sim_file],
            cwd=usmdir,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
            check=True
        )
        print(f"  ✓ APSIM execution completed for {sim_id}")
        
    except subprocess.TimeoutExpired as e:
        print(f"  ⏰ APSIM timed out for {sim_id}", file=sys.stderr, flush=True)
        raise
    except subprocess.CalledProcessError as e:
        print(f"  ❌ APSIM failed for {sim_id} with return code {e.returncode}", file=sys.stderr)
        print(f"STDOUT: {e.stdout}", file=sys.stdout)
        print(f"STDERR: {e.stderr}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"  ❌ Error running APSIM: {e}", file=sys.stderr)
        traceback.print_exc()
        raise


def export(MasterInput, ModelDictionary):
    """
    Create database indexes for optimized performance.
    
    Args:
        MasterInput: Path to MasterInput database
        ModelDictionary: Path to ModelDictionary database
    """
    MasterInput_Connection = sqlite3.connect(MasterInput)
    ModelDictionary_Connection = sqlite3.connect(ModelDictionary)
    
    try:
        print(f"dbMasterInput: {MasterInput}")
        print(f"dbModelsDictionary: {ModelDictionary}")
        # Set PRAGMA synchronous to OFF
        MasterInput_Connection.execute("PRAGMA synchronous = OFF")
        ModelDictionary_Connection.execute("PRAGMA synchronous = OFF")
        # Set PRAGMA journal_mode to WAL
        MasterInput_Connection.execute("PRAGMA journal_mode = WAL")
        ModelDictionary_Connection.execute("PRAGMA journal_mode = WAL")
        # Run full checkpoint
        MasterInput_Connection.execute("PRAGMA wal_checkpoint(FULL)")
    except Exception as ex:
        print(f"Connection Error: {ex}")
    
    try:
        cursor = MasterInput_Connection.cursor()
        
        print("\n📊 Creating database indexes for performance...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsim ON SimUnitList (idsim);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idMangt ON CropManagement (idMangt);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoil ON Soil (IdSoil);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultivars ON ListCultivars (idCultivar);")
        MasterInput_Connection.commit()
        print("Indexes created successfully!")
        
    except sqlite3.Error as e:
        print(f"Error creating indexes: {e}")
    
    MasterInput_Connection.close()
    ModelDictionary_Connection.close()


def fetch_data_from_sqlite(masterInput):
    """Fetch simulation data from MasterInput database."""
    conn = sqlite3.connect(masterInput)
    df = pd.read_sql_query("SELECT * FROM SimUnitList", conn)
    rows = df.to_dict(orient='records')
    conn.close()
    return rows


def chunk_data(data, parts, chunk_size):
    """Split data into chunks for parallel processing."""
    k, m = divmod(len(data), parts * chunk_size)
    sublists = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(parts * chunk_size)]
    return sublists


def main():
    """
    Main function for APSIM conversion workflow.
    Uses GlobalVariables for configuration and parallel_backend for processing.
    """
    mi = GlobalVariables.get("dbMasterInput")
    md = GlobalVariables.get("dbModelsDictionary")
    directoryPath = GlobalVariables.get("directorypath", os.getcwd())
    nthreads = GlobalVariables.get("nthreads", 4)
    dt = GlobalVariables.get("dt", 0)
    parts = GlobalVariables.get("parts", 1)
    apsim_path = GlobalVariables.get("apsim_path", None)
    template_apsimx = GlobalVariables.get("template_apsimx", None)
    
    if not mi or not md:
        raise ValueError("dbMasterInput and dbModelsDictionary must be set in GlobalVariables")
    
    os.makedirs(directoryPath, exist_ok=True)
    
    print("=" * 70)
    print("APSIM CONVERTER - Starting conversion workflow")
    print("=" * 70)
    print(f"MasterInput: {mi}")
    print(f"ModelDictionary: {md}")
    print(f"Output directory: {directoryPath}")
    print(f"APSIM executable: {apsim_path if apsim_path else 'Not provided (file generation only)'}")
    print(f"Template .apsimx: {template_apsimx if template_apsimx else 'None (will create from scratch)'}")
    print(f"Threads: {nthreads}")
    print(f"Parts per thread: {parts}")
    print("=" * 70)
    
    # Create indexes
    export(mi, md)
    
    # Fetch simulation data
    data = fetch_data_from_sqlite(mi)
    
    # Split data into chunks
    chunks = chunk_data(data, parts, chunk_size=nthreads)
    print(f"📊 Total simulations to process: {len(data)}", flush=True)
    del data  # Free original data list after chunking
    
    # Create args list for parallel processing
    import uuid
    args_list = [(chunk, mi, md, directoryPath, apsim_path, dt, template_apsimx) for chunk in chunks]
    del chunks  # Free chunks list after creating args_list
    
    # Create unique result file name
    result_name = str(uuid.uuid4()) + "_apsim"
    result_path = os.path.join(directoryPath, f"{result_name}.csv")
    while os.path.exists(result_path):
        result_name = str(uuid.uuid4()) + "_apsim"
        result_path = os.path.join(directoryPath, f"{result_name}.csv")
    
    try:
        start = time()
        print(f"Processing {len(args_list)} chunks...", flush=True)
        
        write_header = True
        total_chunks_written = 0
        
        if JOBLIB_AVAILABLE:
            # Use joblib Parallel with loky backend for better memory management
            with parallel_backend('loky', n_jobs=nthreads):
                # Process in small batches to avoid holding all results in memory
                batch_size = max(1, nthreads)  # Process nthreads chunks at a time
                
                for batch_idx in range(0, len(args_list), batch_size):
                    batch_args = args_list[batch_idx:batch_idx + batch_size]
                    
                    # Process this batch
                    batch_results = Parallel()(
                        delayed(process_chunk)(*args) for args in batch_args
                    )
                    
                    # Write each result directly to final file
                    for i, chunk_df in enumerate(batch_results):
                        if chunk_df is not None and not chunk_df.empty:
                            # Append to result file (write header only once)
                            chunk_df.to_csv(result_path, mode='a', header=write_header, index=False)
                            write_header = False  # Only write header for first chunk
                            total_chunks_written += 1
                            print(f"✅ Chunk {batch_idx + i + 1}/{len(args_list)}: {len(chunk_df)} rows written", flush=True)
                        
                        # Free memory immediately
                        if chunk_df is not None:
                            del chunk_df
                    
                    # Free batch results
                    del batch_results
        else:
            # Fallback to multiprocessing Pool if joblib not available
            print("Using multiprocessing.Pool (joblib not available)", flush=True)
            with Pool(processes=nthreads) as pool:
                # Process in batches
                batch_size = max(1, nthreads)
                
                for batch_idx in range(0, len(args_list), batch_size):
                    batch_args = args_list[batch_idx:batch_idx + batch_size]
                    
                    # Process this batch
                    batch_results = pool.starmap(process_chunk, batch_args)
                    
                    # Write each result directly to final file
                    for i, chunk_df in enumerate(batch_results):
                        if chunk_df is not None and not chunk_df.empty:
                            chunk_df.to_csv(result_path, mode='a', header=write_header, index=False)
                            write_header = False
                            total_chunks_written += 1
                            print(f"✅ Chunk {batch_idx + i + 1}/{len(args_list)}: {len(chunk_df)} rows written", flush=True)
                        
                        if chunk_df is not None:
                            del chunk_df
                    
                    del batch_results
        
        if total_chunks_written == 0:
            print("No data to process.")
            return
        
        print(f"✅ Results saved to {result_path}")
        print(f"APSIM total time: {time()-start:.2f}s", flush=True)
        
    except Exception as ex:
        print("Error during processing:", ex)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

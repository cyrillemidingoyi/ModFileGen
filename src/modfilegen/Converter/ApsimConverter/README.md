# APSIM Converter Module

This module provides converters for generating APSIM model input files from database sources or pandas DataFrames.

## Components

### ApsimWeatherConverter

Converts weather data to APSIM .met (weather) file format.

### ApsimSoilConverter

Converts soil profile data to APSIM .apsimx soil structures with complete physical, hydraulic, chemical, and organic properties.

### ApsimManagementConverter

Converts management operations (sowing, fertilization, irrigation, harvest, tillage) to APSIM Manager scripts. **Management operations can be shared across multiple simulation files** by creating toolbox files.

## Usage

### ApsimWeatherConverter

#### APSIM Weather File Format

APSIM weather files contain the following columns:
- `year`: Year (YYYY)
- `day`: Day of year (1-365/366)
- `radn`: Solar radiation (MJ/m²/day)
- `maxt`: Maximum temperature (°C)
- `mint`: Minimum temperature (°C)
- `rain`: Rainfall (mm)
- `pan`: Pan evaporation (mm) - optional
- `vp`: Vapor pressure (hPa) - optional
- `wind`: Wind speed (m/s) - optional

#### Method 1: Using export_simple with pandas DataFrame

This is the simplest method, ideal for converting existing weather data:

```python
from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter
import pandas as pd

# Prepare your weather data
weather_data = pd.DataFrame({
    'year': [1999] * 15,
    'day': range(1, 16),  # Day of year
    'radn': [8.0, 8.0, 13.0, 26.0, 25.0, 27.0, 27.0, 30.0, 26.0, 21.0, 
             27.0, 14.0, 27.0, 28.0, 26.0],
    'maxt': [23.0, 23.5, 27.5, 30.5, 30.0, 30.0, 30.5, 32.5, 33.5, 31.0,
             34.5, 33.0, 33.5, 33.5, 32.0],
    'mint': [17.5, 18.0, 18.5, 19.0, 18.0, 17.0, 16.5, 16.5, 18.5, 20.0,
             18.0, 21.0, 21.0, 20.0, 20.0],
    'rain': [4.9, 20.2, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.6,
             5.8, 0.0, 13.4, 0.0, 0.0]
})

# Create converter and export
converter = ApsimWeatherConverter()
converter.export_simple(
    output_path='weather.met',
    weather_data_df=weather_data,
    site_name='MySite',
    year='1999'
)
```

#### With Optional Columns

```python
# Include optional columns (pan, vp, wind)
weather_data = pd.DataFrame({
    'year': [1999] * 5,
    'day': [1, 2, 3, 4, 5],
    'radn': [8.0, 8.0, 13.0, 26.0, 25.0],
    'maxt': [23.0, 23.5, 27.5, 30.5, 30.0],
    'mint': [17.5, 18.0, 18.5, 19.0, 18.0],
    'rain': [4.9, 20.2, 2.0, 0.0, 0.0],
    'pan': [2.0, 1.8, 4.0, 7.0, 6.8],
## APSIM Converter Orchestrator

The APSIM Converter provides two approaches for running complete conversion workflows:

### Approach 1: Using `main()` with GlobalVariables (Recommended)

This approach follows the same pattern as SticsConverter and DssatConverter, using GlobalVariables for configuration and joblib's parallel_backend for efficient memory management.

```python
from modfilegen import GlobalVariables
from modfilegen.Converter.ApsimConverter import main

# Configure GlobalVariables
GlobalVariables["dbMasterInput"] = "/path/to/MasterInput.db"
GlobalVariables["dbModelsDictionary"] = "/path/to/ModelDictionary.db"
GlobalVariables["directorypath"] = "/path/to/output"
GlobalVariables["nthreads"] = 4
GlobalVariables["parts"] = 1
GlobalVariables["dt"] = 0  # 0=keep temp files, 1=delete
GlobalVariables["apsim_path"] = "/path/to/APSIM/Models.exe"  # Optional

# Run conversion
main()
```

**Features:**
- Progressive concatenation to avoid memory issues
- Results written incrementally to CSV
- Uses joblib parallel_backend with 'loky' for better memory management
- Automatic chunking based on threads and parts configuration

### Approach 2: Direct export function call (Legacy)

```python
from modfilegen.Converter.ApsimConverter import export

# Create database indexes
export(
    MasterInput="/path/to/MasterInput.db",
    ModelDictionary="/path/to/ModelDictionary.db"
)
```

**Note:** The `export()` function now only creates database indexes. Use `main()` for full conversion workflow.

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| dbMasterInput | str | - | Path to MasterInput database |
| dbModelsDictionary | str | - | Path to ModelDictionary database |
| directorypath | str | cwd | Output directory for generated files |
| nthreads | int | 4 | Number of parallel threads |
| parts | int | 1 | Chunks per thread (for memory optimization) |
| dt | int | 0 | Delete temp files (0=keep, 1=delete) |
| apsim_path | str | None | Path to APSIM Models executable |

### Memory Management

The converter implements several strategies to avoid memory issues:

1. **Progressive concatenation**: Results are written to CSV incrementally
2. **Batch processing**: Data is processed in small batches (batch_size = nthreads)
3. **Immediate cleanup**: DataFrames are deleted immediately after writing
4. **Cache management**: Weather and soil caches are cleared periodically (every 50 simulations)
5. **Chunking strategy**: `parts * nthreads` determines total number of chunks

### Output Files

For each simulation, the converter generates:

- `{idsim}/weather.met` - Weather data in APSIM format
- `{idsim}/Soil.apsimx` - Soil profile (if available)
- `{idsim}/Management.apsimx` - Management operations
- `{idsim}/Simulation.apsimx` - Main simulation file

Plus a consolidated results file:
- `{uuid}_apsim.csv` - All simulation results

---

## Individual Converters

### ApsimWeatherConverter

Converts weather/climate data to APSIM .met format.

#### Example: Simple Weather Conversion

```python
from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter
import pandas as pd

converter = ApsimWeatherConverter()

weather_data = pd.DataFrame({
    'year': [1999] * 5,
    'day': [1, 2, 3, 4, 5],
    'radn': [25.0, 26.0, 24.0, 27.0, 25.5],
    'maxt': [32.0, 33.0, 31.0, 34.0, 32.5],
    'mint': [18.0, 19.0, 17.0, 20.0, 18.5],
    'rain': [0.0, 5.0, 0.0, 10.0, 2.0],
    'vp': [21.0, 23.0, 22.0, 23.0, 20.0],
    'wind': [2.5, 3.0, 2.8, 3.2, 2.9]
})

converter.export_simple('weather.met', weather_data, 'MySite', '1999')
```

### ApsimSoilConverter

## Column Name Mapping

The converter handles different naming conventions:

| APSIM Name | Alternative Names |
|------------|-------------------|
| year       | Year              |
| day        | DOY               |
| radn       | srad              |
| maxt       | tmax              |
| mint       | tmin              |

Converts soil profile data to APSIM .apsimx JSON format. See the soil converter documentation for details.

### ApsimManagementConverter

Converts management operations to APSIM Manager scripts. This converter supports creating **shared management toolboxes** that can be referenced from multiple simulation files.

#### Supported Operations

- **Sowing**: Rule-based (sowing window with rainfall/soil water criteria) or date-based
- **Fertilization**: At sowing, fixed dates, split applications
- **Irrigation**: Automatic (based on soil water threshold) or scheduled
- **Harvest**: Automatic when crop is ready
- **Tillage**: Scheduled tillage operations

#### Example: Basic Management Operations

```python
from modfilegen.Converter.ApsimConverter import ApsimManagementConverter
import pandas as pd

# Define management operations
management_data = pd.DataFrame([
    {
        'operation_type': 'sowing',
        'crop': 'Wheat',
        'date': '15-may',
        'cultivar': 'Hartog',
        'population': 120.0,
        'depth': 30.0,
        'row_spacing': 250.0
    },
    {
        'operation_type': 'fertilization',
        'crop': 'Wheat',
        'timing': 'at_sowing',
        'fertilizer_type': 'UreaN',
        'amount': 80.0
    },
    {
        'operation_type': 'harvest',
        'crop': 'Wheat'
    }
])

# Create converter and export
converter = ApsimManagementConverter()
converter.export_simple(
    management_data,
    'wheat_management.apsimx',
    toolbox_name='Wheat Management'
)
```

#### Example: Rule-Based Sowing

```python
management_data = pd.DataFrame([{
    'operation_type': 'sowing',
    'crop': 'Maize',
    'cultivar': 'Pioneer_3394',
    'start_date': '1-nov',           # Sowing window start
    'end_date': '31-dec',             # Sowing window end
    'population': 8.0,
    'depth': 40.0,
    'row_spacing': 750.0,
    'min_esw': 100.0,                 # Min soil water (mm)
    'min_rain': 25.0,                 # Min rainfall (mm)
    'rain_days': 7,                   # Over how many days
    'sowing_rule': True               # Enable rule-based sowing
}])

converter.export_simple(management_data, 'maize_sowing.apsimx')
```

#### Example: Automatic Irrigation

```python
management_data = pd.DataFrame([{
    'operation_type': 'irrigation',
    'crop': 'Cotton',
    'automatic': True,
    'threshold': 0.5,                 # Irrigate when <50% available water
    'amount': 30.0                    # Apply 30mm
}])

converter.export_simple(management_data, 'irrigation.apsimx')
```

#### Example: Shared Management Across Simulations

Create a **reusable management toolbox** that multiple simulations can reference:

```python
# Create shared management operations
shared_management = pd.DataFrame([
    {
        'operation_type': 'sowing',
        'crop': 'Wheat',
        'start_date': '1-may',
        'end_date': '31-may',
        'cultivar': 'Hartog',
        'population': 120.0,
        'depth': 30.0,
        'row_spacing': 250.0,
        'min_esw': 80.0,
        'min_rain': 20.0,
        'rain_days': 5,
        'sowing_rule': True
    },
    {
        'operation_type': 'fertilization',
        'crop': 'Wheat',
        'timing': 'at_sowing',
        'fertilizer_type': 'UreaN',
        'amount': 100.0
    },
    {
        'operation_type': 'harvest',
        'crop': 'Wheat'
    }
])

# Create shared toolbox
converter.export_simple(
    shared_management,
    'shared_wheat_management.apsimx',
    toolbox_name='Shared Wheat Management'
)

# Now in APSIM GUI:
# 1. Load shared_wheat_management.apsimx as a reference
# 2. Drag and drop operations into multiple simulation files
# 3. Operations can be updated from the source
```

#### Management DataFrame Columns

**Sowing Operations:**
- `operation_type`: 'sowing'
- `crop`: Crop name (e.g., 'Wheat', 'Maize')
- `cultivar` or `variety`: Cultivar name
- `population` or `density`: Plants per m²
- `depth` or `sowing_depth`: Sowing depth (mm)
- `row_spacing`: Row spacing (mm)
- For rule-based: `start_date`, `end_date`, `min_esw`, `min_rain`, `rain_days`, `sowing_rule=True`
- For date-based: `date` or `sowing_date`

**Fertilization Operations:**
- `operation_type`: 'fertilization'
- `fertilizer_type` or `type`: Type (e.g., 'NO3N', 'UreaN', 'MAP')
- `amount`: Amount (kg/ha)
- `timing`: 'at_sowing' or 'on_date'
- `date`: Application date (if not at sowing)
- `crop`: Crop name (for at_sowing applications)

**Irrigation Operations:**
- `operation_type`: 'irrigation'
- `amount`: Irrigation amount (mm)
- `automatic`: True/False
- `threshold`: Critical fraction of available water (for automatic)
- `date`: Irrigation date (for scheduled)
- `crop`: Crop name

**Harvest Operations:**
- `operation_type`: 'harvest'
- `crop`: Crop name

**Tillage Operations:**
- `operation_type`: 'tillage'
- `date`: Tillage date
- `tillage_type`: Type (e.g., 'disc', 'plough')

## Default Values

### Weather Converter
If optional columns are not provided, the following defaults are used:
- `pan`: 2.0 mm
- `vp`: 20.0 hPa
- `wind`: 3.0 m/s

## Main Orchestrator: apsimconverter.py

The `apsimconverter` module provides a complete workflow orchestrator for APSIM simulations, similar to `SticsConverter` and `DssatConverter`. It handles:
- Automatic extraction from MasterInput database
- Parallel processing of multiple simulations
- Weather, soil, and management file generation
- Optional APSIM execution
- Results aggregation

### Usage with export() function

```python
from modfilegen.Converter.ApsimConverter import export

# Complete workflow with APSIM execution
results = export(
    MasterInput="/path/to/MasterInput.db",
    ModelDictionary="/path/to/ModelDictionary.db",
    directoryPath="/output/directory",
    apsim_path="/path/to/APSIM/Models.exe",  # Optional
    delete_temp=0  # 0=keep files, 1=delete after execution
)

# Returns pandas DataFrame with simulation results
print(results.head())
```

### File Generation Only

If no `apsim_path` is provided, only generates input files without running APSIM:

```python
# Generate files only (no execution)
export(
    MasterInput="/path/to/MasterInput.db",
    ModelDictionary="/path/to/ModelDictionary.db",
    directoryPath="/output/directory",
    apsim_path=None  # No execution
)
```

### Parallel Processing Configuration

Configure parallel processing via `GlobalVariables`:

```python
from modfilegen import GlobalVariables

GlobalVariables.nthreads = 4  # Number of parallel workers
GlobalVariables.parts = 2     # Chunks per worker

# Then run export
results = export(MasterInput, ModelDictionary, directoryPath)
```

### Output Structure

The converter creates the following directory structure:

```
output/
├── {idsim1}/
│   ├── weather.met              # Weather data
│   ├── Soil.apsimx             # Soil profile
│   ├── Management.apsimx       # Management operations
│   ├── Simulation.apsimx       # Main simulation file
│   └── Simulation.db           # Output database (if executed)
├── {idsim2}/
│   └── ...
└── apsim_results.csv           # Aggregated results
```

### Database Schema Requirements

The orchestrator expects standard ModFileGen database structure:

**MasterInput.db:**
- `SimUnitList`: Simulation metadata (idsim, idPoint, StartYear, EndYear, idMangt, idsoil)
- `RaClimateD`: Daily climate data
- `Soil`, `SoilLayers`: Soil properties
- `CropManagement`: Management policy links
- `ListCultivars`: Crop varieties
- `InorganicFOperations`: Fertilization operations
- `SoilTillageOperations`: Tillage operations

**ModelDictionary.db:**
- Variable definitions and default values

### Integration with Other Models

The APSIM converter follows the same interface as STICS and DSSAT converters:

```python
from modfilegen.Converter import SticsConverter, DssatConverter, ApsimConverter

# Run all models with same inputs
stics_results = SticsConverter.export(MasterInput, ModelDictionary)
dssat_results = DssatConverter.export(MasterInput, ModelDictionary)
apsim_results = ApsimConverter.export(MasterInput, ModelDictionary)

# Compare results
import pandas as pd
all_results = pd.concat([stics_results, dssat_results, apsim_results])
```

## Testing

Run the test suites:

```bash
cd src/modfilegen/Converter/ApsimConverter

# Test complete workflow
python test_apsim_converter.py

# Test individual converters
python test_apsimweather.py
python test_apsimsoil.py
python test_apsimmanagement.py

# Test with real database
python test_management_with_real_db.py
```

## References

- APSIM Next Generation: https://www.apsim.info/
- APSIM Documentation: https://www.apsim.info/documentation/
- ModFileGen: https://github.com/your-org/ModFileGen

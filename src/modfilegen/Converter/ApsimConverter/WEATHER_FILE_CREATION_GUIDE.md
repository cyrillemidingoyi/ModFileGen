# APSIM Weather File Creation Guide

## Overview

The `ApsimWeatherConverter` provides two methods for creating weather files:

1. **`export()`** - Generates weather content as a string (no file writing)
2. **`export_to_file()`** - Creates the weather file directly

## When to Use Each Method

### Method 1: `export()` - Content Generation

**Use when:**
- You need to cache weather data for reuse (e.g., same site/year used multiple times)
- You're implementing parallel processing workflows
- You want to store content in memory before writing
- You need to manipulate or validate content before saving

**Example:**
```python
from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter
import sqlite3

# Setup connections
mi_conn = sqlite3.connect("MasterInput.db")
md_conn = sqlite3.connect("ModelDictionary.db")

# Create converter
converter = ApsimWeatherConverter()

# Generate content (no file created)
content = converter.export(
    directory_path="/path/to/Site123/2020",
    ModelDictionary_Connection=md_conn,
    master_input_connection=mi_conn,
    usmdir=None
)

# Cache content for reuse
weather_cache = {}
weather_cache["Site123.2020"] = content

# Later, write file when needed
with open("output/weather.met", 'w') as f:
    f.write(content)

mi_conn.close()
md_conn.close()
```

**In Parallel Processing:**
```python
# This is how it's used in apsimconverter.py
weathertable = {}  # Cache dictionary

# For each simulation
climid = f"{row['idPoint']}.{row['StartYear']}"
if climid not in weathertable:
    # Generate once
    weather_content = converter.export(...)
    weathertable[climid] = weather_content
else:
    # Reuse cached content
    weather_content = weathertable[climid]

# Write file from cached content
weather_file = os.path.join(output_dir, "weather.met")
with open(weather_file, 'w') as f:
    f.write(weather_content)
```

### Method 2: `export_to_file()` - Direct File Creation

**Use when:**
- You want to create a single weather file quickly
- You don't need to cache or reuse the content
- You prefer a simple one-step operation
- You're creating files for individual sites/years

**Example:**
```python
from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter
import sqlite3

# Setup connections
mi_conn = sqlite3.connect("MasterInput.db")
md_conn = sqlite3.connect("ModelDictionary.db")

# Create converter
converter = ApsimWeatherConverter()

# Create file directly (one step)
weather_file = converter.export_to_file(
    directory_path="/path/to/Site123/2020",
    ModelDictionary_Connection=md_conn,
    master_input_connection=mi_conn,
    output_file="output/weather.met"
)

if weather_file:
    print(f"Weather file created: {weather_file}")
else:
    print("Error creating weather file")

mi_conn.close()
md_conn.close()
```

**Creating Multiple Files:**
```python
# Create weather files for multiple sites
sites_years = [
    ("Site123", 2020),
    ("Site456", 2021),
    ("Site789", 2022)
]

converter = ApsimWeatherConverter()

for site, year in sites_years:
    directory_path = f"/path/to/{site}/{year}"
    output_file = f"output/{site}_{year}_weather.met"
    
    result = converter.export_to_file(
        directory_path=directory_path,
        ModelDictionary_Connection=md_conn,
        master_input_connection=mi_conn,
        output_file=output_file
    )
    
    if result:
        print(f"✓ Created: {result}")
```

## Comparison Table

| Feature | `export()` | `export_to_file()` |
|---------|-----------|-------------------|
| Returns | String content | File path (or None) |
| Creates file | No | Yes |
| Use case | Caching, parallel processing | Simple file creation |
| Flexibility | High (manipulate content) | Medium (direct output) |
| Performance (single) | Fast | Fast |
| Performance (multiple same site) | Fast (cache & reuse) | Slower (regenerates) |
| Code simplicity | Requires file writing | One-step operation |

## Testing

Run the test suite to verify both methods work correctly:

```bash
cd /path/to/ModFileGen/src/modfilegen/Converter/ApsimConverter
python test_weather_creation.py
```

### Test Results

The test suite verifies:

1. ✓ **Content Generation** - `export()` generates valid weather content
2. ✓ **Direct File Creation** - `export_to_file()` creates valid files
3. ✓ **Equivalence** - Both methods produce identical content

Expected output:
```
======================================================================
TEST RESULTS SUMMARY
======================================================================
✓ PASS - Generate content (export)
✓ PASS - Create file directly (export_to_file)
✓ PASS - Verify methods are identical
======================================================================
✓ All tests passed successfully!
======================================================================
```

## APSIM Weather File Format

Both methods generate files in APSIM .met format:

```
[weather.met.weather]
latitude = -7.125
longitude = 30.575
tav = 21.50 (oC) ! Annual average ambient temperature
amp = 3.20 (oC) ! Annual amplitude in mean monthly temperature
Station: 30.575_-7.125

year  day  radn  maxt  mint  rain   vp   code
 ()   ()  (MJ/m^2) (oC) (oC)  (mm) (hPa)   ()
2012    1  21.5  28.4  18.3   0.0  15.2  222222
2012    2  22.1  29.1  19.2   2.5  16.1  222222
...
```

### File Structure

1. **Header**: Contains metadata and station information
   - Latitude/longitude (optional)
   - TAV: Annual average temperature (optional)
   - AMP: Temperature amplitude (optional)
   - Station identifier

2. **Column Names**: Variable names
   - Required: year, day, radn, maxt, mint, rain
   - Optional: vp, pan, wind
   - Always: code (quality flag)

3. **Units**: Units for each column

4. **Data Rows**: Daily weather observations

## Best Practices

### For High-Performance Workflows

Use `export()` with caching:
```python
# Create cache dictionary
weather_cache = {}

# Process simulations
for sim in simulations:
    climid = f"{sim.site}.{sim.year}"
    
    # Check cache first
    if climid not in weather_cache:
        content = converter.export(...)
        weather_cache[climid] = content
    else:
        content = weather_cache[climid]
    
    # Write file
    with open(output_file, 'w') as f:
        f.write(content)
```

### For Simple Scripts

Use `export_to_file()`:
```python
# One-step file creation
weather_file = converter.export_to_file(
    directory_path=path,
    ModelDictionary_Connection=md_conn,
    master_input_connection=mi_conn,
    output_file="weather.met"
)
```

### Memory Management

When processing many files:
```python
# Clear cache periodically
CACHE_CLEAR_INTERVAL = 50

for i, sim in enumerate(simulations):
    if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
        weather_cache.clear()
        import gc
        gc.collect()
```

## Error Handling

Both methods return empty string or None on error:

```python
# Method 1
content = converter.export(...)
if not content:
    print("Error: No weather content generated")

# Method 2
weather_file = converter.export_to_file(...)
if not weather_file:
    print("Error: Failed to create weather file")
```

## Integration with APSIM Workflow

The weather file is referenced in the main Simulation.apsimx:

```json
{
  "$type": "Models.Climate.Weather, Models",
  "FileName": "weather.met",
  "Name": "Weather"
}
```

**Important:** The `FileName` should contain just the filename (relative path), not the full file content.

## Summary

- **`export()`**: Returns content string → Best for caching and parallel processing
- **`export_to_file()`**: Creates file directly → Best for simple one-off file creation
- Both methods produce identical APSIM-compatible .met files
- Choose based on your workflow requirements (caching vs simplicity)

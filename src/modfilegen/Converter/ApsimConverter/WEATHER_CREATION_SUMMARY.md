# Weather File Creation - Summary of Changes

## Date: February 6, 2026

## Important: Database Indexes Required

**⚠️ CRITICAL**: The weather conversion requires database indexes for acceptable performance.

Without indexes, a simple weather query can take **several minutes** or timeout. With indexes, the same query takes **milliseconds**.

### Creating Indexes

The `export()` function in `apsimconverter.py` creates these indexes automatically:
```python
cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
```

**When to create indexes:**
- Before running any batch weather conversions
- When setting up a new MasterInput database
- If queries are taking more than a few seconds

**Example:**
```python
import sqlite3

mi_conn = sqlite3.connect("MasterInput.db")
cursor = mi_conn.cursor()

# Create indexes (one-time operation)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
mi_conn.commit()
```

The test suite and example scripts now create indexes automatically.

---

## Problem Statement

After the refactoring to separate `export()` from `main()` and implement progressive concatenation, we discovered that:
1. `export()` returns weather file **content** (string), not a file path
2. This design is optimal for **caching** in parallel processing workflows
3. However, users who want to create a **single weather file** need to write the file themselves

## Solution Implemented

Added a new convenience method `export_to_file()` that creates weather files directly.

### Two Approaches for Weather File Creation

#### Approach 1: `export()` - Content Generation
- **Returns**: String content (not a file)
- **Use case**: Caching for parallel processing, multiple simulations reusing same weather
- **Benefits**: Optimal performance when same site/year used multiple times

```python
converter = ApsimWeatherConverter()
content = converter.export(
    directory_path="/path/site/year",
    ModelDictionary_Connection=md_conn,
    master_input_connection=mi_conn,
    usmdir=None
)

# Cache content
weather_cache[climid] = content

# Write file when needed
with open("weather.met", 'w') as f:
    f.write(content)
```

#### Approach 2: `export_to_file()` - Direct File Creation
- **Returns**: File path (or None on error)
- **Use case**: Simple one-step file creation
- **Benefits**: Convenient for single file generation

```python
converter = ApsimWeatherConverter()
weather_file = converter.export_to_file(
    directory_path="/path/site/year",
    ModelDictionary_Connection=md_conn,
    master_input_connection=mi_conn,
    output_file="output/weather.met"
)

if weather_file:
    print(f"File created: {weather_file}")
```

## Changes Made

### 1. Updated `apsimweatherconverter.py`

#### Added `export_to_file()` method (lines ~131-185)
```python
def export_to_file(self, directory_path, ModelDictionary_Connection, 
                   master_input_connection, output_file):
    """
    Generate weather data and write directly to file.
    
    Returns:
        str: Path to the created weather file (or None if error)
    """
    # Generate content
    content = self.export(...)
    
    # Write to file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(content)
    
    return output_file
```

#### Updated `export()` docstring
- Clarified that it returns **content** (string), not a file
- Added note: "For direct file creation, use export_to_file() instead"

### 2. Created Test Suite

**File**: `test_weather_creation.py`

Tests three scenarios:
1. ✓ Content generation (`export()`)
2. ✓ Direct file creation (`export_to_file()`)
3. ✓ Verify both methods produce identical results

**Test Results**: All tests pass ✓
```
✓ PASS - Generate content (export)
✓ PASS - Create file directly (export_to_file)
✓ PASS - Verify methods are identical
```

### 3. Created Example Script

**File**: `example_weather_usage.py`

Demonstrates:
1. Simple file creation with `export_to_file()`
2. Content caching with `export()` (40% efficiency gain)
3. Multiple sites/years batch creation

### 4. Created Documentation

**File**: `WEATHER_FILE_CREATION_GUIDE.md`

Comprehensive guide covering:
- When to use each method
- Code examples
- Performance comparison
- Best practices
- APSIM file format reference
- Integration with main workflow

## Performance Comparison

### Scenario: 5 simulations using 3 unique site/year combinations

**Without caching** (calling `export_to_file()` 5 times):
- 5 database queries
- 5 content generations
- 5 file writes

**With caching** (using `export()` + cache):
- 3 database queries (40% reduction)
- 3 content generations (40% reduction)
- 5 file writes (same)
- **Result**: 2 regenerations avoided (40% savings)

## Backward Compatibility

✓ **Fully backward compatible**
- `export()` behavior unchanged (returns content)
- Existing code in `apsimconverter.py` works as before
- `export_to_file()` is an additional method

## Integration with Main Workflow

The main workflow in `apsimconverter.py` continues to use `export()` with caching:

```python
# process_chunk() in apsimconverter.py
weathertable = {}  # Cache

for simulation in simulations:
    climid = f"{site}.{year}"
    
    if climid not in weathertable:
        # Generate once
        weather_content = converter.export(...)
        weathertable[climid] = weather_content
    else:
        # Reuse cached
        weather_content = weathertable[climid]
    
    # Write file
    with open(weather_file, 'w') as f:
        f.write(weather_content)
```

This ensures:
- ✓ Optimal caching performance
- ✓ No redundant database queries
- ✓ Memory-efficient (progressive writing)

## Files Modified

1. **apsimweatherconverter.py**
   - Added `export_to_file()` method
   - Updated `export()` docstring

2. **test_weather_creation.py** (new)
   - 3 test scenarios
   - Database path resolution
   - Verification of both methods

3. **example_weather_usage.py** (new)
   - 3 usage examples
   - Performance demonstration
   - Best practices showcase

4. **WEATHER_FILE_CREATION_GUIDE.md** (new)
   - Comprehensive documentation
   - When to use each method
   - Code examples
   - Performance comparison

5. **WEATHER_CREATION_SUMMARY.md** (this file)
   - Summary of changes
   - Design decisions
   - Test results

## Verification

### Test Results
```bash
$ python test_weather_creation.py

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

### Example Output
```bash
$ python example_weather_usage.py

✓ All examples completed successfully!

Key Takeaways:
  1. Use export_to_file() for simple, one-off file creation
  2. Use export() with caching when processing multiple simulations
  3. Both methods produce identical APSIM-compatible .met files
```

### Generated Files Verification
```bash
$ head -10 example_output/simple_weather.met

[weather.met.weather]
Station: -7.125_30.575
year  day  radn  maxt  mint  rain  wind   code
 ()   ()  (MJ/m^2) (oC) (oC)  (mm)  (m/s)    ()
2012   1   17.4  26.5  20.7   0.0   1.1 999999
2012   2   22.6  27.8  19.0   0.0   1.2 999999
...
```

## Conclusion

✓ Added convenient `export_to_file()` method for simple use cases
✓ Maintained `export()` for optimal caching in parallel workflows
✓ Both methods produce identical, APSIM-compatible weather files
✓ Comprehensive tests verify correctness
✓ Full documentation provided
✓ Backward compatible with existing code
✓ Performance optimized for both scenarios

## Recommendations

### For Single File Generation
Use `export_to_file()` - simpler, one-step operation

### For Batch Processing / Parallel Workflows
Use `export()` with caching - better performance, memory efficient

### For Main Workflow (modulostics)
Continue using current approach with `export()` + caching in `process_chunk()`

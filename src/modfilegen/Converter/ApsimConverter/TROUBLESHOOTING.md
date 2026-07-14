# Troubleshooting Guide - APSIM Weather Converter

## Common Issues and Solutions

### 1. Weather Conversion Hangs / Takes Too Long

**Symptom:**
```
📍 Site: -7.125_30.575, Year: 2011
📂 Directory path: /dummy/-7.125_30.575/2011
[Process hangs here for several minutes]
```

**Cause:** Missing database indexes on the `RaClimateD` table

**Solution:**
Create indexes before running conversions:

```python
import sqlite3

mi_conn = sqlite3.connect("MasterInput.db")
cursor = mi_conn.cursor()

# Create indexes (one-time operation)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
mi_conn.commit()

print("✓ Indexes created")
```

**Performance Impact:**
- **Without indexes**: 5-60+ seconds per query (or timeout)
- **With indexes**: < 0.1 seconds per query
- **Speed improvement**: 50x - 600x faster

---

### 2. No Weather Content Generated

**Symptom:**
```
Warning: No weather data found for site X and year Y
```

**Possible causes:**

#### A. Wrong idPoint format
Check what format your database uses:
```sql
SELECT DISTINCT idPoint FROM RaClimateD LIMIT 5;
```

Common formats:
- String: `"-7.125_30.575"` ← Most common
- Integer: `123456`
- Coordinates: `"lat_lon"` format

The converter extracts `idPoint` from the directory path:
```python
# directory_path = "/base/idsim/idPoint/year"
# ST[-2] extracts idPoint
Site = directory_path.split(os.sep)[-2]  
```

#### B. Data not in database
Verify data exists:
```sql
SELECT COUNT(*) FROM RaClimateD 
WHERE idPoint='your_site' AND (Year=2020 OR Year=2021);
```

If count is 0, check:
- Is the site ID correct?
- Is weather data loaded for those years?
- Are you using the correct database file?

#### C. Year out of range
The converter queries for the specified year AND the next year:
```python
fetchAllQuery = f"SELECT * FROM RaClimateD WHERE idPoint='{Site}' " \
                f"AND (Year={Year} OR Year={int(Year) + 1})"
```

This is because simulations often span into the next calendar year.

---

### 3. Column Name Mismatches

**Symptom:**
```
KeyError: 'radn' 
# or
KeyError: 'maxt'
```

**Cause:** Database uses different column names than expected

**Solution:**
The converter handles common variations:
- `radn` or `srad` for solar radiation
- `maxt` or `tmax` for maximum temperature
- `mint` or `tmin` for minimum temperature
- `day` or `DOY` for day of year

If your database uses different names, check the `_build_data_rows()` method:

```python
# Solar radiation
radn = row.get('radn')  # Try APSIM convention first
if pd.isna(radn):
    radn = row.get('srad', -999.9)  # Try database convention
```

To see your column names:
```sql
PRAGMA table_info(RaClimateD);
```

---

### 4. Empty or Corrupted Output Files

**Symptom:**
- File is created but empty
- File has header but no data rows
- File contains error messages

**Checks:**

```python
# 1. Verify content was generated
content = converter.export(...)
if not content:
    print("Error: No content generated")
else:
    print(f"Content length: {len(content)} bytes")

# 2. Check for data
if len(content.split('\n')) < 10:
    print("Warning: Very few lines generated")

# 3. Verify file was written
import os
if os.path.exists(weather_file):
    print(f"File size: {os.path.getsize(weather_file)} bytes")
```

**Common causes:**
- Database query returned no rows → Check site/year
- Permissions error → Check directory write access
- Disk full → Check available space

---

### 5. Memory Issues with Large Datasets

**Symptom:**
```
MemoryError
# or
Process killed (OOM)
```

**Solution:**
Use the caching approach with periodic cache clearing:

```python
weathertable = {}
CACHE_CLEAR_INTERVAL = 50  # Clear every 50 simulations

for i, simulation in enumerate(simulations):
    # Periodic cache clearing
    if i > 0 and i % CACHE_CLEAR_INTERVAL == 0:
        weathertable.clear()
        import gc
        gc.collect()
    
    # Generate/reuse weather content
    climid = f"{site}.{year}"
    if climid not in weathertable:
        weathertable[climid] = converter.export(...)
    
    # Write file
    with open(weather_file, 'w') as f:
        f.write(weathertable[climid])
```

**Memory optimization tips:**
- Clear caches periodically
- Don't hold all results in memory
- Write files progressively
- Use generators for large datasets

---

### 6. Database Locked Errors

**Symptom:**
```
sqlite3.OperationalError: database is locked
```

**Causes:**
- Another process has the database open
- Previous transaction not committed
- File-based database with concurrent access

**Solutions:**

#### A. Set timeout
```python
mi_conn = sqlite3.connect("MasterInput.db", timeout=30)
```

#### B. Use WAL mode (Write-Ahead Logging)
```python
mi_conn.execute("PRAGMA journal_mode = WAL")
```

#### C. Ensure proper closing
```python
try:
    # ... use connection ...
finally:
    mi_conn.close()
```

#### D. For parallel processing
Each worker should open its own connection:
```python
def process_chunk(chunk, db_path):
    # Each worker opens its own connection
    mi_conn = sqlite3.connect(db_path)
    try:
        # ... process ...
    finally:
        mi_conn.close()
```

---

### 7. Verification Steps

After creating weather files, verify they're correct:

```bash
# 1. Check file exists and has content
ls -lh weather.met

# 2. View first 20 lines
head -20 weather.met

# 3. Count data rows (should be 365 or 366 for full year)
grep -v "^[[]" weather.met | grep -v "^ *year" | grep -v "^ *()" | wc -l

# 4. Check for missing data (-999.9 values)
grep "\-999" weather.met | wc -l
```

**Valid weather file should have:**
```
[weather.met.weather]
Station: site_name
year  day  radn  maxt  mint  rain  wind   code
 ()   ()  (MJ/m^2) (oC) (oC)  (mm)  (m/s)    ()
2020   1   21.5  28.4  18.3   0.0   1.2  222222
2020   2   22.1  29.1  19.2   2.5   1.3  222222
...
```

---

## Performance Benchmarks

### Without Indexes
- 1 weather file: ~10 seconds
- 100 weather files: ~17 minutes
- 1000 weather files: ~2.8 hours

### With Indexes
- 1 weather file: ~0.2 seconds
- 100 weather files: ~20 seconds
- 1000 weather files: ~3.5 minutes

### With Indexes + Caching (50% reuse)
- 1000 weather files: ~2 minutes (40% faster)

---

## Quick Diagnostic Commands

```bash
# Check if indexes exist
sqlite3 MasterInput.db "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='RaClimateD';"

# Count weather records
sqlite3 MasterInput.db "SELECT COUNT(*) FROM RaClimateD;"

# Check site format
sqlite3 MasterInput.db "SELECT DISTINCT idPoint FROM RaClimateD LIMIT 10;"

# Check year range
sqlite3 MasterInput.db "SELECT MIN(Year), MAX(Year) FROM RaClimateD;"

# Table structure
sqlite3 MasterInput.db "PRAGMA table_info(RaClimateD);"

# Database file size
du -h MasterInput.db
```

---

## Getting Help

If you encounter issues not covered here:

1. **Check test results:**
   ```bash
   cd src/modfilegen/Converter/ApsimConverter
   python3 test_weather_creation.py
   ```

2. **Run example:**
   ```bash
   python3 example_weather_usage.py
   ```

3. **Enable debug output:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

4. **Check documentation:**
   - [WEATHER_FILE_CREATION_GUIDE.md](WEATHER_FILE_CREATION_GUIDE.md) - Complete usage guide
   - [WEATHER_CREATION_SUMMARY.md](WEATHER_CREATION_SUMMARY.md) - Implementation summary

---

## Summary Checklist

Before running weather conversions:

- [ ] Database indexes created
- [ ] Database connection established
- [ ] Site/year data exists in database
- [ ] Output directory has write permissions
- [ ] Sufficient disk space available
- [ ] Column names match expected format
- [ ] For large datasets: caching strategy in place

For best performance:
- ✓ Create indexes (50-600x speedup)
- ✓ Use caching for repeated sites (40% speedup)
- ✓ Clear cache periodically (memory management)
- ✓ Use WAL mode for concurrent access

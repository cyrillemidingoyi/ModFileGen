# APSIM Tests

This directory contains all tests and examples for the APSIM Converter module.

## Structure

```
tests/apsim/
├── test_*.py          # Test scripts
├── test_*.met         # Test weather files
├── test_*.apsimx      # Test APSIM simulation files
└── example_*.py       # Example usage scripts
```

## Running Tests

### From this directory:
```bash
cd /path/to/ModFileGen/tests/apsim
python test_apsim_main.py
python test_weather_creation.py
```

### From repository root:
```bash
cd /path/to/ModFileGen
python tests/apsim/test_apsim_main.py
python tests/apsim/test_weather_creation.py
```

## Test Files

### Core Tests
- **test_apsim_main.py** - Main converter workflow test using GlobalVariables
- **test_weather_creation.py** - Weather file creation tests (export vs export_to_file)
- **test_apsim_converter.py** - Legacy converter tests
- **test_database_integration.py** - Database integration tests

### Component Tests
- **test_apsimweather.py** - Weather converter unit tests
- **test_apsimsoil.py** - Soil converter unit tests
- **test_apsimmanagement.py** - Management converter unit tests

### Feature Tests
- **test_flexible_columns.py** - Weather file with flexible columns
- **test_management_with_real_db.py** - Management operations with real database

### Example Scripts
- **example_weather_usage.py** - Weather file creation examples (3 scenarios)
- **example_usage.py** - General converter usage examples
- **example_full_workflow.py** - Complete workflow demonstration
- **example_complete_simulation.py** - Full simulation setup

## Test Data Files

### Weather Files (.met)
- **test_weather.met** - Complete weather file with all optional columns
- **test_required_only.met** - Minimal weather file (required columns only)
- **test_vp_only.met** - Weather file with vapor pressure
- **test_wind_only.met** - Weather file with wind speed
- **test_pan_only.met** - Weather file with pan evaporation
- **test_vp_pan.met** - Weather file with vp and pan
- **test_all_optional.met** - Weather file with all optional columns
- **test_weather_minimal.met** - Minimal viable weather file
- **example_weather.met** - Example weather file

### APSIM Simulation Files (.apsimx)
- **test_basic_management.apsimx** - Basic management operations
- **test_sowing_rule.apsimx** - Sowing rule script
- **test_fertilization.apsimx** - Fertilization manager
- **test_irrigation.apsimx** - Irrigation schedule
- **test_complete_cycle.apsimx** - Complete crop cycle
- **test_db_management.apsimx** - Database-driven management
- **example_management.apsimx** - Example management file
- **example_soil.apsimx** - Example soil profile

### Soil Files
- **test_minimal_soil.apsimx** - Minimal soil profile
- **test_deep_soil.apsimx** - Deep soil profile (5+ layers)
- **test_new_soil.apsimx** - Newly created soil
- **test_update_soil.apsimx** - Updated soil properties

### Real Database Test
- **test_real_db_sim_-7.125_30.575_2011_MgtMais0_310_2.apsimx** - Simulation from real database

## Output Location

All test outputs are saved to: `../../data/apsim/`

- Weather test outputs: `data/apsim/weather_test_output/`
- APSIM test outputs: `data/apsim/apsim_test_output/`
- Example outputs: `data/apsim/example_output/`

## Database Requirements

Tests require access to:
- `tests/data/MasterInput_bon_test.db` - Master input database with 160 simulations
- `tests/data/ModelsDictionaryArise.db` - Model dictionary database

## Key Features Tested

### Weather Conversion
✓ Content generation (caching)
✓ Direct file creation
✓ Flexible column handling
✓ Optional columns (vp, wind, pan)
✓ Database index optimization

### Soil Conversion
✓ Physical properties
✓ Water balance parameters
✓ Organic matter
✓ Chemical properties
✓ Initial water content

### Management Conversion
✓ Sowing operations
✓ Fertilization
✓ Irrigation
✓ Harvest rules
✓ Complex management scripts

### Integration
✓ Complete workflow
✓ Parallel processing
✓ Database queries
✓ File generation
✓ Memory management

## Performance Benchmarks

With database indexes:
- 1 weather file: ~0.2 seconds
- 100 weather files: ~20 seconds
- 160 simulations: ~15 seconds (800 files total)

See `../../src/modfilegen/Converter/ApsimConverter/TROUBLESHOOTING.md` for performance details.

## Documentation

Related documentation:
- [WEATHER_FILE_CREATION_GUIDE.md](../../src/modfilegen/Converter/ApsimConverter/WEATHER_FILE_CREATION_GUIDE.md)
- [WEATHER_CREATION_SUMMARY.md](../../src/modfilegen/Converter/ApsimConverter/WEATHER_CREATION_SUMMARY.md)
- [TROUBLESHOOTING.md](../../src/modfilegen/Converter/ApsimConverter/TROUBLESHOOTING.md)
- [README.md](../../src/modfilegen/Converter/ApsimConverter/README.md)

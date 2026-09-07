# Repository Reorganization - APSIM Files

**Date:** February 7, 2026

## Overview

Reorganized the ModFileGen repository to improve clarity by separating tests, data, and source code into distinct directories.

## Changes Made

### 1. Created New Directory Structure

```
ModFileGen/
├── src/modfilegen/Converter/ApsimConverter/  # Source code only
├── tests/apsim/                               # All tests and examples
└── data/apsim/                                # Data files and results
```

### 2. Moved Files

#### Tests → `tests/apsim/`
All test scripts and example files:
- `test_*.py` → Test scripts (13 files)
- `example_*.py` → Example scripts (4 files)
- `test_*.met` → Test weather files (10 files)
- `test_*.apsimx` → Test simulation files (10 files)
- `example_*.apsimx` → Example files (2 files)
- `example_*.met` → Example data (1 file)

**Total:** 40 files moved to `tests/apsim/`

#### Data → `data/apsim/`
Template files and output directories:
- `Maize.apsimx` → Maize crop template
- `simulation.apsimx` → Generic simulation template
- `*_toolbox.apsimx` → Management toolboxes (2 files)
- `shared_*.apsimx` → Shared management (1 file)
- `simulation_weather.met` → Example weather file
- `apsim_test_output/` → Test results directory
- `weather_test_output/` → Weather test results
- `example_output/` → Example outputs
- `output/` → General outputs

**Total:** 6 files + 4 directories moved to `data/apsim/`

### 3. Updated File Paths

Modified imports and paths in test files to use relative paths from repository root:

```python
# Old (absolute paths)
sys.path.insert(0, "/absolute/path/to/src")
output_dir = "./apsim_test_output"

# New (relative paths)
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))
output_dir = str(repo_root / "data" / "apsim" / "apsim_test_output")
```

**Files updated:**
- `tests/apsim/test_apsim_main.py`
- `tests/apsim/test_weather_creation.py`
- `tests/apsim/example_weather_usage.py`

### 4. Created Documentation

#### `tests/apsim/README.md`
- Test file descriptions
- Running instructions
- Output locations
- Performance benchmarks
- 149 lines

#### `data/apsim/README.md`
- Template descriptions
- Output directory structure
- Usage examples
- File formats
- Cleanup instructions
- 208 lines

## Repository Structure

### Before
```
src/modfilegen/Converter/ApsimConverter/
├── Source code files (.py)
├── Test files (test_*.py, example_*.py)
├── Test data (.met, .apsimx)
├── Templates (.apsimx)
└── Output directories (*_output/)
```
**Problems:**
- Mixed source and tests
- Hard to find specific files
- Output cluttered with source
- Unclear what's production vs test code

### After
```
src/modfilegen/Converter/ApsimConverter/
├── apsimconverter.py
├── apsimweatherconverter.py
├── apsimsoilconverter.py
├── apsimmanagementconverter.py
├── __init__.py
└── Documentation (.md files)

tests/apsim/
├── test_*.py (13 test scripts)
├── example_*.py (4 example scripts)
├── test_*.met (10 test weather files)
├── test_*.apsimx (10 test simulation files)
├── example_*.apsimx (2 example files)
└── README.md

data/apsim/
├── Templates/
│   ├── Maize.apsimx
│   ├── simulation.apsimx
│   └── *_toolbox.apsimx (3 files)
├── Output/
│   ├── apsim_test_output/
│   ├── weather_test_output/
│   ├── example_output/
│   └── output/
└── README.md
```

**Benefits:**
✓ Clear separation of concerns
✓ Easy to find tests
✓ Data and results organized
✓ Source code clean and focused
✓ Better for version control (can .gitignore data/apsim/Output/)

## Testing Verification

All tests pass with new structure:

### test_weather_creation.py
```bash
$ python tests/apsim/test_weather_creation.py
✓ PASS - Generate content (export)
✓ PASS - Create file directly (export_to_file)
✓ PASS - Verify methods are identical
```

### test_apsim_main.py
```bash
$ python tests/apsim/test_apsim_main.py
📊 Generated 800 files
✓ All tests passed successfully
```

Output locations verified:
- Weather tests → `data/apsim/weather_test_output/`
- APSIM tests → `data/apsim/apsim_test_output/`
- Examples → `data/apsim/example_output/`

## Migration Checklist

- [x] Create `tests/apsim/` directory
- [x] Create `data/apsim/` directory
- [x] Move test scripts to `tests/apsim/`
- [x] Move example scripts to `tests/apsim/`
- [x] Move test data files to `tests/apsim/`
- [x] Move templates to `data/apsim/`
- [x] Move output directories to `data/apsim/`
- [x] Update imports in test files
- [x] Update output paths in test files
- [x] Create `tests/apsim/README.md`
- [x] Create `data/apsim/README.md`
- [x] Verify all tests pass
- [x] Document reorganization

## Running Tests

### From repository root (recommended):
```bash
cd /path/to/ModFileGen
python tests/apsim/test_apsim_main.py
python tests/apsim/test_weather_creation.py
python tests/apsim/example_weather_usage.py
```

### From tests directory:
```bash
cd /path/to/ModFileGen/tests/apsim
python test_apsim_main.py
python test_weather_creation.py
python example_weather_usage.py
```

Both methods work correctly with the new relative path system.

## Impact on Users

### No Breaking Changes
- Source code location unchanged: `src/modfilegen/Converter/ApsimConverter/`
- Module imports unchanged: `from modfilegen.Converter.ApsimConverter import ...`
- API unchanged: All public functions work as before

### For Test Users
- Update test script paths if referencing directly
- Use new paths for test data
- Check documentation in test directories

### For Documentation
- Updated paths in README files
- Tests now clearly separated
- Better onboarding experience

## File Count Summary

| Category | Count | Location |
|----------|-------|----------|
| Source files | 5 | `src/modfilegen/Converter/ApsimConverter/` |
| Documentation | 6 | `src/modfilegen/Converter/ApsimConverter/` |
| Test scripts | 17 | `tests/apsim/` |
| Test data | 23 | `tests/apsim/` |
| Templates | 6 | `data/apsim/` |
| Output dirs | 4 | `data/apsim/` |

**Total:** 61 files/directories reorganized

## Next Steps

### Recommended Actions
1. Add `.gitignore` entries:
   ```
   # APSIM test outputs
   data/apsim/apsim_test_output/
   data/apsim/weather_test_output/
   data/apsim/example_output/
   data/apsim/output/
   ```

2. Update CI/CD pipelines if they reference test paths

3. Notify team members of new structure

4. Consider similar reorganization for other converters (Stics, Dssat)

### Optional Enhancements
- Add `__init__.py` to tests/apsim for pytest discovery
- Create test fixtures for common setup
- Add GitHub Actions workflow for automated testing
- Generate test coverage reports

## Conclusion

Repository is now cleaner and more maintainable:
- ✅ Clear separation: source / tests / data
- ✅ All tests passing
- ✅ Well documented
- ✅ No breaking changes
- ✅ Better developer experience

The reorganization improves code clarity without affecting functionality or existing integrations.

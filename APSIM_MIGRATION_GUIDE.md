# APSIM Repository Reorganization - Quick Migration Guide

## What Changed?

The APSIM Converter files have been reorganized for better clarity:

```
BEFORE:
src/modfilegen/Converter/ApsimConverter/
├── Source code + tests + data all mixed together

AFTER:
src/modfilegen/Converter/ApsimConverter/  ← Source code only
tests/apsim/                               ← All tests
data/apsim/                                ← Data & results
```

## Do I Need to Change Anything?

### If you're USING the module:
**NO CHANGES NEEDED** ✅

Your code still works:
```python
from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter
# Works exactly as before
```

### If you're RUNNING tests:
**UPDATE TEST PATHS** 📝

Old way:
```bash
cd src/modfilegen/Converter/ApsimConverter
python test_apsim_main.py
```

New way:
```bash
cd /path/to/ModFileGen
python tests/apsim/test_apsim_main.py
```

### If you're REFERENCING test files:
**UPDATE FILE PATHS** 📝

Old paths:
```python
"src/modfilegen/Converter/ApsimConverter/test_weather.met"
"src/modfilegen/Converter/ApsimConverter/Maize.apsimx"
```

New paths:
```python
"tests/apsim/test_weather.met"
"data/apsim/Maize.apsimx"
```

## Quick Reference

| File Type | Old Location | New Location |
|-----------|--------------|--------------|
| Source code | `src/.../ApsimConverter/*.py` | **Unchanged** |
| Tests | `src/.../ApsimConverter/test_*.py` | `tests/apsim/` |
| Examples | `src/.../ApsimConverter/example_*.py` | `tests/apsim/` |
| Templates | `src/.../ApsimConverter/*.apsimx` | `data/apsim/` |
| Test outputs | `src/.../ApsimConverter/*_output/` | `data/apsim/` |
| Documentation | `src/.../ApsimConverter/*.md` | **Unchanged** |

## Running Tests

All tests work from the repository root:

```bash
# Weather tests
python tests/apsim/test_weather_creation.py

# Main converter test
python tests/apsim/test_apsim_main.py

# Examples
python tests/apsim/example_weather_usage.py
```

## Finding Files

### I need test data:
👉 Look in `tests/apsim/`

### I need templates (Maize.apsimx, etc):
👉 Look in `data/apsim/`

### I need test results:
👉 Look in `data/apsim/apsim_test_output/` or `data/apsim/example_output/`

### I need source code:
👉 Still in `src/modfilegen/Converter/ApsimConverter/`

### I need documentation:
👉 Check all three locations:
- `src/modfilegen/Converter/ApsimConverter/README.md` - Main docs
- `tests/apsim/README.md` - Test docs
- `data/apsim/README.md` - Data docs

## Benefits

✅ **Cleaner structure** - Easy to find what you need
✅ **Better separation** - Source code separate from tests
✅ **Easier testing** - All tests in one place
✅ **Better .gitignore** - Can ignore output directories
✅ **No breaking changes** - Module imports still work

## Need Help?

- Check `REORGANIZATION_APSIM.md` for full details
- See `tests/apsim/README.md` for test documentation
- See `data/apsim/README.md` for data documentation
- Source code documentation still in `src/modfilegen/Converter/ApsimConverter/`

## Verification

Test everything still works:
```bash
cd /path/to/ModFileGen
python tests/apsim/test_weather_creation.py
# Should see: ✓ All tests passed successfully!
```

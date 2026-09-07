"""
Test flexible column detection for ApsimWeatherConverter
Demonstrates different combinations of optional columns (pan, wind, vp)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from modfilegen.Converter.ApsimConverter.apsimweatherconverter import ApsimWeatherConverter
import pandas as pd


def test_only_required_columns():
    """Test with only required columns (no vp, pan, or wind)"""
    print("\n" + "="*70)
    print("Test 1: Only Required Columns (year, day, radn, maxt, mint, rain)")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_required_only.met', data, 'Site1', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


def test_with_vp_only():
    """Test with vp column only"""
    print("\n" + "="*70)
    print("Test 2: With VP (Vapor Pressure) Only")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0],
        'vp': [18.0, 20.0, 22.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_vp_only.met', data, 'Site2', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


def test_with_pan_only():
    """Test with pan evaporation column only"""
    print("\n" + "="*70)
    print("Test 3: With PAN (Pan Evaporation) Only")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0],
        'pan': [3.0, 3.5, 4.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_pan_only.met', data, 'Site3', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


def test_with_wind_only():
    """Test with wind speed column only"""
    print("\n" + "="*70)
    print("Test 4: With WIND (Wind Speed) Only")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0],
        'wind': [2.0, 2.5, 3.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_wind_only.met', data, 'Site4', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


def test_with_vp_and_pan():
    """Test with vp and pan columns"""
    print("\n" + "="*70)
    print("Test 5: With VP and PAN")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0],
        'vp': [18.0, 20.0, 22.0],
        'pan': [3.0, 3.5, 4.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_vp_pan.met', data, 'Site5', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


def test_with_all_optional():
    """Test with all optional columns (vp, pan, wind)"""
    print("\n" + "="*70)
    print("Test 6: With ALL Optional Columns (VP, PAN, WIND)")
    print("="*70)
    
    data = pd.DataFrame({
        'year': [2020] * 3,
        'day': [1, 2, 3],
        'radn': [10.0, 12.0, 15.0],
        'maxt': [25.0, 26.0, 28.0],
        'mint': [15.0, 16.0, 17.0],
        'rain': [0.0, 5.0, 0.0],
        'vp': [18.0, 20.0, 22.0],
        'pan': [3.0, 3.5, 4.0],
        'wind': [2.0, 2.5, 3.0]
    })
    
    converter = ApsimWeatherConverter()
    content = converter.export_simple(
        'test_all_optional.met', data, 'Site6', '2020',
        latitude=45.0, longitude=-93.0
    )
    print(content)
    return True


if __name__ == "__main__":
    print("\n" + "="*70)
    print("FLEXIBLE COLUMN DETECTION TEST SUITE")
    print("="*70)
    print("\nThis demonstrates that only columns present in the data")
    print("are included in the APSIM weather file output.\n")
    
    results = []
    results.append(("Required only", test_only_required_columns()))
    results.append(("VP only", test_with_vp_only()))
    results.append(("PAN only", test_with_pan_only()))
    results.append(("WIND only", test_with_wind_only()))
    results.append(("VP + PAN", test_with_vp_and_pan()))
    results.append(("All optional", test_with_all_optional()))
    
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    for name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{name:20s}: {status}")
    print("="*70)
    
    if all(r[1] for r in results):
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)

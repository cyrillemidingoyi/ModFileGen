"""
Test script for ApsimWeatherConverter
This script demonstrates how to use the ApsimWeatherConverter to generate APSIM weather files.
"""

import sys
import os

# Add the parent directory to the path to import modfilegen
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from modfilegen.Converter.ApsimConverter.apsimweatherconverter import ApsimWeatherConverter
import pandas as pd


def test_basic_weather_conversion():
    """Test basic weather file generation with sample data"""
    
    print("=" * 60)
    print("Testing ApsimWeatherConverter")
    print("=" * 60)
    
    # Create sample weather data matching your example
    sample_data = pd.DataFrame({
        'year': [1999] * 15,
        'day': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        'radn': [8.0, 8.0, 13.0, 26.0, 25.0, 27.0, 27.0, 30.0, 26.0, 21.0, 
                 27.0, 14.0, 27.0, 28.0, 26.0],
        'maxt': [23.0, 23.5, 27.5, 30.5, 30.0, 30.0, 30.5, 32.5, 33.5, 31.0,
                 34.5, 33.0, 33.5, 33.5, 32.0],
        'mint': [17.5, 18.0, 18.5, 19.0, 18.0, 17.0, 16.5, 16.5, 18.5, 20.0,
                 18.0, 21.0, 21.0, 20.0, 20.0],
        'rain': [4.9, 20.2, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.6,
                 5.8, 0.0, 13.4, 0.0, 0.0],
        'vp': [21.0, 23.0, 22.0, 23.0, 20.0, 18.0, 20.0, 19.0, 23.0, 25.0,
               24.0, 29.0, 25.0, 25.0, 22.0],
        'wind': [2.0, 1.8, 4.0, 7.0, 6.8, 9.2, 8.2, 9.0, 7.6, 6.0,
                 6.8, 6.0, 8.0, 10.0, 10.2],
        'code': ['999999'] * 15
    })
    
    print("\nSample data (first 5 rows):")
    print(sample_data.head())
    
    # Create converter instance
    converter = ApsimWeatherConverter()
    
    # Test export_simple method
    output_file = "test_weather.met"
    print(f"\nGenerating weather file: {output_file}")
    
    content = converter.export_simple(
        output_file, 
        sample_data, 
        site_name='TestSite', 
        year='1999',
        latitude=-23.8,
        longitude=151.3,
        tav=22.5,
        amp=8.2
    )
    
    if content:
        print("\n" + "=" * 60)
        print("Generated file content (first 20 lines):")
        print("=" * 60)
        lines = content.split('\n')
        for i, line in enumerate(lines[:20]):
            print(f"{i+1:3d}: {line}")
        
        if len(lines) > 20:
            print(f"... ({len(lines) - 20} more lines)")
        
        print("\n" + "=" * 60)
        print(f"✓ Test passed! Weather file created successfully.")
        print(f"  Location: {os.path.abspath(output_file)}")
        print("=" * 60)
        return True
    else:
        print("✗ Test failed! Could not generate weather file.")
        return False


def test_minimal_weather_data():
    """Test with minimal required columns only"""
    
    print("\n\n" + "=" * 60)
    print("Testing with minimal required columns")
    print("=" * 60)
    
    # Create minimal data (only required columns)
    minimal_data = pd.DataFrame({
        'year': [2020] * 5,
        'day': [1, 2, 3, 4, 5],
        'radn': [10.0, 12.0, 15.0, 14.0, 13.0],
        'maxt': [25.0, 26.0, 28.0, 27.0, 26.0],
        'mint': [15.0, 16.0, 17.0, 16.0, 15.0],
        'rain': [0.0, 5.0, 0.0, 0.0, 2.0]
    })
    
    print("\nMinimal data (default values will be used for vp, wind, code):")
    print(minimal_data)
    
    converter = ApsimWeatherConverter()
    output_file = "test_weather_minimal.met"
    
    print(f"\nGenerating weather file: {output_file}")
    content = converter.export_simple(
        output_file,
        minimal_data,
        site_name='MinimalTest',
        year='2020',
        latitude=45.0,
        longitude=-93.0
    )
    
    if content:
        print("\n" + "=" * 60)
        print("Generated file content:")
        print("=" * 60)
        print(content)
        print("=" * 60)
        print(f"✓ Test passed! Minimal weather file created successfully.")
        print("=" * 60)
        return True
    else:
        print("✗ Test failed!")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ApsimWeatherConverter Test Suite")
    print("=" * 60)
    
    test1_result = test_basic_weather_conversion()
    test2_result = test_minimal_weather_data()
    
    print("\n\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Test 1 (Basic conversion): {'PASSED' if test1_result else 'FAILED'}")
    print(f"Test 2 (Minimal data):     {'PASSED' if test2_result else 'FAILED'}")
    print("=" * 60)
    
    if test1_result and test2_result:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)

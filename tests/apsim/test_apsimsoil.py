"""
Test script for ApsimSoilConverter
Demonstrates how to create APSIM soil files in .apsimx format
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from modfilegen.Converter.ApsimConverter.apsimsoilconverter import ApsimSoilConverter
import pandas as pd
import json


def test_create_new_soil():
    """Test creating a new APSIM file with soil"""
    
    print("="*70)
    print("Test 1: Create New Soil File")
    print("="*70)
    
    # Create sample 5-layer soil profile
    soil_data = pd.DataFrame({
        'thickness': [150, 150, 300, 300, 300],
        'bd': [1.02, 1.07, 1.09, 1.16, 1.17],
        'airdry': [0.13, 0.20, 0.28, 0.28, 0.28],
        'll15': [0.26, 0.25, 0.28, 0.28, 0.28],
        'dul': [0.52, 0.50, 0.49, 0.48, 0.47],
        'sat': [0.59, 0.57, 0.56, 0.53, 0.53],
        'ks': [20.0, 20.0, 20.0, 20.0, 20.0],
        'swcon': [0.3, 0.3, 0.3, 0.3, 0.3],
        'carbon': [1.2, 0.96, 0.6, 0.3, 0.18],
        'ph': [8.0, 8.0, 8.0, 8.0, 8.0]
    })
    
    print("\nSoil profile data:")
    print(soil_data)
    
    converter = ApsimSoilConverter()
    result = converter.export_simple(
        soil_data,
        'test_new_soil.apsimx',
        site_name='TestSite',
        latitude=-27.58,
        longitude=151.32,
        soil_type='Clay'
    )
    
    if result:
        # Verify the file
        with open('test_new_soil.apsimx', 'r') as f:
            data = json.load(f)
        
        print("\n✓ File created successfully!")
        print(f"  - Simulation name: {data['Children'][0]['Name']}")
        
        # Find soil in structure
        zone = data['Children'][0]['Children'][0]
        soil = zone['Children'][0]
        print(f"  - Soil site: {soil['Site']}")
        print(f"  - Soil type: {soil['SoilType']}")
        print(f"  - Number of layers: {len(soil['Children'][0]['Thickness'])}")
        print(f"  - Coordinates: ({soil['Latitude']}, {soil['Longitude']})")
        
        return True
    else:
        print("✗ Failed to create soil file")
        return False


def test_minimal_soil():
    """Test with minimal required columns only"""
    
    print("\n" + "="*70)
    print("Test 2: Minimal Soil Data (required columns only)")
    print("="*70)
    
    # Only required columns
    minimal_soil = pd.DataFrame({
        'thickness': [200, 200, 400],
        'bd': [1.1, 1.2, 1.3],
        'll15': [0.20, 0.22, 0.24],
        'dul': [0.40, 0.42, 0.44],
        'sat': [0.50, 0.52, 0.54]
    })
    
    print("\nMinimal soil data (defaults will be used for optional properties):")
    print(minimal_soil)
    
    converter = ApsimSoilConverter()
    result = converter.export_simple(
        minimal_soil,
        'test_minimal_soil.apsimx',
        site_name='MinimalSite',
        latitude=45.0,
        longitude=-93.0,
        soil_type='Sandy Loam'
    )
    
    if result:
        with open('test_minimal_soil.apsimx', 'r') as f:
            data = json.load(f)
        
        zone = data['Children'][0]['Children'][0]
        soil = zone['Children'][0]
        physical = soil['Children'][0]
        
        print("\n✓ Minimal soil file created!")
        print(f"  - Number of layers: {len(physical['Thickness'])}")
        print(f"  - AirDry (calculated): {physical['AirDry']}")
        print(f"  - KS (default): {physical['KS']}")
        
        return True
    else:
        print("✗ Failed to create minimal soil file")
        return False


def test_update_existing_soil():
    """Test updating soil in an existing APSIM file"""
    
    print("\n" + "="*70)
    print("Test 3: Update Soil in Existing File")
    print("="*70)
    
    # First create a file
    initial_soil = pd.DataFrame({
        'thickness': [100, 200],
        'bd': [1.0, 1.1],
        'll15': [0.15, 0.18],
        'dul': [0.35, 0.38],
        'sat': [0.45, 0.48]
    })
    
    converter = ApsimSoilConverter()
    print("\n1. Creating initial file...")
    converter.export_simple(
        initial_soil,
        'test_update_soil.apsimx',
        site_name='InitialSite',
        soil_type='Initial'
    )
    
    # Now update with new soil
    updated_soil = pd.DataFrame({
        'thickness': [150, 150, 300],
        'bd': [1.05, 1.10, 1.15],
        'll15': [0.22, 0.24, 0.26],
        'dul': [0.42, 0.44, 0.46],
        'sat': [0.52, 0.54, 0.56],
        'carbon': [1.5, 1.0, 0.5],
        'ph': [7.0, 7.5, 8.0]
    })
    
    print("2. Updating with new soil profile...")
    result = converter.export_simple(
        updated_soil,
        'test_update_soil.apsimx',
        site_name='UpdatedSite',
        latitude=50.0,
        longitude=10.0,
        soil_type='Updated'
    )
    
    if result:
        with open('test_update_soil.apsimx', 'r') as f:
            data = json.load(f)
        
        zone = data['Children'][0]['Children'][0]
        soil = zone['Children'][0]
        
        print("\n✓ Soil updated successfully!")
        print(f"  - Site changed: InitialSite → {soil['Site']}")
        print(f"  - Type changed: Initial → {soil['SoilType']}")
        print(f"  - Layers changed: 2 → {len(soil['Children'][0]['Thickness'])}")
        print(f"  - New coordinates: ({soil['Latitude']}, {soil['Longitude']})")
        
        return True
    else:
        print("✗ Failed to update soil")
        return False


def test_deep_profile():
    """Test with many layers (deep soil profile)"""
    
    print("\n" + "="*70)
    print("Test 4: Deep Soil Profile (10 layers)")
    print("="*70)
    
    # 10-layer profile
    n_layers = 10
    depths = [150] * 2 + [200] * 8  # 150mm for top 2, 200mm for rest
    
    deep_soil = pd.DataFrame({
        'thickness': depths,
        'bd': [1.0 + i * 0.05 for i in range(n_layers)],
        'airdry': [0.10 + i * 0.02 for i in range(n_layers)],
        'll15': [0.20 + i * 0.01 for i in range(n_layers)],
        'dul': [0.40 + i * 0.01 for i in range(n_layers)],
        'sat': [0.50 + i * 0.005 for i in range(n_layers)],
        'carbon': [2.0 * (0.7 ** i) for i in range(n_layers)],
        'ph': [6.5 + i * 0.15 for i in range(n_layers)]
    })
    
    print(f"\nDeep profile: {n_layers} layers, total depth: {sum(depths)} mm")
    print(deep_soil[['thickness', 'bd', 'dul', 'carbon', 'ph']].to_string())
    
    converter = ApsimSoilConverter()
    result = converter.export_simple(
        deep_soil,
        'test_deep_soil.apsimx',
        site_name='DeepSite',
        latitude=-35.0,
        longitude=149.0,
        soil_type='Deep Clay'
    )
    
    if result:
        print(f"\n✓ Deep soil profile created successfully!")
        print(f"  - Total depth: {sum(depths)} mm ({sum(depths)/1000:.2f} m)")
        return True
    else:
        print("✗ Failed to create deep soil profile")
        return False


if __name__ == "__main__":
    print("\n" + "="*70)
    print("APSIM SOIL CONVERTER TEST SUITE")
    print("="*70)
    
    results = []
    results.append(("Create new soil", test_create_new_soil()))
    results.append(("Minimal soil", test_minimal_soil()))
    results.append(("Update existing", test_update_existing_soil()))
    results.append(("Deep profile", test_deep_profile()))
    
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

"""
Test suite for APSIM Management Converter

Tests various management operations and sharing across simulations.
"""

import os
import sys
import json
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from modfilegen.Converter.ApsimConverter import ApsimManagementConverter


def test_basic_management():
    """Test creating basic management operations."""
    print("=" * 70)
    print("Test 1: Basic Management Operations")
    print("=" * 70)
    
    # Create sample management data
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
    
    print("\nManagement operations:")
    print(management_data[['operation_type', 'crop']].to_string())
    
    converter = ApsimManagementConverter()
    output_file = "test_basic_management.apsimx"
    
    converter.export_simple(
        management_data,
        output_file,
        toolbox_name="Wheat Management"
    )
    
    # Verify file
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    print(f"\n✓ File created: {output_file}")
    print(f"  - Toolbox name: {data['Name']}")
    print(f"  - Number of operations: {len(data['Children'][0]['Children'])}")
    print(f"  - Operation names:")
    for op in data['Children'][0]['Children']:
        print(f"    * {op['Name']}")


def test_rule_based_sowing():
    """Test rule-based sowing operations."""
    print("\n" + "=" * 70)
    print("Test 2: Rule-Based Sowing")
    print("=" * 70)
    
    management_data = pd.DataFrame([
        {
            'operation_type': 'sowing',
            'crop': 'Maize',
            'cultivar': 'Pioneer_3394',
            'start_date': '1-nov',
            'end_date': '31-dec',
            'population': 8.0,
            'depth': 40.0,
            'row_spacing': 750.0,
            'min_esw': 100.0,
            'min_rain': 25.0,
            'rain_days': 7,
            'sowing_rule': True
        }
    ])
    
    print("\nSowing rule parameters:")
    print(f"  - Sowing window: {management_data.iloc[0]['start_date']} to {management_data.iloc[0]['end_date']}")
    print(f"  - Min ESW: {management_data.iloc[0]['min_esw']} mm")
    print(f"  - Min rainfall: {management_data.iloc[0]['min_rain']} mm in {management_data.iloc[0]['rain_days']} days")
    
    converter = ApsimManagementConverter()
    output_file = "test_sowing_rule.apsimx"
    
    converter.export_simple(
        management_data,
        output_file,
        toolbox_name="Rule-Based Sowing"
    )
    
    # Verify
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    sowing_op = data['Children'][0]['Children'][0]
    print(f"\n✓ Rule-based sowing created")
    print(f"  - Operation name: {sowing_op['Name']}")
    print(f"  - Parameters: {len(sowing_op['Parameters'])} parameters configured")


def test_multiple_fertilizations():
    """Test multiple fertilization events."""
    print("\n" + "=" * 70)
    print("Test 3: Multiple Fertilization Events")
    print("=" * 70)
    
    management_data = pd.DataFrame([
        {
            'operation_type': 'fertilization',
            'crop': 'Cotton',
            'timing': 'at_sowing',
            'fertilizer_type': 'NO3N',
            'amount': 50.0
        },
        {
            'operation_type': 'fertilization',
            'date': '15-jan',
            'fertilizer_type': 'UreaN',
            'amount': 75.0
        },
        {
            'operation_type': 'fertilization',
            'date': '1-feb',
            'fertilizer_type': 'UreaN',
            'amount': 75.0
        }
    ])
    
    print("\nFertilization schedule:")
    for idx, row in management_data.iterrows():
        timing = row.get('timing', row.get('date', 'unknown'))
        print(f"  - {timing}: {row['amount']} kg/ha {row['fertilizer_type']}")
    
    converter = ApsimManagementConverter()
    output_file = "test_fertilization.apsimx"
    
    converter.export_simple(
        management_data,
        output_file,
        toolbox_name="Fertilization Schedule"
    )
    
    # Verify
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    print(f"\n✓ Multiple fertilizations created")
    print(f"  - Total events: {len(data['Children'][0]['Children'])}")


def test_irrigation_operations():
    """Test automatic and scheduled irrigation."""
    print("\n" + "=" * 70)
    print("Test 4: Irrigation Operations")
    print("=" * 70)
    
    management_data = pd.DataFrame([
        {
            'operation_type': 'irrigation',
            'crop': 'Cotton',
            'automatic': True,
            'threshold': 0.5,
            'amount': 30.0
        },
        {
            'operation_type': 'irrigation',
            'date': '15-jan',
            'amount': 50.0,
            'automatic': False
        }
    ])
    
    print("\nIrrigation setup:")
    print(f"  - Automatic irrigation: threshold={management_data.iloc[0]['threshold']}, amount={management_data.iloc[0]['amount']} mm")
    print(f"  - Scheduled irrigation: date={management_data.iloc[1]['date']}, amount={management_data.iloc[1]['amount']} mm")
    
    converter = ApsimManagementConverter()
    output_file = "test_irrigation.apsimx"
    
    converter.export_simple(
        management_data,
        output_file,
        toolbox_name="Irrigation Management"
    )
    
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    print(f"\n✓ Irrigation operations created")
    print(f"  - Operations: {[op['Name'] for op in data['Children'][0]['Children']]}")


def test_complete_crop_cycle():
    """Test complete crop cycle with all operations."""
    print("\n" + "=" * 70)
    print("Test 5: Complete Crop Cycle")
    print("=" * 70)
    
    management_data = pd.DataFrame([
        {
            'operation_type': 'tillage',
            'date': '1-oct',
            'tillage_type': 'disc'
        },
        {
            'operation_type': 'sowing',
            'crop': 'Soybean',
            'date': '15-oct',
            'cultivar': 'Dawson',
            'population': 25.0,
            'depth': 40.0,
            'row_spacing': 500.0
        },
        {
            'operation_type': 'fertilization',
            'crop': 'Soybean',
            'timing': 'at_sowing',
            'fertilizer_type': 'MAP',
            'amount': 100.0
        },
        {
            'operation_type': 'irrigation',
            'crop': 'Soybean',
            'automatic': True,
            'threshold': 0.4,
            'amount': 25.0
        },
        {
            'operation_type': 'harvest',
            'crop': 'Soybean'
        }
    ])
    
    print("\nComplete crop cycle operations:")
    for idx, row in management_data.iterrows():
        print(f"  {idx + 1}. {row['operation_type'].capitalize()}")
    
    converter = ApsimManagementConverter()
    output_file = "test_complete_cycle.apsimx"
    
    converter.export_simple(
        management_data,
        output_file,
        toolbox_name="Soybean Complete Cycle"
    )
    
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    print(f"\n✓ Complete cycle created")
    print(f"  - Total operations: {len(data['Children'][0]['Children'])}")
    print(f"  - Operations:")
    for op in data['Children'][0]['Children']:
        print(f"    * {op['Name']}")


def test_shared_management():
    """
    Demonstrate how management operations can be shared across multiple simulations.
    This creates a management toolbox that can be referenced from multiple .apsimx files.
    """
    print("\n" + "=" * 70)
    print("Test 6: Shared Management Across Simulations")
    print("=" * 70)
    
    # Create a shared management toolbox
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
    
    converter = ApsimManagementConverter()
    
    # Create shared toolbox
    toolbox_file = "shared_wheat_management.apsimx"
    converter.export_simple(
        shared_management,
        toolbox_file,
        toolbox_name="Shared Wheat Management"
    )
    
    print("\n✓ Shared management toolbox created")
    print(f"  - File: {toolbox_file}")
    print(f"  - This toolbox contains reusable management operations")
    print(f"  - Multiple simulation files can reference these operations")
    print(f"\nUsage in APSIM GUI:")
    print(f"  1. Load {toolbox_file} as a reference in your simulation")
    print(f"  2. Drag and drop management operations into your simulation")
    print(f"  3. Operations will be copied, but can be updated from the source")


def run_all_tests():
    """Run all management converter tests."""
    print("\n")
    print("=" * 70)
    print("APSIM MANAGEMENT CONVERTER TEST SUITE")
    print("=" * 70)
    
    tests = [
        test_basic_management,
        test_rule_based_sowing,
        test_multiple_fertilizations,
        test_irrigation_operations,
        test_complete_crop_cycle,
        test_shared_management
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n✗ Test failed: {test_func.__name__}")
            print(f"  Error: {str(e)}")
            failed += 1
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")
    print("=" * 70)
    
    if failed == 0:
        print("\n✓ All tests passed!")
    else:
        print(f"\n✗ {failed} test(s) failed")


if __name__ == "__main__":
    run_all_tests()

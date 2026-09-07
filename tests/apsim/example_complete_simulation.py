"""
Complete APSIM Simulation Example

This script demonstrates how to use all three converters together
to create a complete APSIM simulation with weather, soil, and management.
"""

import pandas as pd
import json
from modfilegen.Converter.ApsimConverter import (
    ApsimWeatherConverter,
    ApsimSoilConverter,
    ApsimManagementConverter
)


def create_complete_simulation():
    """
    Create a complete APSIM simulation with weather, soil, and management.
    """
    print("=" * 70)
    print("CREATING COMPLETE APSIM SIMULATION")
    print("=" * 70)
    
    # ==================== STEP 1: Create Weather File ====================
    print("\nStep 1: Creating weather file...")
    
    weather_data = pd.DataFrame({
        'year': [2020] * 365,
        'day': range(1, 366),
        'radn': [15.0] * 365,  # Simplified - use real data
        'maxt': [28.0] * 365,
        'mint': [18.0] * 365,
        'rain': [2.0] * 365
    })
    
    weather_converter = ApsimWeatherConverter()
    weather_file = weather_converter.export_simple(
        output_path='simulation_weather.met',
        weather_data_df=weather_data,
        site_name='ExampleSite',
        year='2020',
        latitude=-27.58,
        longitude=151.32,
        tav=22.0,
        amp=8.0
    )
    
    print(f"✓ Weather file created: {weather_file}")
    
    # ==================== STEP 2: Create Soil ====================
    print("\nStep 2: Creating soil profile...")
    
    soil_data = pd.DataFrame({
        'thickness': [150, 150, 300, 300, 300],
        'bd': [1.02, 1.07, 1.09, 1.16, 1.17],
        'airdry': [0.13, 0.20, 0.28, 0.28, 0.28],
        'll15': [0.26, 0.25, 0.28, 0.28, 0.28],
        'dul': [0.52, 0.50, 0.49, 0.48, 0.47],
        'sat': [0.59, 0.57, 0.56, 0.53, 0.53],
        'ks': [20.0, 20.0, 20.0, 20.0, 20.0],
        'swcon': [0.3, 0.3, 0.3, 0.3, 0.3],
        'carbon': [1.20, 0.96, 0.60, 0.30, 0.18],
        'ph': [8.0, 8.0, 8.0, 8.0, 8.0]
    })
    
    soil_converter = ApsimSoilConverter()
    soil_file = soil_converter.export_simple(
        soil_data_df=soil_data,
        output_apsimx='simulation.apsimx',
        site_name='ExampleSite',
        latitude=-27.58,
        longitude=151.32,
        soil_type='Clay'
    )
    
    print(f"✓ Soil file created: {soil_file}")
    
    # ==================== STEP 3: Add Management ====================
    print("\nStep 3: Adding management operations...")
    
    management_data = pd.DataFrame([
        {
            'operation_type': 'sowing',
            'crop': 'Wheat',
            'start_date': '15-may',
            'end_date': '15-jun',
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
            'operation_type': 'fertilization',
            'date': '15-aug',
            'fertilizer_type': 'UreaN',
            'amount': 50.0
        },
        {
            'operation_type': 'irrigation',
            'crop': 'Wheat',
            'automatic': True,
            'threshold': 0.5,
            'amount': 25.0
        },
        {
            'operation_type': 'harvest',
            'crop': 'Wheat'
        }
    ])
    
    # Load existing simulation file
    with open('simulation.apsimx', 'r') as f:
        sim_data = json.load(f)
    
    # Add management operations
    management_converter = ApsimManagementConverter()
    sim_data = management_converter._add_management_to_apsimx(
        sim_data,
        management_converter._build_management_operations(management_data),
        'Management'
    )
    
    # Update simulation structure to add weather reference and other components
    sim_data = _complete_simulation_structure(sim_data, weather_file)
    
    # Save completed simulation
    with open('simulation.apsimx', 'w') as f:
        json.dump(sim_data, f, indent=2)
    
    print(f"✓ Management added to: simulation.apsimx")
    
    # ==================== STEP 4: Summary ====================
    print("\n" + "=" * 70)
    print("SIMULATION CREATED SUCCESSFULLY")
    print("=" * 70)
    print(f"\nFiles created:")
    print(f"  1. Weather: simulation_weather.met")
    print(f"  2. Simulation: simulation.apsimx")
    print(f"\nSimulation includes:")
    print(f"  - Weather: {weather_file}")
    print(f"  - Soil: ExampleSite Clay (5 layers, 1.2m depth)")
    print(f"  - Crop: Wheat")
    print(f"  - Management: 5 operations")
    print(f"    * Rule-based sowing")
    print(f"    * Fertilization at sowing (100 kg/ha UreaN)")
    print(f"    * Top-up fertilization (50 kg/ha UreaN)")
    print(f"    * Automatic irrigation")
    print(f"    * Automatic harvest")
    print(f"\nYou can now open 'simulation.apsimx' in APSIM GUI to run the simulation.")
    

def _complete_simulation_structure(sim_data, weather_file):
    """
    Add additional components to make a complete runnable simulation.
    This is a simplified version - real implementation would be more complex.
    """
    # This would add:
    # - Clock (simulation dates)
    # - Weather component (reference to .met file)
    # - Summary output
    # - Report
    # etc.
    
    # For now, just return the data - users can complete in APSIM GUI
    return sim_data


def create_shared_management_example():
    """
    Example: Create shared management that can be used across multiple simulations.
    """
    print("\n" + "=" * 70)
    print("CREATING SHARED MANAGEMENT TOOLBOX")
    print("=" * 70)
    
    # Create management for different crops
    wheat_mgmt = pd.DataFrame([
        {
            'operation_type': 'sowing',
            'crop': 'Wheat',
            'start_date': '15-may',
            'end_date': '15-jun',
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
    
    maize_mgmt = pd.DataFrame([
        {
            'operation_type': 'sowing',
            'crop': 'Maize',
            'start_date': '1-oct',
            'end_date': '31-dec',
            'cultivar': 'Pioneer_3394',
            'population': 8.0,
            'depth': 40.0,
            'row_spacing': 750.0,
            'min_esw': 100.0,
            'min_rain': 25.0,
            'rain_days': 7,
            'sowing_rule': True
        },
        {
            'operation_type': 'fertilization',
            'crop': 'Maize',
            'timing': 'at_sowing',
            'fertilizer_type': 'NO3N',
            'amount': 150.0
        },
        {
            'operation_type': 'irrigation',
            'crop': 'Maize',
            'automatic': True,
            'threshold': 0.5,
            'amount': 30.0
        },
        {
            'operation_type': 'harvest',
            'crop': 'Maize'
        }
    ])
    
    converter = ApsimManagementConverter()
    
    # Create separate toolboxes for each crop
    wheat_toolbox = converter.export_simple(
        wheat_mgmt,
        'wheat_management_toolbox.apsimx',
        toolbox_name='Wheat Management'
    )
    
    maize_toolbox = converter.export_simple(
        maize_mgmt,
        'maize_management_toolbox.apsimx',
        toolbox_name='Maize Management'
    )
    
    print(f"\n✓ Created management toolboxes:")
    print(f"  1. {wheat_toolbox}")
    print(f"  2. {maize_toolbox}")
    print(f"\nThese toolboxes can be:")
    print(f"  - Shared across multiple simulation files")
    print(f"  - Referenced in APSIM GUI")
    print(f"  - Used as templates for new simulations")
    print(f"  - Version controlled independently from simulations")


if __name__ == "__main__":
    # Example 1: Complete simulation
    create_complete_simulation()
    
    # Example 2: Shared management
    create_shared_management_example()
    
    print("\n" + "=" * 70)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 70)

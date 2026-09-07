"""
Example: Using APSIM Initialization Converter

This example demonstrates how to use the initialization converter
both standalone and integrated with the main APSIM converter.
"""

import sys
from pathlib import Path

# Add source to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))

from modfilegen.Converter.ApsimConverter import ApsimInitConverter


def example_standalone_initialization():
    """
    Example 1: Create initialization file with parameters (no database).
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Standalone Initialization with Parameters")
    print("="*70)
    
    # Create converter
    converter = ApsimInitConverter()
    
    # Output directory
    output_dir = repo_root / "data" / "apsim" / "example_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate initialization file
    output_file = output_dir / "my_initialization.apsimx"
    
    converter.export_to_file(
        str(output_file),
        # Water initialization (volumetric water content by layer)
        initial_water=[0.30, 0.32, 0.33, 0.34, 0.35],
        
        # Nitrogen initialization (kg/ha by layer)
        initial_no3=[15.0, 12.0, 10.0, 8.0, 5.0],
        initial_nh4=[1.0, 0.8, 0.6, 0.4, 0.2],
        
        # Surface residue
        initial_residue_mass=1000.0,  # kg/ha
        initial_residue_type="maize",
        initial_residue_cnr=60.0,     # C:N ratio
        standing_fraction=0.2,        # 20% standing, 80% on surface
        
        # Soil layer thickness (mm)
        soil_thickness=[150, 150, 300, 300, 300],
        
        # Organic matter (FOM - Fresh Organic Matter, kg/ha)
        initial_fom=[300.0, 250.0, 200.0, 150.0, 100.0],
        fom_cn_ratio=40.0
    )
    
    print(f"\n✓ Created: {output_file}")
    print(f"  - Initial water: 30-35% by volume")
    print(f"  - Initial NO3: 15-5 kg/ha (decreasing with depth)")
    print(f"  - Surface residue: 1000 kg/ha maize stubble")
    print(f"  - Fresh organic matter: 300-100 kg/ha")


def example_with_crop_state():
    """
    Example 2: Initialize with an already established crop.
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: Initialization with Established Crop")
    print("="*70)
    
    converter = ApsimInitConverter()
    output_dir = repo_root / "data" / "apsim" / "example_output"
    output_file = output_dir / "crop_established_init.apsimx"
    
    converter.export_to_file(
        str(output_file),
        # Soil conditions
        initial_water=[0.35, 0.35, 0.35, 0.35, 0.35],
        initial_no3=[10.0, 10.0, 10.0, 10.0, 10.0],
        
        # Crop already growing
        crop_initial_state={
            'lai': 0.5,              # Leaf Area Index
            'biomass': 200.0,        # kg/ha above-ground biomass
            'root_depth': 150.0      # mm root depth
        }
    )
    
    print(f"\n✓ Created: {output_file}")
    print(f"  - Initial crop LAI: 0.5")
    print(f"  - Initial biomass: 200 kg/ha")
    print(f"  - Root depth: 150 mm")


def example_minimal_defaults():
    """
    Example 3: Use mostly default values (minimal parameters).
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Minimal Initialization (Use Defaults)")
    print("="*70)
    
    converter = ApsimInitConverter()
    output_dir = repo_root / "data" / "apsim" / "example_output"
    output_file = output_dir / "minimal_init.apsimx"
    
    # Only specify the essentials, rest will use defaults
    converter.export_to_file(
        str(output_file),
        initial_residue_mass=500.0,
        initial_residue_type="wheat"
    )
    
    print(f"\n✓ Created: {output_file}")
    print(f"  - Uses default water content (35%)")
    print(f"  - Uses default nitrogen levels (10 kg/ha per layer)")
    print(f"  - Uses default FOM distribution")


def example_integrated_workflow():
    """
    Example 4: How initialization is integrated in the main workflow.
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: Integrated Workflow (Conceptual)")
    print("="*70)
    
    print("""
When you run the main APSIM converter (apsimconverter.py), it now
automatically generates initialization files for each simulation:

    from modfilegen import GlobalVariables
    from modfilegen.Converter.ApsimConverter import apsimconverter
    
    # Configure
    GlobalVariables.set("dbMasterInput", "MasterInput.db")
    GlobalVariables.set("dbModelsDictionary", "ModelDict.db")
    GlobalVariables.set("directorypath", "output/")
    GlobalVariables.set("nthreads", 4)
    
    # Run conversion - initialization included automatically!
    apsimconverter.main()

The workflow now includes:
  1. Weather conversion (.met files)
  2. Soil conversion (.apsimx)
  3. Management conversion (.apsimx)
  4. ✨ Initialization conversion (.apsimx) ✨
  5. Main simulation assembly
  6. APSIM execution (optional)

Each simulation gets:
  - Weather.met
  - Soil.apsimx
  - Management.apsimx
  - Initialization.apsimx ← NEW!
  - Simulation.apsimx (combines all)
    """)


def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("APSIM INITIALIZATION CONVERTER - EXAMPLES")
    print("="*70)
    
    example_standalone_initialization()
    example_with_crop_state()
    example_minimal_defaults()
    example_integrated_workflow()
    
    print("\n" + "="*70)
    print("✅ All examples completed successfully!")
    print("="*70)
    print(f"\nOutput files are in: {repo_root / 'data' / 'apsim' / 'example_output'}/")
    print("\nYou can inspect the .apsimx files with:")
    print("  cat my_initialization.apsimx | python -m json.tool")


if __name__ == "__main__":
    main()

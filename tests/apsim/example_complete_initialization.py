"""
Example: Complete Crop Initialization with All Parameters

Demonstrates using all available initial values for crop establishment.
"""

import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))

from modfilegen.Converter.ApsimConverter import ApsimInitConverter


def example_complete_initialization():
    """
    Example: Initialize crop with ALL available parameters.
    """
    print("\n" + "="*70)
    print("EXAMPLE: Complete Crop Initialization (All Parameters)")
    print("="*70)
    
    converter = ApsimInitConverter()
    output_dir = repo_root / "data" / "apsim" / "example_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "complete_crop_init.apsimx"
    
    converter.export_to_file(
        str(output_file),
        
        # ========== SOIL WATER INITIALIZATION ==========
        initial_water=[0.28, 0.30, 0.32, 0.33, 0.35],  # m³/m³ by layer
        water_relative_to='LL15',  # Relative to Lower Limit
        filled_from_top=True,
        soil_thickness=[150, 150, 300, 300, 300],  # mm
        
        # ========== NITROGEN INITIALIZATION ==========
        initial_no3=[20.0, 15.0, 12.0, 8.0, 5.0],  # kg/ha by layer
        initial_nh4=[2.0, 1.5, 1.0, 0.5, 0.3],     # kg/ha by layer
        initial_ph=[7.5, 7.2, 7.0, 7.0, 6.8],      # pH by layer
        
        # ========== ORGANIC MATTER INITIALIZATION ==========
        initial_fom=[350.0, 300.0, 250.0, 180.0, 120.0],  # kg/ha FOM
        fom_cn_ratio=45.0,  # C:N ratio of fresh organic matter
        
        # ========== SURFACE RESIDUE INITIALIZATION ==========
        initial_residue_mass=1500.0,    # kg/ha
        initial_residue_type="wheat",   # wheat, maize, soybean, etc.
        initial_residue_cnr=80.0,       # C:N ratio
        standing_fraction=0.3,          # 30% standing, 70% on surface
        initial_cpr=0.0,                # C:P ratio (optional)
        
        # ========== CROP INITIAL STATE ==========
        crop_initial_state={
            'lai': 0.8,              # Leaf Area Index (m²/m²)
            'biomass': 350.0,        # Above-ground biomass (kg/ha)
            'root_depth': 200.0,     # Root depth (mm)
            'stage': 2.0,            # Phenological stage
            'n_uptake': 15.0         # Total N uptake (kg/ha)
        }
    )
    
    print(f"\n✓ Created: {output_file}")
    print(f"\n📊 INITIALIZATION SUMMARY:")
    print(f"\n  Water Content:")
    print(f"    Layer 1 (0-15cm):   28% vol.")
    print(f"    Layer 2 (15-30cm):  30% vol.")
    print(f"    Layer 3 (30-60cm):  32% vol.")
    print(f"    Layer 4 (60-90cm):  33% vol.")
    print(f"    Layer 5 (90-120cm): 35% vol.")
    
    print(f"\n  Nitrogen (NO3):")
    print(f"    Total: {sum([20.0, 15.0, 12.0, 8.0, 5.0])} kg/ha")
    print(f"    Distribution: 20→15→12→8→5 kg/ha by layer")
    
    print(f"\n  Nitrogen (NH4):")
    print(f"    Total: {sum([2.0, 1.5, 1.0, 0.5, 0.3])} kg/ha")
    print(f"    Distribution: 2.0→1.5→1.0→0.5→0.3 kg/ha by layer")
    
    print(f"\n  Organic Matter (FOM):")
    print(f"    Total: {sum([350.0, 300.0, 250.0, 180.0, 120.0])} kg/ha")
    print(f"    C:N ratio: 45:1")
    
    print(f"\n  Surface Residue:")
    print(f"    Type: Wheat stubble")
    print(f"    Mass: 1500 kg/ha")
    print(f"    Standing: 30%, Surface: 70%")
    print(f"    C:N ratio: 80:1")
    
    print(f"\n  Crop Initial State:")
    print(f"    LAI: 0.8 m²/m²")
    print(f"    Biomass: 350 kg/ha")
    print(f"    Root depth: 200 mm (20 cm)")
    print(f"    Phenological stage: 2.0")
    print(f"    N uptake: 15 kg/ha")
    
    print(f"\n✨ All initial conditions set and ready for simulation!")


def example_minimal_vs_complete():
    """
    Compare minimal vs complete initialization.
    """
    print("\n" + "="*70)
    print("COMPARISON: Minimal vs Complete Initialization")
    print("="*70)
    
    converter = ApsimInitConverter()
    output_dir = repo_root / "data" / "apsim" / "example_output"
    
    # Minimal initialization
    minimal_file = output_dir / "minimal_comparison.apsimx"
    converter.export_to_file(
        str(minimal_file),
        initial_residue_mass=500.0,
        initial_residue_type="wheat"
    )
    
    # Complete initialization
    complete_file = output_dir / "complete_comparison.apsimx"
    converter.export_to_file(
        str(complete_file),
        initial_water=[0.35, 0.35, 0.35, 0.35, 0.35],
        initial_no3=[15.0, 12.0, 10.0, 8.0, 5.0],
        initial_nh4=[1.0, 0.8, 0.6, 0.4, 0.2],
        initial_residue_mass=1000.0,
        initial_residue_type="maize",
        initial_residue_cnr=60.0,
        crop_initial_state={
            'lai': 0.5,
            'biomass': 200.0,
            'root_depth': 150.0
        }
    )
    
    minimal_size = minimal_file.stat().st_size
    complete_size = complete_file.stat().st_size
    
    print(f"\n📁 File Sizes:")
    print(f"   Minimal:  {minimal_size:,} bytes")
    print(f"   Complete: {complete_size:,} bytes")
    print(f"   Difference: +{complete_size - minimal_size:,} bytes")
    
    print(f"\n📋 What's included:")
    print(f"\n   MINIMAL (uses defaults):")
    print(f"     ✓ Water: Default (35% vol.)")
    print(f"     ✓ NO3: Default (10 kg/ha per layer)")
    print(f"     ✓ NH4: Default (0.5 kg/ha per layer)")
    print(f"     ✓ Residue: 500 kg/ha wheat")
    print(f"     ✗ Crop state: None")
    
    print(f"\n   COMPLETE (all specified):")
    print(f"     ✓ Water: Custom by layer")
    print(f"     ✓ NO3: Custom by layer (50 kg/ha total)")
    print(f"     ✓ NH4: Custom by layer (3 kg/ha total)")
    print(f"     ✓ Residue: 1000 kg/ha maize")
    print(f"     ✓ Crop state: LAI, biomass, roots initialized")
    print(f"     ✓ C# code: Actual initialization logic")


def show_generated_csharp_code():
    """
    Display the actual C# code that gets generated.
    """
    print("\n" + "="*70)
    print("GENERATED C# CODE EXAMPLE")
    print("="*70)
    
    print("""
When you provide crop_initial_state, the converter generates actual
working C# code that APSIM will execute at simulation start:

┌─────────────────────────────────────────────────────────────────┐
│ GENERATED C# CODE (excerpt)                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  using Models.PMF;                                              │
│  using Models.PMF.Organs;                                       │
│  using Models.Core;                                             │
│                                                                  │
│  [EventSubscribe("StartOfSimulation")]                          │
│  private void OnStartOfSimulation(object sender, EventArgs e)   │
│  {                                                               │
│      if (Crop != null)                                          │
│      {                                                           │
│          // Initialize Leaf Area Index                          │
│          var leaf = Crop.FindChild<Leaf>();                     │
│          if (leaf != null)                                      │
│          {                                                       │
│              leaf.LAI = 0.5;  ← ACTUAL VALUE SET!               │
│          }                                                       │
│                                                                  │
│          // Initialize biomass                                  │
│          var structure = Crop.FindChild<Structure>();           │
│          if (structure != null)                                 │
│          {                                                       │
│              double totalBiomass = 200.0;                       │
│              leaf.Live.StructuralWt = totalBiomass * 0.4;       │
│              stem.Live.StructuralWt = totalBiomass * 0.6;       │
│          }                                                       │
│                                                                  │
│          // Initialize root depth                               │
│          var root = Crop.FindChild<Root>();                     │
│          root.RootDepth = 150.0;  ← ACTUAL VALUE SET!           │
│      }                                                           │
│  }                                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

This is REAL C# code that APSIM executes - not just comments!
""")


def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("COMPLETE CROP INITIALIZATION - EXAMPLES")
    print("="*70)
    
    example_complete_initialization()
    example_minimal_vs_complete()
    show_generated_csharp_code()
    
    print("\n" + "="*70)
    print("✅ All examples completed!")
    print("="*70)
    print(f"\nFiles created in: {repo_root / 'data' / 'apsim' / 'example_output'}/")
    print("\nInspect the generated C# code:")
    print("  python -c \"import json; data=json.load(open('complete_crop_init.apsimx'));")
    print("             mgr=[c for c in data['Children'] if c['Name']=='CropInitialization'][0];")
    print("             print('\\\\n'.join(mgr['CodeArray']))\"")


if __name__ == "__main__":
    main()

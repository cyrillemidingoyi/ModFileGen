#!/usr/bin/env python3
"""
Test script for APSIM Converter

This script tests the complete APSIM conversion workflow.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Converter.ApsimConverter import export


def test_apsim_converter():
    """Test APSIM converter with real database."""
    
    print("=" * 70)
    print("TESTING APSIM CONVERTER - COMPLETE WORKFLOW")
    print("=" * 70)
    
    # Paths to test databases
    master_input = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/MasterInput_bon_test.db"
    model_dict = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/ModelDictionary.db"
    output_dir = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/src/modfilegen/Converter/ApsimConverter/test_output"
    
    # Note: APSIM executable path is optional
    # If not provided, only files will be generated (no execution)
    apsim_exe = None  # Set to actual path if you want to run APSIM
    # Example: apsim_exe = "/opt/APSIM/bin/Models"
    
    print(f"\nTest configuration:")
    print(f"  MasterInput: {master_input}")
    print(f"  ModelDictionary: {model_dict}")
    print(f"  Output directory: {output_dir}")
    print(f"  APSIM executable: {apsim_exe if apsim_exe else 'Not provided (file generation only)'}")
    
    # Check if databases exist
    if not os.path.exists(master_input):
        print(f"\n❌ ERROR: MasterInput database not found: {master_input}")
        return False
    
    if not os.path.exists(model_dict):
        print(f"\n❌ ERROR: ModelDictionary database not found: {model_dict}")
        return False
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        print("\n🚀 Starting APSIM conversion workflow...")
        
        # Run the converter
        results = export(
            MasterInput=master_input,
            ModelDictionary=model_dict,
            directoryPath=output_dir,
            apsim_path=apsim_exe,
            delete_temp=0  # Keep all files for inspection
        )
        
        # Check results
        if results is not None and not results.empty:
            print(f"\n✓ SUCCESS: Processed {len(results)} simulations")
            print(f"\nFirst few results:")
            print(results.head())
            return True
        else:
            # Even if no results (APSIM not run), check if files were created
            print(f"\n✓ File generation completed")
            print(f"   Check output directory: {output_dir}")
            
            # List generated files
            generated_files = []
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    if file.endswith(('.met', '.apsimx', '.json')):
                        generated_files.append(os.path.join(root, file))
            
            if generated_files:
                print(f"\n✓ Generated {len(generated_files)} APSIM files:")
                for f in generated_files[:10]:  # Show first 10
                    print(f"   - {f}")
                if len(generated_files) > 10:
                    print(f"   ... and {len(generated_files) - 10} more")
                return True
            else:
                print("\n⚠ WARNING: No files were generated")
                return False
                
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_apsim_converter()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ TEST PASSED")
    else:
        print("❌ TEST FAILED")
    print("=" * 70)
    
    sys.exit(0 if success else 1)

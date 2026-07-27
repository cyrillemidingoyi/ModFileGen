#!/usr/bin/env python3
"""
Test script for APSIM converter using main() function.
This follows the same pattern as SticsConverter and DssatConverter.
"""

import os
import sys
from pathlib import Path

# Add src directory to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))

from modfilegen import GlobalVariables
from modfilegen.Converter.ApsimConverter import main

def test_apsim_converter():
    """Test APSIM converter with GlobalVariables configuration."""
    
    # Configuration paths - use relative paths from repo root
    data_dir = repo_root / "tests" / "data"
    master_input = str(data_dir / "MasterInput_bon_test.db")
    model_dict = str(data_dir / "ModelsDictionaryArise.db")
    output_dir = str(repo_root / "data" / "apsim" / "apsim_test_output")
    
    print("=" * 70)
    print("APSIM CONVERTER TEST - Using GlobalVariables")
    print("=" * 70)
    print(f"MasterInput: {master_input}")
    print(f"ModelDict:   {model_dict}")
    print(f"Output:      {output_dir}")
    print("=" * 70)
    
    # Verify databases exist
    if not os.path.exists(master_input):
        print(f"❌ ERROR: MasterInput database not found: {master_input}")
        return False
    
    if not os.path.exists(model_dict):
        print(f"❌ ERROR: ModelDictionary database not found: {model_dict}")
        return False
    
    print("✓ Databases found\n")
    
    # Configure GlobalVariables (like in SticsConverter)
    GlobalVariables["dbMasterInput"] = master_input
    GlobalVariables["dbModelsDictionary"] = model_dict
    GlobalVariables["directorypath"] = output_dir
    GlobalVariables["nthreads"] = 4
    GlobalVariables["parts"] = 1
    GlobalVariables["dt"] = 0
    GlobalVariables["apsim_path"] = None  # Only generate files, don't execute
    
    try:
        # Run main conversion
        print("🚀 Starting APSIM conversion with main()...\n")
        main()
        
        print("\n" + "=" * 70)
        print("✓ Test completed successfully!")
        print("=" * 70)
        
        # Show generated files
        if os.path.exists(output_dir):
            import glob
            files = glob.glob(os.path.join(output_dir, "**", "*.*"), recursive=True)
            print(f"\n📊 Generated {len(files)} files")
            
            # Show first 20 files
            print("\nSample files:")
            for f in files[:20]:
                rel_path = os.path.relpath(f, output_dir)
                size = os.path.getsize(f)
                print(f"  - {rel_path} ({size:,} bytes)")
            
            if len(files) > 20:
                print(f"  ... and {len(files) - 20} more files")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n╔═══════════════════════════════════════════════════════════╗")
    print("║       APSIM CONVERTER - Main Function Test               ║")
    print("╚═══════════════════════════════════════════════════════════╝\n")
    
    success = test_apsim_converter()
    
    if success:
        print("\n╔═══════════════════════════════════════════════════════════╗")
        print("║  ✓ All tests passed successfully                         ║")
        print("╚═══════════════════════════════════════════════════════════╝")
        sys.exit(0)
    else:
        print("\n╔═══════════════════════════════════════════════════════════╗")
        print("║  ✗ Tests failed                                           ║")
        print("╚═══════════════════════════════════════════════════════════╝")
        sys.exit(1)

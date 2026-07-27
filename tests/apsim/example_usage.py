#!/usr/bin/env python3
"""
Quick example of APSIM Converter usage

This demonstrates how to use the ApsimConverter module to convert
ModFileGen databases to APSIM format and optionally run simulations.

NOTE: This example now uses the main() function with GlobalVariables,
following the same pattern as SticsConverter and DssatConverter.
"""

import sys
import os

# Setup path
sys.path.insert(0, '/mnt/d/Mes Donnees/TCMP/github/ModFileGen/src')

from modfilegen import GlobalVariables
from modfilegen.Converter.ApsimConverter import main

def example_main():
    """
    Example usage of APSIM Converter using main() function.
    
    This will:
    1. Read data from MasterInput and ModelDictionary databases
    2. Generate APSIM .met (weather), .apsimx (soil & management) files
    3. Create simulation .apsimx files
    4. Optionally run APSIM and process outputs
    5. Write results progressively to avoid memory issues
    """
    
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║       APSIM CONVERTER - Quick Example                     ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    
    # Configuration
    master_input_db = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/MasterInput_bon_test.db"
    model_dict_db = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/ModelsDictionaryArise.db"
    output_directory = "./apsim_output_example"
    
    # APSIM executable (optional - set to None to only generate files)
    apsim_executable = None  # e.g., "/opt/APSIM/bin/Models"
    
    print(f"\n📁 Configuration:")
    print(f"   MasterInput:    {master_input_db}")
    print(f"   ModelDict:      {model_dict_db}")
    print(f"   Output:         {output_directory}")
    print(f"   APSIM exe:      {apsim_executable or 'Not provided (file generation only)'}")
    
    # Check if databases exist
    if not os.path.exists(master_input_db):
        print(f"\n❌ ERROR: MasterInput database not found!")
        print(f"   Expected: {master_input_db}")
        return 1
    
    if not os.path.exists(model_dict_db):
        print(f"\n❌ ERROR: ModelDictionary database not found!")
        print(f"   Expected: {model_dict_db}")
        return 1
    
    print(f"\n✓ Databases found")
    
    # Configure GlobalVariables (like SticsConverter and DssatConverter)
    GlobalVariables["dbMasterInput"] = master_input_db
    GlobalVariables["dbModelsDictionary"] = model_dict_db
    GlobalVariables["directorypath"] = output_directory
    GlobalVariables["nthreads"] = 4  # Number of parallel threads
    GlobalVariables["parts"] = 1     # Parts per thread (for memory management)
    GlobalVariables["dt"] = 0        # 0=keep files, 1=delete temp files
    GlobalVariables["apsim_path"] = apsim_executable  # Optional: APSIM executable path
    
    # Run conversion
    print(f"\n🚀 Starting APSIM conversion...")
    print("=" * 60)
    
    try:
        # Call main() - it will use GlobalVariables configuration
        main()
        
        print("\n" + "=" * 60)
        print("✓ Conversion completed successfully!")
        
        # Check what was generated
        print(f"\n📊 Results:")
        print(f"   Files generated in: {output_directory}")
        
        # List generated files
        if os.path.exists(output_directory):
            file_count = sum(1 for _, _, files in os.walk(output_directory) for f in files)
            print(f"\n   Total files generated: {file_count}")
            
            # Show some example files
            print(f"\n   Example files:")
            shown = 0
            for root, dirs, files in os.walk(output_directory):
                for file in files:
                    if shown >= 20:  # Show first 20 files
                        break
                    rel_path = os.path.relpath(os.path.join(root, file), output_directory)
                    print(f"      - {rel_path}")
                    shown += 1
                if shown >= 20:
                    break
            
            if file_count > 20:
                print(f"      ... and {file_count - 20} more files")
        
        # Check for results CSV
        import glob
        csv_files = glob.glob(os.path.join(output_directory, "*_apsim.csv"))
        if csv_files:
            print(f"\n   Results CSV: {os.path.basename(csv_files[0])}")
        
        print(f"\n✓ Check the output directory for all generated files:")
        print(f"   {os.path.abspath(output_directory)}")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR during conversion:")
        print(f"   {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = example_main()
    
    print("\n╔═══════════════════════════════════════════════════════════╗")
    if exit_code == 0:
        print("║  ✓ Example completed successfully                         ║")
    else:
        print("║  ❌ Example failed - see errors above                      ║")
    print("╚═══════════════════════════════════════════════════════════╝\n")
    
    sys.exit(exit_code)

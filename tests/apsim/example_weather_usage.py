"""
Simple example demonstrating weather file creation methods.

This script shows how to use both export() and export_to_file() methods.
"""

import sqlite3
import os
from pathlib import Path
import sys

# Add src directory to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))

from modfilegen.Converter.ApsimConverter import ApsimWeatherConverter


def example_1_simple_file_creation():
    """
    Example 1: Create a single weather file (simplest approach)
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Simple Weather File Creation")
    print("="*70)
    
    # Paths to databases (adjust to your setup)
    test_data_dir = repo_root / "tests" / "data"
    db_master = str(test_data_dir / "MasterInput_bon_test.db")
    db_model = str(test_data_dir / "ModelsDictionaryArise.db")
    output_dir = str(repo_root / "data" / "apsim" / "example_output")
    
    # Connect to databases
    mi_conn = sqlite3.connect(db_master)
    md_conn = sqlite3.connect(db_model)
    
    # Create indexes for performance (one-time operation)
    print("📊 Setting up database indexes...")
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    mi_conn.commit()
    print("   ✓ Indexes ready\n")
    
    # Create converter
    converter = ApsimWeatherConverter()
    
    # Create weather file directly (one-step operation)
    weather_file = converter.export_to_file(
        directory_path="/dummy/-7.125_30.575/2012",  # Site and year info
        ModelDictionary_Connection=md_conn,
        master_input_connection=mi_conn,
        output_file=os.path.join(output_dir, "simple_weather.met")
    )
    
    if weather_file:
        print(f"✓ Success! Weather file created: {weather_file}")
        
        # Show first few lines
        with open(weather_file, 'r') as f:
            lines = f.readlines()[:15]
        print("\nFirst 15 lines of weather file:")
        print("".join(lines))
    else:
        print("❌ Failed to create weather file")
    
    mi_conn.close()
    md_conn.close()


def example_2_content_caching():
    """
    Example 2: Cache weather content for reuse (optimal for multiple simulations)
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: Content Caching for Multiple Simulations")
    print("="*70)
    
    # Paths to databases
    test_data_dir = repo_root / "tests" / "data"
    db_master = str(test_data_dir / "MasterInput_bon_test.db")
    db_model = str(test_data_dir / "ModelsDictionaryArise.db")
    output_dir = repo_root / "data" / "apsim" / "example_output"
    
    mi_conn = sqlite3.connect(db_master)
    md_conn = sqlite3.connect(db_model)
    
    # Create indexes for performance
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    mi_conn.commit()
    
    # Get some simulations that use the same site/year
    simulations = mi_conn.execute("""
        SELECT idsim, idPoint, StartYear 
        FROM SimUnitList 
        LIMIT 5
    """).fetchall()
    
    # Create converter and cache
    converter = ApsimWeatherConverter()
    weather_cache = {}
    
    print(f"\nProcessing {len(simulations)} simulations...")
    
    for idsim, idPoint, year in simulations:
        climid = f"{idPoint}.{year}"
        
        # Check cache first
        if climid not in weather_cache:
            print(f"  → Generating weather content for {climid}")
            content = converter.export(
                directory_path=f"/dummy/{idPoint}/{year}",
                ModelDictionary_Connection=md_conn,
                master_input_connection=mi_conn,
                usmdir=None
            )
            weather_cache[climid] = content
        else:
            print(f"  → Reusing cached weather for {climid} (simulation {idsim})")
            content = weather_cache[climid]
        
        # Write file for this simulation
        output_file = str(output_dir / idsim / "weather.met")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write(content)
        
        print(f"     ✓ Created: {output_file}")
    
    print(f"\n📊 Cache efficiency: {len(weather_cache)} unique weather files generated for {len(simulations)} simulations")
    print(f"   Savings: {len(simulations) - len(weather_cache)} regenerations avoided!")
    
    mi_conn.close()
    md_conn.close()


def example_3_multiple_sites():
    """
    Example 3: Create weather files for multiple sites/years
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Multiple Sites and Years")
    print("="*70)
    
    # Paths to databases
    test_data_dir = repo_root / "tests" / "data"
    db_master = str(test_data_dir / "MasterInput_bon_test.db")
    db_model = str(test_data_dir / "ModelsDictionaryArise.db")
    output_dir = repo_root / "data" / "apsim" / "example_output" / "weather_library"
    
    mi_conn = sqlite3.connect(db_master)
    md_conn = sqlite3.connect(db_model)
    
    # Create indexes for performance
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    mi_conn.commit()
    
    # Get unique site/year combinations
    sites_years = mi_conn.execute("""
        SELECT DISTINCT idPoint, StartYear 
        FROM SimUnitList 
        LIMIT 5
    """).fetchall()
    
    # Create converter
    converter = ApsimWeatherConverter()
    
    print(f"\nCreating weather files for {len(sites_years)} site-year combinations...\n")
    
    for site, year in sites_years:
        output_file = str(output_dir / f"{site}_{year}_weather.met")
        
        result = converter.export_to_file(
            directory_path=f"/dummy/{site}/{year}",
            ModelDictionary_Connection=md_conn,
            master_input_connection=mi_conn,
            output_file=output_file
        )
        
        if result:
            file_size = os.path.getsize(result)
            print(f"✓ {site} ({year}): {file_size:,} bytes → {os.path.basename(result)}")
    
    print(f"\n📁 All weather files saved in: {output_dir}")
    
    mi_conn.close()
    md_conn.close()


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("APSIM WEATHER FILE CREATION - EXAMPLES")
    print("="*80)
    
    # Create output directory
    output_dir = repo_root / "data" / "apsim" / "example_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Run examples
    example_1_simple_file_creation()
    example_2_content_caching()
    example_3_multiple_sites()
    
    print("\n" + "="*80)
    print("✓ All examples completed successfully!")
    print("="*80)
    print(f"\nGenerated files are in: {repo_root / 'data' / 'apsim' / 'example_output'}")
    print("\nKey Takeaways:")
    print("  1. Use export_to_file() for simple, one-off file creation")
    print("  2. Use export() with caching when processing multiple simulations")
    print("  3. Both methods produce identical APSIM-compatible .met files")
    print("="*80)


if __name__ == "__main__":
    main()

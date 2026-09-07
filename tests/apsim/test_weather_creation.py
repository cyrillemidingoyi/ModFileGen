"""
Test script for APSIM weather file creation.

This script demonstrates two approaches for creating weather files:
1. export() - Returns content as string (useful for caching)
2. export_to_file() - Creates file directly (convenient for single file generation)
"""

import sqlite3
import os
import sys
from pathlib import Path

# Add src directory to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / "src"))

from modfilegen.Converter.ApsimConverter.apsimweatherconverter import ApsimWeatherConverter


def test_export_content():
    """
    Test 1: Generate weather content without writing file
    Use case: Caching content for reuse in parallel processing
    """
    print("\n" + "="*70)
    print("TEST 1: export() - Generate weather CONTENT (no file)")
    print("="*70)
    
    # Setup database connections - use test data directory
    test_data_dir = repo_root / "tests" / "data"
    db_master = test_data_dir / "MasterInput_bon_test.db"
    db_model = test_data_dir / "ModelsDictionaryArise.db"
    
    if not db_master.exists() or not db_model.exists():
        print(f"❌ Test databases not found")
        print(f"   Expected: {db_master}")
        print(f"   Expected: {db_model}")
        return False
    
    mi_conn = sqlite3.connect(str(db_master))
    md_conn = sqlite3.connect(str(db_model))
    
    # Create indexes for performance (if they don't exist)
    print("📊 Creating database indexes for performance...")
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
    mi_conn.commit()
    print("   ✓ Indexes created")
    
    # Get first simulation from database
    df = mi_conn.execute("SELECT idPoint, StartYear FROM SimUnitList LIMIT 1").fetchone()
    if not df:
        print("❌ No simulations found in database")
        return False
    
    idPoint, year = df
    directory_path = f"/dummy/{idPoint}/{year}"
    
    print(f"📍 Site: {idPoint}, Year: {year}")
    print(f"📂 Directory path: {directory_path}")
    
    # Create converter and generate content
    converter = ApsimWeatherConverter()
    content = converter.export(
        directory_path=directory_path,
        ModelDictionary_Connection=md_conn,
        master_input_connection=mi_conn,
        usmdir=None
    )
    
    mi_conn.close()
    md_conn.close()
    
    # Verify content
    if not content:
        print("❌ No content generated")
        return False
    
    lines = content.split('\n')
    print(f"✓ Content generated: {len(lines)} lines")
    print(f"✓ First 10 lines:")
    for i, line in enumerate(lines[:10]):
        print(f"  {line}")
    
    # Check for required elements
    has_header = "[weather.met.weather]" in content
    has_columns = "year" in content and "day" in content
    has_data = len(lines) > 10
    
    if has_header and has_columns and has_data:
        print("✓ Content structure valid")
        return True
    else:
        print("❌ Content structure invalid")
        return False


def test_export_to_file():
    """
    Test 2: Generate weather file directly
    Use case: Simple one-step file creation
    """
    print("\n" + "="*70)
    print("TEST 2: export_to_file() - Create weather FILE directly")
    print("="*70)
    
    # Setup database connections - use test data directory
    test_data_dir = repo_root / "tests" / "data"
    db_master = test_data_dir / "MasterInput_bon_test.db"
    db_model = test_data_dir / "ModelsDictionaryArise.db"
    output_dir = str(repo_root / "data" / "apsim" / "weather_test_output")
    
    if not db_master.exists() or not db_model.exists():
        print(f"❌ Test databases not found")
        print(f"   Expected: {db_master}")
        return False
    
    mi_conn = sqlite3.connect(str(db_master))
    md_conn = sqlite3.connect(str(db_model))
    
    # Create indexes for performance
    print("📊 Creating database indexes...")
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    mi_conn.commit()
    print("   ✓ Indexes ready")
    
    # Get first 3 simulations
    df = mi_conn.execute("SELECT idPoint, StartYear FROM SimUnitList LIMIT 3").fetchall()
    if not df:
        print("❌ No simulations found in database")
        return False
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Create converter
    converter = ApsimWeatherConverter()
    created_files = []
    
    for idPoint, year in df:
        directory_path = f"/dummy/{idPoint}/{year}"
        output_file = os.path.join(output_dir, f"weather_{idPoint}_{year}.met")
        
        print(f"\n📍 Site: {idPoint}, Year: {year}")
        
        # Create file directly
        result = converter.export_to_file(
            directory_path=directory_path,
            ModelDictionary_Connection=md_conn,
            master_input_connection=mi_conn,
            output_file=output_file
        )
        
        if result:
            created_files.append(result)
            
            # Verify file exists and has content
            if os.path.exists(result):
                file_size = os.path.getsize(result)
                with open(result, 'r') as f:
                    lines = len(f.readlines())
                print(f"  ✓ File size: {file_size} bytes, {lines} lines")
            else:
                print(f"  ❌ File not found: {result}")
        else:
            print(f"  ❌ Failed to create file")
    
    mi_conn.close()
    md_conn.close()
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"  ✓ Created {len(created_files)} weather files")
    print(f"  📂 Output directory: {output_dir}")
    
    for f in created_files:
        print(f"     - {os.path.basename(f)}")
    
    return len(created_files) == 3


def test_comparison():
    """
    Test 3: Compare both approaches (content vs file)
    Verify they produce identical results
    """
    print("\n" + "="*70)
    print("TEST 3: Comparison - Verify both methods produce identical results")
    print("="*70)
    
    # Setup - use test data directory
    test_data_dir = repo_root / "tests" / "data"
    db_master = test_data_dir / "MasterInput_bon_test.db"
    db_model = test_data_dir / "ModelsDictionaryArise.db"
    output_dir = str(repo_root / "data" / "apsim" / "weather_test_output")
    
    if not db_master.exists() or not db_model.exists():
        print(f"❌ Test databases not found")
        return False
    
    mi_conn = sqlite3.connect(str(db_master))
    md_conn = sqlite3.connect(str(db_model))
    
    # Create indexes
    cursor = mi_conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
    mi_conn.commit()
    
    # Get one simulation
    df = mi_conn.execute("SELECT idPoint, StartYear FROM SimUnitList LIMIT 1").fetchone()
    idPoint, year = df
    directory_path = f"/dummy/{idPoint}/{year}"
    
    # Method 1: Generate content
    converter = ApsimWeatherConverter()
    content1 = converter.export(
        directory_path=directory_path,
        ModelDictionary_Connection=md_conn,
        master_input_connection=mi_conn,
        usmdir=None
    )
    
    # Method 2: Create file
    output_file = os.path.join(output_dir, "weather_comparison.met")
    result = converter.export_to_file(
        directory_path=directory_path,
        ModelDictionary_Connection=md_conn,
        master_input_connection=mi_conn,
        output_file=output_file
    )
    
    mi_conn.close()
    md_conn.close()
    
    if not result:
        print("❌ Failed to create file")
        return False
    
    # Read file content
    with open(output_file, 'r') as f:
        content2 = f.read()
    
    # Compare
    if content1 == content2:
        print("✓ Both methods produce IDENTICAL content")
        print(f"  Content length: {len(content1)} bytes")
        return True
    else:
        print("❌ Contents DIFFER")
        print(f"  Method 1 (export): {len(content1)} bytes")
        print(f"  Method 2 (export_to_file): {len(content2)} bytes")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("APSIM WEATHER FILE CREATION - TEST SUITE")
    print("="*70)
    
    results = []
    
    # Test 1: Content generation
    results.append(("Generate content (export)", test_export_content()))
    
    # Test 2: Direct file creation
    results.append(("Create file directly (export_to_file)", test_export_to_file()))
    
    # Test 3: Comparison
    results.append(("Verify methods are identical", test_comparison()))
    
    # Summary
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70)
    
    for test_name, passed in results:
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "="*70)
    if all_passed:
        print("✓ All tests passed successfully!")
    else:
        print("❌ Some tests failed")
    print("="*70)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

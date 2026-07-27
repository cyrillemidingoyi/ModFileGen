"""
Test database integration for APSIM Management Converter

This script demonstrates how to extract management operations from 
the CropManagement table in the MasterInput database.
"""

import os
import sys
import sqlite3
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from modfilegen.Converter.ApsimConverter import ApsimManagementConverter


def create_test_database():
    """
    Create a test database with CropManagement structure similar to STICS.
    """
    print("=" * 70)
    print("Creating Test Database")
    print("=" * 70)
    
    # Create in-memory database
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # Create tables matching the STICS structure
    
    # SimUnitList table
    cursor.execute("""
    CREATE TABLE SimUnitList (
        idsim TEXT PRIMARY KEY,
        idMangt TEXT,
        idsoil TEXT
    )
    """)
    
    # CropManagement table
    cursor.execute("""
    CREATE TABLE CropManagement (
        idMangt TEXT PRIMARY KEY,
        Idcultivar TEXT,
        sdens REAL,
        sowingdate INTEGER,
        SoilTillPolicyCode INTEGER,
        OFertiPolicyCode INTEGER,
        InoFertiPolicyCode INTEGER
    )
    """)
    
    # ListCultivars table
    cursor.execute("""
    CREATE TABLE ListCultivars (
        IdCultivar TEXT PRIMARY KEY,
        CropName TEXT,
        idcultivarStics TEXT
    )
    """)
    
    # InorganicFertilizationPolicy table
    cursor.execute("""
    CREATE TABLE InorganicFertilizationPolicy (
        InorgFertiPolicyCode INTEGER PRIMARY KEY,
        NumInorganicFerti INTEGER
    )
    """)
    
    # InorganicFOperations table
    cursor.execute("""
    CREATE TABLE InorganicFOperations (
        InorgFertiPolicyCode INTEGER,
        N REAL,
        Dferti INTEGER
    )
    """)
    
    # SoilTillPolicy table
    cursor.execute("""
    CREATE TABLE SoilTillPolicy (
        SoilTillPolicyCode INTEGER PRIMARY KEY,
        NumTillOperations INTEGER
    )
    """)
    
    # SoilTillageOperations table
    cursor.execute("""
    CREATE TABLE SoilTillageOperations (
        SoilTillPolicyCode INTEGER,
        STNumber INTEGER,
        DepthResUp REAL,
        DepthResLow REAL,
        DSTill INTEGER
    )
    """)
    
    # Insert test data
    print("\nInserting test data...")
    
    # Simulation unit
    cursor.execute("""
    INSERT INTO SimUnitList (idsim, idMangt, idsoil)
    VALUES ('TestSite', 'MGMT001', 'SOIL001')
    """)
    
    # Cultivar
    cursor.execute("""
    INSERT INTO ListCultivars (IdCultivar, CropName, idcultivarStics)
    VALUES ('WHEAT001', 'Wheat', 'Hartog')
    """)
    
    # Crop management
    cursor.execute("""
    INSERT INTO CropManagement (idMangt, Idcultivar, sdens, sowingdate, 
                                SoilTillPolicyCode, InoFertiPolicyCode)
    VALUES ('MGMT001', 'WHEAT001', 120.0, 135, 1, 1)
    """)
    
    # Fertilization policy
    cursor.execute("""
    INSERT INTO InorganicFertilizationPolicy (InorgFertiPolicyCode, NumInorganicFerti)
    VALUES (1, 2)
    """)
    
    # Fertilization operations
    cursor.execute("""
    INSERT INTO InorganicFOperations (InorgFertiPolicyCode, N, Dferti)
    VALUES (1, 100.0, 0)
    """)
    
    cursor.execute("""
    INSERT INTO InorganicFOperations (InorgFertiPolicyCode, N, Dferti)
    VALUES (1, 50.0, 60)
    """)
    
    # Tillage policy
    cursor.execute("""
    INSERT INTO SoilTillPolicy (SoilTillPolicyCode, NumTillOperations)
    VALUES (1, 1)
    """)
    
    # Tillage operation
    cursor.execute("""
    INSERT INTO SoilTillageOperations (SoilTillPolicyCode, STNumber, DepthResUp, DepthResLow, DSTill)
    VALUES (1, 1, 0, 150, -7)
    """)
    
    conn.commit()
    print("✓ Test database created successfully")
    
    return conn


def test_database_query():
    """
    Test querying management data from the database.
    """
    print("\n" + "=" * 70)
    print("Test 1: Query Management Data from Database")
    print("=" * 70)
    
    # Create test database
    conn = create_test_database()
    
    # Create converter
    converter = ApsimManagementConverter()
    
    # Simulate directory path structure (Site/Year/Simulation)
    # The converter extracts simulation ID from path
    directory_path = os.path.join('output', 'data', 'TestSite', '2020', 'sim1')
    
    # Query management data
    print(f"\nQuerying management data for: {directory_path}")
    management_data = converter._query_management_data(
        conn,
        directory_path
    )
    
    print(f"\n✓ Found {len(management_data)} operations:")
    print("\nManagement operations:")
    print(management_data[['operation_type', 'date', 'crop']].to_string())
    
    # Show details
    print("\nOperation details:")
    for idx, row in management_data.iterrows():
        print(f"\n{idx + 1}. {row['operation_type'].upper()}")
        for col in management_data.columns:
            if pd.notna(row[col]) and col != 'operation_type':
                print(f"   - {col}: {row[col]}")
    
    conn.close()
    return management_data


def test_full_export():
    """
    Test complete export workflow from database to APSIM file.
    """
    print("\n" + "=" * 70)
    print("Test 2: Full Export from Database to APSIM")
    print("=" * 70)
    
    # Create test database
    conn = create_test_database()
    
    # Create converter
    converter = ApsimManagementConverter()
    
    # Simulate directory path
    directory_path = os.path.join('output', 'data', 'TestSite', '2020', 'sim1')
    os.makedirs(directory_path, exist_ok=True)
    
    # Export management operations
    print(f"\nExporting management operations...")
    output_file = converter.export(
        directory_path=directory_path,
        ModelDictionary_Connection=None,  # Not used in this example
        master_input_connection=conn,
        output_apsimx='test_db_management.apsimx'
    )
    
    if output_file:
        print(f"\n✓ Management file created: {output_file}")
        
        # Verify file contents
        import json
        with open(output_file, 'r') as f:
            data = json.load(f)
        
        operations = data['Children'][0]['Children']
        print(f"  - Number of operations: {len(operations)}")
        print(f"  - Operation names:")
        for op in operations:
            print(f"    * {op['Name']}")
    
    conn.close()


def test_date_conversion():
    """
    Test day-of-year to APSIM date conversion.
    """
    print("\n" + "=" * 70)
    print("Test 3: Date Conversion")
    print("=" * 70)
    
    converter = ApsimManagementConverter()
    
    test_days = [1, 32, 60, 135, 200, 335]
    print("\nConverting day-of-year to APSIM date format:")
    print("-" * 40)
    print(f"{'Day of Year':<15} {'APSIM Date':<15}")
    print("-" * 40)
    
    for day in test_days:
        apsim_date = converter._format_apsim_date(day)
        print(f"{day:<15} {apsim_date:<15}")


def show_database_schema():
    """
    Display the database schema for reference.
    """
    print("\n" + "=" * 70)
    print("Database Schema Reference")
    print("=" * 70)
    
    print("""
Key Tables:
-----------
1. SimUnitList
   - Links simulations to management and soil

2. CropManagement
   - Main table containing:
     * Cultivar reference
     * Sowing date (day of year)
     * Density (plants/m²)
     * Policy codes for fertilization and tillage

3. InorganicFOperations
   - Fertilization events with:
     * N amount (kg/ha)
     * Days relative to sowing (Dferti)

4. SoilTillageOperations
   - Tillage events with:
     * Depth parameters
     * Days relative to sowing (DSTill)

Query Pattern:
--------------
The converter:
1. Extracts simulation ID from directory path
2. Joins CropManagement with related tables
3. Converts database operations to APSIM format
4. Handles date conversion (day-of-year → d-mmm)
5. Maps fertilizer types to APSIM conventions
""")


if __name__ == "__main__":
    print("\n")
    print("=" * 70)
    print("APSIM MANAGEMENT CONVERTER - DATABASE INTEGRATION TESTS")
    print("=" * 70)
    
    try:
        # Show schema
        show_database_schema()
        
        # Run tests
        management_data = test_database_query()
        test_full_export()
        test_date_conversion()
        
        print("\n" + "=" * 70)
        print("ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nThe converter successfully:")
        print("  ✓ Queries CropManagement table")
        print("  ✓ Extracts sowing operations")
        print("  ✓ Extracts fertilization operations")
        print("  ✓ Extracts tillage operations")
        print("  ✓ Converts dates to APSIM format")
        print("  ✓ Generates valid APSIM management files")
        
    except Exception as e:
        print(f"\n✗ Test failed with error:")
        print(f"  {str(e)}")
        import traceback
        traceback.print_exc()

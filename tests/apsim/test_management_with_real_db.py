"""
Test ApsimManagementConverter with real MasterInput database
"""

import sqlite3
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from modfilegen.Converter.ApsimConverter.apsimmanagementconverter import ApsimManagementConverter


def test_with_real_database():
    """Test management converter with real MasterInput_bon_test.db"""
    
    print("=" * 70)
    print("TESTING APSIM MANAGEMENT CONVERTER WITH REAL DATABASE")
    print("=" * 70)
    
    # Path to real database
    db_path = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/MasterInput_bon_test.db"
    
    print(f"\n1. Connecting to database: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}")
        return False
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    # First, let's see what simulations are available
    print("\n2. Querying available simulations...")
    cursor = conn.cursor()
    cursor.execute("SELECT idsim FROM SimUnitList LIMIT 10")
    sims = cursor.fetchall()
    
    print(f"   Found {len(sims)} simulations:")
    for sim_id in sims[:5]:
        print(f"   - idsim={sim_id[0]}")
    
    if len(sims) > 5:
        print(f"   ... and {len(sims) - 5} more")
    
    # Pick first simulation for testing
    test_sim_id = sims[0][0]
    
    print(f"\n3. Testing with simulation: idsim={test_sim_id}")
    
    # Create directory path with simulation ID in correct position
    # The converter expects: /path/{sim_id}/work/output  (sim_id at position -3)
    # It extracts ST[-3] from the split path (which is the sim_id)
    test_dir = f"/test/{test_sim_id}/work/output"
    
    print(f"   Directory path: {test_dir}")
    print(f"   Extracted sim_id (ST[-3]): {test_dir.split(os.sep)[-3]}")
    
    # Create converter
    converter = ApsimManagementConverter()
    
    # Export management (no operation_types parameter needed!)
    print("\n4. Exporting management operations...")
    output_file = f"test_real_db_sim_{test_sim_id}.apsimx"
    
    try:
        result = converter.export(
            directory_path=test_dir,
            ModelDictionary_Connection=conn,  # Using same connection for demo
            master_input_connection=conn,
            output_apsimx=output_file
        )
        
        if result:
            print(f"   ✓ Successfully created: {result}")
            
            # Check what was generated
            print("\n5. Checking generated operations...")
            import json
            with open(result, 'r') as f:
                data = json.load(f)
            
            # Count operations
            mgmt_folder = None
            for child in data.get('Children', []):
                if child.get('$type') == 'Models.Core.Folder, Models':
                    mgmt_folder = child
                    break
            
            if mgmt_folder:
                operations = mgmt_folder.get('Children', [])
                print(f"   Total operations: {len(operations)}")
                
                for op in operations:
                    op_name = op.get('Name', 'Unknown')
                    print(f"   - {op_name}")
                
                return True
            else:
                print("   Warning: No management folder found")
                return False
        else:
            print("   ⚠ No operations extracted")
            return False
            
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


def test_query_operations_directly():
    """Test the _query_management_data method directly"""
    
    print("\n" + "=" * 70)
    print("TESTING DIRECT QUERY OF MANAGEMENT DATA")
    print("=" * 70)
    
    db_path = "/mnt/d/Mes Donnees/TCMP/github/ModFileGen/tests/data/MasterInput_bon_test.db"
    conn = sqlite3.connect(db_path)
    
    # Get first simulation
    cursor = conn.cursor()
    cursor.execute("SELECT idsim FROM SimUnitList LIMIT 1")
    test_sim_id = cursor.fetchone()[0]
    
    print(f"\n1. Testing query for simulation ID: {test_sim_id}")
    
    # Create directory path (sim_id must be at position -3 when split)
    # Format: /parent/{sim_id}/work/output
    test_dir = f"/test/{test_sim_id}/work/output"
    
    # Create converter and call query method
    converter = ApsimManagementConverter()
    
    print("\n2. Querying management operations...")
    try:
        df = converter._query_management_data(
            connection=conn,
            directory_path=test_dir
        )
        
        print(f"   ✓ Query successful")
        print(f"   Operations found: {len(df)}")
        print(df.head())
        
        if not df.empty:
            print(f"\n3. Operations breakdown:")
            for op_type in df['operation_type'].unique():
                count = len(df[df['operation_type'] == op_type])
                print(f"   - {op_type}: {count}")
            
            print(f"\n4. Sample operations:")
            print(df[['operation_type', 'date', 'crop']].head(10).to_string())
            return True
        else:
            print("   ⚠ No operations found")
            return False
            
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


if __name__ == '__main__':
    print("\n")
    
    # Test 1: Direct query
    success1 = test_query_operations_directly()
    
    print("\n")
    
    # Test 2: Full export
    success2 = test_with_real_database()
    
    print("\n" + "=" * 70)
    if success1 and success2:
        print("✓ ALL TESTS PASSED")
    else:
        print("⚠ SOME TESTS FAILED")
    print("=" * 70)

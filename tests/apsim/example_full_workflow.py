"""
Example: Complete APSIM conversion workflow using database integration

This example demonstrates how to:
1. Query CropManagement table from MasterInput database
2. Convert management operations to APSIM format
3. Combine with weather and soil data
4. Generate complete .apsimx simulation file

Author: ModFileGen Team
Date: 2024
"""

import sqlite3
import pandas as pd
from pathlib import Path
from apsimmanagementconverter import ApsimManagementConverter
from apsimweatherconverter import ApsimWeatherConverter
from apsimsoilconverter import ApsimSoilConverter


def create_example_database():
    """Create example database matching MasterInput/ModelDictionary structure"""
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # SimUnitList table - simulation metadata
    cursor.execute('''
        CREATE TABLE SimUnitList (
            idsim INTEGER PRIMARY KEY,
            idMangt INTEGER,
            SName TEXT,
            Site TEXT,
            Year INTEGER
        )
    ''')
    
    # ListCultivars table - crop variety information  
    cursor.execute('''
        CREATE TABLE ListCultivars (
            IdCultivar INTEGER PRIMARY KEY,
            CropName TEXT,
            idcultivarStics TEXT,
            Photoperiod REAL,
            Vernalisation REAL
        )
    ''')
    
    # CropManagement table - links management to simulations
    cursor.execute('''
        CREATE TABLE CropManagement (
            idMangt INTEGER PRIMARY KEY,
            Idcultivar INTEGER,
            sdens REAL,
            sowingdate INTEGER,
            SoilTillPolicyCode INTEGER,
            OFertiPolicyCode INTEGER,
            InoFertiPolicyCode INTEGER,
            FOREIGN KEY(Idcultivar) REFERENCES ListCultivars(IdCultivar)
        )
    ''')
    
    # InorganicFertilizationPolicy table
    cursor.execute('''
        CREATE TABLE InorganicFertilizationPolicy (
            InoFertiPolicyCode INTEGER PRIMARY KEY,
            PolicyName TEXT
        )
    ''')
    
    # InorganicFOperations table - fertilization details
    cursor.execute('''
        CREATE TABLE InorganicFOperations (
            InoFertiPolicyCode INTEGER,
            Iferti INTEGER,
            Dferti INTEGER,
            Qapplied REAL,
            NTypeFerti TEXT,
            FOREIGN KEY(InoFertiPolicyCode) REFERENCES InorganicFertilizationPolicy(InoFertiPolicyCode)
        )
    ''')
    
    # SoilTillagePolicy table
    cursor.execute('''
        CREATE TABLE SoilTillagePolicy (
            SoilTillPolicyCode INTEGER PRIMARY KEY,
            PolicyName TEXT
        )
    ''')
    
    # SoilTillageOperations table - tillage details
    cursor.execute('''
        CREATE TABLE SoilTillageOperations (
            SoilTillPolicyCode INTEGER,
            Itill INTEGER,
            DSTill INTEGER,
            Profres REAL,
            FOREIGN KEY(SoilTillPolicyCode) REFERENCES SoilTillagePolicy(SoilTillPolicyCode)
        )
    ''')
    
    # Insert example data - Wheat simulation
    cursor.execute('INSERT INTO SimUnitList VALUES (1, 101, "Wheat_Site1_2020", "Site1", 2020)')
    
    cursor.execute('INSERT INTO ListCultivars VALUES (1, "wheat", "Hartog", 3.0, 50.0)')
    
    cursor.execute('''
        INSERT INTO CropManagement VALUES (101, 1, 150.0, 135, 1001, NULL, 2001)
    ''')
    
    cursor.execute('INSERT INTO InorganicFertilizationPolicy VALUES (2001, "Standard wheat N")')
    
    cursor.executemany('''
        INSERT INTO InorganicFOperations VALUES (?, ?, ?, ?, ?)
    ''', [
        (2001, 1, 0, 100.0, 'NO3N'),    # At sowing
        (2001, 2, 60, 50.0, 'UreaN'),   # 60 days after sowing
    ])
    
    cursor.execute('INSERT INTO SoilTillagePolicy VALUES (1001, "Standard tillage")')
    
    cursor.execute('INSERT INTO SoilTillageOperations VALUES (1001, 1, -15, 150.0)')
    
    conn.commit()
    return conn


def create_example_weather_data():
    """Create example weather data"""
    dates = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    
    weather_data = pd.DataFrame({
        'year': dates.year,
        'day': dates.dayofyear,
        'radn': 20.0,  # MJ/m2/day
        'maxt': 25.0,  # °C
        'mint': 15.0,  # °C
        'rain': 2.0    # mm
    })
    
    return weather_data


def create_example_soil_data():
    """Create example soil profile data"""
    soil_data = pd.DataFrame({
        'Depth': [150, 300, 600, 900, 1200, 1500, 1800],
        'BD': [1.02, 1.03, 1.02, 1.02, 1.06, 1.11, 1.17],
        'AirDry': [0.050, 0.190, 0.190, 0.190, 0.180, 0.170, 0.170],
        'LL15': [0.200, 0.200, 0.200, 0.200, 0.190, 0.170, 0.170],
        'DUL': [0.410, 0.410, 0.410, 0.410, 0.400, 0.380, 0.380],
        'SAT': [0.430, 0.430, 0.430, 0.430, 0.420, 0.400, 0.400],
        'KS': [20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0],
        'OC': [1.24, 0.89, 0.89, 0.89, 0.47, 0.47, 0.47],
        'PH': [8.4, 8.8, 9.0, 9.2, 9.2, 9.1, 9.0]
    })
    
    return soil_data


def main():
    """
    Full workflow example: database → APSIM files
    
    This demonstrates how operations are extracted from CropManagement table
    and converted to APSIM Manager scripts, matching the pattern used in
    sticsfictec1converter.py
    """
    print("=" * 70)
    print("APSIM CONVERSION WORKFLOW - DATABASE INTEGRATION")
    print("=" * 70)
    
    # Step 1: Create example database
    print("\n1. Setting up example database (MasterInput/ModelDictionary structure)...")
    conn = create_example_database()
    print("   ✓ Database created matching ModFileGen schema")
    print("   ✓ 1 simulation: Wheat (idsim=1)")
    
    # Step 2: Query management data
    print("\n2. Querying CropManagement operations...")
    
    # Example directory path structure: /path/to/data/001/...
    # The converter extracts simulation ID from directory_path.split(os.sep)[-3]
    # For idsim=1, use "001"
    wheat_path = "/example/data/001/wheat_simulation"
    
    print(f"   ✓ Directory path: {wheat_path}")
    print(f"   ✓ Simulation ID will be extracted as: 001 → 1")
    
    # Step 3: Convert to APSIM format
    print("\n3. Converting to APSIM Manager scripts...")
    output_file = "example_complete_simulation.apsimx"
    
    # Initialize converter
    converter = ApsimManagementConverter()
    
    # Export management operations from database
    # Note: Using the same connection for both parameters as this is a demo
    result_file = converter.export(
        directory_path=wheat_path,
        ModelDictionary_Connection=conn,
        master_input_connection=conn,
        output_apsimx=output_file,
        operation_types=['sowing', 'fertilization', 'tillage', 'harvest']
    )
    
    if result_file:
        print(f"   ✓ Generated: {result_file}")
    else:
        print("   ⚠ No operations found, check simulation ID extraction")
    
    # Step 4: Show what was generated
    print("\n4. Generated APSIM operations:")
    print("   " + "-" * 60)
    
    # Query to show what was extracted
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 
            sl.idsim,
            sl.SName,
            cm.sowingdate,
            lc.idcultivarStics,
            cm.sdens,
            cm.InoFertiPolicyCode
        FROM CropManagement cm
        JOIN SimUnitList sl ON cm.idMangt = sl.idMangt
        JOIN ListCultivars lc ON cm.Idcultivar = lc.IdCultivar
        WHERE sl.idsim = 1
    ''')
    
    result = cursor.fetchone()
    sow_day = result[2]
    sow_date = converter._format_apsim_date(sow_day, 2020)
    print(f"   Simulation: {result[1]}")
    print(f"   Sowing: Day {sow_day} → {sow_date}")
    print(f"   Variety: {result[3]}")
    print(f"   Population: {result[4]} plants/m²")
    
    # Show fertilizations
    cursor.execute('''
        SELECT Dferti, Qapplied, NTypeFerti
        FROM InorganicFOperations
        WHERE InoFertiPolicyCode = ?
        ORDER BY Iferti
    ''', (result[5],))
    
    fert_ops = cursor.fetchall()
    print(f"\n   Fertilizations ({len(fert_ops)}):")
    for dferti, amount, fert_type in fert_ops:
        if dferti == 0:
            print(f"   - At sowing ({sow_date}): {amount} kg/ha {fert_type}")
        else:
            fert_day = sow_day + dferti
            fert_date = converter._format_apsim_date(fert_day, 2020)
            print(f"   - {dferti} days after sowing ({fert_date}): {amount} kg/ha {fert_type}")
    
    print("\n   " + "-" * 60)
    
    # Step 5: Integration notes
    print("\n5. Integration with weather and soil data:")
    print("   - Weather data: Use ApsimWeatherConverter for .met files")
    print("   - Soil data: Use ApsimSoilConverter for soil profiles")
    print("   - Management: Database-driven operations (as shown above)")
    print("\n   Complete workflow:")
    print("   weather.met + soil.json + management.apsimx → Full simulation")
    
    print("\n" + "=" * 70)
    print("WORKFLOW COMPLETE - Check example_complete_simulation.apsimx")
    print("=" * 70)
    
    conn.close()


if __name__ == '__main__':
    main()

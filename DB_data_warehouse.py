import sqlite3
import csv
import os
from datetime import datetime

def create_star_schema_database():
    """Create SQLite database with star schema structure"""
    
    # Connect to SQLite database (creates it if it doesn't exist)
    conn = sqlite3.connect('star_schema.db')
    cursor = conn.cursor()
    
    # Drop existing tables if they exist (for clean recreation)
    tables_to_drop = ['Fact_Earthquake', 'Dim_Time', 'Dim_Location', 'Dim_Fault', 'Dim_Type']
    for table in tables_to_drop:
        cursor.execute(f'DROP TABLE IF EXISTS {table}')
    
    # Create Dimension Tables with Surrogate Keys
    
    # Dim_Fault table
    cursor.execute('''
        CREATE TABLE Dim_Fault (
            Fault_SK INTEGER PRIMARY KEY AUTOINCREMENT,
            Fault_ID TEXT,
            fault TEXT,
            macro_fault TEXT
        )
    ''')
    
    # Dim_Type table
    cursor.execute('''
        CREATE TABLE Dim_Type (
            Type_SK INTEGER PRIMARY KEY AUTOINCREMENT,
            Type_ID TEXT,
            magType TEXT,
            type TEXT
        )
    ''')
    
    # Dim_Location table
    cursor.execute('''
        CREATE TABLE Dim_Location (
            Location_SK INTEGER PRIMARY KEY AUTOINCREMENT,
            Location_ID TEXT,
            latitude REAL,
            longitude REAL,
            location TEXT,
            country TEXT
        )
    ''')
    
    # Dim_Time table
    cursor.execute('''
        CREATE TABLE Dim_Time (
            Time_SK INTEGER PRIMARY KEY AUTOINCREMENT,
            Time_ID TEXT,
            time TEXT,
            hour INTEGER,
            day INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            day_of_week TEXT,
            month_name TEXT
        )
    ''')
    
    # Fact_Earthquake table (central fact table)
    cursor.execute('''
        CREATE TABLE Fact_Earthquake (
            Earthquake_SK INTEGER PRIMARY KEY AUTOINCREMENT,
            Time_SK INTEGER,
            Fault_SK INTEGER,
            Location_SK INTEGER,
            Type_SK INTEGER,
            count_events INTEGER DEFAULT 1,
            depth REAL,
            mag REAL,
            gap REAL,
            dmin REAL,
            rms REAL,
            original_earthquake_id TEXT,
            FOREIGN KEY (Time_SK) REFERENCES Dim_Time(Time_SK),
            FOREIGN KEY (Fault_SK) REFERENCES Dim_Fault(Fault_SK),
            FOREIGN KEY (Location_SK) REFERENCES Dim_Location(Location_SK),
            FOREIGN KEY (Type_SK) REFERENCES Dim_Type(Type_SK)
        )
    ''')
    
    conn.commit()
    return conn, cursor

def parse_datetime(time_str):
    """Parse datetime string and extract time components"""
    try:
        dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return {
            'hour': dt.hour,
            'day': dt.day,
            'month': dt.month,
            'quarter': (dt.month - 1) // 3 + 1,
            'year': dt.year,
            'day_of_week': dt.strftime('%A'),
            'month_name': dt.strftime('%B')
        }
    except:
        # Return default values if parsing fails
        return {
            'hour': 0,
            'day': 1,
            'month': 1,
            'quarter': 1,
            'year': 1970,
            'day_of_week': 'Unknown',
            'month_name': 'Unknown'
        }

def populate_dimension_tables(conn, cursor, csv_file_path):
    """Populate dimension tables first to avoid integrity constraint violations"""
    
    # Dictionaries to store mappings for surrogate keys
    fault_mapping = {}
    type_mapping = {}
    location_mapping = {}
    time_mapping = {}
    
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        # First pass: collect unique dimension values
        faults = set()
        types = set()
        locations = set()
        times = set()
        
        for row in csv_reader:
            faults.add((row['fault'], 'Unknown'))  # Using 'Unknown' as macro_fault placeholder
            types.add((row['magType'], row['type']))
            locations.add((row['latitude'], row['longitude'], row['location'], 'Italy'))
            times.add(row['time'])
    
    # Populate Dim_Fault
    fault_counter = 1
    for fault, macro_fault in faults:
        cursor.execute('''
            INSERT INTO Dim_Fault (Fault_ID, fault, macro_fault) 
            VALUES (?, ?, ?)
        ''', (f'F{fault_counter:04d}', fault, macro_fault))
        fault_sk = cursor.lastrowid
        fault_mapping[fault] = fault_sk
        fault_counter += 1
    
    # Populate Dim_Type
    type_counter = 1
    for mag_type, event_type in types:
        cursor.execute('''
            INSERT INTO Dim_Type (Type_ID, magType, type) 
            VALUES (?, ?, ?)
        ''', (f'T{type_counter:04d}', mag_type, event_type))
        type_sk = cursor.lastrowid
        type_mapping[(mag_type, event_type)] = type_sk
        type_counter += 1
    
    # Populate Dim_Location
    location_counter = 1
    for lat, lon, location, country in locations:
        cursor.execute('''
            INSERT INTO Dim_Location (Location_ID, latitude, longitude, location, country) 
            VALUES (?, ?, ?, ?, ?)
        ''', (f'L{location_counter:04d}', float(lat), float(lon), location, country))
        location_sk = cursor.lastrowid
        location_mapping[(lat, lon, location)] = location_sk
        location_counter += 1
    
    # Populate Dim_Time
    time_counter = 1
    for time_str in times:
        time_components = parse_datetime(time_str)
        cursor.execute('''
            INSERT INTO Dim_Time (Time_ID, time, hour, day, month, quarter, year, day_of_week, month_name) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f'TM{time_counter:04d}',
            time_str,
            time_components['hour'],
            time_components['day'],
            time_components['month'],
            time_components['quarter'],
            time_components['year'],
            time_components['day_of_week'],
            time_components['month_name']
        ))
        time_sk = cursor.lastrowid
        time_mapping[time_str] = time_sk
        time_counter += 1
    
    conn.commit()
    return fault_mapping, type_mapping, location_mapping, time_mapping

def populate_fact_table(conn, cursor, csv_file_path, fault_mapping, type_mapping, location_mapping, time_mapping):
    """Populate the fact table using surrogate keys from dimension tables"""
    
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Get surrogate keys from mappings
            fault_sk = fault_mapping[row['fault']]
            type_sk = type_mapping[(row['magType'], row['type'])]
            location_sk = location_mapping[(row['latitude'], row['longitude'], row['location'])]
            time_sk = time_mapping[row['time']]
            
            # Insert into fact table
            cursor.execute('''
                INSERT INTO Fact_Earthquake (
                    Time_SK, Fault_SK, Location_SK, Type_SK, count_events,
                    depth, mag, gap, dmin, rms, original_earthquake_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                time_sk,
                fault_sk,
                location_sk,
                type_sk,
                1,  # count_events
                float(row['depth']),
                float(row['mag']),
                None,  # gap not in CSV
                float(row['dist']) if row['dist'] else None,  # using dist as dmin
                None,  # rms not in CSV
                row['ID']  # original earthquake ID
            ))
    
    conn.commit()

def verify_star_schema(cursor):
    """Verify the star schema was created and populated correctly"""
    print("Star Schema Database Verification:")
    print("=" * 60)
    
    # Check dimension table counts
    dim_tables = ['Dim_Location', 'Dim_Fault', 'Dim_Type', 'Dim_Time']
    for table in dim_tables:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f"{table}: {count} records")
    
    # Check fact table count
    cursor.execute('SELECT COUNT(*) FROM Fact_Earthquake')
    count = cursor.fetchone()[0]
    print(f"Fact_Earthquake: {count} records")
    
    print("\nSample Star Schema Query (with JOINs):")
    print("-" * 60)
    cursor.execute('''
        SELECT 
            fe.Earthquake_SK,
            dt.year,
            dt.month_name,
            dl.location,
            df.fault,
            dty.magType,
            fe.mag,
            fe.depth
        FROM Fact_Earthquake fe
        JOIN Dim_Time dt ON fe.Time_SK = dt.Time_SK
        JOIN Dim_Location dl ON fe.Location_SK = dl.Location_SK
        JOIN Dim_Fault df ON fe.Fault_SK = df.Fault_SK
        JOIN Dim_Type dty ON fe.Type_SK = dty.Type_SK
        ORDER BY fe.mag DESC
        LIMIT 5
    ''')
    
    for row in cursor.fetchall():
        print(f"SK: {row[0]}, Year: {row[1]}, Month: {row[2]}, Location: {row[3]}, "
              f"Fault: {row[4]}, Type: {row[5]}, Mag: {row[6]}, Depth: {row[7]}")
    
    print("\nDimension Table Samples:")
    print("-" * 60)
    
    # Sample from each dimension table
    cursor.execute('SELECT * FROM Dim_Time LIMIT 3')
    print("Dim_Time sample:")
    for row in cursor.fetchall():
        print(f"  SK: {row[0]}, ID: {row[1]}, Time: {row[2]}, Year: {row[6]}")
    
    cursor.execute('SELECT * FROM Dim_Location LIMIT 3')
    print("Dim_Location sample:")
    for row in cursor.fetchall():
        print(f"  SK: {row[0]}, ID: {row[1]}, Location: {row[4]}")
    
    cursor.execute('SELECT * FROM Dim_Fault LIMIT 3')
    print("Dim_Fault sample:")
    for row in cursor.fetchall():
        print(f"  SK: {row[0]}, ID: {row[1]}, Fault: {row[2]}")

def main():
    """Main function to create and populate the star schema database"""
    csv_file_path = '4_earthquake_italy_faults.csv'  # Update this path to your CSV file
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' not found.")
        print("Please ensure the CSV file is in the same directory as this script.")
        return
    
    try:
        # Create star schema database and tables
        print("Creating star schema database and tables...")
        conn, cursor = create_star_schema_database()
        print("Star schema structure created successfully!")
        
        # Populate dimension tables first (to avoid foreign key constraints)
        print("Populating dimension tables...")
        fault_mapping, type_mapping, location_mapping, time_mapping = populate_dimension_tables(conn, cursor, csv_file_path)
        print("Dimension tables populated successfully!")
        
        # Populate fact table using surrogate keys
        print("Populating fact table...")
        populate_fact_table(conn, cursor, csv_file_path, fault_mapping, type_mapping, location_mapping, time_mapping)
        print("Fact table populated successfully!")
        
        # Verify star schema
        verify_star_schema(cursor)
        
        # Close connection
        conn.close()
        print(f"\nStar schema database 'earthquake_star_schema.db' created successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
import sqlite3
import csv
import os
from datetime import datetime

def create_database():
    """Create SQLite database with tables based on the ER diagram"""
    
    # Connect to SQLite database (creates it if it doesn't exist)
    conn = sqlite3.connect('reconciled_db.db')
    cursor = conn.cursor()
    
    # Drop existing tables if they exist (for clean recreation)
    tables_to_drop = ['Contributors', 'Error', 'Status', 'Type', 'Earthquake', 'Location', 'Fault']
    for table in tables_to_drop:
        cursor.execute(f'DROP TABLE IF EXISTS {table}')
    
    # Create Location table
    cursor.execute('''
        CREATE TABLE Location (
            ID INTEGER PRIMARY KEY,
            location TEXT
        )
    ''')
    
    # Create Fault table
    cursor.execute('''
        CREATE TABLE Fault (
            ID INTEGER PRIMARY KEY,
            fault TEXT
        )
    ''')
    
    # Create Type table
    cursor.execute('''
        CREATE TABLE Type (
            ID INTEGER PRIMARY KEY,
            magType TEXT,
            type TEXT
        )
    ''')
    
    # Create Status table
    cursor.execute('''
        CREATE TABLE Status (
            ID INTEGER PRIMARY KEY,
            status TEXT,
            updated TEXT
        )
    ''')
    
    # Create Contributors table
    cursor.execute('''
        CREATE TABLE Contributors (
            ID INTEGER PRIMARY KEY,
            nst TEXT,
            magNst TEXT,
            net TEXT,
            locationSource TEXT,
            magSource TEXT
        )
    ''')
    
    # Create Error table
    cursor.execute('''
        CREATE TABLE Error (
            ID INTEGER PRIMARY KEY,
            horizontalError REAL,
            depthError REAL,
            magError REAL
        )
    ''')
    
    # Create main Earthquake table with foreign key references
    cursor.execute('''
        CREATE TABLE Earthquake (
            ID INTEGER PRIMARY KEY,
            time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            mag REAL,
            gap REAL,
            dmin REAL,
            rms REAL,
            locationID INTEGER,
            faultID INTEGER,
            typeID INTEGER,
            statusID INTEGER,
            contributorsID INTEGER,
            errorID INTEGER,
            FOREIGN KEY (locationID) REFERENCES Location(ID),
            FOREIGN KEY (faultID) REFERENCES Fault(ID),
            FOREIGN KEY (typeID) REFERENCES Type(ID),
            FOREIGN KEY (statusID) REFERENCES Status(ID),
            FOREIGN KEY (contributorsID) REFERENCES Contributors(ID),
            FOREIGN KEY (errorID) REFERENCES Error(ID)
        )
    ''')
    
    conn.commit()
    return conn, cursor

def insert_or_get_id(cursor, table, column, value):
    """Insert value into table if it doesn't exist, return ID"""
    cursor.execute(f'SELECT ID FROM {table} WHERE {column} = ?', (value,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cursor.execute(f'INSERT INTO {table} ({column}) VALUES (?)', (value,))
        return cursor.lastrowid

def insert_type_or_get_id(cursor, mag_type, event_type):
    """Insert or get ID for Type table with both magType and type"""
    cursor.execute('SELECT ID FROM Type WHERE magType = ? AND type = ?', (mag_type, event_type))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO Type (magType, type) VALUES (?, ?)', (mag_type, event_type))
        return cursor.lastrowid

def populate_database_from_csv(conn, cursor, csv_file_path):
    """Read CSV file and populate the database"""
    
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Insert/get IDs for reference tables
            location_id = insert_or_get_id(cursor, 'Location', 'location', row['location'])
            fault_id = insert_or_get_id(cursor, 'Fault', 'fault', row['fault'])
            type_id = insert_type_or_get_id(cursor, row['magType'], row['type'])
            
            # For now, create default entries for Status, Contributors, and Error
            # since they're not in the CSV data
            status_id = insert_or_get_id(cursor, 'Status', 'status', 'reviewed')
            
            # Insert default Contributors entry
            cursor.execute('INSERT INTO Contributors (nst, magNst, net, locationSource, magSource) VALUES (?, ?, ?, ?, ?)',
                         ('N/A', 'N/A', 'N/A', 'N/A', 'N/A'))
            contributors_id = cursor.lastrowid
            
            # Insert default Error entry
            cursor.execute('INSERT INTO Error (horizontalError, depthError, magError) VALUES (?, ?, ?)',
                         (None, None, None))
            error_id = cursor.lastrowid
            
            # Insert into Earthquake table
            cursor.execute('''
                INSERT INTO Earthquake (
                    ID, time, latitude, longitude, depth, mag, gap, dmin, rms,
                    locationID, faultID, typeID, statusID, contributorsID, errorID
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(row['ID']),
                row['time'],
                float(row['latitude']),
                float(row['longitude']),
                float(row['depth']),
                float(row['mag']),
                None,  # gap not in CSV
                float(row['dist']) if row['dist'] else None,  # using dist as dmin
                None,  # rms not in CSV
                location_id,
                fault_id,
                type_id,
                status_id,
                contributors_id,
                error_id
            ))
    
    conn.commit()

def verify_database(cursor):
    """Verify the database was created and populated correctly"""
    print("Database verification:")
    print("=" * 50)
    
    # Check table counts
    tables = ['Location', 'Fault', 'Type', 'Status', 'Contributors', 'Error', 'Earthquake']
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f"{table}: {count} records")
    
    print("\nSample earthquake records:")
    print("-" * 50)
    cursor.execute('''
        SELECT e.ID, e.time, e.latitude, e.longitude, e.mag, l.location, f.fault, t.magType
        FROM Earthquake e
        JOIN Location l ON e.locationID = l.ID
        JOIN Fault f ON e.faultID = f.ID
        JOIN Type t ON e.typeID = t.ID
        LIMIT 5
    ''')
    
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Time: {row[1]}, Lat: {row[2]}, Lon: {row[3]}, "
              f"Mag: {row[4]}, Location: {row[5]}, Fault: {row[6]}, Type: {row[7]}")

def main():
    """Main function to create and populate the database"""
    csv_file_path = '4_earthquake_italy_faults.csv'  # Update this path to your CSV file
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' not found.")
        print("Please ensure the CSV file is in the same directory as this script.")
        return
    
    try:
        # Create database and tables
        print("Creating database and tables...")
        conn, cursor = create_database()
        print("Database structure created successfully!")
        
        # Populate database from CSV
        print("Populating database from CSV...")
        populate_database_from_csv(conn, cursor, csv_file_path)
        print("Database populated successfully!")
        
        # Verify database
        verify_database(cursor)
        
        # Close connection
        conn.close()
        print("\nDatabase 'earthquake_database.db' created successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
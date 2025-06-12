import pandas as pd # type: ignore
from math import radians, sin, cos, sqrt, atan2
from datetime import timedelta
import os

# --- Haversine Function ---
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees).
    """
    R = 6371 # Earth's radius in kilometers
    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def analyze_swarm_details(
    input_file_path,
    output_file_path,
    min_mainshock_mag=1.0,
    max_days_before=29,
    max_search_radius_km=500,
    country_to_filter=None
    ):
    """
    Analyzes earthquake data to find potential mainshock-swarm pairs and saves detailed information.
    """
    print(f"Reading input file: {input_file_path}...")
    try:
        # Specify data types for columns that might have mixed values or need specific handling
        dtype_spec = {
            'id': str,       # Keep original ID as string
            'magType': str,
            'type': str,
            'location': str, # Ensure location is read as string
            'country': str,  # Ensure country is read as string
            'continent': str,
            'fault': str     # Ensure fault is read as string
        }
        df = pd.read_csv(input_file_path, low_memory=False, dtype=dtype_spec)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file_path}' not found.")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    print("Converting data types and initial cleaning...")
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    
    # Rename USGS 'id' to 'original_event_id' to avoid conflicts
    if 'id' in df.columns and 'ID' not in df.columns:
        df.rename(columns={'id': 'original_event_id'}, inplace=True)

    numeric_cols_to_convert = ['latitude', 'longitude', 'depth', 'mag']
    # Add 'ID' to numeric conversion if it exists
    if 'ID' in df.columns:
        numeric_cols_to_convert.append('ID')

    for col in numeric_cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"Warning: Column '{col}' not found for numeric conversion.")
    
    # Columns essential for the analysis
    required_cols_for_analysis = ['time', 'latitude', 'longitude', 'mag', 'type', 'country']
    if 'ID' in df.columns:
        required_cols_for_analysis.append('ID')
    else:
        print("Error: Required numeric 'ID' column not found or defined for the analysis.")
        return

    # Check if all required columns exist
    missing_required_cols = [col for col in required_cols_for_analysis if col not in df.columns]
    if missing_required_cols:
        print(f"Error: Required columns missing from the input file: {', '.join(missing_required_cols)}")
        return
        
    df.dropna(subset=required_cols_for_analysis, inplace=True)
    if 'ID' in df.columns:
        df['ID'] = df['ID'].astype(int)

    print(f"Total number of events after cleaning: {len(df)}")

    # Filter for type "earthquake"
    df_earthquakes = df[df['type'].astype(str).str.strip().str.lower() == 'earthquake'].copy()
    
    # Filter by country (if specified)
    if country_to_filter:
        df_earthquakes = df_earthquakes[
            df_earthquakes['country'].notna() & 
            (df_earthquakes['country'].astype(str).str.strip().str.lower() == country_to_filter.lower())
        ].copy()
    
    print(f"Number of earthquakes for analysis ({country_to_filter or 'All countries'}): {len(df_earthquakes)}")

    # Identify potential mainshocks
    potential_mainshocks = df_earthquakes[df_earthquakes['mag'] >= min_mainshock_mag].copy()
    print(f"Number of potential mainshocks (mag >= {min_mainshock_mag}): {len(potential_mainshocks)}")

    # Define output columns
    output_columns = [
        'MS_ID', 'MS_Time', 'MS_Lat', 'MS_Lon', 'MS_Mag', 'MS_Depth', 
        'MS_Country', 'MS_Fault', 'MS_location', 
        'CS_ID', 'CS_Time', 'CS_Lat', 'CS_Lon', 'CS_Mag', 'CS_Depth',
        'CS_Country', 'CS_Fault', 'CS_location', 
        'Days_Before_Exact', 'Distance_Exact_km'
    ]

    if potential_mainshocks.empty:
        print("No potential mainshocks found with the specified criteria.")
        # Create an empty CSV with headers if no mainshocks are found
        pd.DataFrame(columns=output_columns).to_csv(output_file_path, index=False, encoding='utf-8')
        return

    all_event_pairs_data = []
    
    print("Starting detailed calculation of mainshock-swarm pairs...")
    
    mainshock_count_processed = 0
    for _, mainshock_row in potential_mainshocks.iterrows():
        mainshock_count_processed += 1
        
        # Extract Mainshock (MS) details
        ms_id = mainshock_row['ID']
        ms_time = mainshock_row['time']
        ms_lat = mainshock_row['latitude']
        ms_lon = mainshock_row['longitude']
        ms_mag = mainshock_row['mag']
        ms_depth = mainshock_row['depth']
        ms_country = mainshock_row['country']
        ms_fault = mainshock_row.get('fault', None)
        ms_location = mainshock_row.get('location', None)

        # Define time window for candidate swarm events (before the mainshock)
        max_swarm_time_start = ms_time - timedelta(days=max_days_before)
        
        # Filter for temporal candidates (events occurring before MS and not the MS itself)
        temporal_candidates = df_earthquakes[
            (df_earthquakes['time'] >= max_swarm_time_start) &
            (df_earthquakes['time'] < ms_time) &
            (df_earthquakes['ID'] != ms_id) 
        ].copy()

        for _, swarm_candidate_row in temporal_candidates.iterrows():
            # Extract Candidate Swarm (CS) details
            cs_id = swarm_candidate_row['ID']
            cs_time = swarm_candidate_row['time']
            cs_lat = swarm_candidate_row['latitude']
            cs_lon = swarm_candidate_row['longitude']
            cs_mag = swarm_candidate_row['mag']
            cs_depth = swarm_candidate_row['depth']
            cs_country = swarm_candidate_row['country'] 
            cs_fault = swarm_candidate_row.get('fault', None)
            cs_location = swarm_candidate_row.get('location', None)

            # Calculate distance between mainshock and swarm candidate
            distance = haversine(ms_lat, ms_lon, cs_lat, cs_lon)

            # Check if the candidate is within the search radius
            if distance <= max_search_radius_km:
                days_before_calc = (ms_time - cs_time).total_seconds() / (24 * 60 * 60)
                
                # Append data for the valid pair
                all_event_pairs_data.append({
                    'MS_ID': ms_id, 'MS_Time': ms_time, 'MS_Lat': ms_lat, 'MS_Lon': ms_lon, 
                    'MS_Mag': ms_mag, 'MS_Depth': ms_depth, 'MS_Country': ms_country,
                    'MS_Fault': ms_fault,
                    'MS_location': ms_location,
                    'CS_ID': cs_id, 'CS_Time': cs_time, 'CS_Lat': cs_lat, 'CS_Lon': cs_lon,
                    'CS_Mag': cs_mag, 'CS_Depth': cs_depth, 'CS_Country': cs_country,
                    'CS_Fault': cs_fault,
                    'CS_location': cs_location,
                    'Days_Before_Exact': round(days_before_calc, 4),
                    'Distance_Exact_km': round(distance, 2)
                })
        
        if mainshock_count_processed % 20 == 0 or mainshock_count_processed == len(potential_mainshocks):
            print(f"Processed {mainshock_count_processed}/{len(potential_mainshocks)} potential mainshocks. Found {len(all_event_pairs_data)} valid pairs so far.")

    # Create DataFrame from the collected data
    output_df = pd.DataFrame(all_event_pairs_data, columns=output_columns)
    output_df.to_csv(output_file_path, index=False, encoding='utf-8')
    print(f"Detailed operation completed. Output saved to: {output_file_path}")
    print(f"Found {len(output_df)} valid (Mainshock, Candidate Swarm Event) pairs.")

# --- CONFIGURATION AND EXECUTION ---
if __name__ == '__main__':
    # This input file should be the result of a previous data fetching and processing step,
    # potentially including fault and location information.
    INPUT_CSV = '4_earthquake_italy_faults.csv'
    OUTPUT_CSV_DETAILED_PAIRS = '5_swarm_details_fault_location.csv'

    # Parameters for the script
    MIN_MAINSHOCK_MAG = 3.0          # Minimum magnitude for an event to be considered a potential mainshock
    MAX_DAYS_BEFORE = 29             # Max days before mainshock to look for swarm events
    MAX_SEARCH_RADIUS_KM = 500       # Max search radius in km around mainshock for swarm events
    COUNTRY_FILTER = "Italy"         # Set to None to process all countries, or specify a country name

    print("Starting seismic swarm analysis script...")
    if not os.path.exists(INPUT_CSV):
        print(f"WARNING: Input file '{INPUT_CSV}' does not exist.")
        print("Ensure the file is in the same directory as the script,")
        print("or specify the full path for INPUT_CSV.")
        print("This script is for analyzing existing CSV data, not for downloading it.")
    else:
        analyze_swarm_details(
            INPUT_CSV,
            OUTPUT_CSV_DETAILED_PAIRS,
            min_mainshock_mag=MIN_MAINSHOCK_MAG,
            max_days_before=MAX_DAYS_BEFORE,
            max_search_radius_km=MAX_SEARCH_RADIUS_KM,
            country_to_filter=COUNTRY_FILTER
        )
    print("Script finished.")
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
from datetime import timedelta
import os # Per controlli file

# --- Funzione Haversine (invariata) ---
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    R = 6371 # Raggio della Terra in chilometri
    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def calcola_dettagli_completi_sciami(
    file_input_path,
    file_output_path,
    min_mag_mainshock_script=1.0,
    max_days_before_script=29,
    max_search_radius_km_script=500,
    country_to_filter=None
    ):
    """
    Analyzes earthquake data to find potential mainshock-swarm pairs and saves detailed information.
    """
    print(f"Lettura del file di input: {file_input_path}...")
    try:
        # Specify data types for columns that might have mixed values or need specific handling
        dtype_spec = {
            'id': str, # Keep original ID as string if it's not purely numeric or for joining later
            'magType': str,
            'type': str,
            'location': str, # Ensure location is read as string
            'country': str,  # Ensure country is read as string
            'continent': str,
            'fault': str     # Ensure fault is read as string
        }
        # Note: The USGS CSV uses 'id' for the event identifier. 
        # The script later renames/uses 'ID' after converting to numeric.
        # If your input CSV uses 'ID' directly and it's numeric, adjust dtype_spec or handling.
        df = pd.read_csv(file_input_path, low_memory=False, dtype=dtype_spec)
    except FileNotFoundError:
        print(f"Errore: File di input '{file_input_path}' non trovato.")
        return
    except Exception as e:
        print(f"Errore durante la lettura del CSV: {e}")
        return

    print("Conversione tipi di dato e pulizia iniziale...")
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    
    # Rename USGS 'id' to 'original_id' to avoid conflict if 'ID' is also a column name
    # or if you want to preserve the original string ID.
    # If your CSV already has a numeric 'ID' column, this step might not be needed
    # or you might need to adjust column names.
    if 'id' in df.columns and 'ID' not in df.columns: # Assuming 'id' is the original string ID from USGS
        df.rename(columns={'id': 'original_event_id'}, inplace=True)
        # Attempt to create a numeric ID if possible, or use original_event_id
        # For this script, it seems a numeric 'ID' is expected later.
        # If 'original_event_id' is like 'us7000kufc', it can't be directly numeric.
        # The script uses 'ID' as int. Let's assume 'ID' is a separate column or can be derived.
        # If 'ID' is not in your CSV, you need to define how it's created.
        # For now, we'll proceed assuming 'ID' will be present or created and converted.

    numeric_cols_to_convert = ['latitude', 'longitude', 'depth', 'mag']
    # Add 'ID' to numeric conversion if it exists and is meant to be numeric
    if 'ID' in df.columns: # Check if 'ID' column exists before trying to convert
        numeric_cols_to_convert.append('ID')

    for col in numeric_cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"Attenzione: Colonna '{col}' non trovata per la conversione numerica.")
    
    # Columns essential for the analysis
    # 'location' and 'fault' are handled with .get() later, so not strictly required to be non-NaN here.
    required_cols_for_analysis = ['time', 'latitude', 'longitude', 'mag', 'type', 'country']
    if 'ID' in df.columns : # 'ID' is used as integer later
        required_cols_for_analysis.append('ID')
    else:
        print("Errore: Colonna 'ID' numerica richiesta per l'analisi non trovata o non definita.")
        return # Exit if numeric ID is crucial and missing

    # Check if all required columns exist
    missing_required_cols = [col for col in required_cols_for_analysis if col not in df.columns]
    if missing_required_cols:
        print(f"Errore: Colonne richieste mancanti nel file di input: {', '.join(missing_required_cols)}")
        return
        
    df.dropna(subset=required_cols_for_analysis, inplace=True)
    if 'ID' in df.columns: # Ensure 'ID' is int if it exists
        df['ID'] = df['ID'].astype(int)

    print(f"Numero totale di eventi dopo pulizia: {len(df)}")

    # Filter for type "earthquake"
    df_earthquakes = df[df['type'].astype(str).str.strip().str.lower() == 'earthquake'].copy()
    
    # Filter by country (if specified)
    if country_to_filter:
        df_earthquakes = df_earthquakes[
            df_earthquakes['country'].notna() & 
            (df_earthquakes['country'].astype(str).str.strip().str.lower() == country_to_filter.lower())
        ].copy()
    
    print(f"Numero di terremoti per l'analisi ({country_to_filter or 'Tutti i paesi'}): {len(df_earthquakes)}")

    # Identify potential mainshocks
    potential_mainshocks = df_earthquakes[df_earthquakes['mag'] >= min_mag_mainshock_script].copy()
    print(f"Numero di potenziali mainshock (mag >= {min_mag_mainshock_script}): {len(potential_mainshocks)}")

    # Define output columns - ensuring all requested fields are here
    output_columns = [
        'MS_ID', 'MS_Time', 'MS_Lat', 'MS_Lon', 'MS_Mag', 'MS_Depth', 
        'MS_Country', 'MS_Fault', 'MS_location', 
        'CS_ID', 'CS_Time', 'CS_Lat', 'CS_Lon', 'CS_Mag', 'CS_Depth',
        'CS_Country', 'CS_Fault', 'CS_location', 
        'Days_Before_Exact', 'Distance_Exact_km'
    ]

    if potential_mainshocks.empty:
        print("Nessun potenziale mainshock trovato con i criteri specificati.")
        # Create an empty CSV with the defined headers if no mainshocks are found
        pd.DataFrame(columns=output_columns).to_csv(file_output_path, index=False, encoding='utf-8')
        return

    all_event_pairs_data = []
    
    print("Inizio calcolo dettagliato coppie mainshock-sciame...")
    
    mainshock_count_processed = 0
    for _, mainshock_row in potential_mainshocks.iterrows():
        mainshock_count_processed += 1
        
        # Extract Mainshock (MS) details
        ms_id = mainshock_row['ID'] # Assumes 'ID' is the numeric identifier
        ms_time = mainshock_row['time']
        ms_lat = mainshock_row['latitude']
        ms_lon = mainshock_row['longitude']
        ms_mag = mainshock_row['mag']
        ms_depth = mainshock_row['depth']
        ms_country = mainshock_row['country']
        ms_fault = mainshock_row.get('fault', None) # Use .get() for optional columns
        ms_location = mainshock_row.get('location', None) # Use .get() for optional columns

        # Define time window for candidate swarm events (before the mainshock)
        max_swarm_time_start = ms_time - timedelta(days=max_days_before_script)
        
        # Filter for temporal candidates (events occurring before MS and not MS itself)
        temporal_candidates = df_earthquakes[
            (df_earthquakes['time'] >= max_swarm_time_start) &
            (df_earthquakes['time'] < ms_time) &
            (df_earthquakes['ID'] != ms_id) 
        ].copy() # Use .copy() to avoid SettingWithCopyWarning if modifying temporal_candidates later

        for _, swarm_candidate_row in temporal_candidates.iterrows():
            # Extract Candidate Swarm (CS) details
            cs_id = swarm_candidate_row['ID'] # Assumes 'ID' is the numeric identifier
            cs_time = swarm_candidate_row['time']
            cs_lat = swarm_candidate_row['latitude']
            cs_lon = swarm_candidate_row['longitude']
            cs_mag = swarm_candidate_row['mag']
            cs_depth = swarm_candidate_row['depth']
            cs_country = swarm_candidate_row['country'] 
            cs_fault = swarm_candidate_row.get('fault', None) # Use .get()
            cs_location = swarm_candidate_row.get('location', None) # Use .get()

            # Calculate distance between mainshock and swarm candidate
            distance = haversine(ms_lat, ms_lon, cs_lat, cs_lon)

            # Check if candidate is within the search radius
            if distance <= max_search_radius_km_script:
                days_before = (ms_time - cs_time).total_seconds() / (24 * 60 * 60)
                
                # Append data for the valid pair
                all_event_pairs_data.append({
                    'MS_ID': ms_id, 'MS_Time': ms_time, 'MS_Lat': ms_lat, 'MS_Lon': ms_lon, 
                    'MS_Mag': ms_mag, 'MS_Depth': ms_depth, 'MS_Country': ms_country,
                    'MS_Fault': ms_fault,        # Corrected key and value
                    'MS_location': ms_location,  # Added key and value
                    'CS_ID': cs_id, 'CS_Time': cs_time, 'CS_Lat': cs_lat, 'CS_Lon': cs_lon,
                    'CS_Mag': cs_mag, 'CS_Depth': cs_depth, 'CS_Country': cs_country,
                    'CS_Fault': cs_fault,        # Corrected key and value
                    'CS_location': cs_location,  # Added key and value
                    'Days_Before_Exact': round(days_before, 4),
                    'Distance_Exact_km': round(distance, 2)
                })
        
        if mainshock_count_processed % 20 == 0 or mainshock_count_processed == len(potential_mainshocks):
            print(f"Processati {mainshock_count_processed}/{len(potential_mainshocks)} potenziali mainshock. Trovate {len(all_event_pairs_data)} coppie valide finora.")

    # Create DataFrame from the collected data using the defined column order
    output_df = pd.DataFrame(all_event_pairs_data, columns=output_columns)
    output_df.to_csv(file_output_path, index=False, encoding='utf-8')
    print(f"Operazione dettagliata completata. Output salvato in: {file_output_path}")
    print(f"Trovate {len(output_df)} coppie (Mainshock, Evento Sciame Candidato) valide.")

# --- CONFIGURATION AND EXECUTION ---
# Ensure these file paths and parameters are correctly set for your environment.
# INPUT_CSV should be the path to your earthquake data CSV.
# This script assumes the CSV has columns like:
# time, latitude, longitude, depth, mag, type, country, fault (optional), location (optional),
# and a numeric 'ID' column for events.
# If your 'ID' column is named differently or is not numeric, adjustments are needed.

INPUT_CSV = '4_earthquake_italy_faults.csv' # EXAMPLE: Replace with your actual input file name
# This input file should be the result of a previous data fetching and processing step,
# potentially including fault and location information if available.
# The original "earthquake_script_italy" was for fetching; this one is for analysis.

OUTPUT_CSV_DETAILED_PAIRS = '5_swarm_details_fault_location.csv' # Updated output file name

# Parameters for the script
MIN_MAG_MAINSHOCK_SCRIPT = 3.0       # Minimum magnitude for an event to be considered a potential mainshock
MAX_DAYS_BEFORE_SCRIPT = 29          # Max days before mainshock to look for swarm events
MAX_SEARCH_RADIUS_KM_SCRIPT = 500    # Max search radius in km around mainshock for swarm events
COUNTRY_FILTER_FOR_SCRIPT = "Italy"  # Set to None to process all countries, or specify a country name
                                     # (ensure it matches the 'country' column values in your CSV)

if __name__ == '__main__':
    print("Avvio script analisi sciami sismici...")
    if not os.path.exists(INPUT_CSV):
        print(f"ATTENZIONE: Il file di input '{INPUT_CSV}' non esiste.")
        print("Assicurati che il file sia nella stessa directory dello script,")
        print("o specifica il percorso completo per INPUT_CSV.")
        print("Questo script è per analizzare dati CSV esistenti, non per scaricarli.")
    else:
        calcola_dettagli_completi_sciami(
            INPUT_CSV,
            OUTPUT_CSV_DETAILED_PAIRS,
            min_mag_mainshock_script=MIN_MAG_MAINSHOCK_SCRIPT,
            max_days_before_script=MAX_DAYS_BEFORE_SCRIPT,
            max_search_radius_km_script=MAX_SEARCH_RADIUS_KM_SCRIPT,
            country_to_filter=COUNTRY_FILTER_FOR_SCRIPT
        )
    print("Script terminato.")

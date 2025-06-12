import pandas as pd # type: ignore
import numpy as np # To handle potential missing values (NaN)

# --- Configuration ---
CSV_INPUT_PATH = '5_swarm_details_fault_location.csv'
CSV_OUTPUT_PATH = '6_macro_fault.csv'

# Column names
COL_MS_FAULT = 'MS_Fault'
COL_MS_LAT = 'MS_Lat'
COL_CS_FAULT = 'CS_Fault'
COL_CS_LAT = 'CS_Lat'

# APPROXIMATE LATITUDE THRESHOLDS FOR NORTH/CENTER/SOUTH ITALY
LAT_MIN_NORTH = 44.0  # Minimum latitude to be considered "North"
LAT_MIN_CENTER = 41.5 # Minimum latitude to be considered "Center"
                      # Everything below LAT_MIN_CENTER will be "South"

# OPTION: Do you want to create new columns for the macro-regions (True)
# or overwrite the existing MS_Fault/CS_Fault columns (False)?
# True is safer to avoid losing the original fault names.
CREATE_NEW_MACRO_REGION_COLUMNS = True
NEW_COL_MS_MACRO_FAULT = 'MS_Macro_Fault' # Name if CREATE_NEW_MACRO_REGION_COLUMNS = True
NEW_COL_CS_MACRO_FAULT = 'CS_Macro_Fault' # Name if CREATE_NEW_MACRO_REGION_COLUMNS = True
# --- End of Configuration ---

def assign_macro_region(latitude):
    """
    Assigns a macro-region ('North', 'Center', 'South') based on latitude.
    Returns 'Unknown' if the latitude is invalid.
    """
    if pd.isna(latitude):
        return 'Unknown'
    try:
        lat = float(latitude)
        if lat >= LAT_MIN_NORTH:
            return 'North'
        elif lat >= LAT_MIN_CENTER:
            return 'Center'
        else:
            return 'South'
    except ValueError:
        return 'Unknown' # If the float conversion fails

def main():
    print(f"Loading data from: {CSV_INPUT_PATH}")
    try:
        df = pd.read_csv(CSV_INPUT_PATH)
    except FileNotFoundError:
        print(f"Error: File not found '{CSV_INPUT_PATH}'")
        return
    except Exception as e:
        print(f"Error while loading the CSV: {e}")
        return

    # Check for the presence of the required latitude columns
    required_lat_columns = [COL_MS_LAT, COL_CS_LAT]
    for col in required_lat_columns:
        if col not in df.columns:
            print(f"Error: The specified latitude column '{col}' is not present in the CSV.")
            print("Check the column names in 'COL_MS_LAT' and 'COL_CS_LAT' in the script.")
            return

    print("Assigning macro-regions based on latitude...")

    # Apply the function to create the macro-region strings
    ms_macro_regions = df[COL_MS_LAT].apply(assign_macro_region)
    cs_macro_regions = df[COL_CS_LAT].apply(assign_macro_region)

    if CREATE_NEW_MACRO_REGION_COLUMNS:
        print(f"Creating new columns '{NEW_COL_MS_MACRO_FAULT}' and '{NEW_COL_CS_MACRO_FAULT}'.")
        df[NEW_COL_MS_MACRO_FAULT] = ms_macro_regions
        df[NEW_COL_CS_MACRO_FAULT] = cs_macro_regions
    else:
        print(f"Overwriting existing columns '{COL_MS_FAULT}' and '{COL_CS_FAULT}'.")
        # Check for the presence of fault columns before modifying them
        required_fault_columns = [COL_MS_FAULT, COL_CS_FAULT]
        for col in required_fault_columns:
            if col not in df.columns:
                print(f"Warning: The fault column '{col}' to be modified is not present in the CSV.")
                print(f"The column '{col}' will be created with the macro-region values.")
        df[COL_MS_FAULT] = ms_macro_regions
        df[COL_CS_FAULT] = cs_macro_regions

    print(f"Saving modified data to: {CSV_OUTPUT_PATH}")
    try:
        df.to_csv(CSV_OUTPUT_PATH, index=False)
        print("Operation completed successfully!")
        if CREATE_NEW_MACRO_REGION_COLUMNS:
            print(f"The macro-regions have been saved in the '{NEW_COL_MS_MACRO_FAULT}' and '{NEW_COL_CS_MACRO_FAULT}' columns.")
            print(f"The original fault names in '{COL_MS_FAULT}' and '{COL_CS_FAULT}' have been preserved.")
        else:
            print(f"The fault names in '{COL_MS_FAULT}' and '{COL_CS_FAULT}' have been replaced with the macro-regions.")
            print("It is recommended to have a backup of the original file.")

    except Exception as e:
        print(f"Error while saving the CSV: {e}")

if __name__ == '__main__':
    main()
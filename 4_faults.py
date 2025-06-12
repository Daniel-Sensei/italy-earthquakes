import pandas as pd # type: ignore
import geopandas as gpd # type: ignore
from shapely.geometry import Point # type: ignore
import os

def filter_for_italy(input_csv, output_csv):
    """Loads the CSV, keeps only rows where country == 'Italy', and saves the result."""
    df = pd.read_csv(input_csv)
    df_italy = df[df['country'].astype(str).str.strip().str.lower() == 'italy'].copy()
    df_italy.to_csv(output_csv, index=False)
    print(f"Saved {len(df_italy)} Italian events out of {len(df)} total to {output_csv}")
    return output_csv

def enrich_with_italian_faults(earthquakes_df, faults_gml_path, fault_name_column):
    """
    Enriches the Italian earthquake DataFrame with fault information.
    Finds the nearest fault for each earthquake and updates the 'fault' column.
    """
    if not faults_gml_path or not os.path.exists(faults_gml_path):
        print("Italian faults file not found. Skipping enrichment.")
        return earthquakes_df

    gdf_faults = gpd.read_file(faults_gml_path)
    
    # Prepare the earthquake DataFrame for spatial operations
    df_temp = earthquakes_df.copy()
    df_temp.dropna(subset=['longitude', 'latitude'], inplace=True)
    geometry = [Point(xy) for xy in zip(df_temp['longitude'], df_temp['latitude'])]
    gdf_earthquakes = gpd.GeoDataFrame(df_temp, geometry=geometry, crs="EPSG:4326")

    # Ensure both GeoDataFrames have the same Coordinate Reference System (CRS)
    if gdf_faults.crs != gdf_earthquakes.crs:
        gdf_faults = gdf_faults.to_crs(gdf_earthquakes.crs)

    if fault_name_column not in gdf_faults.columns:
        print(f"Column '{fault_name_column}' not found in the faults file.")
        return earthquakes_df

    # Keep only the necessary columns from the faults GeoDataFrame
    gdf_faults = gdf_faults[[fault_name_column, 'geometry']].rename(
        columns={fault_name_column: 'New_Nearest_Fault'}
    )

    # Perform the spatial join to find the nearest fault for each earthquake
    gdf_earthquakes_enriched = gpd.sjoin_nearest(
        gdf_earthquakes, gdf_faults, how='left', distance_col='temp_distance_degrees'
    )

    # Set the original DataFrame's index to 'ID' for efficient updating
    earthquakes_df = earthquakes_df.set_index('ID', drop=False)
    
    # Update the 'fault' column in the original DataFrame with the newly found fault names
    for _, row in gdf_earthquakes_enriched.iterrows():
        earthquake_id = row['ID']
        new_fault_name = row['New_Nearest_Fault']
        earthquakes_df.loc[earthquake_id, 'fault'] = new_fault_name

    return earthquakes_df.reset_index(drop=True)

if __name__ == "__main__":
    # --- Parameters to be configured ---
    INPUT_CSV = "3_earthquake_location.csv"            
    OUTPUT_CSV_ITALY = "earthquake_italy_location.csv"   
    OUTPUT_CSV_ENRICHED = "4_earthquake_italy_faults.csv" 

    # --- Parameters for the faults file ---
    FAULTS_GML_PATH = "ITHACAFaults/ITHACAFaults.gml"
    FAULT_NAME_COLUMN = "name"    

    # Step 1: Filter the dataset to include only events in Italy
    filter_for_italy(INPUT_CSV, OUTPUT_CSV_ITALY)

    # Step 2: Enrich the Italy-specific data with fault information
    df_italy = pd.read_csv(OUTPUT_CSV_ITALY)
    df_italy_enriched = enrich_with_italian_faults(df_italy, FAULTS_GML_PATH, FAULT_NAME_COLUMN)
    df_italy_enriched.to_csv(OUTPUT_CSV_ENRICHED, index=False)
    print(f"Enriched file saved: {OUTPUT_CSV_ENRICHED}")

    # Step 3: Clean up by deleting the temporary file
    if os.path.exists(OUTPUT_CSV_ITALY):
        os.remove(OUTPUT_CSV_ITALY)
        print(f"Temporary file {OUTPUT_CSV_ITALY} deleted.")
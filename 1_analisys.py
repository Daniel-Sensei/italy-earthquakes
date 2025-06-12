import pandas as pd # type: ignore

def analyze_data_quality_complete(file_path):
    """
    Performs an advanced data quality analysis on an earthquake dataset,
    including completeness, validity, and uniqueness,
    and presents the results in a consolidated tabular format.

    Args:
        file_path (str): The path to the CSV file to be analyzed.
    """
    try:
        # Load the dataset from the CSV file
        df = pd.read_csv(file_path)
        print(f"Dataset loaded successfully. Total number of records: {len(df)}\n")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return

    # Dictionary to store the results in a structured way
    results = {}
    
    # List of fields that will appear as rows in the final report
    report_fields = ['mag', 'depth', 'gap', 'rms', 'time', 'latitude/longitude']

    # Initialize the results dictionary with default values
    for field in report_fields:
        results[field] = {
            'Completeness (%)': '-',
            'Validity (%)': '-',
            'Uniqueness (%)': '-'
        }
    
    total_records = len(df)

    # --- 1. COMPLETENESS CALCULATION ---
    single_fields_for_completeness = ['mag', 'depth', 'gap', 'rms', 'time']
    for field in single_fields_for_completeness:
        not_null_count = df[field].notna().sum()
        results[field]['Completeness (%)'] = f"{(not_null_count / total_records) * 100:.2f}"
    
    # Completeness for the latitude/longitude pair (complete only if both values are present)
    complete_coords_count = df[['latitude', 'longitude']].notna().all(axis=1).sum()
    results['latitude/longitude']['Completeness (%)'] = f"{(complete_coords_count / total_records) * 100:.2f}"

    # --- 2. VALIDITY CALCULATION ---
    results['mag']['Validity (%)'] = f"{(df['mag'].between(0.1, 10).sum() / total_records) * 100:.2f}"
    for field in ['depth', 'gap', 'rms']:
        results[field]['Validity (%)'] = f"{(df[field] >= 0).sum() / total_records * 100:.2f}"
    results['time']['Validity (%)'] = f"{(pd.to_datetime(df['time'], errors='coerce').notna().sum() / total_records) * 100:.2f}"
    valid_coords_count = (df['latitude'].between(35, 47) & df['longitude'].between(6, 19)).sum()
    results['latitude/longitude']['Validity (%)'] = f"{(valid_coords_count / total_records) * 100:.2f}"
    
    # --- 3. UNIQUENESS CALCULATION ---
    unique_time_count = df['time'].nunique()
    results['time']['Uniqueness (%)'] = f"{(unique_time_count / total_records) * 100:.2f}"

    # --- 4. FINAL REPORT ---
    # Convert the dictionary to a pandas DataFrame for a tabular view
    report_df = pd.DataFrame.from_dict(results, orient='index')
    report_df.index.name = 'Field'
    
    print("--- Data Quality Report ---")
    print(report_df.to_string())

if __name__ == "__main__":
    file_path = '0_earthquake_italy_full.csv'
    analyze_data_quality_complete(file_path)
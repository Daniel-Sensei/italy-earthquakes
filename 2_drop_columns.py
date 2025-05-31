import pandas as pd

# Define the input and output CSV filenames.
input_csv = "0_earthquake_italy_full.csv"   # This should be your original CSV file
output_csv = "2_earthquake_italy_dropped_columns.csv"     # The cleaned CSV file will be saved here

# List of columns to drop from the dataset
columns_to_drop = [
    "place", "locationSource", "magSource",
    "nst", "gap", "dmin", "rms", "net", "id", "updated",
    "horizontalError", "depthError", "magError", "magNst", "status"
]

def clean_data(input_file, output_file, drop_columns):
    """Load the CSV, drop unnecessary columns, and save the cleaned dataset."""
    try:
        # Load the dataset into a DataFrame
        df = pd.read_csv(input_file)
        print("Original DataFrame shape:", df.shape)
        print("Original columns:", list(df.columns))

        # Drop the specified columns (only drop if they exist)
        df_clean = df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore')
        
        print("\nColumns dropped:", [col for col in drop_columns if col in df.columns])
        print("New DataFrame shape:", df_clean.shape)
        print("Remaining columns:", list(df_clean.columns))
        
        # Optional: further cleaning can be done here (e.g., dealing with missing values)
        # For example, remove rows where the 'mag' column is NaN:
        if 'mag' in df_clean.columns:
            df_clean = df_clean.dropna(subset=['mag'])
            print("After dropping NaNs in 'mag', shape:", df_clean.shape)
        
        # Add an auto-incremental integer ID column
        df_clean.insert(0, "ID", range(1, len(df_clean) + 1))
        print("Added 'ID' column.")

        # Save the cleaned DataFrame to a new CSV file
        df_clean.to_csv(output_file, index=False)
        print(f"\nCleaned data successfully saved to {output_file}")

    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    clean_data(input_csv, output_csv, columns_to_drop)

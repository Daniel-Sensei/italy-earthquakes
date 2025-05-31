import pandas as pd
import numpy as np

# Read the CSV file (adjust the file path if needed)
csv_file = '0_earthquake_italy_full.csv'
df = pd.read_csv(csv_file)

def is_valid_value(val, col_dtype):
    """
    Checks validity based on a simple domain rule:
    - For numeric types, checks if it can be converted to a float.
    - For datetime types, tries converting with pd.to_datetime.
    - For object (string) types, checks if the string is not empty.
    """
    if pd.isnull(val):
        return False
    if np.issubdtype(col_dtype, np.number):
        try:
            float(val)
            return True
        except:
            return False
    elif np.issubdtype(col_dtype, np.datetime64):
        try:
            pd.to_datetime(val)
            return True
        except:
            return False
    else:
        # For strings or objects: consider non-empty strings as valid.
        if isinstance(val, str) and val.strip() != '':
            return True
        else:
            return False

def count_precision(val, col_dtype):
    """
    For numeric types, counts the value as 'precise' if it has at most 2 decimal places.
    For other types, assumes non-null values are precise.
    """
    if pd.isnull(val):
        return 0
    if np.issubdtype(col_dtype, np.floating):
        try:
            s = str(val)
            if '.' in s:
                decimals = s.split('.')[1]
                return 1 if len(decimals) <= 2 else 0
            else:
                return 1
        except:
            return 0
    else:
        # For non-numeric types, assume a non-null value is precise.
        return 1

# Dictionary to store the metrics per column.
metrics = {}

for col in df.columns:
    total_count = len(df)
    non_null_count = df[col].notnull().sum()
    
    # Completeness: percentage of non-null entries.
    completeness = (non_null_count / total_count) * 100
    
    # Validity: apply our simple check function.
    col_dtype = df[col].dtype
    valid_count = df[col].apply(lambda x: is_valid_value(x, col_dtype)).sum()
    validity = (valid_count / total_count) * 100
    
    # Precision: for numeric columns we check the decimal precision, otherwise assume precise if not null.
    if np.issubdtype(col_dtype, np.number):
        precise_count = df[col].apply(lambda x: count_precision(x, col_dtype)).sum()
    else:
        precise_count = non_null_count  # assume all non-null values are precise for non-numeric types
    precision = (precise_count / total_count) * 100

    # Consistency: percentage of non-null entries that have the same type as the most common type.
    non_null_types = df[col].dropna().map(lambda x: type(x))
    if not non_null_types.empty:
        mode_type = non_null_types.mode()[0]
        consistency_count = non_null_types.map(lambda t: t == mode_type).sum()
        consistency = (consistency_count / non_null_count) * 100
    else:
        consistency = 0

    # Uniqueness: percentage of duplicated (non-unique) values among the non-null entries.
    unique_count = df[col].nunique(dropna=True)
    duplicate_percentage = (1 - unique_count / non_null_count) * 100 if non_null_count > 0 else 0

    metrics[col] = {
        'Validity (%)': round(validity, 2),
        'Completeness (%)': round(completeness, 2),
        'Precision (%)': round(precision, 2),
        'Consistency (%)': round(consistency, 2),
        'Uniqueness (duplicated %)': round(duplicate_percentage, 2)
    }

# Create a DataFrame from the metrics dictionary and display the results.
metrics_df = pd.DataFrame(metrics).T
print(metrics_df)

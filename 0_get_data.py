import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO

# Configuration: set your start and end dates as needed
start_date = datetime(1900, 1, 1)
end_date   = datetime(2025, 5, 31)  # Modify to your desired end date
min_magnitude = 0

# Approximate bounding box for Italy
italy_min_latitude = 34.0
italy_max_latitude = 48.0
italy_min_longitude = 5.0
italy_max_longitude = 20.0

# URL template with placeholders for starttime, endtime, magnitude, and geographic bounds
base_url = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query.csv?"
    "starttime={start}&endtime={end}&minmagnitude={mag}"
    "&minlatitude={minlat}&maxlatitude={maxlat}"
    "&minlongitude={minlon}&maxlongitude={maxlon}"
    "&orderby=time"
)

# Create an empty DataFrame to accumulate the data
final_df = pd.DataFrame()

current_start = start_date

print(f"Fetching earthquake data for Italy (Magnitude >= {min_magnitude})")
print(f"Bounding Box: Lat({italy_min_latitude}-{italy_max_latitude}), Lon({italy_min_longitude}-{italy_max_longitude})")
print("-" * 30)

while current_start < end_date:
    # Set the end time for a four-month period (or the overall end_date)
    # Using 4 months to be cautious with API request limits, can be adjusted
    current_end = current_start + relativedelta(months=4)
    if current_end > end_date:
        current_end = end_date

    # Format the dates as required by the API (YYYY-MM-DD HH:MM:SS)
    start_str = current_start.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = current_end.strftime("%Y-%m-%d %H:%M:%S")
    
    # Build the URL for the current interval
    url = base_url.format(
        start=start_str, 
        end=end_str, 
        mag=min_magnitude,
        minlat=italy_min_latitude,
        maxlat=italy_max_latitude,
        minlon=italy_min_longitude,
        maxlon=italy_max_longitude
    )
    print(f"Fetching data from {start_str} to {end_str}...")

    try:
        # Send the request to the USGS API endpoint
        response = requests.get(url, timeout=30) # Added timeout
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)

        if response.text.strip(): # Check if response is not empty
            # Use StringIO to wrap the CSV text so pandas can read it
            temp_df = pd.read_csv(StringIO(response.text))
            
            if not temp_df.empty:
                # Append the temporary DataFrame to the final DataFrame
                final_df = pd.concat([final_df, temp_df], ignore_index=True)
                print(f"Retrieved {len(temp_df)} records for this period.")
            else:
                print("No records found for this period.")
        else:
            print("Received empty response from API for this period.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} for period {start_str} to {end_str}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err} for period {start_str} to {end_str}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err} for period {start_str} to {end_str}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err} for period {start_str} to {end_str}")
    except pd.errors.EmptyDataError:
        print(f"No data to parse (empty CSV) for {start_str} to {end_str}.")
    except Exception as e:
        print(f"An unexpected error occurred while processing data for {start_str} to {end_str}: {e}")
        print(f"Response text was: '{response.text[:200]}...'") # Print first 200 chars of problematic response

    # Move the current_start pointer forward
    current_start = current_end

# Reset the index and save the final DataFrame to CSV
if not final_df.empty:
    final_df.reset_index(drop=True, inplace=True)
    final_csv_filename = "0_earthquake_italy_full.csv"
    final_df.to_csv(final_csv_filename, index=False)
    print("-" * 30)
    print(f"Final CSV with {len(final_df)} records for Italy saved to {final_csv_filename}.")
else:
    print("-" * 30)
    print("No earthquake data was retrieved for Italy in the specified period.")
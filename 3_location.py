import pandas as pd # type: ignore
import geopandas as gpd # type: ignore
from shapely.geometry import Point # type: ignore
import reverse_geocoder as rg # type: ignore
import numpy as np

# ----- Configuration -----
input_csv = '2_earthquake_italy_dropped_columns.csv'
output_csv = '3_earthquake_location.csv'
world_shapefile_path = 'ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp'

# ----- Step 1: Read the CSV file into a DataFrame -----
# For memory constraints, consider using chunksize if required.
df = pd.read_csv(input_csv)

# ----- Step 2: Generate a GeoDataFrame for spatial operations -----
# Create a geometry column from longitude and latitude
geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# ----- Step 3: Load World Data from the local shapefile -----
# Read the downloaded Natural Earth shapefile
world = gpd.read_file(world_shapefile_path)

# If your CSV points are in WGS84 (EPSG:4326), ensure both datasets use the same CRS.
gdf.set_crs(epsg=4326, inplace=True)
if world.crs != gdf.crs:
    world = world.to_crs(gdf.crs)

# ----- Step 4: Spatial Join to add Country and Continent -----
# Adjust field names if necessary based on the shapefile's schema. 
# Commonly, the country name is stored in a column such as 'NAME' or similar.
gdf = gpd.sjoin_nearest(gdf, world[['geometry', 'CONTINENT', 'NAME']], how='left', distance_col="dist")
gdf.rename(columns={'NAME': 'country', 'CONTINENT': 'continent'}, inplace=True)

# ----- Step 5: Reverse Geocoding for the "location" column -----
# Use reverse_geocoder to find the nearest city or town.
# We use coordinate rounding and caching to enhance performance for large datasets.
precision = 3  # Adjust the rounding precision as needed.
rounded_coords = np.array(list(zip(gdf['latitude'].round(precision), gdf['longitude'].round(precision))))

# Build cache to avoid duplicate geocoding for identical coordinates.
cache = {}
locations = []

# Find unique coordinate pairs and create a mapping
unique_coords, inverse_indices = np.unique(rounded_coords, axis=0, return_inverse=True)
unique_coords_list = [tuple(coord) for coord in unique_coords]

print("Performing reverse geocoding on unique coordinate pairs...")
unique_results = rg.search(unique_coords_list, mode=1)  # Fast approximate search

# Populate the cache with the nearest city names.
for coord, result in zip(unique_coords_list, unique_results):
    cache[coord] = result['name']

# Map the results back to the entire dataset.
locations = [cache[tuple(unique_coords[idx])] for idx in inverse_indices]
gdf['location'] = locations

# ----- Step 6: Final Adjustments and Exporting the Data -----
# Drop columns not needed (like geometry or indices from the spatial join)
gdf.drop(columns=['geometry', 'index_right'], inplace=True)

# Write the augmented dataframe to a new CSV.
gdf.to_csv(output_csv, index=False)
print(f"Augmented data saved to {output_csv}")
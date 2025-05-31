import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

def pulisci_per_italia(input_csv, output_csv):
    """Carica il CSV, tiene solo le righe con country == 'Italy' e salva il risultato."""
    df = pd.read_csv(input_csv)
    df_italia = df[df['country'].astype(str).str.strip().str.lower() == 'italy'].copy()
    df_italia.to_csv(output_csv, index=False)
    print(f"Salvati {len(df_italia)} eventi italiani su {len(df)} totali in {output_csv}")
    return output_csv

def arricchisci_faglie_italia(df_input_terremoti, percorso_file_faglie_gml, nome_colonna_faglia_gml):
    """
    Arricchisce il DataFrame dei terremoti italiani con info sulle faglie.
    Aggiorna 'nearest_fault' e imposta 'distance_to_fault_km' a NaN per gli eventi aggiornati.
    """
    if not percorso_file_faglie_gml or not os.path.exists(percorso_file_faglie_gml):
        print("File faglie Italia non trovato. Salto arricchimento.")
        return df_input_terremoti

    gdf_faglie = gpd.read_file(percorso_file_faglie_gml)
    df_temp = df_input_terremoti.copy()
    df_temp.dropna(subset=['longitude', 'latitude'], inplace=True)
    geometry = [Point(xy) for xy in zip(df_temp['longitude'], df_temp['latitude'])]
    gdf_terremoti = gpd.GeoDataFrame(df_temp, geometry=geometry, crs="EPSG:4326")

    if gdf_faglie.crs != gdf_terremoti.crs:
        gdf_faglie = gdf_faglie.to_crs(gdf_terremoti.crs)

    if nome_colonna_faglia_gml not in gdf_faglie.columns:
        print(f"Colonna '{nome_colonna_faglia_gml}' non trovata nelle faglie.")
        return df_input_terremoti

    gdf_faglie = gdf_faglie[[nome_colonna_faglia_gml, 'geometry']].rename(
        columns={nome_colonna_faglia_gml: 'New_Nearest_Fault'}
    )

    gdf_terremoti_arr = gpd.sjoin_nearest(
        gdf_terremoti, gdf_faglie, how='left', distance_col='temp_distance_degrees'
    )

    df_input_terremoti = df_input_terremoti.set_index('ID', drop=False)
    for _, row in gdf_terremoti_arr.iterrows():
        terremoto_id = row['ID']
        nuova_faglia = row['New_Nearest_Fault']
        df_input_terremoti.loc[terremoto_id, 'fault'] = nuova_faglia

    return df_input_terremoti.reset_index(drop=True)

if __name__ == "__main__":
    # Parametri da modificare secondo i tuoi file
    INPUT_CSV = "3_earthquake_location.csv"  # CSV di partenza
    OUTPUT_CSV_ITALY = "earthquake_italy_location.csv"      # CSV solo Italia
    OUTPUT_CSV_ARRICCHITO = "4_earthquake_italy_faults.csv"  # CSV arricchito con faglie

    # Parametri per le faglie
    PERCORSO_FILE_FAGLIE_GML = "ITHACAFaults/ITHACAFaults.gml"  # Percorso file GML faglie
    NOME_COLONNA_FAGLIA = "name"  # Nome colonna con il nome della faglia nel GML

    # Step 1: pulizia per Italia
    pulisci_per_italia(INPUT_CSV, OUTPUT_CSV_ITALY)

    # Step 2: arricchimento faglie
    df_italia = pd.read_csv(OUTPUT_CSV_ITALY)
    df_italia_arricchito = arricchisci_faglie_italia(df_italia, PERCORSO_FILE_FAGLIE_GML, NOME_COLONNA_FAGLIA)
    df_italia_arricchito.to_csv(OUTPUT_CSV_ARRICCHITO, index=False)
    print(f"Salvato file arricchito: {OUTPUT_CSV_ARRICCHITO}")

    # Elimina il file temporaneo OUTPUT_CSV_ITALY
    if os.path.exists(OUTPUT_CSV_ITALY):
        os.remove(OUTPUT_CSV_ITALY)
        print(f"File temporaneo {OUTPUT_CSV_ITALY} eliminato.")
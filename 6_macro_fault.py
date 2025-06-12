import pandas as pd # type: ignore
import numpy as np # Per gestire eventuali valori mancanti (NaN)

# --- Configurazione Utente ---
CSV_INPUT_PATH = '5_swarm_details_fault_location.csv'  # <<< MODIFICA QUESTO
CSV_OUTPUT_PATH = '6_macro_fault.csv' # <<< MODIFICA QUESTO (opzionale)

# Nomi delle colonne nel tuo CSV (adattali se necessario!)
COL_MS_FAULT = 'MS_Fault'
COL_MS_LAT = 'MS_Lat'
COL_CS_FAULT = 'CS_Fault'
COL_CS_LAT = 'CS_Lat'

# SOGLIE DI LATITUDINE APPROSSIMATIVE PER NORD/CENTRO/SUD ITALIA
# !!! ATTENZIONE: Queste sono soglie indicative! Adattale alle tue necessità specifiche
#                 o a definizioni geografiche più precise per la tua analisi.
LAT_MIN_NORD = 44.0  # Latitudine minima per considerare una località "Nord"
LAT_MIN_CENTRO = 41.5 # Latitudine minima per considerare una località "Centro"
                     # Tutto ciò che è sotto LAT_MIN_CENTRO sarà "Sud"

# OPZIONE: Vuoi creare nuove colonne per le macro-regioni (True)
# o sovrascrivere le colonne MS_Fault/CS_Fault esistenti (False)?
# True è più sicuro per non perdere i nomi originali delle faglie.
CREA_NUOVE_COLONNE_MACROREGIONE = True
NUOVA_COL_MS_MACRO_FAGLIA = 'MS_Macro_Fault' # Nome se CREA_NUOVE_COLONNE_MACROREGIONE = True
NUOVA_COL_CS_MACRO_FAGLIA = 'CS_Macro_Fault' # Nome se CREA_NUOVE_COLONNE_MACROREGIONE = True
# --- Fine Configurazione Utente ---

def assegna_macro_regione(latitudine):
    """
    Assegna una macro-regione ('Nord', 'Centro', 'Sud') basata sulla latitudine.
    Restituisce 'Sconosciuto' se la latitudine non è valida.
    """
    if pd.isna(latitudine):
        return 'Sconosciuto'
    try:
        lat = float(latitudine)
        if lat >= LAT_MIN_NORD:
            return 'Nord'
        elif lat >= LAT_MIN_CENTRO:
            return 'Centro'
        else:
            return 'Sud'
    except ValueError:
        return 'Sconosciuto' # Se la conversione a float fallisce

def main():
    print(f"Caricamento dati da: {CSV_INPUT_PATH}")
    try:
        df = pd.read_csv(CSV_INPUT_PATH)
    except FileNotFoundError:
        print(f"Errore: File non trovato '{CSV_INPUT_PATH}'")
        return
    except Exception as e:
        print(f"Errore durante il caricamento del CSV: {e}")
        return

    # Verifica la presenza delle colonne di latitudine necessarie
    colonne_lat_necessarie = [COL_MS_LAT, COL_CS_LAT]
    for col in colonne_lat_necessarie:
        if col not in df.columns:
            print(f"Errore: La colonna di latitudine '{col}' specificata non è presente nel CSV.")
            print("Controlla i nomi delle colonne in 'COL_MS_LAT' e 'COL_CS_LAT' nello script.")
            return

    print("Assegnazione macro-regioni basate sulla latitudine...")

    # Applica la funzione per creare le stringhe di macro-regione
    ms_macro_regioni = df[COL_MS_LAT].apply(assegna_macro_regione)
    cs_macro_regioni = df[COL_CS_LAT].apply(assegna_macro_regione)

    if CREA_NUOVE_COLONNE_MACROREGIONE:
        print(f"Creazione nuove colonne '{NUOVA_COL_MS_MACRO_FAGLIA}' e '{NUOVA_COL_CS_MACRO_FAGLIA}'.")
        df[NUOVA_COL_MS_MACRO_FAGLIA] = ms_macro_regioni
        df[NUOVA_COL_CS_MACRO_FAGLIA] = cs_macro_regioni
    else:
        print(f"Modifica delle colonne esistenti '{COL_MS_FAULT}' e '{COL_CS_FAULT}'.")
        # Verifica la presenza delle colonne delle faglie prima di modificarle
        colonne_faglie_necessarie = [COL_MS_FAULT, COL_CS_FAULT]
        for col in colonne_faglie_necessarie:
            if col not in df.columns:
                print(f"Attenzione: La colonna faglia '{col}' da modificare non è presente nel CSV.")
                print(f"Verrà creata la colonna '{col}' con i valori della macro-regione.")
        df[COL_MS_FAULT] = ms_macro_regioni
        df[COL_CS_FAULT] = cs_macro_regioni

    print(f"Salvataggio dati modificati in: {CSV_OUTPUT_PATH}")
    try:
        df.to_csv(CSV_OUTPUT_PATH, index=False)
        print("Operazione completata con successo!")
        if CREA_NUOVE_COLONNE_MACROREGIONE:
            print(f"Le macro-regioni sono state salvate nelle colonne '{NUOVA_COL_MS_MACRO_FAGLIA}' e '{NUOVA_COL_CS_MACRO_FAGLIA}'.")
            print(f"I nomi originali delle faglie in '{COL_MS_FAULT}' e '{COL_CS_FAULT}' sono stati conservati.")
        else:
            print(f"I nomi delle faglie in '{COL_MS_FAULT}' e '{COL_CS_FAULT}' sono stati sostituiti con le macro-regioni.")
            print("Si consiglia di avere un backup del file originale.")

    except Exception as e:
        print(f"Errore durante il salvataggio del CSV: {e}")

if __name__ == '__main__':
    main()
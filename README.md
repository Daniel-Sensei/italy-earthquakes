# A Dimensional Model for the Analysis of Seismic Events in Italy

## Core Workflow

* **Data Acquisition & Cleaning**: Fetches and filters raw data from the USGS FDSN web service and assesses data quality.
* **Data Integration**: Enriches records with geographic information from Natural Earth and geological fault data from the ITHACA database.
* **Swarm Analysis**: Builds a materialized view to efficiently query for seismic swarms preceding major earthquakes.

## Quickstart

1.  **Install dependencies**:
    ```bash
    pip install pandas geopandas shapely
    ```
2.  **Run scripts**: Execute the Python scripts in numerical order.

## Author

* **Daniel Curcio** - [Daniel-Sensei](https://github.com/Daniel-Sensei)
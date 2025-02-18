import os
import sys

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_geonames_data
from src.pipeline.transform_filters import filter_populated_places
from src.pipeline.enrich_boundaries import enrich_boundaries

def main():

    # Country code according to the Geonames file list (https://download.geonames.org/export/dump/)
    country_code = "PL"

    # Geoapify API key (free) for optional step of removing locations nested in city borders
    API_KEY = 'YOUR GEOAPIFY API KEY'
    
    enriched_csv = os.path.join(base_dir, "data", "processed", f"boundry_enriched_{country_code}.csv")
    
    # 1) Download and load with country code
    df = load_geonames_data(country_code)
    print(f"Loaded {len(df)} rows from Geonames.")

    # 2) Filter for populated places / certain feature_codes
    df_filtered = filter_populated_places(df)
    print(f"Filtering for populated places, {len(df_filtered)} rows remain.")
    
    # 2.1) Save filtered data to CSV
    filtered_csv = os.path.join(base_dir, "data", "processed", f"filtered_{country_code}.csv")
    df_filtered.to_csv(filtered_csv, index=False)
    print(f"Saved filtered results to {filtered_csv}")

    # 3) OPTIONAL Enrich selected cities with borders to remove nested locations
    df_enriched = enrich_boundaries(API_KEY, df_filtered, country_code)
    
    # 3.1) Save enriched data to CSV
    filtered_csv = os.path.join(base_dir, "data", "processed", f"enriched_boundaries_{country_code}.csv")
    df_enriched.to_csv(enriched_csv, index=False)
    print(f"Saved enriched boundaries to {enriched_csv}")
        

if __name__ == "__main__":
    import pandas as pd
    main()

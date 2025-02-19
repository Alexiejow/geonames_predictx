import os
import sys

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_geonames_data
from src.pipeline.data_loader import load_csv, save_csv
from src.pipeline.transform_filters import filter_populated_places
from src.pipeline.enrich_boundaries import enrich_boundaries
from src.pipeline.transform_remove_nested import remove_nested_points
from src.pipeline.metropolis_assignment import assign_metros

def main():

    # Country code according to the Geonames file list (https://download.geonames.org/export/dump/)
    country_code = "PL"

    # Geoapify API key (free) for optional step of removing locations nested in city borders
    API_KEY = 'YOUR GEOAPIFY API KEY'
    
    enriched_csv = os.path.join(base_dir, "data", "processed", f"boundary_enriched_{country_code}.csv")
    
    # If dataframe enriched with city borders is already present
    if os.path.exists(enriched_csv):
        print(f"Loading enriched boundaries from {enriched_csv}")
        df_enriched = load_csv(enriched_csv)
    
    # If need to download data and enrich with borders
    else:
        # 1) Download and load with country code
        df = load_geonames_data(country_code)
        print(f"Loaded {len(df)} rows from Geonames.")

        # 2) Filter for populated places / certain feature_codes
        df_filtered = filter_populated_places(df)
        print(f"Filtering for populated places, {len(df_filtered)} rows remain.")
        
        # 2.1) Save filtered data to CSV
        filtered_csv = os.path.join(base_dir, "data", "processed", f"filtered_{country_code}.csv")
        save_csv(df_filtered, filtered_csv)
        print(f"Saved filtered results to {filtered_csv}")

        # 3) OPTIONAL Enrich selected cities with borders to remove nested locations
        df_enriched = enrich_boundaries(API_KEY, df_filtered, country_code)
        
        # 3.1) Save enriched data to CSV
        save_csv(df_enriched, enriched_csv)
        print(f"Saved enriched boundaries to {enriched_csv}")

    no_nested_csv = os.path.join(base_dir, "data", "processed", f"no_nested_{country_code}.csv")

    # If dataframe with no nested locations is present
    if os.path.exists(no_nested_csv):
        print(f"Loading with no nested from {no_nested_csv}")
        df_no_nested = load_csv(no_nested_csv)

    else:
        # 3.2) Remove locations within borders of selected locations
        print("Removing locations within borders")
        df_no_nested = remove_nested_points(df_enriched)

        # 3.3) Save dataframe without nested locations
        no_nested_csv = os.path.join(base_dir, "data", "processed", f"no_nested_{country_code}.csv")
        save_csv(df_no_nested, no_nested_csv)
        print(f"Saved results without nested locations to {no_nested_csv}")
        
    # 4) Run metro assignment
    print("Running metro assignment")
    df_metro_assignment = assign_metros(df_no_nested)
    
    # 4.1) Save assigned data to CSV
    output_csv = os.path.join(base_dir, "data", "processed", f"metros_assigned_{country_code}.csv")
    save_csv(df_metro_assignment, output_csv)
    print(f"Saved assigned metros {output_csv}")

if __name__ == "__main__":
    import pandas as pd
    main()

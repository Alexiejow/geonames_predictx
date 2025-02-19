import os
import sys
from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_geonames_data
from src.pipeline.data_loader import load_csv, save_csv
from src.pipeline.transform_filters import filter_populated_places
# from src.pipeline.enrich_boundaries import enrich_boundaries
# from src.pipeline.transform_remove_nested import remove_nested_points
from src.pipeline.metropolis_assignment import assign_metros

def main():

    ############### CONFIGURATION #################

    # Country code according to the Geonames file list (https://download.geonames.org/export/dump/)
    country_code = "PL"

    # Geoapify API key (free) for optional step of removing locations nested in city borders
    load_dotenv()  # Load environment variables from .env
    API_KEY = os.getenv("GEOAPIFY_API_KEY") # Remove this if you prefer to enter API key below
    #API_KEY = "YOUR API KEY"
    
     ##############################################

    enriched_csv = os.path.join(base_dir, "data", "processed", f"boundary_enriched_{country_code}.csv")
    
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
        
    # 3) Run metro assignment
    print("Running metro assignment")
    df_metro_assignment = assign_metros(df_filtered)
    
    # 3.1) Save assigned data to CSV
    output_csv = os.path.join(base_dir, "data", "processed", f"metros_assigned_{country_code}.csv")
    save_csv(df_metro_assignment, output_csv)
    print(f"Saved assigned metros {output_csv}")

if __name__ == "__main__":
    import pandas as pd
    main()

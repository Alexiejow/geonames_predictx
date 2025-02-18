import os
import sys

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_geonames_data
from src.pipeline.transform_filters import filter_populated_places

def main():
    country_code = "PL"
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
        

if __name__ == "__main__":
    import pandas as pd
    main()

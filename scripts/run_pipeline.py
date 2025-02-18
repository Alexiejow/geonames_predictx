import os
import sys

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_geonames_data

def main():
    country_code = "PL"
    enriched_csv = os.path.join(base_dir, "data", "processed", f"boundry_enriched_{country_code}.csv")

    # 1) Download and load with country code
    df = load_geonames_data(country_code)
    print(f"Loaded {len(df)} rows from Geonames.")
        

if __name__ == "__main__":
    import pandas as pd
    main()

import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Set up the base directory and ensure it's on the Python path.
base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline_spark.data_loader import load_geonames_data, load_csv, save_csv, save_csv_single_file
from src.pipeline_spark.transform_filters import filter_populated_places
# from src.pipeline_spark.enrich_boundaries import enrich_boundaries_spark
# from src.pipeline_spark.transform_remove_nested import remove_nested_points_spark
from src.pipeline_spark.metropolis_assignment import assign_metros

def main():
    ############### CONFIGURATION #################
    # Country code according to the Geonames file list.
    country_code = "PL"
    
    # Load environment variables (e.g., Geoapify API key) from .env
    load_dotenv()  
    API_KEY = os.getenv("GEOAPIFY_API_KEY")  # Use as needed in your Spark modules
    
    ###############################################
    
    # Initialize Spark in local mode (for development and testing)
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GeonamesPredictX_Spark") \
            .getOrCreate()
    
    # 1) Download and load data for the specified country (Spark version)
    df = load_geonames_data(spark, country_code)
    print(f"Loaded {df.count()} rows from Geonames.")
    
    # 2) Filter for populated places using Spark transformations.
    df_filtered = filter_populated_places(df)
    print(f"After filtering, {df_filtered.count()} rows remain.")
    
    # 3) Run metro assignment using Spark.
    # (Optionally, if you later implement enrichment or removal steps, include them here.)
    print("Running metro assignment")
    df_metro_assigned = assign_metros(df_filtered)
    
    # 3.1) Save the metro-assigned data to CSV.
    output_csv = os.path.join(base_dir, "data", "processed", f"metros_assigned_{country_code}.csv")
    # save_csv(df_metro_assignment, output_csv)
    save_csv_single_file(df_metro_assigned, output_csv)
    print(f"Saved assigned metros to {output_csv}")
    
    # Stop Spark session.
    spark.stop()

if __name__ == "__main__":
    main()

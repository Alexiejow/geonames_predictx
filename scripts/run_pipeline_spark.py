import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Set up the base directory and ensure it's on the Python path.
base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

# Import your Spark modules.
from src.pipeline_spark.data_loader import (
    load_geonames_data,
    load_allcountries_data,
    save_csv_partition_countries,
    save_csv_single_file
)
from src.pipeline_spark.transform_filters import filter_populated_places
from src.pipeline_spark.metropolis_assignment import assign_metros

def main():
    ############### CONFIGURATION #################
    # Use a variable "DATASET" to decide which mode to run.
    # For a specific country, set DATASET to its country code (e.g., "PL").
    # For processing all countries, set DATASET="allCountries".
    dataset = "IN"  # default to "PL" if not set
    load_dotenv()  
    API_KEY = os.getenv("GEOAPIFY_API_KEY")  # if needed in further processing
    
    ###############################################
    
    # Initialize Spark in local mode (for development and testing).
    # Increased memory of driver and executor because of errors (also for dev and testing)
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("GeoNamesPredictX_Spark") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    # 1) Download and load data.
    if dataset == "allCountries":
        print("Running for allCountries")
        df = load_allcountries_data(spark)
        # Repartition by "country_code" so that data for each country is processed in parallel.
        df = df.repartition("country_code")
    else:
        print(f"Running for country code: {dataset}")
        df = load_geonames_data(spark, dataset)
    
    print(f"Loaded {df.count()} rows from GeoNames.")
    
    # 2) Filter for populated places using Spark transformations.
    df_filtered = filter_populated_places(df)
    print(f"After filtering, {df_filtered.count()} rows remain.")
    
    # 3) Run metro assignment using Spark.
    print("Running metro assignment")
    if dataset == "allCountries":
        df_metro_assigned = assign_metros(df_filtered, all_countries=True)
    else:
        df_metro_assigned = assign_metros(df_filtered)
    
    # 4) Save the output.
    if dataset == "allCountries":
        # This will save the data partitioned by "country_code" (i.e., separate folders for each country).
        output_path = os.path.join(base_dir, "data", "processed", "metros_assigned_allCountries.csv")
        save_csv_partition_countries(df_metro_assigned, output_path)
        print(f"Saved assigned metros (partitioned by country) to {output_path}")
    else:
        output_path = os.path.join(base_dir, "data", "processed", f"metros_assigned_{dataset}.csv")
        # save_csv_single_file(df_metro_assigned, output_path)
        save_csv_partition_countries(df_metro_assigned, output_path)
        print(f"Saved assigned metros to {output_path}")
    
    # Stop the Spark session.
    spark.stop()

if __name__ == "__main__":
    main()

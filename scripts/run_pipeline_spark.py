import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Set up the base directory and ensure it's on the Python path.
base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

# Import your Spark modules.
from src.pipeline_spark.data_loader import (
    load_geonames_data,
    load_allcountries_data,
    save_csv,
    save_csv_partition_countries,
    save_csv_single_file
)
from src.pipeline_spark.transform_filters import filter_populated_places, get_boundary_mask
from src.pipeline_spark.metropolis_assignment import assign_metros
from src.pipeline_spark.metropolis_assignment_iterative import assign_metros_iterative

def main():
    ############### CONFIGURATION #################

    # Use a variable "DATASET" to decide which mode to run.
    # For a specific country, set DATASET to its country code (e.g., "PL").
    # For processing all countries, set DATASET="allCountries".
    dataset = "DE"  # default to "PL" if not set
    load_dotenv()  
    API_KEY = os.getenv("GEOAPIFY_API_KEY")  # if needed in further processing
    
    ###############################################
    
    # Initialize Spark in local mode (for development and testing).
    # Increased memory of driver and executor because of errors
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
        # Repartition by "country_code" so that multiple countries can be processed in parallel.
        df = df.repartition("country_code")
    else:
        print(f"Running for country code: {dataset}")
        df = load_geonames_data(spark, dataset)
    
    print(f"Loaded {df.count()} rows from GeoNames.")
    
    # 2) Filter for populated places using Spark transformations.
    df_filtered = filter_populated_places(df)
    print(f"After filtering, {df_filtered.count()} rows remain.")
    
    # 2.5) Identify metros and non-metros
    mask = get_boundary_mask(df_filtered)
    df_split = df_filtered.withColumn("isMetro", F.when(mask, True).otherwise(False))

    # 3) Run metro assignment using Spark.
    print("Running metro assignment")
    df_metro_assigned = assign_metros(df_split, all_countries=(dataset=="allCountries"))

    # 4) Save the output.

    if dataset == "allCountries":
        # This will save the data partitioned by "country_code" (i.e., separate folders for each country).
        output_path = os.path.join(base_dir, "data", "processed")
        save_csv_partition_countries(df_metro_assigned, output_path)
        print(f"Saved assigned metros (partitioned by country) to {output_path}")
    else:
        output_path = os.path.join(base_dir, "data", "processed", "country_code="+dataset)
        save_csv(df_metro_assigned, output_path)
        print(f"Saved assigned metros to {output_path}")
    
    # Stop the Spark session.
    spark.stop()

if __name__ == "__main__":
    main()

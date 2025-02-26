import os
import io
import requests
import zipfile
import math
import numpy as np
import pandas as pd
import shutil
from tqdm import tqdm 

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType


GEONAMES_SCHEMA = StructType([
    StructField("geonameid", IntegerType(), True),         # e.g., 1163293
    StructField("name", StringType(), True),                 # e.g., "Tithwāl"
    StructField("asciiname", StringType(), True),            # e.g., "Tithwal"
    StructField("alternatenames", StringType(), True),       # e.g., "" (or a comma-separated list)
    StructField("latitude", FloatType(), True),              # e.g., 34.39351
    StructField("longitude", FloatType(), True),             # e.g., 73.77416
    StructField("feature_class", StringType(), True),        # e.g., "P"
    StructField("feature_code", StringType(), True),         # e.g., "PPL"
    StructField("country_code", StringType(), True),         # e.g., "IN"
    StructField("cc2", StringType(), True),                  # e.g., "" (can be empty)
    StructField("admin1_code", StringType(), True),          # e.g., "12"
    StructField("admin2_code", StringType(), True),          # e.g., "001"
    StructField("admin3_code", StringType(), True),          # e.g., "3"
    StructField("admin4_code", StringType(), True),          # e.g., "" (or possibly null)
    StructField("population", IntegerType(), True),          # e.g., 0 (or a positive integer)
    StructField("elevation", StringType(), True),            # Elevation might be missing or non-numeric; if it's always numeric, you could use IntegerType or FloatType
    StructField("dem", IntegerType(), True),                 # e.g., 1080
    StructField("timezone", StringType(), True),             # e.g., "Asia/Kolkata"
    StructField("modification_date", DateType(), True)       # ISO format (e.g., "2024-01-06") can be parsed into DateType
])

def load_allcountries_data(spark: SparkSession) -> DataFrame:
    """
    Downloads the allCountries.zip file from GeoNames, unzips it in memory,
    writes the contained allCountries.txt to a temporary file, and loads it
    into a Spark DataFrame with the proper schema.
    
    A progress bar (via tqdm) is shown during download.
    """
    url = "https://download.geonames.org/export/dump/allCountries.zip"
    print(f"Downloading {url} ...")
    
    # Stream download with progress bar.
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024
    tmp_bytes = io.BytesIO()
    with tqdm(total=total_size, unit='iB', unit_scale=True) as t:
        for data in response.iter_content(block_size):
            t.update(len(data))
            tmp_bytes.write(data)
    tmp_bytes.seek(0)
    
    # Open the ZIP file in memory and read the allCountries.txt file.
    with zipfile.ZipFile(tmp_bytes) as zf:
        txt_filename = "allCountries.txt"
        with zf.open(txt_filename) as txt_file:
            file_bytes = txt_file.read()
    
    # Write the contents to a temporary file.
    tmp_file_path = f"/tmp/{txt_filename}"
    with open(tmp_file_path, "wb") as f:
        f.write(file_bytes)
    
    # Load the temporary file into a Spark DataFrame using the defined schema.
    df = spark.read.csv(
        tmp_file_path,
        sep="\t",
        header=False,
        schema=GEONAMES_SCHEMA
    )
    
    # Optionally, delete the temporary file (uncomment the next line if desired)
    # os.remove(tmp_file_path)
    
    print(f"Loaded GeoNames data for allCountries into Spark DataFrame with {df.count()} rows.")
    return df



def load_geonames_data(spark: SparkSession, country_code: str):
    """
    Downloads the ZIP file for the specified country code from GeoNames,
    unzips it in memory, writes the contained .txt file to a temporary file, 
    and loads it into a Spark DataFrame with the proper schema.
    
    Example usage:
        df = load_geonames_data_spark(spark, "PL")
    
    Args:
        spark (SparkSession): The active Spark session.
        country_code (str): The country code (e.g., "PL").
        
    Returns:
        Spark DataFrame: A DataFrame with the GeoNames data.
    """
    # Build the download URL.
    url = f"https://download.geonames.org/export/dump/{country_code}.zip"
    print(f"Downloading {url} ...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if download fails
    
    # Open the ZIP file in memory.
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        txt_filename = f"{country_code}.txt"
        with zf.open(txt_filename) as txt_file:
            file_bytes = txt_file.read()
    
    # Write the content to a temporary file.
    tmp_file_path = f"/tmp/{country_code}.txt"
    with open(tmp_file_path, "wb") as f:
        f.write(file_bytes)
    
    # Read the temporary file into a Spark DataFrame using the defined schema.
    df = spark.read.csv(
        tmp_file_path,
        sep="\t",
        header=False,
        schema=GEONAMES_SCHEMA
    )
    
    # Optionally, delete the temporary file after reading.
    # os.remove(tmp_file_path)
    print(f"Loaded GeoNames data for {country_code} into Spark DataFrame with {df.count()} rows.")
    return df

def load_csv(spark: SparkSession, filepath: str, schema: StructType = None):
    """
    Loads a CSV file into a Spark DataFrame.
    
    If a schema is provided, it will be used to enforce data types.
    """
    reader = spark.read.option("header", "true")
    if schema:
        reader = reader.schema(schema)
    return reader.csv(filepath)

import os
import pandas as pd
import glob

def load_partitioned_csv(directory_path: str) -> pd.DataFrame:
    """
    Loads partitioned CSV data written by Spark from a directory into a Pandas DataFrame.

    - Reads all `part-xxxxx.csv` files inside the directory.
    - Concatenates them into a single Pandas DataFrame.

    Parameters:
    - directory_path (str): Path to the partitioned CSV directory.

    Returns:
    - Pandas DataFrame
    """
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"🚨 Directory not found: {directory_path}")

    # Find all part files in the partitioned Spark output directory
    csv_files = glob.glob(os.path.join(directory_path, "part-*.csv"))

    if not csv_files:
        raise ValueError(f"🚨 No partitioned CSV files found in: {directory_path}")

    print(f"📂 Loading partitioned CSV files from: {directory_path}")

    # Load all part CSVs into Pandas and concatenate
    df_list = [pd.read_csv(file) for file in csv_files]
    df = pd.concat(df_list, ignore_index=True)

    print(f"✅ Loaded {len(df)} rows from partitioned output in {directory_path}")

    return df


def save_csv(df, filepath: str):
    """
    Saves a Spark DataFrame as a CSV file to the specified filepath.
    Note: Spark writes out as a folder containing part files.
    """
    # Write as a folder of part files.
    df.write.option("header", "true").mode("overwrite").csv(filepath)
    print(f"✅ Data saved to folder {filepath}")

def save_csv_partition_countries(df, filepath: str):
    df.write \
        .option("header", "true") \
        .mode("overwrite") \
        .partitionBy("country_code") \
        .csv(filepath)
    print(f"✅ Data saved to {filepath}")

def save_csv_single_file(df: DataFrame, output_path: str) -> None:
    """
    Writes `df` as a single CSV file to `output_path`.
    Internally, Spark will write a folder. This function then
    renames the single part file to the desired output path.
    
    If output_path already exists, it will be overwritten.
    """
    # 1) Write to a temporary folder (Spark will create part files here).
    tmp_folder = output_path + "_tmp"
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmp_folder)
    
    # 2) Find the single part file in the temporary folder.
    part_file = None
    for filename in os.listdir(tmp_folder):
        if filename.startswith("part-") and filename.endswith(".csv"):
            part_file = filename
            break
    
    if not part_file:
        raise FileNotFoundError("No part-xxxxx.csv file found in Spark output folder.")
    
    # 3) Ensure the parent directory of output_path exists.
    parent_dir = os.path.dirname(output_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    
    # 4) Move the part file to the final output path (rename the file to a single CSV).
    part_file_path = os.path.join(tmp_folder, part_file)
    
    # If there's already a file at output_path, remove it.
    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)
    
    shutil.move(part_file_path, output_path)
    
    # 5) Clean up the temporary folder.
    shutil.rmtree(tmp_folder)
    
    print(f"✅ Single-file CSV saved to {output_path}")

# For testing or standalone execution:
if __name__ == "__main__":
    # Create a Spark session in local mode.
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GeonamesPredictX_Spark_DataLoader") \
            .getOrCreate()
    
    # Example: Load data for Poland ("PL")
    country_code = "PL"
    df = load_geonames_data(spark, country_code)
    
    # Optionally, show the schema and a few rows.
    df.printSchema()
    df.show(5, truncate=False)
    
    # Stop Spark session.
    spark.stop()

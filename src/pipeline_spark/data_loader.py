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

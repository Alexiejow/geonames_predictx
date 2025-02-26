import math
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def debug_partition(index, iterator):
    records = list(iterator)
    if records:
        countries = {row["country_code"] for row in records if "country_code" in row}
        print(f"Partition {index}: {len(records)} records, countries: {countries}")
    for record in records:
        yield record

def assign_metros(df: DataFrame, all_countries=False) -> DataFrame:
    """
    Fully distributed Spark implementation of metro assignment.
    
    This function assumes:
      - The input Spark DataFrame `df` has columns "geonameid", "name", "latitude",
        "longitude", "population", and "feature_code".
      - A Spark version of get_boundary_mask (from your transform_filters module) is used
        to split the DataFrame into metros and non-metros.
    
    The function does the following:
      1. Splits the DataFrame using a Spark boolean mask.
      2. Casts the necessary columns.
      3. Computes an influence radius for each metro using a UDF.
      4. Cross joins non-metro rows with metros (broadcasting the metros DataFrame),
         computes the haversine distance and a force value for each pair.
      5. Uses a window function to select the metro with the highest force for each non-metro.
      6. Joins these best matches back to the non-metro DataFrame, assigns their metro,
         and unions with the metros (which assign themselves).
    
    Returns a Spark DataFrame with an extra column "assigned_metro_id" for non-metro rows.
    """

    from src.pipeline_spark.transform_filters import get_boundary_mask

    # 1. **Split DataFrame into metros and non-metros**
    metros_df = df.filter(F.col("isMetro") == True)
    non_metros_df = df.filter(F.col("isMetro") == False)

    print(f"📊 Start: Total Rows: {df.count()} | Metros: {metros_df.count()} | Non-Metros: {non_metros_df.count()}")

    # # **Debug: Partition details**
    # non_metros_debug_rdd = non_metros_df.rdd.mapPartitionsWithIndex(debug_partition)
    # metros_debug_rdd = metros_df.rdd.mapPartitionsWithIndex(debug_partition)
    # non_metros_debug_rdd.take(1)
    # metros_debug_rdd.take(1)

    # non_metros_df = df.sparkSession.createDataFrame(non_metros_debug_rdd, schema=non_metros_df.schema)
    # metros_df = df.sparkSession.createDataFrame(metros_debug_rdd, schema=metros_df.schema)

    # 2. **Cast necessary columns**
    metros_df = metros_df.withColumn("population", F.col("population").cast("int"))
    non_metros_df = non_metros_df.withColumn("latitude", F.col("latitude").cast("float")) \
                                 .withColumn("longitude", F.col("longitude").cast("float"))

    # 3. **Define UDFs**
    def haversine_distance(lat1, lon1, lat2, lon2):
        R = 6371.0  # Earth radius in km
        phi1, phi2 = map(math.radians, [lat1, lat2])
        dphi, dlambda = map(math.radians, [lat2 - lat1, lon2 - lon1])
        a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    haversine_udf = F.udf(haversine_distance, "float")

    def compute_radius(population):
        METRO_CITY_POPULATION_CONSTANT = -1 / 1443000
        MIN_RADIUS, MAX_RADIUS = 10, 90
        return float(MIN_RADIUS + MAX_RADIUS * (1 - math.exp(METRO_CITY_POPULATION_CONSTANT * population)))

    radius_udf = F.udf(compute_radius, "float")

    def compute_force(distance, radius):
        return float(math.exp(-1.4 * distance / radius))

    force_udf = F.udf(compute_force, "float")

    # 4. **Compute influence radius for metros**
    metros_df = metros_df.withColumn("influence_radius", radius_udf(F.col("population")))

    # 5. **Compute bounding box limits**
    metros_df = metros_df.withColumn("lat_min", F.col("latitude") - (F.col("influence_radius") / 111.0))
    metros_df = metros_df.withColumn("lat_max", F.col("latitude") + (F.col("influence_radius") / 111.0))
    metros_df = metros_df.withColumn("lon_min", F.col("longitude") - (F.col("influence_radius") / (111.0 * F.cos(F.radians(F.col("latitude"))))))
    metros_df = metros_df.withColumn("lon_max", F.col("longitude") + (F.col("influence_radius") / (111.0 * F.cos(F.radians(F.col("latitude"))))))

    # 6. **Join non-metros with metros using bounding box filter**
    if all_countries:
        # Join **without** country restriction
        joined = non_metros_df.alias("n").join(
            metros_df.alias("m"),
            (F.col("n.latitude") >= F.col("m.lat_min")) &
            (F.col("n.latitude") <= F.col("m.lat_max")) &
            (F.col("n.longitude") >= F.col("m.lon_min")) &
            (F.col("n.longitude") <= F.col("m.lon_max")),
            "inner"
        )
    else:
        # Join **with** country restriction
        joined = non_metros_df.alias("n").join(
            metros_df.alias("m"),
            (F.col("n.latitude") >= F.col("m.lat_min")) &
            (F.col("n.latitude") <= F.col("m.lat_max")) &
            (F.col("n.longitude") >= F.col("m.lon_min")) &
            (F.col("n.longitude") <= F.col("m.lon_max")) &
            (F.col("n.country_code") == F.col("m.country_code")),
            "inner"
        )

    print(f"📌 Total (non-metro, metro) candidate pairs after bounding box filter: {joined.count()}")

    # 7. **Compute distances & filter**
    joined = joined.withColumn("distance", haversine_udf(F.col("n.latitude"), F.col("n.longitude"),
                                                          F.col("m.latitude"), F.col("m.longitude")))

    joined = joined.filter(F.col("distance") < F.col("m.influence_radius"))

    print(f"📌 Total pairs after influence radius filter: {joined.count()}")

    # 8. **Compute influence force**
    joined = joined.withColumn("force", force_udf(F.col("distance"), F.col("m.influence_radius")))

    # 9. **Select the strongest metro for each non-metro**
    windowSpec = Window.partitionBy("n.geonameid").orderBy(F.col("force").desc())
    best_matches = joined.withColumn("rank", F.row_number().over(windowSpec)) \
                         .filter(F.col("rank") == 1) \
                         .select(
                             F.col("n.geonameid"),
                             F.col("m.geonameid").alias("assigned_metro_id"),
                         )

    assigned_count = best_matches.count()
    print(f"✅ Total non-metros assigned: {assigned_count}")

    # 10. **Assign metros to themselves**
    metros_df = metros_df.withColumn("assigned_metro_id", F.col("geonameid"))

    # 11. **Merge all data back**
    non_metros_df = non_metros_df.join(best_matches, "geonameid", "left")
    final_df = metros_df.unionByName(non_metros_df, allowMissingColumns=True)

    print(f"✅ Final dataset size: {final_df.count()}")
    return final_df


# Example usage:
if __name__ == "__main__":
    import os
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]").appName("GeoNamesPredictX_Spark").getOrCreate()
    
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    data_path = os.path.join(BASE_DIR, "..", "data", "processed", "filtered_PL.csv")
    
    # Load CSV as Spark DataFrame. (Adjust options/schema as needed.)
    df = spark.read.option("header", "true").csv(data_path)
    df = df.withColumn("latitude", F.col("latitude").cast("float")) \
           .withColumn("longitude", F.col("longitude").cast("float")) \
           .withColumn("population", F.col("population").cast("int"))
    
    final_df = assign_metros(df)
    
    output_path = os.path.join(BASE_DIR, "data", "processed")
    # Save as a single CSV file; note Spark writes a folder with part files.
    final_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
    print(f"Unified CSV saved as '{output_path}'")
    
    spark.stop()

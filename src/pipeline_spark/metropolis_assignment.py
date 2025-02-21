import math
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def assign_metros(df: DataFrame) -> DataFrame:
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
    # Import the Spark version of the boundary mask from your transform_filters module.
    # Ensure that module is Spark-friendly.
    from src.pipeline_spark.transform_filters import get_boundary_mask

    # 1. Split DataFrame into metros and non-metros using the boundary mask.
    mask = get_boundary_mask(df)  # mask is a Spark Column (boolean)
    metros_df = df.filter(mask)
    non_metros_df = df.filter(~mask)
    
    print("Total rows:", df.count())
    print("Metros:", metros_df.count())
    print("Non-metros:", non_metros_df.count())
    
    # 2. Cast necessary columns.
    metros_df = metros_df.withColumn("population", F.col("population").cast("int"))
    non_metros_df = non_metros_df.withColumn("latitude", F.col("latitude").cast("float")) \
                                 .withColumn("longitude", F.col("longitude").cast("float"))
    
    # 3. Define UDFs (defined here, after the SparkSession is active).
    
    # UDF for haversine distance.
    def haversine_distance_udf(lat1, lon1, lat2, lon2):
        R = 6371.0  # km
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return float(R * c)
    
    haversine_udf = F.udf(haversine_distance_udf, "float")
    
    # UDF for influence radius based on population.
    def radius_udf(pop):
        METRO_CITY_POPULATION_CONSTANT = -1 / 1443000
        MIN_METRO_CITY_RADIUS = 10
        MAX_METRO_CITY_RADIUS = 90
        return float(MIN_METRO_CITY_RADIUS + MAX_METRO_CITY_RADIUS * (1 - math.exp(METRO_CITY_POPULATION_CONSTANT * pop)))
    
    radius_udf_func = F.udf(radius_udf, "float")
    
    # UDF for force calculation.
    def force_udf(distance, influence_radius):
        return float(math.exp(-1.4 * distance / influence_radius))
    
    force_udf_func = F.udf(force_udf, "float")
    
    # 4. For metros_df, compute the influence radius.
    metros_df = metros_df.withColumn("influence_radius", radius_udf_func(F.col("population")))
    
    # 5. Create a small DataFrame from metros_df for the join.
    metros_small = metros_df.select(
        F.col("geonameid").alias("metro_id"),
        F.col("latitude").alias("metro_lat"),
        F.col("longitude").alias("metro_lon"),
        F.col("population").alias("metro_population"),
        F.col("name").alias("metro_name"),
        F.col("influence_radius")
    )
    
    # 6. Cross join non_metros_df with metros_small.
    # Broadcasting metros_small if it's relatively small.
    joined = non_metros_df.crossJoin(F.broadcast(metros_small))
    
    # 7. Compute the distance between each non-metro and metro pair.
    joined = joined.withColumn("distance", haversine_udf(F.col("latitude"), F.col("longitude"),
                                                         F.col("metro_lat"), F.col("metro_lon")))
    
    # 8. Keep only pairs where distance is less than the metro's influence radius.
    joined = joined.filter(F.col("distance") < F.col("influence_radius"))
    
    # 9. Compute the force for each pair.
    joined = joined.withColumn("force", force_udf_func(F.col("distance"), F.col("influence_radius")))
    
    # 10. For each non-metro, select the metro with the highest force using a window function.
    windowSpec = Window.partitionBy("geonameid").orderBy(F.col("force").desc())
    joined = joined.withColumn("rank", F.row_number().over(windowSpec))
    best_matches = joined.filter(F.col("rank") == 1).select("geonameid", "metro_id")
    
    # 11. Join best matches back to non_metros_df.
    non_metros_assigned = non_metros_df.join(best_matches, on="geonameid", how="left") \
                                    .withColumnRenamed("metro_id", "assigned_metro_id")

    
    # 12. For metros_df, assign their own ID as the assigned metro.
    metros_assigned = metros_df.withColumn("assigned_metro_id", F.col("geonameid"))
    
    # 13. Union the two subsets.
    final_df = metros_assigned.unionByName(non_metros_assigned, allowMissingColumns=True)
    final_df = final_df.orderBy("geonameid")
    
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
    
    output_path = os.path.join(BASE_DIR, "data", "processed", "final_with_metropolis_assignment_spark.csv")
    # Save as a single CSV file; note Spark writes a folder with part files.
    final_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
    print(f"Unified CSV saved as '{output_path}'")
    
    spark.stop()

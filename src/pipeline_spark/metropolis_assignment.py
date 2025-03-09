import math
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from sedona.register import SedonaRegistrator
from pyspark.sql.functions import expr


def assign_metros(spark, df: DataFrame, all_countries=False) -> DataFrame:
    """
    Fully distributed Spark implementation of metro assignment using spatial indexing.

    1. Data Preparation
       - Splits the DataFrame into metro and non-metro locations.
       - Casts necessary columns for consistency.
       - Computes an influence radius for each metro using an exponential scaling function.
    
    2. Geospatial Transformation
       - Converts latitude/longitude coordinates into spatial point objects with SRID 4326.
       - Registers DataFrames as temporary views for SQL-based geospatial queries.
    
    3. Distance Calculation & Filtering
       - Uses a SQL query to calculate the distance between all non-metro and metro locations.
       - Converts distances from projected meters to kilometers, adjusting for latitude distortion.
       - Filters out non-metro to metro pairs where the distance exceeds the metro's influence radius.
    
    4. Force Calculation & Assignment
       - Computes an influence force metric for each metro based on distance and influence radius.
       - Assigns each non-metro to the metro with the highest force using a window function.
    
    5. Final Dataset Merging
       - Metro locations retain themselves as their assigned metro.
       - Merges metro and non-metro assignments into a single DataFrame.
       - Drops unnecessary spatial geometry columns before returning.

    Returns:
        A Spark DataFrame with an extra column "assigned_metro_id" indicating 
        the assigned metro for each location.
    """

    # Register Sedona functions
    SedonaRegistrator.registerAll(spark)

    # 1. Split DataFrame into metros and non-metros
    metros_df = df.filter(F.col("isMetro") == True)
    non_metros_df = df.filter(F.col("isMetro") == False)

    print(f"📊 Start: Total Rows: {df.count()} | Metros: {metros_df.count()} | Non-Metros: {non_metros_df.count()}")

    # 2. Cast necessary columns
    metros_df = metros_df.withColumn("population", F.col("population").cast("int"))
    non_metros_df = non_metros_df.withColumn("latitude", F.col("latitude").cast("float")) \
                                 .withColumn("longitude", F.col("longitude").cast("float"))

    # 3. Define UDFs
    def compute_radius(population):
        METRO_CITY_POPULATION_CONSTANT = -1 / 1443000
        MIN_RADIUS = 10
        MAX_RADIUS = 100 - MIN_RADIUS
        return float(MIN_RADIUS + MAX_RADIUS * (1 - math.exp(METRO_CITY_POPULATION_CONSTANT * population)))
    radius_udf = F.udf(compute_radius, "float")

    def compute_force(distance, radius):
        return float(np.exp(-1.4 * distance / radius))
    force_udf = F.udf(compute_force, "float")

    # 4. Compute influence radius for metros
    metros_df = metros_df.withColumn("influence_radius", radius_udf(F.col("population")))

    # 5. Convert lat/lon to spatial points
    metros_df = metros_df.withColumn("geom", expr("ST_SetSRID(ST_Point(longitude, latitude), 4326)"))
    non_metros_df = non_metros_df.withColumn("geom", expr("ST_SetSRID(ST_Point(longitude, latitude), 4326)"))

    # 6. Register DataFrames as temporary views
    metros_df.createOrReplaceTempView("metros")
    non_metros_df.createOrReplaceTempView("non_metros")

    # 7. Run SQL Query for distance calculation and filtering with results in meters
    query = """
        SELECT 
            n.geonameid, 
            m.geonameid, 
            ST_Distance(
                ST_Transform(n.geom, 'epsg:4326', 'epsg:3857'),
                ST_Transform(m.geom, 'epsg:4326', 'epsg:3857')
            ) * COS(RADIANS(n.latitude)) / 1000 AS distance, 
            m.influence_radius
        FROM non_metros n
        JOIN metros m
        ON ST_Distance(
            ST_Transform(n.geom, 'epsg:4326', 'epsg:3857'),
            ST_Transform(m.geom, 'epsg:4326', 'epsg:3857')
        ) * COS(RADIANS(n.latitude)) / 1000 < m.influence_radius
        """
    
    joined = spark.sql(query)

    # 8. Compute influence force
    joined = joined.withColumn("force", force_udf(F.col("distance"), F.col("m.influence_radius")))

    # 9. Select the strongest metro for each non-metro
    windowSpec = Window.partitionBy("n.geonameid").orderBy(F.col("force").desc())
    best_matches = joined.withColumn("rank", F.row_number().over(windowSpec)) \
                         .filter(F.col("rank") == 1) \
                         .select(
                             F.col("n.geonameid"),
                             F.col("m.geonameid").alias("assigned_metro_id"),
                         )

    assigned_count = best_matches.count()
    print(f"✅ Total non-metros assigned: {assigned_count}")

    # 10. Assign metros to themselves
    metros_df = metros_df.withColumn("assigned_metro_id", F.col("geonameid"))

    # 11. Merge all data back
    non_metros_df = non_metros_df.join(best_matches, "geonameid", "left")
    final_df = metros_df.unionByName(non_metros_df, allowMissingColumns=True)
    final_df = final_df.drop("geom")

    return final_df

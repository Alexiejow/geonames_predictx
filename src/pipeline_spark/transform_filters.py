import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

def filter_populated_places(df: DataFrame) -> DataFrame:
    """
    Filters a Spark DataFrame to only include rows where the feature_code is one of the below
    """
    wanted_codes = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC']
    return df.filter(F.col("feature_code").isin(wanted_codes))

def get_boundary_mask(df: DataFrame) -> Column:
    """
    Returns a Spark Column representing a boolean mask for identifying metropolitan areas.

    **Metro Criteria:**
      1. The city must have a population **≥ 10% of the largest city's population.**
      2. If the city is `PPL`, it must **not be in the same `admin2_code` as any `PPLA` or `PPLC`** 
         (to prevent large districts within cities, and cities in the same areas as administrative locations from being considered metros).

    Assumes 'population' can be cast to integer.
    """
    df = df.withColumn("population", F.col("population").cast("int"))

    # Find the maximum population in the dataset
    max_population = df.agg(F.max("population")).collect()[0][0]
    threshold_population = max_population * 0.1

    # Collect set of admin2 codes from PPLA and PPLC locations
    admin2_excluded = df.filter(F.col("feature_code").isin(["PPLA", "PPLC"])) \
                        .select("admin2_code").distinct() \
                        .where(F.col("admin2_code").isNotNull()) \
                        .rdd.flatMap(lambda x: x).collect()

    # General metro condition: Population >= 10% of max city
    condition_population = F.col("population") >= threshold_population

    # PPL additional exclusion (admin2 for PPLA/PPLC)
    condition_ppl_exclusion = (F.col("population") >= threshold_population) & \
                              (F.col("feature_code") == "PPL") & \
                              (~F.col("admin2_code").isin(admin2_excluded))

    # Allow all cities that meet the population threshold OR PPL cities that also satisfy the exclusion rule
    return condition_population | condition_ppl_exclusion


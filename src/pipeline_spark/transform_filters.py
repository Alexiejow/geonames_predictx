import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

def filter_populated_places(df: DataFrame) -> DataFrame:
    """
    Filters a Spark DataFrame to only include rows where the feature_code is one of:
    ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC'].
    """
    wanted_codes = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC']
    return df.filter(F.col("feature_code").isin(wanted_codes))

def get_boundary_mask(df: DataFrame) -> Column:
    """
    Returns a Spark Column representing a boolean mask for locations that should have boundaries.
    This includes:
      - All rows with feature_code in ["PPLA", "PPLA2", "PPLC"]
      - Rows with feature_code "PPL" that have a population > 100000 and whose admin2_code 
        is not in the set of admin2 codes from rows with feature_code in ["PPLC", "PPLA", "PPLA2"].
    
    Note: This function assumes that 'population' can be cast to integer.
    """
    # Cast 'population' to integer.
    df = df.withColumn("population", F.col("population").cast("int"))
    
    # Condition for administrative centers: PPLA, PPLA2, PPLC.
    condition_admin = F.col("feature_code").isin(["PPLA", "PPLA2", "PPLC"])
    
    # Collect admin2 codes from rows with feature_code in ["PPLC", "PPLA", "PPLA2"].
    mask_admin_df = df.filter(F.col("feature_code").isin(["PPLC", "PPLA", "PPLA2"]))
    admin2_list = mask_admin_df.select("admin2_code") \
                               .where(F.col("admin2_code").isNotNull()) \
                               .distinct() \
                               .rdd.flatMap(lambda x: x) \
                               .collect()
    
    # For PPL rows: population > 100000 and admin2_code not in the collected admin2_list.
    condition_ppl = (F.col("feature_code") == "PPL") & \
                    (F.col("population") > 100000) & \
                    (~F.col("admin2_code").isin(admin2_list))
    
    return condition_admin | condition_ppl

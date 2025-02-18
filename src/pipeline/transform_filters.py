import pandas as pd

def filter_populated_places(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters a Pandas DataFrame to only include certain feature_codes.
    """
    wanted_codes = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC']
    df_filtered = df[df["feature_code"].isin(wanted_codes)]
    return df_filtered

def get_boundary_mask(df: pd.DataFrame) -> pd.Series:
    """
    Returns a boolean mask for locations that should have boundaries.
    This includes:
      - All rows with feature_code in ["PPLA", "PPLA2", "PPLC"]
      - Rows with feature_code "PPL" that have a population over 100000
        and are not part of the same admin2_code as any PPLC or PPLA.
    """
    # Ensure 'population' is numeric
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    
    # Condition for administrative centers: PPLA, PPLA2, PPLC
    condition_admin = df["feature_code"].isin(["PPLA", "PPLA2", "PPLC"])
    
    # Get admin codes for locations with feature_code PPLC or PPLA
    mask_admin = df["feature_code"].isin(["PPLC", "PPLA"])
    admin2_set = set(df.loc[mask_admin, "admin2_code"].dropna().unique())
    
    # For PPL rows: must have population > 100000 and not be in those admin codes.
    condition_ppl = (df["feature_code"] == "PPL") & (df["population"] > 100000) & \
                    (~df["admin2_code"].isin(admin2_set))
    
    return condition_admin | condition_ppl
import pandas as pd

def filter_populated_places(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters a Pandas DataFrame to only include certain feature_codes.
    """
    wanted_codes = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC']
    df_filtered = df[df["feature_code"].isin(wanted_codes)]
    return df_filtered

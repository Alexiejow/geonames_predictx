import pandas as pd
import requests
import zipfile
import io
import ast
import numpy as np

GEONAMES_HEADERS = [
    "geonameid", "name", "asciiname", "alternatenames", 
    "latitude", "longitude", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population", "elevation",
    "dem", "timezone", "modification_date"
]

def load_geonames_data(country_code: str) -> pd.DataFrame:
    """
    Downloads the ZIP file for the specified country code from geonames.org,
    unzips it in memory, and loads the resulting .txt file into a Pandas DataFrame
    with the correct column names.
    
    Example usage:
        df = load_geonames_data("PL")
    """
    # 1. Build the download URL for the given country code (e.g. 'PL')
    url = f"https://download.geonames.org/export/dump/{country_code}.zip"
    
    # 2. Download the ZIP file in memory
    print(f"Downloading from {url} ...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the download failed
    
    # 3. Open the ZIP file in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        txt_filename = f"{country_code}.txt"
        
        # 4. Read the .txt file directly from the ZIP
        with zf.open(txt_filename) as txt_file:
            df = pd.read_csv(
                txt_file,
                sep="\t",
                header=None,
                names=GEONAMES_HEADERS,
                dtype=str  # Keep as string initially, convert later
            )
    
    return df


### 📌 **Saving and Loading Process with Proper Formatting** ###

def format_for_saving(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures that all columns are stored correctly before saving as CSV.
    Converts `candidate_metro_ids` and `polygons` to string format.
    """
    df = df.copy()

    # Ensure `polygons` column is stored as a string
    if "polygons" in df.columns:
        df["polygons"] = df["polygons"].apply(lambda x: str(x) if isinstance(x, list) else "[]")

    # Ensure `candidate_metro_ids` column is stored as a clean string format
    df["candidate_metro_ids"] = df["candidate_metro_ids"].apply(lambda x: 
    str([{ 
        "metro_id": c["metro_id"], 
        "metro_name": c["metro_name"],  # Ensure metro_name is stored
        "force": float(c["force"]) if isinstance(c["force"], np.float64) else c["force"], 
        "distance": float(c["distance"]) if isinstance(c["distance"], np.float64) else c["distance"]
    } for c in x]) if isinstance(x, list) else "[]"
)

    return df


def save_csv(df: pd.DataFrame, filepath: str):
    """
    Saves the DataFrame to CSV after formatting necessary columns.
    """
    df = format_for_saving(df)
    df.to_csv(filepath, index=False)
    print(f"✅ Data saved to {filepath}")


def safe_parse_polygons(val):
    """
    Safely parses the 'polygons' column from the CSV.
    Returns a list of polygon coordinates or an empty list if parsing fails.
    """
    if pd.isna(val) or val == "[]" or val == "":
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
        except Exception as e:
            print(f"⚠️ Error parsing polygons: {e}, value: {val}")
    return []


def safe_parse_candidates(val):
    """
    Safely parses the 'candidate_metro_ids' column from the CSV.
    Returns a list of dictionaries or an empty list if parsing fails.
    """
    if pd.isna(val) or val == "[]" or val == "":
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
        except Exception as e:
            print(f"⚠️ Error parsing candidate_metro_ids: {e}, value: {val}")
    return []


def load_csv(filepath: str) -> pd.DataFrame:
    """
    Loads a CSV file and ensures proper parsing of complex fields.
    """
    df = pd.read_csv(
        filepath,
        converters={
            "polygons": safe_parse_polygons,
            "candidate_metro_ids": safe_parse_candidates
        }
    )
    print(f"✅ Data loaded from {filepath}")
    return df

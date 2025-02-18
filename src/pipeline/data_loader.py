import pandas as pd
import requests
import zipfile
import io

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
    
    This function requires the `requests` library for HTTP and `zipfile` for
    in-memory extraction.
    """
    # 1. Build the download URL for the given country code (e.g. 'PL')
    url = f"https://download.geonames.org/export/dump/{country_code}.zip"
    
    # 2. Download the ZIP file in memory
    print(f"Downloading from {url} ...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the download failed
    
    # 3. Open the ZIP file in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        # The main .txt file usually has the same name as the country code
        txt_filename = f"{country_code}.txt"
        
        # 4. Read the .txt file directly from the ZIP
        with zf.open(txt_filename) as txt_file:
            df = pd.read_csv(
                txt_file,
                sep="\t",
                header=None,
                names=GEONAMES_HEADERS,
                dtype=str,  # optionally keep everything as string if large or mixed
                # low_memory=False,  # optionally to avoid dtype warnings
            )
    
    return df

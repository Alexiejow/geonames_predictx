import time
import pandas as pd
from src.pipeline.transform_filters import get_boundary_mask
import requests
import urllib.parse

def get_city_boundary(API_KEY: str, city_name: str, country_name: str = None):
    """
    Fetches the city boundary (polygons) for a given city and optional country
    from the Geoapify API.
    """

    if country_name:
        query_text = f"{city_name}, {country_name}"
    else:
        query_text = city_name

    try:
        geocode_base_url = "https://api.geoapify.com/v1/geocode/search"
        params = {
            "text": query_text,
            "type": "city",
            "format": "json",
            "apiKey": API_KEY
        }

        geocode_url = f"{geocode_base_url}?{urllib.parse.urlencode(params)}"
        response = requests.get(geocode_url)
        response.raise_for_status()
        data = response.json()

        if "results" in data and len(data["results"]) > 0:
            place_id = data["results"][0]["place_id"]
        else:
            print(f"⚠️ City '{query_text}' not found!")
            return None

        boundary_base_url = "https://api.geoapify.com/v2/place-details"
        boundary_params = {
            "id": place_id,
            "apiKey": API_KEY
        }
        boundary_url = f"{boundary_base_url}?{urllib.parse.urlencode(boundary_params)}"
        boundary_response = requests.get(boundary_url)
        boundary_response.raise_for_status()
        boundary_data = boundary_response.json()

        if "features" in boundary_data and len(boundary_data["features"]) > 0:
            geometry = boundary_data["features"][0].get("geometry", {})
            coords = geometry.get("coordinates", [])

            if geometry.get("type") == "MultiPolygon":
                polygons = []
                for poly in coords:
                    first_ring = poly[0]
                    polygons.append([(lat, lon) for lon, lat in first_ring])
            elif geometry.get("type") == "Polygon":
                first_ring = coords[0]
                polygons = [[(lat, lon) for lon, lat in first_ring]]
            else:
                print(f"⚠️ Geometry type '{geometry.get('type')}' not supported.")
                return None

            return polygons
        else:
            print(f"⚠️ No boundary data found for '{query_text}'")
            return None

    except requests.exceptions.RequestException as re:
        print(f"⚠️ Request error: {re}")
        return None
    except Exception as e:
        print(f"⚠️ Unexpected error fetching '{query_text}': {e}")
        return None


def enrich_boundaries(API_KEY: str, df: pd.DataFrame, country_code = "PL") -> pd.DataFrame:
    """
    Enriches the input DataFrame with boundary polygons.
    
    This function:
      - Splits the DataFrame into two parts:
          * df_borders: rows where boundary should be appleid 
          * df_rest: all other rows.
      - For each row in df_borders, it fetches the boundary polygons using
        get_city_boundary (which calls an external API).
      - Merges the boundary data back into df_borders and then concatenates with df_rest.
      
    Args:
        df (pd.DataFrame): The input DataFrame containing GeoNames data.
        
    Returns:
        pd.DataFrame: The enriched DataFrame with an added "polygons" column for the rows
                      that were enriched.
    """
    # Filter rows that should have boundary data
    mask = get_boundary_mask(df)
    df_borders = df[mask].copy()
    df_rest = df[~mask].copy()

    boundaries = []
    for _, row in df_borders.iterrows():
        city_name = row["name"]
        print(f"Getting borders for city: {city_name}")
        # Fetch polygons using external_data.get_city_boundary
        polygons = get_city_boundary(API_KEY, city_name, country_code)
        boundaries.append({"name": city_name, "polygons": polygons})
        time.sleep(0.2)  # Rate-limit the API calls

    df_boundaries = pd.DataFrame(boundaries)
    # Merge the boundary data on the "name" column
    df_borders = df_borders.merge(df_boundaries, on="name", how="left")
    # Concatenate the enriched borders with the rest
    df_updated = pd.concat([df_rest, df_borders], ignore_index=True)
    return df_updated
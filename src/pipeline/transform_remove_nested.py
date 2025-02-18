import pandas as pd
from shapely.geometry import Polygon, MultiPolygon, Point
import pandas as pd
import ast

def safe_eval_polygons(val):
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except (SyntaxError, ValueError):
            return None
    return val

def create_multipolygon_from_borders(df_borders):
    """
    Convert 'polygons' column (list of (lat, lon) tuples) into a single MultiPolygon,
    applying buffer(0) to fix invalid geometries.
    """
    polygons_list = []

    df_borders["polygons"] = df_borders["polygons"].apply(safe_eval_polygons)

    for _, row in df_borders.iterrows():
        city_polygons = row["polygons"]  # list of polygons, each is [(lat, lon), ...]
        if not isinstance(city_polygons, list):
            print("NOT A LIST")
            continue  # skip if not a list

        for poly_coords in city_polygons:
            # Flip (lat, lon) -> (lon, lat) for Shapely
            coords_lonlat = [(lon, lat) for (lat, lon) in poly_coords]

            try:
                shapely_poly = Polygon(coords_lonlat)

                # 1) Repair at the individual polygon level
                shapely_poly = shapely_poly.buffer(0)

                # If it's valid and not empty, add to our list
                if shapely_poly.is_valid and not shapely_poly.is_empty:
                    polygons_list.append(shapely_poly)
                # else, we skip it
            except ValueError:
                # If coords are invalid, skip
                pass

    # If we have no valid polygons, return None
    if not polygons_list:
        return None

    # 2) Combine all polygons into a MultiPolygon
    mpoly = MultiPolygon(polygons_list)

    # 3) (Optional) Final repair pass on the MultiPolygon
    mpoly = mpoly.buffer(0)

    # Now mpoly should be valid (most of the time)
    if not mpoly.is_valid:
        print("⚠️ Final MultiPolygon is still invalid even after buffer(0).")

    return mpoly

def filter_points_outside_polygons(df_rest, mpoly):
    """
    Given df_rest with 'latitude' and 'longitude', remove rows inside mpoly.
    Returns a new DataFrame where points are NOT inside the MultiPolygon.
    """
    if mpoly is None:
        return df_rest  # no polygons, so just return original

    inside_mask = []
    for i, row in df_rest.iterrows():
        lat = row["latitude"]
        lon = row["longitude"]
        point = Point(lon, lat)
        inside_mask.append(mpoly.contains(point))

    # inside_mask[i] = True means the point is inside
    df_rest["inside_polygon"] = inside_mask
    df_result = df_rest.loc[~df_rest["inside_polygon"]].copy()
    df_result.drop(columns=["inside_polygon"], inplace=True)
    return df_result

def remove_nested_points(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes points from the DataFrame that fall inside the boundaries of larger areas.
    
    This function assumes that the input DataFrame has a "polygons" column,
    where rows with valid polygon data (df_borders) are considered the boundaries,
    and rows with missing polygon data (df_rest) are candidate points that might be inside those boundaries.
    
    The function:
      1. Splits the DataFrame into two: one with polygons (df_borders) and one without (df_rest).
      2. Creates a MultiPolygon from the polygons in df_borders using create_multipolygon_from_borders.
      3. Filters out rows in df_rest that are inside the MultiPolygon using filter_points_outside_polygons.
      4. Concatenates the remaining df_rest with df_borders and returns the final DataFrame.
    
    Args:
        df (pd.DataFrame): Input DataFrame with at least a "polygons" column.
    
    Returns:
        pd.DataFrame: The updated DataFrame with nested points removed.
    """
    # Split into rows that have polygon data and those that don't
    df_borders = df[df["polygons"].notna()].copy()
    df_rest = df[df["polygons"].isna()].copy()
    
    # Create a MultiPolygon from the boundaries
    mpoly = create_multipolygon_from_borders(df_borders)
    
    # Filter out rows in df_rest that fall inside the MultiPolygon
    df_rest_filtered = filter_points_outside_polygons(df_rest, mpoly)
    
    # Combine the two subsets
    df_final = pd.concat([df_borders, df_rest_filtered], ignore_index=True)
    
    return df_final
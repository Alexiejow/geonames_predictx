import os
import sys
import folium
import pandas as pd
import ast

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline.data_loader import load_csv, save_csv

def get_marker_radius(population):
    if population is None or population <= 0:
        return 3
    if population < 1000:
        return 3
    elif population < 50000:
        return 3 + (population - 1000) / (50000 - 1000) * (8 - 3)
    elif population < 200000:
        return 8 + (population - 50000) / (200000 - 50000) * (12 - 8)
    else:
        return 12

def draw_assigned_lines(df, geonameid_to_coords, line_group,
                        assigned_col="assigned_metro_id",
                        line_color="darkred", line_weight=2, line_opacity=0.5):
    """
    Draws lines from each non-metro town to its assigned metro,
    and adds them to the provided FeatureGroup (line_group).
    """
    for idx, row in df.iterrows():
        assigned_id = row.get(assigned_col)
        if pd.notna(assigned_id):
            try:
                assigned_id = int(assigned_id)
            except ValueError:
                continue
            if assigned_id in geonameid_to_coords:
                city_lat = row["latitude"]
                city_lon = row["longitude"]
                metro_lat, metro_lon = geonameid_to_coords[assigned_id]
                folium.PolyLine(
                    locations=[(city_lat, city_lon), (metro_lat, metro_lon)],
                    color=line_color,
                    weight=line_weight,
                    opacity=line_opacity
                ).add_to(line_group)

def generate_map(df, output_html="map.html", draw_lines=False):
    """
    Generates a Folium map with:
      - A background layer for connection lines (if draw_lines is True)
      - Markers for each city (colored and sized by feature_code/population)
      - Polygons if available in the 'polygons' column
      - Optionally, if draw_lines is True, draws assigned connection lines 
        from non-metro towns to their assigned metros in a separate FeatureGroup.
    """
    # Build a lookup dictionary: geonameid -> (latitude, longitude)
    geonameid_to_coords = {}
    for _, row in df.iterrows():
        try:
            key = int(row["geonameid"])
        except ValueError:
            continue
        geonameid_to_coords[key] = (row["latitude"], row["longitude"])
    
    # Create the base map, centered on the average lat/lon
    mean_lat = df["latitude"].mean()
    mean_lon = df["longitude"].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6)
    
    # Create a FeatureGroup for lines so they can appear at the bottom.
    line_group = folium.FeatureGroup(name="Assigned Lines", overlay=True)
    if draw_lines and "assigned_metro_id" in df.columns:
        draw_assigned_lines(df, geonameid_to_coords, line_group,
                            assigned_col="assigned_metro_id",
                            line_color="darkred", line_weight=2, line_opacity=0.5)
    # Add the lines group to the map first (so it's at the bottom).
    line_group.add_to(m)
    
    # Plot markers and polygons for each city.
    for idx, row in df.iterrows():
        lat = row["latitude"]
        lon = row["longitude"]
        feature_code = str(row["feature_code"])
        name = row["name"]
        population = row.get("population", 0)
        admin1_code = row.get("admin1_code", "")
        admin2_code = row.get("admin2_code", "")
        admin3_code = row.get("admin3_code", "")
        
        # Determine marker color
        if feature_code.startswith("PPLA"):
            color = "red"
        elif feature_code == "PPLC":
            color = "green"
        else:
            color = "blue"
        
        radius_value = get_marker_radius(population)
        
        # Build basic tooltip text.
        tooltip_text = (
            f"City: {name}<br>"
            f"Population: {population}<br>"
            f"Feature: {feature_code}<br>"
            f"Admin1: {admin1_code}, Admin2: {admin2_code}, Admin3: {admin3_code}"
        )
        
        # If drawing lines and candidate info exists, append candidate details to tooltip.
        if draw_lines and "candidate_metro_ids" in row:
            # If drawing lines and candidate info exists, append candidate details to tooltip.
            candidates = row.get("candidate_metro_ids", [])
            if isinstance(candidates, list) and len(candidates) > 0:
                candidate_str = "<br>Candidate Metros:<br>" + "<br>".join(
                    [f"{cand['metro_name']} | {cand['metro_id']} (force: {cand['force']:.3f}, dist: {cand['distance']:.1f} km)"
                    for cand in candidates if isinstance(cand, dict) and 'metro_id' in cand]
                )
                tooltip_text += candidate_str
        
        # Add the marker.
        folium.CircleMarker(
            location=[lat, lon],
            radius=radius_value,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=tooltip_text
        ).add_to(m)
        
        # Overlay polygons if present.
        polygons = row.get("polygons", None)
        if isinstance(polygons, list) and len(polygons) > 0:
            for poly in polygons:
                folium.Polygon(
                    locations=poly,
                    color="cyan",
                    weight=2,
                    fill=True,
                    fill_color="yellow",
                    fill_opacity=0.3,
                    tooltip=tooltip_text
                ).add_to(m)
    
    m.save(output_html)
    print(f"Map saved to {output_html}")


def safe_parse_polygons(val):
    """
    Safely parse the 'polygons' column from the CSV.
    Returns a list of polygon coordinates or an empty list if parsing fails.
    """
    # If it's NaN or empty
    if pd.isna(val):
        return []
    # If it's already a list (rare in CSV, but possible if it was saved in some way)
    if isinstance(val, list):
        return val
    # If it's a string, try literal_eval
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            # Ensure parsed is a list of polygon coords
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except Exception:
            return []
    # If none of the above, return empty
    return []

def safe_parse_candidates(val):
    """
    Safely parse 'candidate_metro_ids' from CSV.
    Returns a list of dictionaries or an empty list if parsing fails.
    """
    if pd.isna(val) or val == "[]":
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
        except Exception as e:
            print(f"Error parsing candidate_metro_ids: {e}, value: {val}")
            return []
    return []


if __name__ == "__main__":

    ############# CONFIGURATION ##############
    
    df = load_csv(
        "data/processed/assigned-metros-spark.csv",
    )

    # Generate the map with draw_lines enabled.
    generate_map(df,
                 output_html="data/visualisations/cities_map_PL.html",
                 draw_lines=True
                 )
    
    ###########################################

import os
import sys
import folium
import pandas as pd
import ast
from tqdm import tqdm  # ✅ Import tqdm for progress bars

base_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(base_dir)

from src.pipeline_spark.data_loader import load_partitioned_csv, save_csv

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
    Draws lines from each non-metro town to its assigned metro.
    """
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Drawing lines", unit="lines"):
        assigned_id = row.get(assigned_col)
        if pd.notna(assigned_id):
            try:
                assigned_id_int = int(assigned_id)
            except ValueError:
                print(f"Row {idx}: assigned_metro_id '{assigned_id}' cannot be converted to int.")
                continue
            if assigned_id_int in geonameid_to_coords:
                city_lat = row["latitude"]
                city_lon = row["longitude"]
                metro_lat, metro_lon = geonameid_to_coords[assigned_id_int]
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
    print("📌 Creating city lookup dictionary...")
    geonameid_to_coords = {int(row["geonameid"]): (row["latitude"], row["longitude"])
                           for _, row in tqdm(df.iterrows(), total=len(df), desc="Building lookup", unit="cities") 
                           if pd.notna(row["geonameid"])}

    mean_lat = df["latitude"].mean()
    mean_lon = df["longitude"].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6)

    line_group = folium.FeatureGroup(name="Assigned Lines", overlay=True)
    if draw_lines and "assigned_metro_id" in df.columns:
        draw_assigned_lines(df, geonameid_to_coords, line_group,
                            assigned_col="assigned_metro_id",
                            line_color="darkred", line_weight=2, line_opacity=0.5)
    line_group.add_to(m)

    print("📌 Placing city markers on the map...")
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Adding markers", unit="cities"):
        lat = row["latitude"]
        lon = row["longitude"]
        feature_code = str(row["feature_code"])
        name = row["name"]
        population = row.get("population", 0)
        admin1_code = row.get("admin1_code", "")
        admin2_code = row.get("admin2_code", "")
        admin3_code = row.get("admin3_code", "")

        color = "red" if feature_code.startswith("PPLA") else "green" if feature_code == "PPLC" else "blue"
        radius_value = get_marker_radius(population)
        tooltip_text = (
            f"City: {name}<br>"
            f"Population: {population}<br>"
            f"Feature: {feature_code}<br>"
            f"Admin1: {admin1_code}, Admin2: {admin2_code}, Admin3: {admin3_code}"
        )

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius_value,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=tooltip_text
        ).add_to(m)

    print(f"Saving map to {output_html}")
    m.save(output_html)
    print(f"✅ Map saved to {output_html}")

if __name__ == "__main__":
    country_code = "PL"

    # Load the CSV file.
    df = load_partitioned_csv(f"data/processed/country_code=" + country_code)
    
    # Generate the map with draw_lines enabled.
    generate_map(df,
                 output_html="data/visualisations/" + country_code + "_vis.html",
                 draw_lines=True)

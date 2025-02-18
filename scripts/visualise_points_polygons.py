import folium
import pandas as pd
import ast

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

def generate_map(df, output_html="map.html"):
    """
    Generates a Folium map with:
      - Markers for each city (colored and sized by feature_code/population)
      - Polygons if available in the 'polygons' column
    """

    # Create the base map, centered on the average lat/lon
    mean_lat = df["latitude"].mean()
    mean_lon = df["longitude"].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6)

    # Check if the 'polygons' column exists
    has_polygons = "polygons" in df.columns

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

        # Build tooltip text
        tooltip_text = (
            f"City: {name}<br>"
            f"Population: {population}<br>"
            f"Feature: {feature_code}<br>"
            f"Admin1: {admin1_code}, Admin2: {admin2_code}, Admin3: {admin3_code}"
        )

        # Add the marker
        folium.CircleMarker(
            location=[lat, lon],
            radius=radius_value,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=tooltip_text
        ).add_to(m)

        # Overlay polygons if available
        if has_polygons:
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
    if pd.isna(val):
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
            else:
                return []
        except Exception:
            return []
    return []

if __name__ == "__main__":
    df = pd.read_csv(
        "data/processed/boundary_enriched_PL.csv",
        converters={
            "polygons": safe_parse_polygons
        }
    )

    # Generate the map
    generate_map(df, output_html="data/visualisations/cities_map.html")

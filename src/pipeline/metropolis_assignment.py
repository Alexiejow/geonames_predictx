import math
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
from src.pipeline.transform_filters import get_boundary_mask

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Compute the great-circle distance (in kilometers) between two points on Earth.
    """
    R = 6371.0  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def radius(population):
    """
    Compute the influence radius of a metropolis given its population.
    
    Using:
        METRO_CITY_POPULATION_CONSTANT = -1/1443000,
        MIN_METRO_CITY_RADIUS = 10,
        MAX_METRO_CITY_RADIUS = 90
    """
    METRO_CITY_POPULATION_CONSTANT = -1 / 1443000
    MIN_METRO_CITY_RADIUS = 10
    MAX_METRO_CITY_RADIUS = 90
    return MIN_METRO_CITY_RADIUS + MAX_METRO_CITY_RADIUS * (1 - np.exp(METRO_CITY_POPULATION_CONSTANT * population))

def calculate_metrocity_impact(max_radius, distance_to_metro_city):
    """
    Calculate the impact (or "force") that a metropolis exerts on another city.
    
    Using:
        METRO_CITY_POWER_CONSTANT = -1.4
    """
    METRO_CITY_POWER_CONSTANT = -1.4
    return np.exp(METRO_CITY_POWER_CONSTANT * distance_to_metro_city / max_radius)

def assign_candidate_metro_ids_balltree(non_metros_df, metros_df):
    """
    For each non-metropolis town in non_metros_df, use a BallTree to find candidate metros 
    (from metros_df) whose influence radius (computed from their population) covers the town.
    
    Each candidate is stored as a dictionary:
       {"metro_id": <id>, "force": <impact>, "distance": <distance_in_km>}
    
    Returns a copy of non_metros_df with a new column 'candidate_metro_ids'.
    """
    non_metros = non_metros_df.copy()
    EARTH_RADIUS = 6371.0  # km

    # Convert latitude and longitude to numeric values (coerce errors to NaN)
    non_metros[['latitude', 'longitude']] = non_metros[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')

    # Build an array of non-metro coordinates in radians.
    non_metro_coords = np.radians(non_metros[['latitude', 'longitude']].values)
    
    # Build a BallTree for non-metro points using the haversine metric.
    tree = BallTree(non_metro_coords, metric='haversine')
    
    # Use array positions (0, 1, 2, ...) rather than the DataFrame index.
    n = len(non_metros)
    candidate_dict = {i: [] for i in range(n)}
    
    # For each metro, determine its influence radius and query the BallTree.
    for _, metro in metros_df.iterrows():
        metro_id = metro['geonameid']
        metro_lat = metro['latitude']
        metro_lon = metro['longitude']
        influence_radius_km = radius(metro['population'])
        influence_radius_rad = influence_radius_km / EARTH_RADIUS
        metro_coord_rad = np.radians([float(metro_lat), float(metro_lon)]).reshape(1, -1)
        
        # Query the BallTree: get indices of non-metro towns within influence_radius_rad.
        indices = tree.query_radius(metro_coord_rad, r=influence_radius_rad)[0]
        
        # For each candidate non-metro, compute exact distance and impact.
        for i in indices:
            town = non_metros.iloc[i]  # Use .iloc so that i is the positional index.
            distance = haversine_distance(metro_lat, metro_lon, town['latitude'], town['longitude'])
            if distance < influence_radius_km:
                force = calculate_metrocity_impact(influence_radius_km, distance)
                candidate_dict[i].append({
                    "metro_id": metro_id,
                    "force": force,
                    "distance": distance
                })
    
    # Assign the candidate lists back to the DataFrame.
    non_metros['candidate_metro_ids'] = [candidate_dict[i] for i in range(n)]
    return non_metros

def assign_best_metro_id(candidates):
    """
    Given a list of candidate dictionaries (each with keys "metro_id", "force", and "distance"),
    return the metro_id of the candidate with the highest force.
    
    If there's only one candidate, return its metro_id.
    If no candidate exists, return None.
    """
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]["metro_id"]
    best_candidate = max(candidates, key=lambda cand: cand["force"])
    return best_candidate["metro_id"]

def assign_metros(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits the DataFrame into metros and non-metros based on a metropolis condition,
    assigns candidate metro IDs to non-metro towns using a BallTree, and then assigns
    the best metro ID to each non-metro.
    
    Returns the unified DataFrame with an additional column 'assigned_metro_id' for non-metro towns.
    """
    # Define the metropolis condition, for now using boundary mask
    mask = get_boundary_mask(df)
    
    # Split the DataFrame.
    metros_df = df[mask].copy()
    non_metros_df = df[~mask].copy()

    print("len df: ", len(df))
    print("len metros: ", len(metros_df))
    print("len non_metros_df: ", len(non_metros_df))
    
    # Use BallTree to assign candidate metro IDs to each non-metro town.
    non_metros_df = assign_candidate_metro_ids_balltree(non_metros_df, metros_df)
    
    # Directly assign the best candidate's metro ID.
    non_metros_df['assigned_metro_id'] = non_metros_df['candidate_metro_ids'].apply(assign_best_metro_id)

    # Concatenate the two subsets.
    final_df = pd.concat([metros_df, non_metros_df], ignore_index=True)
    final_df.sort_values("geonameid", inplace=True)
    return final_df

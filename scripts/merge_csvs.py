import os
import pandas as pd
import glob

def merge_and_sort_csv(folder_path, output_file):
    """
    Merges all CSV files in a folder and sorts by 'geonameid'.
    
    Args:
        folder_path (str): Path to the folder containing partitioned CSV files.
        output_file (str): Path to save the final sorted CSV file.
    """
    # Find all CSV files in the folder (ignore hidden files like _SUCCESS)
    csv_files = glob.glob(os.path.join(folder_path, "part-*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    print(f"Found {len(csv_files)} CSV files. Merging...")
    
    # Read all CSV files and concatenate into a single DataFrame
    df_list = [pd.read_csv(file) for file in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)
    
    # Sort by 'geonameid'
    if "geonameid" in merged_df.columns:
        merged_df = merged_df.sort_values(by="geonameid")
    else:
        print("Warning: 'geonameid' column not found, skipping sorting.")
    
    # Save the merged and sorted DataFrame as a single CSV file
    merged_df.to_csv(output_file, index=False)
    print(f"✅ Merged CSV saved to {output_file}")

# Example usage
if __name__ == "__main__":
    folder_name = "data/processed/country_code=PL"  # Change this to your folder
    output_csv = "data/processed/PL_sorted.csv"  # Output file path
    merge_and_sort_csv(folder_name, output_csv)

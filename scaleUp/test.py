import pandas as pd
import glob
import os

# Define the directory containing the Parquet files
directory = "/Users/apple/PROJECT/hons_project/data/SP500/analysis_reports/filtered"

# Get a list of all Parquet files in the directory
parquet_files = glob.glob(os.path.join(directory, "*.parquet"))

# Read and concatenate all Parquet files
df_list = [pd.read_parquet(file) for file in parquet_files]
df_combined = pd.concat(df_list, ignore_index=True)

# Save the concatenated DataFrame to a new Parquet file
output_file = os.path.join(directory, "merged_output.parquet")
df_combined.to_parquet(output_file, index=False)



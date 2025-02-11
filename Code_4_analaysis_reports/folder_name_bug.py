import os
import pandas as pd
import json
import datetime
import time
import atexit
import asyncio
import aiohttp

# Global variables

save_folder = "analysis_reports"
year_until = 2025
year_since = 2024


# File path for CSV
path = '/Users/apple/PROJECT/Code_4_SECfilings/sp500_total_constituents.csv'

# Read and process CSV
try:
    df = pd.read_csv(path, encoding='utf-8')
    cik = df['CIK'].drop_duplicates().tolist()
    ticker = df['Symbol'].tolist()
    cik_ticker = dict(zip(cik, ticker))
except UnicodeDecodeError:
    df = pd.read_csv(path, encoding='ISO-8859-1')
    cik = df['CIK'].drop_duplicates().tolist()
    ticker = df['Symbol'].tolist()
    cik_ticker = dict(zip(cik, ticker))


def rename_subfolders(parent_folder):
    # List all subfolders in the parent folder
    subfolders = [f for f in os.listdir(parent_folder) if os.path.isdir(os.path.join(parent_folder, f))]
    
    # Sort subfolders to ensure correct order
    subfolders.sort()
    
    for subfolder in subfolders:
        try:
            # Convert subfolder name to an integer and decrement by 1
            new_name = str(int(subfolder) - 1)
            
            # Construct full paths for the old and new subfolder names
            old_path = os.path.join(parent_folder, subfolder)
            new_path = os.path.join(parent_folder, new_name)
            
            # Rename the subfolder
            os.rename(old_path, new_path)
            print(f'Renamed {old_path} to {new_path}')
        except ValueError:
            # Skip subfolders that do not have integer names
            print(f'Skipping {subfolder} as it is not an integer')

for cik, ticker in cik_ticker.items():
    ticker = ticker.lower()
    ticker_id_folder = os.path.join(save_folder, ticker)
    if os.path.exists(ticker_id_folder):
        rename_subfolders(ticker_id_folder)
import requests
import os
import pandas as pd
import json
from tqdm import tqdm
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import atexit
from threading import Lock
from collections import defaultdict

path = '/Users/apple/PROJECT/Code_4_10k/sp500_total_constituents.csv'

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

total_len = 0

valid = 0
total_requests = 0
save_folder = "analysis_reports"
id_folder = "analysis_report_ids"
year_until = 2024
year_since = 2006

# Rate limiting variables
rate_limit = 5
request_counter = 0
last_request_time = time.time()
lock = Lock()

# Returns a nested dicionary for a ticker; { 'year1': {'ids': 'titles'}, 'year2' : {}, ...  }
def fetch_ids_for_ticekr(ticker: str):
    year_id2title = defaultdict(dict)
    total_numIds = 0
    year_ids = 0
    ticker = ticker.lower()
    ticker_id_folder = os.path.join(id_folder, ticker)
    if not os.path.exists(ticker_id_folder):
        os.makedirs(ticker_id_folder)
        
    for year in range(year_until, year_since, -1):
        id2title = defaultdict(lambda: 'title')
        year_file_path = os.path.join(ticker_id_folder, f"{year-1}.json")
        if not os.path.exists(year_file_path):
            continue
        with open(year_file_path, 'r') as json_file:
            json_data = json.load(json_file)
            if 'data' in json_data and json_data['data']:                
                for i in range(len(json_data['data'])):
                    id = json_data['data'][i]['id']
                    title = json_data['data'][i]['attributes']['title']
                    id2title[id] = title
        year_id2title[year] = id2title
        year_ids = len(id2title)
        total_numIds += year_ids

    return year_id2title, total_numIds
            
def fetch_reports(ids: dict):
    years = ids.keys()
    id2title = ids.values()
    for year in years:
        for id in id2title:
            return 0
    return 0        
def save_reports():
    
    
    
    
    
    

                
        
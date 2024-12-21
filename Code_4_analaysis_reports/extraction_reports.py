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

import asyncio
import aiohttp

path = '/Users/apple/PROJECT/Code_4_10k/sp500_total_constituents.csv'

# Configuration
RATE_LIMIT = 5 # Maximum requests per second
CONCURRENCY_LIMIT = 60 # the number of workers working concurrently
BATCH_SIZE = 30 # Process 30 tickers at a time


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
        year_ids_counts = len(id2title)
        total_numIds += year_ids_counts

    return year_id2title, total_numIds
            
# def fetch_reports(ids: dict):
#     years = ids.keys()
#     id2title = ids.values()
#     for year in years:
#         for id in id2title:
#             return 0
#     return 0        
def fetch_reports(ticker, session, rate_limiter):
    year_id2title, total_numIds = fetch_ids_for_ticekr(ticker)
    years = year_id2title.keys()
    id2title = year_id2title.values()
    ticker = ticker.lower()
    report_save_folder = os.path.join(save_folder, ticker)
    if not os.path.exists(report_save_folder):
        os.makedirs(report_save_folder)
        
    for year in years:
        year_folder = os.path.join(report_save_folder, year)
        if not os.path.exists(year_folder):
            os.makedirs(year_folder)
        
        for id in year_id2title[year].keys():
            title = year_id2title[year][id]
            report_file_path = os.path.join(year_folder, f"{id}.json")
            if os.path.exists(report_file_path):
                with open(report_file_path, 'r') as json_file:
                    existing_data = json.load(json_file)
                    if 'data' in existing_data and existing_data['data']:
                        print(f"File for {ticker}, year {year}, id {id} already exists and contains data. Skipping download.")
                        continue
            url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/get"
            querystring = {
                "id": id
            }
            headers = {
                "x-rapidapi-key": "13f98f0478msha8ec97b6a805b1fp174175jsn98c8458d4397",
                "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
            }
            
            retries = 0
            backoff = 1
            
            while retries < 3:
                try:
                    async with rate_limiter:
                        async with session.get(url, headers=headers, params=querystring, timeout=30) as response:
                            if response.status == 429:
                                print(f"Rate limit hit for {ticker}, retrying in {backoff} seconds...")
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                retries += 1
                                continue
                            response.raise_for_status()
                            try:
                                response_json = await response.json()
                                with open(report_file_path, 'w') as json_file:
                                    json.dump(response_json, json_file)
                                print(f"Downloaded {ticker}, year {year-1}, id {id}")
                            except json.JSONDecodeError:
                                print(f"Failed to decode JSON for {ticker}, year {year-1}, id {id}")
                            break
                except aiohttp.ClientResponseError as e:
                    print(f"Failed to fetch data for {ticker}, year {year-1}, id {id} with error {e}")
                    retries += 1
                    continue
            
            
    
    

async def main():
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)
    connector = aiohttp.TCPConnector(limit_per_host = CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = list(cik_ticker.values())
        
        # Process in batches
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
            tasks = [fetch_ids_for_ticekr(ticker, session, rate_limiter) for ticker in batch]
            await asyncio.gather(*tasks)
            
if __name__ == '__main__':
    asyncio.run(main())
        
    
    
    
    
    
    

                
        
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

# Load API key and Host name
with open('/Users/apple/PROJECT/api/rapidapi_key.txt', 'r') as file:
    api_key = file.read().strip()
    
with open('/Users/apple/PROJECT/api/rapidapi_host.txt', 'r') as file:
    api_host = file.read().strip()

# Global variables
save_folder = "analysis_reports"
id_folder = "analysis_report_ids"
year_until = 2024
year_since = 2006
total_len = 0
valid = 0
total_requests = 0
request_counter = 0

# File path for CSV
path = '/Users/apple/PROJECT/Code_4_SECfilings/sp500_total_constituents.csv'
log_folder_path = '/Users/apple/PROJECT/Code_4_analaysis_reports/log'

# Configuration
RATE_LIMIT = 5 # Maximum requests per second
CONCURRENCY_LIMIT = 50 # the number of workers working concurrently
BATCH_SIZE = 50 # Process 30 tickers at a time
INITIAL_BACKOFF = 1 # Start with a 1-second delay
MAX_RETRIES = 5 # Retry up to 5 times on failures


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

async def fetch_reports(ticker, session, rate_limiter):
    global total_len, valid, total_requests, request_counter
    
    year_id2title, total_numIds = fetch_ids_for_ticekr(ticker)
    years = year_id2title.keys()
    id2title = year_id2title.values()
    ticker = ticker.lower()
    report_save_folder = os.path.join(save_folder, ticker)
    if not os.path.exists(report_save_folder):
        os.makedirs(report_save_folder)
        
    for year in years:
        year_folder_path = os.path.join(report_save_folder, str(year))
        if not os.path.exists(year_folder_path):
            os.makedirs(year_folder_path)
        
        for id in year_id2title[year].keys():
            title = year_id2title[year][id]
            report_file_path = os.path.join(year_folder_path, f"{id}.json")
            if os.path.exists(report_file_path):
                with open(report_file_path, 'r') as json_file:
                    existing_data = json.load(json_file)
                    if existing_data is not None and 'data' in existing_data and existing_data['data']:
                        # print(f"File for {ticker}, year {year}, id {id} already exists and contains data. Skipping download.")
                        continue
            url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/get-details"
            querystring = {
                "id": id
            }
            headers = {
                "x-rapidapi-key": f"{api_key}",
                "x-rapidapi-host": f"{api_host}"
            }
            
            retries = 0
            backoff = INITIAL_BACKOFF
            
            while retries < MAX_RETRIES:
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
                                request_counter += 1
                                total_requests += 1
                                    
                                if response_json.get('data'):
                                    total_len += len(response_json['data'])
                                    valid += 1
                                
                            except (aiohttp.ContentTypeError, json.JSONDecodeError):
                                print(f"Failed to decode JSON for {ticker} id {id}")
                                response_json = None    
                            
                            with open(report_file_path, 'w') as json_file:
                                json.dump(response_json, json_file)
                                
                            break
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    error_message = f"Failed to fetch data for {ticker} id {id} with error {e}"
                    log_path = os.path.join(log_folder_path, 'report_error_log.txt')
                    with open(log_path, 'a') as error_log_file:
                        error_log_file.write(error_message + '\n')
                    retries *= 1
                    backoff *= 2
                    await asyncio.sleep(backoff)
                    continue
                
# Function to log the current state
def log_state():
    end_time = time.time()
    elapsed_time = end_time - start_time
    log_path = os.path.join(log_folder_path, 'report_api_requests_log.txt')
    with open(log_path, 'a') as log_file:
        log_file.write(f"Total id counts: {total_len}, Total requests: {total_requests}, Valid requests: {valid}, start time:{start_time}, end time:{end_time}, Elapsed time:{elapsed_time:.2f} second \n")
    print(f"Total data length: {total_len}")
    print(f"Valid requests: {valid}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    print(f"Total requests: {total_requests}")

# Register the log_state function to be called on script exit
atexit.register(log_state)
            
# Record the start time
start_time = time.time()

async def main():
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)
    connector = aiohttp.TCPConnector(limit_per_host = CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = list(cik_ticker.values())
        
        # Process in batches
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
            tasks = [fetch_reports(ticker, session, rate_limiter) for ticker in batch]
            await asyncio.gather(*tasks)
            
if __name__ == '__main__':
    asyncio.run(main())
        
    
    
    
    
    
    

                
        
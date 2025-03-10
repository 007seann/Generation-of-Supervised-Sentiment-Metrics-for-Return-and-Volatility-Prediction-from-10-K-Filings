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
import random

import asyncio
import aiohttp

with open("/Users/apple/PROJECT/api/ninjaapi_key.txt", "r") as file:
    api_key = file.read().strip()

# Global variables
save_folder = "transcripts_ninjaAPI"

year_until = 2023 # Getting the data points untill y.01.01
year_since = 2000 # Getting the data points since x.01.01
total_len = 0
valid = 0
total_requests = 0
request_counter = 0

# File path for CSV
path = '../Code_4_SECfilings/sp500_total_constituents_final.csv'
log_folder_path = '../Code_4_transcripts/log'
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


async def fetch_reports(ticker, session, rate_limiter, year_until, year_since):
    global total_len, valid, total_requests, request_counter

    ticker_lower = ticker.lower()
    report_save_folder = os.path.join(save_folder, ticker_lower)
    if not os.path.exists(report_save_folder):
        os.makedirs(report_save_folder)
    
    for year in range(year_until, year_since - 1, -1):
        year_folder_path = os.path.join(report_save_folder, str(year))
        if not os.path.exists(year_folder_path):
            os.makedirs(year_folder_path)
        
        
        for quarter in range(1, 5):
        
            report_file_path = os.path.join(year_folder_path, f"calls_{ticker}_{year}_{quarter}.json")
            if os.path.exists(report_file_path):
                with open(report_file_path, 'r') as json_file:
                    existing_data = json.load(json_file)
                    if existing_data is not None and 'transcript' in existing_data and existing_data['transcript']:
                        continue
                    
            url = 'https://api.api-ninjas.com/v1/earningstranscript?ticker={}&year={}&quarter={}'.format(ticker, year, quarter)
            headers = {'X-Api-Key': api_key}
            response = requests.get(url, headers=headers)
                
            retries = 0
            backoff = INITIAL_BACKOFF
            
            while retries < MAX_RETRIES:
                try:
                    async with rate_limiter:
                        async with session.get(url, headers=headers, timeout=30) as response:
                            if response.status == 429:
                                print(f"Rate limit hit for {ticker}, retrying in {backoff} seconds...")
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                retries += 1
                                continue
                            response.raise_for_status()
                            try:
                                response_json = await response.json()
                                
                                total_requests += 1

                                if isinstance(response_json, list) and not response_json:
                                    break
                                
                                if response_json and response_json.get('transcript'):
                                    total_len += len(response_json['transcript'])
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
            tasks = [fetch_reports(ticker, session, rate_limiter, year_until=year_until, year_since=year_since) for ticker in batch]
            await asyncio.gather(*tasks)
            
if __name__ == '__main__':
    asyncio.run(main())
        
    
    
    
    
    
    

                
        
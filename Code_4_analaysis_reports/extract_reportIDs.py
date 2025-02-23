import os
import pandas as pd
import json
from datetime import datetime, timezone
import time
import atexit
import asyncio
import aiohttp

# Load API key and Host name
with open('/Users/apple/PROJECT/api/rapidapi_key.txt', 'r') as file:
    api_key = file.read().strip()
    
with open('/Users/apple/PROJECT/api/rapidapi_host.txt', 'r') as file:
    api_host = file.read().strip()

# Global variables
total_len = 0
valid = 0
total_requests = 0
request_counter = 0
save_folder = "analysis_report_ids"
year_until = 2025 # Getting the data points untill y.12.31
year_since = 2000 # Getting the data points since x.01.01
pages = [i for i in range(1, 3)] # Assume the total number of annual reports per firm is less than 80

# Configuration
RATE_LIMIT = 5 # Maximum requests per second
MAX_RETRIES = 5 # Retry up to 5 times on failures
INITIAL_BACKOFF = 1  # Start with a 1-second delay
CONCURRENCY_LIMIT = 60 # Limit to 60 concurrent tasks
BATCH_SIZE = 30  # Process 30 tickers at a time

# File path for CSV
path = '../Code_4_SECfilings/update_only2025.csv'

# Read and process CSV
try:
    df = pd.read_csv(path, encoding='utf-8')
    cik = df['CIK'].drop_duplicates().tolist()
    ticker = df['Ticker'].tolist()
    cik_ticker = dict(zip(cik, ticker))
except UnicodeDecodeError:
    df = pd.read_csv(path, encoding='ISO-8859-1')
    cik = df['CIK'].drop_duplicates().tolist()
    ticker = df['Ticker'].tolist()
    cik_ticker = dict(zip(cik, ticker))


# Convert years to Unix time
year2unixTime = {}
for year in range(year_until + 1, year_since - 1, -1): # eg) 2026, 2025, 2024
    current_year_timestamp = int(datetime(year, 1, 1).timestamp())
    year2unixTime[year] = current_year_timestamp

# Fetch data for a specific ticker
async def fetch_data_for_ticker(ticker, session, rate_limiter):
    global total_len, valid, total_requests, request_counter, last_request_time
    ticker = ticker.lower()
    ticker_save_folder = os.path.join(save_folder, ticker)
    if not os.path.exists(ticker_save_folder):
        os.makedirs(ticker_save_folder)

    for year in range(year_until + 1, year_since, -1): # eg) 2026, 2025
        year_file_path = os.path.join(ticker_save_folder, f"{year-1}.json")
        if os.path.exists(year_file_path):
            with open(year_file_path, 'r') as json_file:
                existing_data = json.load(json_file)
                if existing_data is not None and 'data' in existing_data and existing_data['data']:
                    continue
        
        merged_data = {"data": []}
        for page in pages:

            url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/list"
            querystring = {
                "id": ticker,
                "until": year2unixTime[year], # until x.01.01
                "since": year2unixTime[year - 1], # since y.01.01
                "size": "40",
                "number": page
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
                            # Retry on Parsing Errors:
                            except aiohttp.ContentTypeError:
                                print(f"Invalid content type or empty response. Raw response: {await response.text()}")
                                response_json = None
                            
                            if response_json and response_json.get('data'):
                                merged_data['data'].extend(response_json['data'])
                                total_len += len(response_json['data'])
                                valid += 1
                                
                                # Stop pagination if there are not more pages
                                if len(response_json['data']) < 1:
                                    error_message = f"No more pages for {ticker}, year {year-1}. Stopping pagination."
                                    print(error_message)

                                    break
                            else:
                                error_message = f"No data found for {ticker}, year {year -1}, page {page}."
                                with open('error_page_log.txt', 'a') as error_log_file:
                                    error_log_file.write(error_message + '\n')
                                break # Exit pagination if no data returned
                            break # Exit retry loop on success
                        
                # Retry on Unexpected Server Behavior - json.JSONDecodeError
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    error_message = f"Error fetching data for {ticker}, year {year-1}, page {page}: {e}"
                    print(error_message)
                    with open('error_log.txt', 'a') as error_log_file:
                        error_log_file.write(error_message + '\n')
                    retries *= 1
                    backoff *= 2
                    await asyncio.sleep(backoff)
                    continue


        with open(year_file_path, 'w') as json_file:
            json.dump(merged_data, json_file, indent=4)

# Function to log the current state
def log_state():
    end_time = time.time()
    elapsed_time = end_time - start_time
    log_file_path = 'ids_api_requests_log.txt'
    readable_start_time = datetime.fromtimestamp(start_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    readble_end_time = datetime.fromtimestamp(end_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Total id counts: {total_len}, Total requests: {total_requests}, Valid requests: {valid}, start time:{readable_start_time}, end time:{readble_end_time}, Elapsed time:{elapsed_time:.2f} second \n")
    print(f"Total data length: {total_len}")
    print(f"Valid requests: {valid}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
    print(f"Total requests: {total_requests}")

# Register the log_state function to be called on script exit
atexit.register(log_state)

# Record the start time
start_time = time.time()

async def main():
    # Semaphore for rate limiting
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = list(cik_ticker.values())
        
        # Process in batches
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
            tasks = [fetch_data_for_ticker(ticker, session, rate_limiter) for ticker in batch]
            await asyncio.gather(*tasks)
            
if __name__ == "__main__":
    asyncio.run(main())

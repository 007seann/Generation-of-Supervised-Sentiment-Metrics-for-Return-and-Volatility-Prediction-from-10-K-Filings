from collections import defaultdict
import json
import os
import asyncio
import aiohttp
import pandas as pd

# Global variables
id_folder = "analysis_report_ids"
save_folder = "analysis_reports_count"
year_since = 2006 # Getting the data points since x.01.01
year_until = 2023 # Getting the data points untill 2023.12.31


# Configuration
RATE_LIMIT = 5 
CONCURRENCY_LIMIT = 60 
BATCH_SIZE = 30 

# File path for CSV
path = '../Code_4_SECfilings/sp500_total_constituents.csv'

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
    

async def fetch_ids_for_ticekr(ticker: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        year_id2title = defaultdict(dict)
        total_numIds = 0
        ticker = ticker.lower()
        ticker_id_folder = os.path.join(id_folder, ticker)
        if not os.path.exists(ticker_id_folder):
            os.makedirs(ticker_id_folder)
            
        for year in range(year_until, year_since - 1, -1): #2023, ..., 2006
            id2title = defaultdict(lambda: 'title')
            year_file_path = os.path.join(ticker_id_folder, f"{year}.json") # 2023.json, ..., 2006.json
            if not os.path.exists(year_file_path):
                continue
            with open(year_file_path, 'r') as json_file:
                json_data = json.load(json_file)
                if 'data' in json_data and json_data['data']:                
                    for i in range(len(json_data['data'])):
                        id = json_data['data'][i]['id']
                        title = json_data['data'][i]['attributes']['title']
                        id2title[id] = title
            year_id2title[year] = id2title # 2023: 2023's data, ..., 2006: 2006's data
            year_ids_counts = len(id2title)
            total_numIds += year_ids_counts
            # save annual counts
            if not os.path.exists(save_folder):
                os.makedirs(save_folder)
            save_path = os.path.join(save_folder, f'{ticker}_report_count.txt')
            with open(save_path, 'a') as file:
                file.write(f"{year}: {year_ids_counts}\n")
                
            
        # save counts
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)
        save_path = os.path.join(save_folder, f'{ticker}_report_count.txt')
        with open(save_path, 'a') as file:
            file.write(f"Total Count: {total_numIds}\n")
            
        with open('sp500_reports_counts.txt', 'a') as file:
            file.write(f"{ticker}:{total_numIds}\n ")




async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tickers = list(cik_ticker.values())
    
    # Process in batches
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
        tasks = [fetch_ids_for_ticekr(ticker, semaphore) for ticker in batch]
        await asyncio.gather(*tasks)
            
if __name__ == '__main__':
    asyncio.run(main())
        
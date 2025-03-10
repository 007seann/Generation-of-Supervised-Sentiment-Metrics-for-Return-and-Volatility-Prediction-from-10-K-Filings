from collections import defaultdict
import json
import os
import asyncio
import aiohttp
import pandas as pd

# Global variables
id_folder = "transcript_ids_test"
year_since = 2006
year_until = 2024


# Configuration
RATE_LIMIT = 5 
CONCURRENCY_LIMIT = 60 
BATCH_SIZE = 30 

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
    

async def fetch_ids_for_ticker(ticker: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        ticker = ticker.lower()
        
        id2title = defaultdict(lambda: 'title')
        year_file_path = os.path.join(id_folder, f"{ticker}_transcript_ids.json")
        with open(year_file_path, 'r') as json_file:
            json_data = json.load(json_file)
            if 'data' in json_data and json_data['data']:                
                for i in range(len(json_data['data'])):
                    if json_data['data'][i]['type'] == 'transcript':
                        id = json_data['data'][i]['id']
                        title = json_data['data'][i]['attributes']['title']
                        id2title[id] = title
        id_counts = len(id2title)
        with open('sp500_update_transcripts_counts.txt', 'a') as file:
            file.write(f"{ticker}:{id_counts}\n ")


async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tickers = list(cik_ticker.values())
    
    # Process in batches
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1}: {batch}")
        tasks = [fetch_ids_for_ticker(ticker, semaphore) for ticker in batch]
        await asyncio.gather(*tasks)
            
if __name__ == '__main__':
    asyncio.run(main())
        
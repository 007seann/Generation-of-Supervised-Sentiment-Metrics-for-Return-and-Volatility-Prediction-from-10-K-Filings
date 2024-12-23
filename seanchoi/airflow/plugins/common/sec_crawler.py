import datetime
import numpy as np 
import pandas as pd 
from ratelimit import limits, sleep_and_retry
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
from collections import Counter
import re
import csv
from nltk.tokenize import word_tokenize
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_fixed

class LimitRequest:
    SEC_CALL_LIMIT = {'calls': 10, 'seconds': 1}

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))  # Retry up to 5 times with a 2-second delay
    @sleep_and_retry # Handle rate limiting by waiting before the next request
    @limits(calls=SEC_CALL_LIMIT['calls'], period=SEC_CALL_LIMIT['seconds']) # Limit to 10 requests per second
    def _call_sec(url, headers):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        else:
            response.raise_for_status()  # Raise exception for failed requests

    @classmethod
    def get(cls, url, headers):
        return cls._call_sec(url, headers)



def submission_api(cik, ticker, doc_type, headers, start_date, end_date):
    # SEC submissions URL
    rss_url = f'https://data.sec.gov/submissions/CIK{cik}.json'

    # Retrieve the filing data from SEC
    sec_data = requests.get(url=rss_url, headers=headers)

    filings = sec_data.json().get('filings', {}).get('recent', {})

    entries = []

    # Iterate over the filings and filter by type and date range
    for i in range(len(filings['accessionNumber'])):
        filing_date = pd.to_datetime(filings['filingDate'][i])
        filing_type = filings['form'][i]


        if filing_type == doc_type and start_date <= filing_date <= end_date:

            accession_number = filings['accessionNumber'][i].replace('-', '')
            filing_href = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/index.json"

            # Fetch the specific filing details
            filing_response = requests.get(filing_href, headers=headers)

            if filing_response.status_code == 200:
                filing_json = filing_response.json()
                for file in filing_json['directory']['item']:

                    if file['name'].endswith('.htm'):
                        if doc_type.lower() in file['name'] or '10k' in file['name'] or ticker.lower() in file['name']:
                            if 'ex' not in file['name']:
                                html_href = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{file['name']}"
                                entries.append((html_href, filing_type, filing_date))

                    
    return entries


def get_sec_data(cik, ticker, doc_type, headers, start_date, end_date):


    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # SEC XBRL data APIs
    xbrl_url = f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/AccountsPayableCurrent.json'
    sec_data = requests.get(url=xbrl_url, headers=headers)
    entries = []
    processed_accns = set()  # Keep track of processed accession numbers
    try: 
        units = sec_data.json().get('units', {}).get('USD', [])
    except (ValueError, KeyError, requests.exceptions.RequestException) as e:
        print(f"Error: {e}")
        try:
            return submission_api(cik, ticker, doc_type, headers, start_date, end_date)
        except Exception as e:
            print(f"Error: {e}")
    
    for i in range(len(units)):
        filing_date = pd.to_datetime(units[i]['filed'])
        filing_type = units[i]['form']
        filing_accn = units[i]['accn']
        
        # Check if we already processed this accession number
        if filing_accn in processed_accns:
            continue  # Skip this filing since it's already processed
        
        if filing_type == doc_type.upper() and start_date <= filing_date <= end_date:

            filing_href = f"https://www.sec.gov/Archives/edgar/data/{cik}/{filing_accn.replace('-', '')}/index.json"
            filing_response = requests.get(filing_href, headers=headers)
            print('filing_response:', filing_response)
            if filing_response.status_code == 200:
                filing_json = filing_response.json()
                for file in filing_json['directory']['item']:
                    if file['name'].endswith('.htm'):
                        if doc_type.lower() in file['name'] or "".join(doc_type.lower().split("-")) in file['name'] or ticker.lower() in file['name']:
                            if 'ex' not in file['name']:
                                html_href = f"https://www.sec.gov/Archives/edgar/data/{cik}/{filing_accn.replace('-', '')}/{file['name']}"
                                entries.append((html_href, filing_type, filing_date))
            # Add the accession number to the processed set
            processed_accns.add(filing_accn)
    
    entries = list(dict.fromkeys(entries))
    print('entries:', entries)
    return entries

def get_document_type(doc):
    """
    Return the document type lowercased

    Parameters
    ----------
    doc : str
        The document string

    Returns
    -------
    doc_type : str
        The document type lowercased
    """
    
    # Regex explaination : Here I am tryng to do a positive lookbehind
    # (?<=a)b (positive lookbehind) matches the b (and only the b) in cab, but does not match bed or debt.
    # More reference : https://www.regular-expressions.info/lookaround.html
    
    type_regex = re.compile(r'(?<=<TYPE>)\w+[^\n]+') # gives out \w
    type_idx = re.search(type_regex, doc).group(0).lower()
    return type_idx

def get_document_format(doc):
    """
    Return the document type lowercased

    Parameters
    ----------
    doc : str
        The document string

    Returns
    -------
    doc_type : str
        The document type lowercased
    """
    
    format_regex = re.compile(r'(?<=<FILENAME>)\w+[^\n]+') # gives out \w
    doc_type  = re.search(format_regex, doc).group(0).lower()
    if doc_type.endswith((".htm", ".html")):
        return 'HTML'
    if doc_type.endswith(".txt"):
        return 'TXT'
    else:
        return None
    
    
def get_documents(text):
    document_start_regex = re.compile(r'<DOCUMENT>')
    document_end_regex = re.compile(r'<\/DOCUMENT>')
    
    document_start_indices = [match.start() for match in document_start_regex.finditer(text)]
    document_end_indices = [match.start() for match in document_end_regex.finditer(text)]
    
    documents = []
    for start_index, end_index in zip(document_start_indices, document_end_indices):
        document = text[start_index:end_index]
        documents.append(document)
    
    # If the filing is written in the XBRL content
    if not documents:
        # Parse the XBRL content
        documents.append(text)
    
    return documents


def download_fillings(ciks_tickers, root_folder, doc_type, headers, start_date, end_date):
    
    for idx, (cik, ticker) in enumerate(ciks_tickers.items()):

        cik = str(cik).zfill(10)
        
        report_info = get_sec_data(cik, ticker, doc_type, headers, start_date, end_date)


        # check if 10-K exists, otherwise skip it
        if not report_info:
            continue
        else:
            folder_path = os.path.join(root_folder, cik)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

        for index_url, _ , file_date in tqdm(report_info, desc='Downloading {} Fillings'.format(cik), unit='filling'):
            file_date = file_date.strftime('%Y-%m-%d')


            file = LimitRequest.get(url=index_url, headers=headers)


            file_name = os.path.join(folder_path, file_date + '.html')
            with open(file_name,'w+') as f:
                f.write(file.text)
            f.close()

def test(path):
    df = pd.read_csv(path, encoding = 'utf-8')
    QQQ_cik = df['CIK'].drop_duplicates().tolist() 
    
    return QQQ_cik
    

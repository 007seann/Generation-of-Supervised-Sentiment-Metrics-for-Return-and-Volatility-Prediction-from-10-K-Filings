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


QQQ_path = './QQQ_constituents.csv'

try:
    df = pd.read_csv(QQQ_path, encoding = 'utf-8')
    QQQ_cik = df['CIK'].drop_duplicates().tolist()
except UnicodeDecodeError:
    df = pd.read_csv(QQQ_path, encoding = 'ISO-8859-1')
    QQQ_cik = df['CIK'].drop_duplicates().tolist()

# Download reports
class LimitRequest(object):
    SEC_CALL_LIMIT = {'calls': 10, 'seconds': 1}
    @sleep_and_retry
    @limits(calls=SEC_CALL_LIMIT['calls'], period=SEC_CALL_LIMIT['seconds'])
    def _call_sec(url,headers):
        return requests.get(url,headers=headers)
    
    @classmethod
    def get(cls,url,headers):
        return cls._call_sec(url, headers)


def get_sec_data(cik, doc_type, headers,end_date, start_date, start, count):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    rss_url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany' \
        '&CIK={}&type={}&start={}&count={}&owner=exclude&output=atom' \
        .format(cik, doc_type, start, count)
    
    sec_data = LimitRequest.get(url = rss_url,headers=headers)
    soup = BeautifulSoup(sec_data.content, 'xml')    
    entries = [
        (   entry.content.find('filing-href').getText(),
            entry.content.find('filing-type').getText(),
            entry.content.find('filing-date').getText())
        for entry in soup.find_all('entry')
        if pd.to_datetime(entry.content.find('filing-date').getText()) <= end_date and pd.to_datetime(entry.content.find('filing-date').getText()) >= start_date]  
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
    
    return documents

from tqdm import tqdm
def download_fillings(ciks, root_folder, doc_type, headers, end_date=datetime.datetime.now(), start_date = '1990-01-01', start=0, count=60):
    doc_type= doc_type.lower()
    for cik in ciks:
        cik = str(cik).zfill(10)
        folder_path = os.path.join(root_folder, cik)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        report_info = get_sec_data(cik, doc_type, headers, end_date=end_date, start_date=start_date, start=start, count=count)
        for index_url, file_type, file_date in tqdm(report_info, desc='Downloading {} Fillings'.format(cik), unit='filling'):
            if (file_type.lower() == doc_type):
                file_url = index_url.replace('-index.htm', '.txt').replace('.txtl', '.txt')
                file = LimitRequest.get(url=file_url, headers=headers)
                for document in get_documents(file.text):
                    if get_document_type(document) == doc_type and get_document_format(document) == 'HTML':
                        file_name = os.path.join(folder_path, file_date + '.html')
                        with open(file_name,'w+') as f:
                            f.write(document)
                        f.close()
                    if get_document_type(document) == doc_type and get_document_format(document) == 'TXT':
                        file_name = os.path.join(folder_path, file_date + '.txt')
                        with open(file_name,'w+') as f:
                            f.write(document)
                        f.close()
                        
root_folder = 'data'
doc_type = '10-k'
headers = {'User-Agent': 'UOE / 0.1'}
start_date = '2006-01-01',
end_date = datetime.datetime.now()
if not os.path.exists(root_folder):
    os.makedirs(root_folder)
download_fillings(QQQ_cik,root_folder,doc_type,headers,end_date=end_date,start_date=start_date)
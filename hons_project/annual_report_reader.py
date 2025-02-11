#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 2023

@author: Sean Sanggyu Choi
"""

import pandas as pd
import numpy as np
from datetime import datetime
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
from nltk.stem import WordNetLemmatizer
#from nltk.stem.porter import PorterStemmer
#from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import words
# nltk.download('words')
# nltk.download('wordnet')
# nltk.download('omw-1.4')
# nltk.download('stopwords')
# nltk.download('averaged_perceptron_tagger')
from difflib import SequenceMatcher
import re
from collections import Counter
from tqdm import tqdm
from dateutil.parser import parse
import logging
import os

NLTK_DATA_DIR = '/Users/apple/PROJECT/package/nltk_data'
os.environ['NLTK_DATA'] = NLTK_DATA_DIR 
# Check if NLTK data files are available
def check_nltk_data():
    required_datasets = [
        'words',
        'wordnet',
        'omw-1.4',
        'stopwords',
        'averaged_perceptron_tagger'
    ]
    for dataset in required_datasets:
        try:
            nltk.data.find(f'corpora/{dataset}', paths=[NLTK_DATA_DIR])
        except LookupError:
            print(f"Downloading NLTK data: {dataset}")
            nltk.download(dataset, download_dir=NLTK_DATA_DIR)

# Call the function to check and download NLTK data if necessary
check_nltk_data()


def reader(file_name, file_loc):
    file_path = f'{file_loc}/{file_name}'
    print('preprocessing...')
    try:
        # Read the Parquet file
        df = pd.read_parquet(file_path, engine='pyarrow')
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None, None
    
    N = df.shape[0]
    # print(f'--- Total Articles: {N} ---')
    
    #%% DETERMINE VOCABULARY
    
    # print('Extracting types')
    vocab = set()
    for doc in tqdm(range(N)):
        vocab.update(set(word_tokenize(df['Body'][doc].lower())))
    # print(f'- Vocam size: {len(vocab)}')
    
    # Remove non-words
    # print('Removing non-alphabetic tokens')
    vocab = set([w for w in vocab if re.match(r'[^\W\d]*$', w)])
    # print(f'- Vocab size: {len(vocab)}')
    
    # Remove stopwords
    # print('Removing stopwords')
    stop_words = set(stopwords.words('english'))
    vocab -= stop_words
    # print(f' -- Vocab size: {len(vocab)}')
    
    # Remove Lemmatising
    # print('Lemmatising')
    lemmatizer = WordNetLemmatizer()
    vocab = set([lemmatizer.lemmatize(w) for w in vocab])
    # print(f'- Vocab size: {len(vocab)}')
    
    # Remove non-english words ==> also removes proper nouns
    # print('Removing non-english words')
    english_words = set(words.words())
    vocab &= english_words
    # print(f'- Vocab size: {len(vocab)}')
    
    M = len(vocab) # Vocab size
    vocab_list = list(vocab)
    
    #%% CONSTRUCT DOCUMENT_TERM MATRIX
    
    # Function for cleaning text, based on vocabulary
    def clean(text):
        terms = word_tokenize(text.lower())
        terms = [w for w in terms if re.match(r'[^\W\d]*$', w) and not w in stopwords.words('english')]
        terms = [lemmatizer.lemmatize(w) for w in terms]
        terms = [w for w in terms if w in vocab]
        return terms
    
    # Construct document-term matrix
    # print("Constructing document-term matrix")
    data = []
    index = []
    
    for doc in tqdm(range(N)):
        terms = clean(df['Body'][doc])
        term_counts = Counter(terms)
        row = {term: float(term_counts[term]) for term in term_counts if term in vocab_list}
        data.append(row)
        index.append(df['Date'][doc])

    # Create DataFrame from list of dictionaries
    D_df = pd.DataFrame(data, index=index).fillna(0)
    D_df.index = pd.to_datetime(D_df.index)
   
    
    
    """
    # Option 2
    D2 = pd.DataFrame(0, index=df['Date'], columns=vocab_list)
    for doc in tqdm(range(N)):
        terms = clean(df['Body'][doc])
        term_counts = Counter(terms)
        #D2[doc,:] = [term_counts[term] for term in vocab_list]
        for term in list(term_counts):
            D2.loc[df['Date'][doc],term] = term_counts[term]
    print('\n')
    """
    
    return D_df
    
    
    
    
    
    
    
    

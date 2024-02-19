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
nltk.download('words')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')
from difflib import SequenceMatcher
import re
from collections import Counter
from tqdm import tqdm
from dateutil.parser import parse

def reader(file_name, file_loc='/Users/apple/PROJECT/Code_4_10k/risk_factors'):
    # df = pd.read_csv(file_dir)
    df = pd.read_csv(f'{file_loc}/{file_name}', header = 0)
    # df = df.drop(df.columns[0], axis = 1) # Drop index column
    # df = df.drop(columns = [])
    N = df.shape[0]
    print(f'--- Total Articles: {N} ---')
    
    #%% DETERMINE VOCABULARY
    
    print('Extracting types')
    vocab = set()
    for doc in tqdm(range(N)):
        vocab = vocab.union(set(word_tokenize(df['Body'][doc].lower())))
    
    print(f'- Vocam size: {len(vocab)}')
    
    # Remove non-words
    print('Removing non-alphabetic tokens')
    vocab = set([w for w in vocab if re.match(r'[^\W\d]*$', w)])
    print(f'- Vocab size: {len(vocab)}')
    
    # Rmove stopwords
    print('Removing stopwords')
    vocab = vocab.difference(set(stopwords.words('english')))
    print(f' -- Vocab size: {len(vocab)}')
    
    # Remove Lemmatising
    print('Lemmatising')
    lemmatizer = WordNetLemmatizer()
    vocab = set([lemmatizer.lemmatize(w) for w in vocab])
    print(f'- Vocab size: {len(vocab)}')
    
    # Remove non-english words ==> also removes proper nouns
    print('Removing non-english words')
    vocab = vocab.intersection(words.words())
    print(f'- Vocab size: {len(vocab)}')
    
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
    
    print('Constructing document-term matrix')
    D = np.zeros((N , M)) # NxM document-term matrix
    
    for doc in tqdm(range(N)):
        terms = clean(df['Body'][doc])
        term_counts = Counter(terms)
        D[doc,:] = [term_counts[term] for term in vocab_list]
    print('\n')
    
    # Converting to dataframe
    D_df = pd.DataFrame(D, index=df['Date'], columns=vocab_list)
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
    
    
    
    
    
    
    
    

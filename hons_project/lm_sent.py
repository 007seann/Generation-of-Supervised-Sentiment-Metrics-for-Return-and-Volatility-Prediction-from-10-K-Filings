#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11
Updated on The Feb 18
@author: Sean Sanggyu Choi
"""
import pandas as pd
import os
SAVE_PATH = "./data/SP500/LM/SEC"
input_path = "./data/SP500/SEC/SEC_DTM_SP500.parquet"

lm = pd.read_csv("LM_dict.csv")
lm = lm[['Word','Negative','Positive','Uncertainty']]

neg_words = []
for x in list(set(lm['Negative']).difference(set([0]))):
    neg_words += list(lm['Word'][lm['Negative'] == x])
neg_words = [w.lower() for w in neg_words]
pos_words = []
for x in list(set(lm['Positive']).difference(set([0]))):
    pos_words += list(lm['Word'][lm['Positive'] == x])
pos_words = [w.lower() for w in pos_words]
unc_words = []
for x in list(set(lm['Positive']).difference(set([0]))):
    unc_words += list(lm['Word'][lm['Uncertainty'] == x])
unc_words = [w.lower() for w in unc_words]
lm_dict = dict.fromkeys(pos_words, 1)
lm_dict.update(dict.fromkeys(neg_words, -1))
lm_words = set(neg_words + pos_words)
lm_df = pd.concat([pd.Series(1, index = pos_words), pd.Series(-1, index=neg_words)])


#%% Intuitive, i.e. substracting neg from pos
# comps = ['0000320193', '0000789019', '0001018724', "0001730168", "0001326801",
#                     '0001045810', '0001318605', '0001652044', '0000909832']

# Read firms' ciks
constituents_path =  "../Code_4_SECfilings/sp500_total_constituents_final.csv"
firms_df = pd.read_csv(constituents_path)
columns_to_drop = ['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded']
firms_df = firms_df.drop(columns=columns_to_drop, errors='ignore')
firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
firms_dict = {value: key for key, value in firms_dict.items()}
firms_ciks = list(firms_dict.keys())

# seen = set()
# firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 


def processing_LM_individually(ciks, path):
    df_lm = pd.DataFrame()
    first = True
    for cik in ciks:
        print(f'--- Processing {cik} ---')
        print('Opening file')
        
        filepath = f'{path}/dtm_{cik}.parquet'
        if not os.path.exists(filepath):
            continue
        else:
            try:
                comb = pd.read_parquet(filepath)

                comb = comb.set_index('Date')
                print('Finding LM sent words')
                arts = comb.drop(columns = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])
                total_words = arts.sum(axis=1)
                words_in_doc = set(arts.columns)
                ww = list(words_in_doc.intersection(lm_words))
                # ww = list(words_in_doc.intersection(set(neg_words)))
                print(f'{len(ww)} terms in LM list')
                arts_ww = arts[ww]
                lm_ww = lm_df[ww]
                lm_sent = pd.DataFrame((arts_ww @ lm_ww) / total_words, index = comb.index)
                lm_sent['_cik'] = cik
                if first:
                    df_lm = lm_sent
                    first = False
                else:
                    df_lm = pd.concat([df_lm, lm_sent], axis=0)
            except Exception as e:
                print(f"Error processing file: {f} for CIK {cik}: {e}")

    return df_lm

def processing_LM(ciks, file_path):
    df_lm = pd.DataFrame()
    first = True
    comb = pd.read_parquet(file_path)
    
    for cik in ciks:
        print(f'--- Processing {cik} ---')
        comb_cik = comb[comb["_cik"] == cik]
        comb_cik = comb_cik.set_index('Date')
        arts = comb_cik.drop(columns = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])
        total_words = arts.sum(axis=1)
        words_in_doc = set(arts.columns)
        ww = list(words_in_doc.intersection(lm_words))
        # ww = list(words_in_doc.intersection(set(neg_words)))
        print(f'{len(ww)} terms in LM list')
        arts_ww = arts[ww]
        lm_ww = lm_df[ww]
        lm_sent = pd.DataFrame((arts_ww @ lm_ww) / total_words, index = comb_cik.index)
        lm_sent['_cik'] = cik
        if first:
            df_lm = lm_sent
            first = False
        else:
            df_lm = pd.concat([df_lm, lm_sent], axis=0)
            
    return df_lm
    


df = processing_LM(firms_ciks, input_path)
df = df.rename(columns={0:'_lm'})
# df = df.drop_duplicates()
df = df.reset_index().rename(columns={'index':'Date'})
df.to_parquet(os.path.join(SAVE_PATH,f'lm_sent_SEC_test2.parquet'))

    

# first = True
# for cik in firms_ciks:
#     print(f'--- Processing {cik} ---')
#     print('Opening file')
    
#     filepath = f'/Users/apple/PROJECT/Hons-project/data/all/df_all_{cik}.csv'
#     if not os.path.exists(filepath):
#         continue
#     else:
#         # comb = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/all/df_all_{cik}.csv', header=0)
#         # comb = pd.read_parquet('/Users/apple/PROJECT/hons_project/data/SP500/SEC/SEC_DTM.parquet')
#         cik_folder = os.path.join(input_path, cik)
#         all_files = [ os.path.join(cik_folder, f) for f in os.listdir(cik_folder) if f.endswith('.parquet') ]
#         for f in all_files:
#             comb = pd.read_parquet(f)

#             comb = comb.set_index('Date')
#             print('Finding LM sent words')
#             arts = comb.drop(columns = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])
#             total_words = arts.sum(axis=1)
#             words_in_doc = set(arts.columns)
#             ww = list(words_in_doc.intersection(lm_words))
#             # ww = list(words_in_doc.intersection(set(neg_words)))
#             print(f'{len(ww)} terms in LM list')
#             arts_ww = arts[ww]
#             lm_ww = lm_df[ww]
#             lm_sent = pd.DataFrame((arts_ww @ lm_ww) / total_words, index = comb.index)
#             lm_sent['_cik'] = cik
#             if first:
#                 df_lm = lm_sent
#                 first = False
#             else:
#                 df_lm = pd.concat([df_lm, lm_sent], axis=0)
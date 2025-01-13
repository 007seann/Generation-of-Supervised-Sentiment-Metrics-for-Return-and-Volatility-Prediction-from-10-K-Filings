#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11

@author: Sean Sanggyu Choi
"""
import pandas as pd
import os
SAVE_PATH = "/Users/apple/PROJECT/Hons-project/data"
lm = pd.read_csv("/Users/apple/PROJECT/Hons-project/LM_dict.csv")
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
print('lm_df', lm_df)

#%% Intuitive, i.e. substracting neg from pos
# comps = ['0000320193', '0000789019', '0001018724', "0001730168", "0001326801",
#                     '0001045810', '0001318605', '0001652044', '0000909832']

# Read firms' ciks
QQQfirms_csv_file_path =  "/Users/apple/PROJECT/Code_4_10k/QQQ_constituents.csv"
firms_df = pd.read_csv(QQQfirms_csv_file_path)
firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
seen = set()
firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 


first = True
for cik in firms_ciks:
    print(f'--- Processing {cik} ---')
    print('Opening file')
    
    filepath = f'/Users/apple/PROJECT/Hons-project/data/all/df_all_{cik}.csv'
    if not os.path.exists(filepath):
        continue
    else:
        comb = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/all/df_all_{cik}.csv', header=0)
        print("cik", cik)
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
        
df_lm = df_lm.rename(columns={0:'_lm'})
# df_lm.to_csv(os.path.join(SAVE_PATH,f'lm_sent_all_QQQ_2.csv'))
# df_lm.to_csv(SAVE_PATH, f'lm_sent_{cik}.csv')
    
    
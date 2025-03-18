#%% PACKAGES
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 10:34:18 2023

@author: Sean Sanggyu Choi
"""


import os
os.chdir('/Users/apple/PROJECT/hons_project')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col
import seaborn as sns
import time
import sys
from time_log_decorator import time_log
import pyarrow.parquet as pq

from model import train_model, predict_sent, loss, loss_perc, kalman
from vol_reader_fun import vol_reader2, price_reader, vol_reader




############################ #%% Sector level ############################

constituents_path =  "../Code_4_SECfilings/sp500_total_constituents_final.csv"
firms_df = pd.read_csv(constituents_path)
columns_to_drop = ['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded']
firms_df = firms_df.drop(columns=columns_to_drop, errors='ignore')
firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
firms_dict = {value: key for key, value in firms_dict.items()}
firms_ciks = list(firms_dict.keys())
# Need 2025 SEP portfolio and its constituents

weights_file_path =  "../Code_4_SECfilings/sp500_2025_weights.csv"
weight_df = pd.read_csv(weights_file_path)
comp_to_weight_value = pd.Series(weight_df.Weight.values, index=weight_df.CIK).to_dict()
comp_to_weight_value = {str(k).zfill(10) : v for k, v in comp_to_weight_value.items()}
NAME = 'US Market'
TEMP = 'S&P 500'
DATA = 'transcripts'
PORT = 'equal' # 'value' or 'equal'. 'equal' is for a single firm only. 'value' is for a sector portfolio. You should controls allocaiton proporitons.
# What if just get SP500 historical data, and put and compare them?

fig_loc = f'./outcome/figures_df_{DATA}'
if not os.path.exists(fig_loc):
    os.makedirs(fig_loc)
    
input_path = "./data/SP500/transcripts_ninjaAPI/dtm/transcripts_DTM_SP500_2.parquet"
dataset = pq.ParquetDataset(input_path)
table = dataset.read()
df_all = table.to_pandas()
df_all = df_all.reset_index()
df_all = df_all.sort_values(by=['Date', '_cik']).reset_index(drop=True)
df_all = df_all.drop(columns=["level_0"], errors='ignore')
df_all = df_all.set_index('Date')
df_all.index = pd.to_datetime(df_all.index, utc=True)
df_all.index = df_all.index.to_series().dt.strftime('%Y-%m-%d')
df_all['_ret'] = df_all['_ret']/100
input_path = "./data/SP500/LM/transcripts/lm_sent_transcripts_SP500.parquet"
dataset = pq.ParquetDataset(input_path)
table = dataset.read()
lm_sent = table.to_pandas()
lm_sent = lm_sent.reset_index()
lm_sent = lm_sent.sort_values(by=['Date', '_cik']).reset_index(drop=True)
lm_sent = lm_sent.drop(columns=["index"], errors='ignore')
lm_sent = lm_sent.set_index('Date')
lm_sent.index = pd.to_datetime(lm_sent.index, utc=True)
lm_sent.index = lm_sent.index.to_series().dt.strftime('%Y-%m-%d')
# print('length check', len(lm_sent), len(df_all))
# print('lm_sent', lm_sent)
# print('df_all', df_all)
assert all(lm_sent.index == df_all.index) and all(lm_sent['_cik'] == df_all['_cik'])


############################ #%% Portfolio level ############################
# QQQfirms_csv_file_path =  "/Users/apple/PROJECT/Code_4_10k/top10_QQQ_constituents.csv"
# firms_df = pd.read_csv(QQQfirms_csv_file_path)
# firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
# firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
# seen = set()
# firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 

# comp_to_weight_value = {
#     '0000320193': 7.44,  # Apple
#     '0000789019': 8.60,  # Microsoft
#     '0001018724': 5.17,  # Amazon
#     '0001730168': 4.80,   # Broadcom
#     '0001326801': 5.12,   # Meta (formerly Facebook)
#     '0001045810': 6.48,   # NVIDIA
#     '0001318605': 2.43,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
#     '0001652044': 4.43, # Alphabet (Google)
#     '0000909832': 2.54    # Costco
# }
# NAME = 'Portfolio(Top10)'
# TEMP = 'Top10'
# DATA = 'all_top10_2'
# PORT = 'value' 

# fig_loc = f'/Users/apple/PROJECT/Hons-project/figures_df_{DATA}'
# if not os.path.exists(fig_loc):
#     os.makedirs(fig_loc)
# df_all = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/df_{DATA}.csv')
# df_all = df_all.set_index('Date')
# df_all.index = pd.to_datetime(df_all.index)
# df_all['_ret'] = df_all['_ret']/100

# lm_sent = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/lm_sent_{DATA}.csv')
# lm_sent = lm_sent.set_index('Date')
# lm_sent.index = pd.to_datetime(lm_sent.index)

############################# #%% Company level ############################

# firms_ciks = ['0001045810']

# comp_to_weight_value = {
#     '0000320193': 7.44,  # Apple
#     '0000789019': 8.60,  # Microsoft
#     '0001018724': 5.17,  # Amazon
#     '0001730168': 4.80,   # Broadcom
#     '0001326801': 5.12,   # Meta (formerly Facebook)
#     '0001045810': 6.48,   # NVIDIA
#     '0001318605': 2.43,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
#     '0001652044': 4.43, # Alphabet (Google)
#     '0000909832': 2.54    # Costco
# }
# NAME = 'Nvidia'
# TEMP = 'Nvidia'
# DATA = 'all_0001045810'
# PORT = 'equal' 

# fig_loc = f'/Users/apple/PROJECT/Hons-project/figures_df_{DATA}'
# if not os.path.exists(fig_loc):
#     os.makedirs(fig_loc)
# df_all = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/df_{DATA}.csv')
# df_all = df_all.set_index('Date')
# df_all.index = pd.to_datetime(df_all.index)
# df_all['_ret'] = df_all['_ret']/100

# lm_sent = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/lm_sent_{DATA}.csv')
# lm_sent = lm_sent.set_index('Date')
# lm_sent.index = pd.to_datetime(lm_sent.index)

################################################################################################################



def adj_kappa(k, k_min = 0.85):
    return 1-(1-k)/(1-k_min)

#%% INPUT
start_date = '2006-01-01'
end_date = '2024-12-31'
trn_window = ['2006-01-01', '2024-12-31']
val_window = ['2020-01-01', '2024-12-31']
window = ['2006-01-01', '2024-12-31']


#%% MODEL TRAINING & IN_SAMPLE PREDICTIONS
# Hyperparameters
kappa = adj_kappa(0.9) # 90% quantile of the count distribution
alpha_high = 100 # 50 words in both groups
alpha_low = alpha_high
llambda = 0.1 # Prenalty to shrink estimated sentiment towards 0.5 (i.e neutral)

# Train model
df_trn = df_all.sort_index()[:f'{trn_window[1]}']
df_val = df_all.sort_index()[val_window[0]:val_window[1]]
t0 = time.time()
for dep in ['_ret', '_vol']:
    
    if dep == '_ret':
        mod = train_model(df_trn, dep, kappa, alpha_high, pprint = False)
        S_pos_ret, S_neg_ret = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_ret = mod
    else:
        mod = train_model(df_trn, dep, kappa, alpha_high, pprint = False, vol_q = None)
        S_pos_vol, S_neg_vol = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_vol = mod
        # mod_vol2 = train_model(df_trn, dep, kappa, alpha_high, pprint = False, vol_q = None)
    
    # Make predictions on training data
    train_set = df_trn
    train_arts = train_set.drop(columns= ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])
    train_y = train_set[dep]

    preds = predict_sent(mod, train_arts, llambda)

    
    print(f'All estimated sentiments in [{round(preds.min(),3)},{round(preds.max(), 3)}]')
    ll = loss(preds, train_y)
    print(f'Loss: {round(ll,4)} (Benchmark = 0.25)')
    ll2 = loss_perc(preds, train_y, marg=0.02)[0]
    print(f'Correct (%): {round(ll2,4)} (Benchmark = 0.5)')
    print('\n')
    
    
    if dep == '_ret':
        mod_sent_ret = pd.Series(preds, index=df_trn.index)

    else:
        mod_sent_vol = pd.Series(preds, index=df_trn.index)
        

t1 = time.time()
print(f'Total tranining time: {t1-t0}')
#%% Most Impractful Words
## print
def impactful_words(mod, dep):
    df_mod = pd.DataFrame(mod[1], index = mod[0], columns=['0+', '0-'])
    plt.plot(df_mod['0+'] - df_mod['0-'])
    plt.xticks(fontsize=4)
    plt.xticks(rotation=90)
    plt.savefig(f'{fig_loc}/most_impactful_words'+'_'+dep, dpi=500)
    # plt.show()

    S_hat = mod[0]
    alpha = int(len(S_hat)/2)
    S_pos, S_neg = S_hat[:alpha], S_hat[alpha:]
    O_hat = mod[1]
    tone = O_hat[:, 0] - O_hat[:, 1]
    tone_pos, tone_neg = tone[:alpha], tone[alpha:]

    pos_ind = np.argsort(-tone_pos)
    neg_ind = np.argsort(tone_neg)
    print(f"Positive words: {S_pos[pos_ind[:15]]}")
    print(f'Negative worsds: {S_neg[neg_ind[:15]]}')

    df_pos = pd.DataFrame(tone_pos[pos_ind], index = S_pos[pos_ind], columns = ['tone'])[:15]
    df_pos.to_csv(f'{fig_loc}/most_impactful_pos_words'+'_'+dep +'.csv', index=True)

    df_neg = pd.DataFrame(tone_neg[neg_ind], index = S_neg[neg_ind], columns = ['tone'])[:15]
    df_neg.to_csv(f'{fig_loc}/most_impactful_neg_words'+'_'+dep +'.csv', index=True)
    

for i, mod  in enumerate([mod_ret, mod_vol]):
    if i == 0:
        impactful_words(mod, 'ret')
    if i == 1:
        impactful_words(mod, 'vol')

    

#%% Sentiment predictions
def rescale(x, unit=True):
    if unit:
        return (x - x.min())/(x.max() - x.min())
    else:
        return (x - x.mean() + 0.5)
    
    
# Plotting portfolio
dfts, firms_ciks = price_reader(firms_ciks, firms_dict, trn_window[0], trn_window[1])

print('--- Constructing portfolio ---')
if PORT == 'equal':
    port_val = dfts.mean(axis=1)

    print("Hi you are using equal portfolio")

    
else:
    if PORT == 'value':
        port_weights = np.array([comp_to_weight_value[c] for c in firms_ciks])
        port_weights = port_weights/sum(port_weights)
        weight_ret = pd.DataFrame(pd.Series(port_weights, index=dfts.columns, name=0))
        port_val = dfts.dot(weight_ret[0])

        print("Hi you are using value portfolio")


    
# Ret sentiments

print(f'% of neutral sentiments RET: {round((mod_sent_ret == 0.5).sum()/len(mod_sent_ret) * 100, 2)}')
mod_avg_ret = mod_sent_ret.groupby(mod_sent_ret.index).mean()
mod_kal_ret = kalman(mod_avg_ret, smooth=True)


# Save data points to CSV files
mod_avg_ret.to_csv(f'{fig_loc}/mod_avg_ret.csv')
mod_kal_ret.to_csv(f'{fig_loc}/mod_kal_ret.csv')

# Vol sentiments
print(f'% of neutral sentiments VOL: {round((mod_sent_vol == 0.5).sum()/len(mod_sent_vol) * 100, 2)}')
mod_avg_vol = mod_sent_vol.groupby(mod_sent_vol.index).mean()
mod_kal_vol = kalman(mod_avg_vol, smooth=False)

# Save data points to CSV files
mod_avg_vol.to_csv(f'{fig_loc}/mod_avg_vol.csv')
mod_kal_vol.to_csv(f'{fig_loc}/mod_kal_vol.csv')

# LM sentiments

print("length check", lm_sent['_lm'].sort_index())
lm_trn = lm_sent['_lm'].sort_index()[:f'{trn_window[1]}']

print(f'% of netural sentiments LM: {round(lm_trn == 0).sum()/len(lm_trn) * 100, 2}')
lm_avg = lm_trn.groupby(lm_trn.index).mean()
lm_avg = (lm_avg - lm_avg.min())/(lm_avg.max() - lm_avg.min())
lm_kal = kalman(lm_avg, smooth=True)

# Save data points to CSV files
lm_avg.to_csv(f'{fig_loc}/lm_avg.csv')
lm_kal.to_csv(f'{fig_loc}/lm_kal.csv')


port_val = port_val.groupby(port_val.index).mean()
mod_sent_ret = mod_sent_ret.groupby(mod_sent_ret.index).mean()
mod_sent_vol = mod_sent_vol.groupby(mod_sent_vol.index).mean()
lm_trn = lm_trn.groupby(lm_trn.index).mean()
port_val_aligned = port_val.reindex(mod_sent_ret.index)
port_val_aligned.dropna(inplace=True)

sents = pd.concat([mod_sent_ret, mod_sent_vol, lm_trn, port_val_aligned], axis = 1)
sents.columns = ['ret', 'vol', 'lm', 'stock']


#pearson
print('p_hat pearson correlation')
print(sents.corr(method='pearson'))

sents_tilde = pd.concat([mod_kal_ret, mod_kal_vol, lm_kal, port_val_aligned], axis = 1)
sents_tilde.columns = ['ret', 'vol', 'lm', 'stock']

# Save portfolio data points
port_val.to_csv(f'{fig_loc}/port_val.csv')

# Save correlation data points
sents.to_csv(f'{fig_loc}/sents.csv')
sents_tilde.to_csv(f'{fig_loc}/sents_tilde.csv')

# Save other necessary data points
port_val_aligned.to_csv(f'{fig_loc}/port_val_aligned.csv')



    
    
#%% PACKAGES
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 10:34:18 2023

@author: Sean Sanggyu Choi
"""

# Reference Note
# TO DO:
# Align articles to 2/3-day volatility (instead of same-day) -> maybe not?
# Investigate lambda (why can I not use values like 1,5,10, but rather have to use 0.1)
# Check estimates of O+,O- are correct:
    # There is now a near-linear relationship between parameters O+ and O-
    # The term with the highest significance ('japan') is the one with the highest freq count in D_hat
    # Linear relationship between term count and estimate in both O+ and O-:
        #x=[]
        #for i in D_hat.columns:
        #    x.append(D_hat[i].sum())
        #plt.plot(x,O_hat[:,0],'r.')
# Re-write loss function (for loss2, might now be very beneficial to just predict most arts as neutral)
# Complete hyperparameter grid search, by rolling window validation for three years

# Create basic regression tables in latex for returns and volatility prediction


import os
os.chdir('/Users/apple/PROJECT/Hons-project')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col
import seaborn as sns
import time

from model import train_model, predict_sent, loss, loss_perc, kalman
from vol_reader_fun import vol_reader2, price_reader



def adj_kappa(k, k_min = 0.85):
    return 1-(1-k)/(1-k_min)

#%% INPUT
start_date = '2006-01-01'
end_date = '2023-12-31'
# trn_window = ['2006-01-01', '2019-12-31']
val_window = ['2020-01-01', '2023-12-31']
trn_window = ['2006-01-01', '2023-12-31']
# comps = ['0000320193']
comps = ['0000320193', '0000789019', '0001018724', "0001730168", "0001326801",
                    '0001045810', '0001318605', '0001652044', '0000909832']
#%% READ ALL DATA
NAME = 'Technology Sector '
DATA = 'top10_2'
fig_loc = f'/Users/apple/PROJECT/Hons-project/figures_df_{DATA}'
if not os.path.exists(fig_loc):
    os.makedirs(fig_loc)
df_all = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/df_{DATA}.csv')
df_all = df_all.set_index('Date')
df_all.index = pd.to_datetime(df_all.index)
df_all['_ret'] = df_all['_ret']/100

lm_sent = pd.read_csv(f'/Users/apple/PROJECT/Hons-project/data/lm_sent_{DATA}.csv')
lm_sent = lm_sent.set_index('Date')
lm_sent.index = pd.to_datetime(lm_sent.index)

#%% MODEL TRAINING & IN_SAMPLE PREDICTIONS
# Hyperparameters
kappa = adj_kappa(0.9) # 90% quantile of the count distribution
alpha_high = 100 # 50 words in both groups
alpha_low = alpha_high
llambda = 0.1 # Prenalty to shrink estimated sentiment towards 0.5 (i.e neutral)

# Train model
df_trn = df_all.sort_index()[:'2023-12-31']
t0 = time.time()
for dep in ['_ret', '_vol']:
    
    if dep == '_ret':
        mod = train_model(df_trn, dep, kappa, alpha_high, pprint = False)
        S_pos_ret, S_neg_ret = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_ret = mod
    else:
        mod = train_model(df_trn, dep, kappa, alpha_high, pprint = False, vol_q = 0.8)
        S_pos_vol, S_neg_vol = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_vol = mod
        mod_vol2 = train_model(df_trn, dep, kappa, alpha_high, pprint = False, vol_q = None)
    
    # Define test set (now equal to train set)
    test_set = df_trn
    test_arts = test_set.drop(columns= ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])
    test_y = test_set[dep]
    
    # Make predictions
    print('mod', mod)
    preds = predict_sent(mod, test_arts, llambda)
    print('preds', preds)
    print(f'All estimated sentiments in [{round(preds.min(),3)},{round(preds.max(), 3)}]')
    ll = loss(preds, test_y)
    print(f'Loss: {round(ll,4)} (Benchmark = 0.25)')
    ll2 = loss_perc(preds, test_y, marg=0.02)[0]
    print(f'Correct (%): {round(ll2,4)} (Benchmark = 0.5)')
    
    if dep == '_ret':
        mod_sent_ret = pd.Series(preds, index=df_trn.index)

    else:
        mod_sent_vol = pd.Series(preds, index=df_trn.index)

t1 = time.time()
print(f'Total tranining time: {t1-t0}')

#%% Most Impractful Words
## print
def impactful_words(mod):
    df_mod = pd.DataFrame(mod[1], index = mod[0], columns=['0+', '0-'])
    plt.plot(df_mod['0+'] - df_mod['0-'])
    plt.xticks(fontsize=4)
    plt.xticks(rotation=90)
    plt.savefig(f'{fig_loc}/most_impactful_words', dpi=500)
    plt.show()

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
    df_neg = pd.DataFrame(tone_neg[neg_ind], index = S_neg[neg_ind], columns = ['tone'])[:15]

    plt.bar(df_pos.index,df_pos['tone'])
    plt.xticks(rotation=30)
    plt.savefig(f'{fig_loc}/most_impactful_pos_words', dpi=500)
    plt.show()

    plt.bar(df_neg.index,df_neg['tone'])
    plt.xticks(rotation=30)
    plt.savefig(f'{fig_loc}/most_impactful_neg_words', dpi=500)
    plt.show()
    
for mod in [mod_ret, mod_vol]:
    impactful_words(mod)

#%% Sentiment predictions
def rescale(x, unit=True):
    if unit:
        return (x - x.min())/(x.max() - x.min())
    else:
        return (x - x.mean() + 0.5)
    
# Ret sentiments
print(f'% of neutral sentiments RET: {round((mod_sent_ret == 0.5).sum()/len(mod_sent_ret) * 100, 2)}')
mod_avg_ret = mod_sent_ret.groupby(mod_sent_ret.index).mean()
mod_kal_ret = kalman(mod_avg_ret, smooth=True)
plt.plot(mod_avg_ret, label = 'unfiltered')
plt.plot(mod_kal_ret, '--', label = 'filtered')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc='upper right')
plt.title('RET Sentiment')
plt.tight_layout()
plt.savefig(f'{fig_loc}/ret_filter', dpi=500)
plt.show()

# Vol sentiments
print(f'% of neutral sentiments VOL: {round((mod_sent_vol == 0.5).sum()/len(mod_sent_vol) * 100, 2)}')
mod_avg_vol = mod_sent_vol.groupby(mod_sent_vol.index).mean()
mod_kal_vol = kalman(mod_avg_vol, smooth=True)
plt.plot(mod_avg_vol, label = 'unfiltered')
plt.plot(mod_kal_vol, '--', label = 'filtered')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc = 'upper right')
plt.title('VOL Sentiment')
plt.tight_layout()
plt.savefig(f'{fig_loc}/vol_filter', dpi=500)
plt.show()

# LM sentiments
assert all(lm_sent.index == df_all.index) and all(lm_sent['_cik'] == df_all['_cik'])
lm_trn = lm_sent['_lm'].sort_index()[:'2023-12-31']
print(lm_trn)
print(f'% of netural sentiments LM: {round(lm_trn == 0).sum()/len(lm_trn) * 100, 2}')
lm_avg = lm_trn.groupby(lm_trn.index).mean()
lm_avg = (lm_avg - lm_avg.min())/(lm_avg.max() - lm_avg.min())
lm_kal = kalman(lm_avg, smooth=True)
plt.plot(lm_avg, label = 'unfiltered')
plt.plot(lm_kal, label = 'filtered')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc = 'upper right')
plt.title('LM Sentiments')
plt.tight_layout()
plt.savefig(f'{fig_loc}/LM_filter', dpi=500)
plt.show()

# Correlation
print("mod_sent_ret", mod_sent_ret)
print("mod_rec_ret", mod_sent_vol)
print("lm_trn", lm_trn)
sents = pd.concat([mod_sent_ret, mod_sent_vol, lm_trn], axis = 1)
sents.columns = ['ret', 'vol', 'lm']
print('p_hat correlation')
print(sents.corr())

sents_tilde = pd.concat([mod_kal_ret, mod_kal_vol, lm_kal], axis = 1)
sents_tilde.columns = ['ret', 'vol', 'lm']
sents_tilde.to_csv("sents_tilde.csv")
print('p_tilde correlation')
print(sents_tilde.corr())

from scipy.stats.stats import pearsonr
pearsonr(sents['ret'], sents['vol'])



# plt.plot(mod_kal_ret, label = 'RET')
# plt.plot(mod_kal_vol, label = 'VOL')
# plt.plot(lm_kal + 0.5 - lm_kal.mean(), label = 'LM')
# plt.ylabel('Sentiment')
# plt.xlabel('Date')
# plt.legend(loc = 'upper right')
# plt.title(f'{NAME} Sentiment Prediction')
# plt.savefig(f'{fig_loc}/{NAME} Sentiment Prediction', dpi=500)
# plt.show()

# Plotting
dfts = price_reader(comps, trn_window[0], trn_window[1])
fig, ax = plt.subplots()
ax.plot(dfts.mean(axis=1), color = 'silver', linestyle = 'dashed', label = 'QQQ Stock')
ax.set_xlabel("Date")
ax.set_ylabel("QQQ Stock")
ax2 = ax.twinx()
ax2.plot(mod_kal_ret, label = 'RET')
ax2.plot(mod_kal_vol, label = 'VOL')
ax2.plot(lm_kal + 0.5 - lm_kal.mean(), label = "LM")
ax2.set_ylabel('Sentiment')
fig.legend(bbox_to_anchor = (0.33, 0.7))
fig.autofmt_xdate(rotation=50)
plt.title(f'{NAME} Sentiment Prediction')
plt.savefig(f'{fig_loc}/{NAME} Sentiment Prediction', dpi=500)
plt.show()


# print('-----------comparision----------')
# # Plotting portfolio
# dfts = price_reader(comps, trn_window[0], trn_window[1])
# port = 'equal'


# # Weights
# comp_to_weight_value = {
#     '0000320193': 1287,  # Apple
#     '0000789019': 1200,  # Microsoft
#     '0001018724': 920.22,  # Amazon
#     '0001730168': 125,   # Broadcom
#     '0001326801': 585.37,   # Meta (formerly Facebook)
#     '0001045810': 144,   # NVIDIA
#     '0001318605': 75.71,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
#     '0001652044': 921.13, # Alphabet (Google)
#     '0000909832': 129.84    # Costco
# }
# print('--- Constructing portfolio ---')
# if port == 'equal':
#     port_val = dfts.mean(axis=1)
#     print("Hi you are using equal portfolio")
# else:
#     if port == 'value':
#         port_weights = np.array([comp_to_weight_value[c] for c in comps])
#     port_weights = port_weights/sum(port_weights)
#     weight = pd.DataFrame(pd.Series(port_weights, index=dfts.columns, name=0))
#     port_val = dfts.dot(weight[0])
#     print("Hi you are using value portfolio")

# fig, ax = plt.subplots()
# ax.plot(port_val, color = 'silver', linestyle = 'dashed', label = 'Portfolio')
# ax.set_xlabel('Date')
# ax.set_ylabel('Portfolio value')
# ax2 = ax.twinx()
# ax2.plot(mod_kal_ret, label = "RET" )
# ax2.plot(mod_kal_vol, label = "VOL" )
# ax2.plot(lm_kal, label = 'LM' )
# ax2.set_ylabel('Sentiment')
# fig.legend(bbox_to_anchor = (0.33, 0.7))

# #%% ECONOMETRIC VALIDATION: LM & MODEL predicitons
# print('--- Splitting data set ---')
# df_sorted = df_all.sort_index()
# df_trn = df_sorted[trn_window[0]:trn_window[1]]
# df_val = df_sorted[val_window[0]:val_window[1]]
# val_arts = df_val.drop(columns = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1'])

# print('CONSTRUCTING lm PERDICTIONS')
# preds_lm = lm_sent['_lm'].sort_index()[val_window[0]:val_window[1]]
# df_plm = pd.Series(preds_lm, index=df_val.index).sort_index()
# df_plm2 = df_plm.groupby(df_plm.index).mean()
# lm_trn = lm_sent['_lm'].sort_index()[:'2019-12-31']
# lm_avg = lm_trn.groupby(lm_trn.index).mean()
# df_plm2 = (df_plm2 - lm_avg.min())/(lm_avg.max() - lm_avg.min())
# df_plm3 = kalman(df_plm2).to_frame('lm')
# plt.plot(df_plm2)
# plt.plot(df_plm3, '--')
# plt.title('LM')
# plt.xticks(rotation=50)
# plt.show()

# df_phat = df_plm
# df_ptil = df_plm3

# # Input
# for dep in ['ret', 'vol']:
#     print(f'CONSTRUCTING {dep} PREDICTIONS')
#     kappa = adj_kappa(0.90) # 90% quantile of the count distribution
#     alpha_high = 100 # 50  words in both groups
#     alpha_low = alpha_high
#     llambda = 0.1
#     trn_with_fut = False
    
#     if trn_with_fut:
#         depdep = f'_{dep}+1'
#     else:
#         depdep = f'_{dep}'
#     print('--- Training model ---')
#     if dep == 'ret':
#         mod = train_model(df_trn, depdep, kappa, alpha_high, pprint = False)
#     else:
#         assert dep == 'vol', 'Wrong dependent variable name'
#         mod = train_model(df_trn, depdep, kappa, alpha_high, pprint = False, vol_q=0.8)
        
#     print('--- Making predictions ---')
#     preds_val = predict_sent(mod, val_arts, llambda)
#     df_preds = pd.Series(preds_val, index=df_val.index)
#     df_preds = df_preds.sort_index()
#     df_preds2 = df_preds.groupby(df_preds.index).mean()
    
#     news_counts = df_preds.groupby(df_preds.index).count().to_frame('volume')
    
#     print('--- Smoothing ---')
#     df_preds3 = kalman(df_preds2).to_frame('sent')
#     plt.plot(df_preds2, label = 'averaged')
#     plt.plot(df_preds3, '--', label='filtered')
#     plt.xticks(rotation=30)
#     plt.legend(loc = 'upper right')
#     plt.xlabel('Date')
#     plt.ylabel('Sentiment')
#     plt.title(dep.upper())
#     plt.savefig(f'{fig_loc}/{dep}_smoothing', dpi=500)
#     plt.show()
    
#     df_phat = pd.concat([df_phat, df_preds], axis=1)
#     df_ptil = pd.concat([df_ptil, df_preds3], axis=1)
# df_phat.columns, df_ptil.columns = ["LM", "RET", "VOL"], ["LM", "RET", "VOL"]



# ##### Errors Belows
# # Weights
# comp_to_weight_value = {
#     '0000320193': 1287,  # Apple
#     '0000789019': 1200,  # Microsoft
#     '0001018724': 920.22,  # Amazon
#     '0001730168': 125,   # Broadcom
#     '0001326801': 585.37,   # Meta (formerly Facebook)
#     '0001045810': 144,   # NVIDIA
#     '0001318605': 75.71,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
#     '0001652044': 921.13, # Alphabet (Google)
#     '0000909832': 129.84    # Costco
# }
# comp_to_weight_volume = {}
# for c in comps:
#     comp_to_weight_volume[c] = df_trn[df_trn['_cik'] == c].shape[0]/df_trn.shape[0]
    
# # Plotting
# dfts = price_reader(comps, val_window[0], val_window[1])
# fig, ax = plt.subplots()
# ax.plot(dfts.mean(axis=1), color = 'silver', linestyle = 'dashed', label = 'Portfolio')
# ax.set_xlabel("Date")
# ax.set_ylabel("Portfolio value")
# ax2 = ax.twinx()
# ax2.plot(df_ptil['RET'], label = 'RET')
# ax2.plot(df_ptil['VOL'], label = 'VOL')
# ax2.plot(df_ptil['LM'], label = "LM")
# ax2.set_ylabel('Sentiment')
# fig.legend(bbox_to_anchor = (0.33, 0.7))
# fig.autofmt_xdate(rotation=50)
# plt.title(f'{NAME} Sentiment Prediction')
# plt.savefig(f'{fig_loc}/{NAME} Sentiment Prediction', dpi=500)
# plt.show()

# #%% VOL QUANTILE ANALYSIS
# window = 5
# vol_trn = vol_reader2(comps, trn_window[0], val_window[1], window=window, extra_end=True, extra_start=True)[1]
# port_trn = vol_trn.mean(axis=1)
# port_trn.plot()
# print('--------------')
# print(f'Window:       {window}')
# print(f'80% quantile: {port_trn.quantile(0.8)}')


# #%% ECONOMETRIC VALIDATION: Constructing Portfolio & Regression Analysis
# comp_to_weight_value = {
#     '0000320193': 1287,  # Apple
#     '0000789019': 1200,  # Microsoft
#     '0001018724': 920.22,  # Amazon
#     '0001730168': 125,   # Broadcom
#     '0001326801': 585.37,   # Meta (formerly Facebook)
#     '0001045810': 144,   # NVIDIA
#     '0001318605': 75.71,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
#     '0001652044': 921.13, # Alphabet (Google)
#     '0000909832': 129.84    # Costco
# }
# # Parameter specifications
# port = 'value' # 'value
# AR_lag = 3
# G_lag = 1
# ARCH_lag = 1
# # mod paraters
# comp = comps
# window = 21
# dep = 'vol'
# print('----------------------Problem?-----------------------------')
# for window in [5, 21]:
#     for port in ['equal', 'value']:
#         # Loading ts data
#         assert (all(c in comps for c in comp)), 'Invalid companies specified'
#         ret_val, vol_val = vol_reader2(comp, val_window[0], val_window[1], window = window, extra_end=True, extra_start=True, AR=AR_lag)
#         if dep == 'ret':
#             dfts = ret_val
#         else:
#             assert dep == 'vol', 'Wrong dependent variable name'
#             dfts = vol_val
#         dfts = dfts[comp]
        
#         # Constructing relevant time-series df
#         print('--- Constructing portfolio')
#         if port == 'equal':
#             port_val = dfts.mean(axis=1)
#         else:
#             if port == 'value':
#                 port_weights = np.array([comp_to_weight_value[c] for c in comp])
#             port_weights = port_weights/sum(port_weights)
#             weight = pd.DataFrame(pd.Series(port_weights, index=dfts.columns, name=0))
#             port_val = dfts.dot(weight[0])
#         port_val.plot()
#         if dep == 'vol':
#             print(f'trn quantile: {port_trn.quantile(0.8)}')
#             print(f'val quantile: {port_val.quantile(0.8)}')
            
        
#         # Binary prediction --> Does not working
#         #PerfectSeparationWarning, ConvergenceWarning, HessianInversionWarning, RuntimeWarning, Maximum Number of Iterations Exceeded


#         print(f'Portfolio: {port}')
#         print(f'Window: {window}')
#         print('@@@@@df_ptil', df_ptil)
#         print('port_val', port_val)
#         print(port_val.to_frame(dep))
#         bin_tab = df_ptil.join(port_val.to_frame(dep).shift(-1))
#         print('@@@@@bin_tab', bin_tab)
#         if dep == 'ret':
#             bin_tab[dep] = (bin_tab[dep] > 0 )*1
#         else:
#             bin_tab[dep] = (bin_tab[dep] < port_trn.quantile(0.8))*1
#         bin_tab['const'] = 1.0
#         prsq = []
#         for p in ["VOL", "LM"]:
#             clf = sm.Logit(bin_tab[dep], bin_tab[[p, 'const']]).fit()
#             prsq.append(clf.prsquared)   
        
#         print('--------- Errors -----------')
#         print(max(prsq))
        
        
        


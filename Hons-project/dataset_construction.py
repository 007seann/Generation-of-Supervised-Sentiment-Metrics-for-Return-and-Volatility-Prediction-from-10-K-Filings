#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 

@author: Sean Sanggyu Choi
"""
from annual_report_reader import reader
from vol_reader_fun import vol_reader, vol_reader2

import os
import pandas as pd
import matplotlib.pyplot as plt

"""
    comp_to_stoc = {'0000320193': 'AAPL', '0000789019': 'MSFT', '0001018724' : "AMZN", "0001730168" : 'AVGO', "0001326801" : 'META',
                    '0001045810' : 'NVDA', '0001318605' : 'TSLA', '0001652044' : "GOOGL", '0001652044' : 'GOOG', '0000909832' : 'COST' }
"""

#%% READING ARTICLES AND COMBINING WITH VOL DATA
start_date = '2006-01-01'
end_date = '2023-12-31'

# # proxy = ['daily-range', 'return']# for time series

# Read firms' ciks
QQQfirms_csv_file_path =  "/Users/apple/PROJECT/Code_4_10k/QQQ_constituents.csv"
firms_df = pd.read_csv(QQQfirms_csv_file_path)
firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
seen = set()
firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 



# Reach out to risk factor path
files_path = '/Users/apple/PROJECT/Code_4_10k/fillings'
folder = 'company_df'     
folder_path = os.path.join(files_path, folder)
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Construct companies' dataframe
num = 0
for cik in firms_ciks:
    print(f'============= Processing {cik} =============')
    for filename in os.listdir(folder_path):
        if filename.rsplit('.', 1)[0] == cik:
            filename = folder + '/' + filename
            D_comp = reader(filename, file_loc=files_path)
            vol_comp = vol_reader(cik,start_date=start_date, end_date=end_date)
            # # Plotting (maybe remove from here)
            # fig,ax = plt.subplots()
            # ax.plot(vol_comp['_ret'])
            # ax.set_ylabel('Daily Return')
            # ax.set_xlabel('Date')
            # fig.suptitle(cik)
            # plt.show()
            # #
            comb = D_comp.join(vol_comp)
            # comb = vol_comp.join(D_comp)
            comb['_cik'] = cik 
            columns_to_move = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1']
            new_column_order = columns_to_move + [col for col in comb.columns if col not in columns_to_move]
            comb = comb[new_column_order]
            no_match = comb['_ret'].isnull()
            print(f'No volatitity data for {round(no_match.sum()/comb.shape[0]*100,1)}% of articles')
            comb = comb[comb['_ret'].notna()]
            comb.reset_index(inplace=True)
            print("comb", comb)
            path = "/Users/apple/PROJECT/Hons-project/data/all"
            if not os.path.exists(path):
                os.makedirs(path)
            filename = f'df_all_{cik}.csv'
            comb.to_csv(os.path.join(path, filename))
            
# Concatenating dataframes
save_file_path = "/Users/apple/PROJECT/Hons-project/data/"
filename = "df_all_QQQ"
first = True
for cik in firms_ciks:
    print('Concatenating dataframes')
    filepath = f'/Users/apple/PROJECT/Hons-project/data/all/df_all_{cik}.csv'
    if not os.path.exists(filepath):
        continue
    else:
        comb = pd.read_csv(filepath)
        if first:
            df_all = comb
            first = False
        else:
            df_all = pd.concat([df_all, comb], axis=0)
        print(f'=> Dimensions: {df_all.shape[0]} annual reports, {df_all.shape[1]-6} terms')
df_all = df_all.reset_index()
df_all.drop('Unnamed: 0', axis=1, inplace=True)
df_all.drop('level_0', axis=1, inplace=True)
df_all.fillna(0.0, inplace=True)
df_all.to_csv(save_file_path + filename + '.csv')

# save_file_path = "/Users/apple/PROJECT/Hons-project/data/"
# filename = "df_qqq"
# df_all = pd.read_csv(save_file_path + filename + '.csv')
#%% ALIGN WITH 3-DAY RETURN
start_date = '2006-01-01'
end_date = '2023-12-31'

x1, x2 = vol_reader2(firms_ciks, start_date, end_date, window=3, extra_end=True, extra_start=True)
x1 = x1.shift(1)
x2 = x2.shift(1)
x1 = x1[start_date:end_date]
x2 = x2[start_date:end_date]


first = True
for cik in firms_ciks:
    print(f'Processing{cik}')
    x1c = x1[cik]
    x2c = x2[cik]
    x = pd.concat([x1c, x2c], axis=1)
    x.columns = ['n_ret', 'n_vol']
    y = df_all[df_all['_cik'] == int(cik.lstrip('0'))]
    y.set_index('Date', inplace=True)
    x.index = pd.to_datetime(x.index)
    y.index = pd.to_datetime(y.index)
    z = y.join(x)
    zz = z[['n_ret', 'n_vol']]

    if first:
        df_add = zz
        first = False
    else:
        df_add = pd.concat([df_add, zz], axis=0)

df_add.reset_index(inplace = True)
# print(df_all)
# print('------')
# print(df_add)
assert all(df_all.index == df_add.index), 'Do not merge!'

plt.plot(df_all['_vol+1'], df_add['n_vol'], 'r.')

df_all2 = df_all.copy()
df_all2['_ret'] = df_add['n_ret']
df_all2['_vol'] = df_add['n_vol']

df_all2.to_csv(save_file_path + filename + '_2' +'.csv')
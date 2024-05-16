#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 

@author: Sean Sanggyu Choi
"""

import os
import pandas as pd
# import matplotlib.pyplot as plt
from annual_report_reader import reader
from vol_reader_fun import vol_reader, vol_reader2
from concurrent.futures import ThreadPoolExecutor

class DicConstructor:
    def __init__(self, csv_file_path, files_path, firms_ciks, firms_dict):
        self.csv_file_path = csv_file_path
        self.files_path = files_path
        self.firms_ciks = firms_ciks
        self.firms_dict = firms_dict
        self.start_date = '2006-01-01'
        self.end_date = '2023-12-31'

    def process_filings_for_cik(self):
        folder = 'company_df'
        folder_path = os.path.join(self.files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        for cik in self.firms_ciks:
            print(f'============= Processing {cik} =============')
            for filename in os.listdir(folder_path):
                if filename.rsplit('.', 1)[0] == cik:
                    filename = os.path.join(folder, filename)
                    D_comp = reader(filename, file_loc=self.files_path)
                    rev_firms_dict = {value: key for key, value in self.firms_dict.items()}
                    vol_comp = vol_reader(cik, rev_firms_dict ,start_date=self.start_date, end_date=self.end_date)
                    comb = D_comp.join(vol_comp)
                    comb['_cik'] = cik
                    columns_to_move = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1']
                    new_column_order = columns_to_move + [col for col in comb.columns if col not in columns_to_move]
                    comb = comb[new_column_order]
                    no_match = comb['_ret'].isnull()
                    print(f'No volatility data for {round(no_match.sum()/comb.shape[0]*100,1)}% of articles')
                    comb = comb[comb['_ret'].notna()]
                    comb.reset_index(inplace=True)
                    filename = f'df_all_{cik}.csv'
                    comb.to_csv(os.path.join(self.files_path, filename))


    def concatenate_dataframes(self):
        first = True
        for cik in self.firms_ciks:
            filepath = os.path.join(self.files_path, f'df_all_{cik}.csv')
            if os.path.exists(filepath):
                comb = pd.read_csv(filepath, index_col=0)
                if first:
                    df_all = comb
                    first = False
                else:
                    df_all = pd.concat([df_all, comb], axis=0)
        df_all = df_all.reset_index(drop=True)
        df_all.fillna(0.0, inplace=True)
        filename = "df_0001045810.csv"
        df_all.to_csv(os.path.join(self.files_path, filename))
        
        # align with 3-day return/volatility 
        rev_firms_dict = {value: key for key, value in self.firms_dict.items()}
        x1, x2 = vol_reader2(self.firms_ciks ,rev_firms_dict, self.start_date, self.end_date, window=3, extra_end=True, extra_start=True)
        x1 = x1.shift(1)
        x2 = x2.shift(1)
        x1 = x1[self.start_date:self.end_date]
        x2 = x2[self.start_date:self.end_date]
                
        
        first = True
        for cik in self.firms_ciks:
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

        # plt.plot(df_all['_vol+1'], df_add['n_vol'], 'r.')

        df_all2 = df_all.copy()
        df_all2['_ret'] = df_add['n_ret']
        df_all2['_vol'] = df_add['n_vol']
        df_all2.to_csv(os.path.join(self.files_path, filename))

                
        


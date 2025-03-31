
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 2024

@author: Sean Sanggyu Choi
"""
import os
import sys
from collections import defaultdict
import pandas as pd
from vol_reader_fun import vol_reader2, vol_reader_vectorised
import time

start_date = '2000-01-01'
end_date = '2026-01-01'

CONSTITUENTS_PATH = "/Users/apple/PROJECT/hons_project/data/SP500/tesla/tesla_constituents_final.csv"
INPUT_FILE_PATH = "/Users/apple/PROJECT/hons_project/data/SP500/tesla/SEC/dtm_0001318605.parquet"
OUTPUT_FILE_PATH = "/Users/apple/PROJECT/hons_project/data/SP500/tesla/SEC/dtm_0001318605_2.parquet"
TEMP_FILE_PATH = "/Users/apple/PROJECT/hons_project/data/SP500/temp/"
df_final = pd.read_csv(CONSTITUENTS_PATH)
df_final["CIK"] = df_final["CIK"].astype(str).str.zfill(10)  # Zero-pad CIK


df_dtm = pd.read_parquet(INPUT_FILE_PATH) 
ciks = df_dtm['_cik'].unique().astype(str)  # Ensure consistency in CIK format


filtered_df = df_final[df_final["CIK"].isin(ciks)]
firms_dict = filtered_df.set_index('CIK')['Symbol'].to_dict()


x1, x2 = vol_reader_vectorised(ciks, firms_dict, start_date, end_date, window=3, extra_end=True, extra_start=True, AR=None)

x1 = x1[start_date:end_date]
x2 = x2[start_date:end_date]
x1 = x1.dropna()
x2 = x2.dropna()
x1 = x1.loc[~(x1 == 0).all(axis=1)]
x2 = x2.loc[~(x2 == 0).all(axis=1)]
x1.to_csv(TEMP_FILE_PATH + "x1_debug.csv")
x2.to_csv(TEMP_FILE_PATH + "x2_debug.csv")

x1_path = TEMP_FILE_PATH + "x1_debug.csv"
x2_path = TEMP_FILE_PATH + "x2_debug.csv"
x1 = pd.read_csv(x1_path)
x2 = pd.read_csv(x2_path)
x1.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
x1 = x1.iloc[2:] # Drop first two rows (NaNs)
x1 = x1.set_index('Date')
x2.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
x2 = x2.iloc[2:] 
x2 = x2.set_index('Date')

start_time = time.time()
# Merge results efficiently
df_add = pd.concat([x1.stack(), x2.stack()], axis=1).reset_index() # df_add (6291, 1230)

df_add.columns = ['Date', '_cik', 'n_ret', 'n_vol']
df_add['_cik'] = df_add['_cik'].astype(str).str.zfill(10)  # Ensure format consistency



# Merge with df_dtm efficiently
df_dtm["Date"] = df_dtm["Date"].astype(str)
df_dtm['_cik'] = df_dtm['_cik'].astype(str).str.zfill(10) # df_dtm (25925, 14137)
df_merged = df_dtm.merge(df_add, how='left', left_on=['Date', '_cik'], right_on=['Date', '_cik'])

# Ensure merging is successful
assert df_dtm.shape[0] == df_merged.shape[0], 'Mismatch in row count after merging!'

# Assign new columns and save

df_merged['_ret'], df_merged['_vol'] = df_merged['n_ret'], df_merged['n_vol']
df_merged.drop(columns=['n_ret', 'n_vol'], inplace=True)  # Clean up
df_merged.to_parquet(OUTPUT_FILE_PATH)
end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")  
print("Final dataset shape:", df_merged.shape)


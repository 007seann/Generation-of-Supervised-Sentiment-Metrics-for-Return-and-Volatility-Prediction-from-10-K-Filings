#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 2023

@author: Sean Sanggyu Choi
"""

import numpy as np
import pandas as pd
import datetime as dt
from time_log_decorator import time_log

import yfinance as yf


def transform_parquet_to_yf_format(parquet_path):
    # Read the Parquet file
    df = pd.read_parquet(parquet_path)
    df.rename(columns={'date': 'Date'}, inplace=True)
    # Ensure the 'date' column is a datetime type
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Set the 'date' column as the index
    df.set_index('Date', inplace=True)
    
    # Rename columns to match yfinance format
    df.rename(columns={
        'symbol': 'Symbol',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }, inplace=True)
    
    # Add 'Adj Close' column if it doesn't exist
    if 'Adj Close' not in df.columns:
        df['Adj Close'] = df['Close']
    
    return df

def vol_reader(comp, firms_dict, start_date=None, end_date=None):
    price_data_path = "../data/stock_price_daily.parquet"
    stock = firms_dict[comp]
    # print(f'Downloading {stock} stock data')
    # time_series = yf.download(stock, 
    #                         start = start_date,
    #                         end = end_date,
    #                         progress = False)
    price_df = transform_parquet_to_yf_format(price_data_path)

    time_series = price_df[price_df['Symbol'] == stock]
    time_series = time_series.loc[start_date:end_date]
    def vol_proxy(ret, proxy):
        proxies = ['sqaured return', 'realized', 'daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'realized':
            raise 'Realized volatility proxy not yet implemented'
        elif proxy == 'daily-range':
            ran = np.log(ret['High']) - np.log(ret['Low'])
            adj_factor = 4 * np.log(2)
            return np.square(ran)/adj_factor
        elif proxy == 'return':
            def ret_fun(xt_1, xt):
                return np.log(xt/xt_1) ### used to xt/xt_1 -> I think it's right
            return ret_fun(ret['Open'], ret['Close'])
        else:
            assert proxy == 'squared return'
            raise 'Squared return proxy not yet implemented'
        
    vol_list = []
    for p in ['daily-range', 'return']:
        vol = vol_proxy(time_series, p)
        vol_list.append(vol.to_frame())
    
    df_vol = pd.concat(vol_list, axis=1)
    df_vol.columns = ['_vol', '_ret']
    df_vol = df_vol.reset_index()
    df_vol['_vol+1'] = df_vol['_vol'].shift(-1)
    df_vol['_ret+1'] = df_vol['_ret'].shift(-1)
    df_vol = df_vol.dropna()
    df_vol.set_index('Date', inplace=True)
    return df_vol

def vol_reader2(comps, firms_dict, start_date, end_date, window = None, extra_end = False, extra_start = False, AR = None):
    def ret_fun(xt_1, xt): # log difference
        return np.log(xt/xt_1) ### used to xt/xt_1
    price_data_path = "../data/stock_price_daily.parquet"
    price_df = transform_parquet_to_yf_format(price_data_path)
    ts = []
    empty = []
    if extra_end:
        if window:
            end_date = str(dt.datetime.strptime(end_date, '%Y-%m-%d')+dt.timedelta(days=window + 3))[:10]
        else:
            end_date = str(dt.datetime.strptime(end_date, '%Y-%m-%d')+dt.timedelta(days= 1))[:10]
    if extra_start and window:
        if AR:
            start_date = str(dt.datetime.strptime(start_date, '%Y-%m-%d')-dt.timedelta(days=window*AR + 1))[:10]
        else:
            start_date = str(dt.datetime.strptime(start_date, '%Y-%m-%d')-dt.timedelta(days=window + 1))[:10]
            
    for cc in comps:
        print("Processing", cc)
        stock = firms_dict[cc]
        time_series = price_df[price_df['Symbol'] == stock]
        time_series = time_series.loc[start_date:end_date]
        if time_series.empty:
            print(f'{stock} data is empty')
            empty.append(cc)
            continue
        ts.append(time_series)
    comps = list(set(comps) - set(empty))
        
    def vol_proxy(ret, proxy):
        proxies = ['squared return','realized','daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'realized':
            raise 'Realized volatiliy proxy not yet implemented'
        elif proxy == 'daily-range':
            ran = np.log(ret['High']) - np.log(ret['Low'])
            adj_factor = 4*np.log(2)
            return np.square(ran)/adj_factor
        elif proxy == 'return':
            #print('Computing Open-Close log-diff returns')
            return ret_fun(ret['Open'], ret['Close'])
        else:
            assert proxy == 'squared return'
            raise 'Sqaured return proxy not yet implemented'
    
    def vol_proxy_window(ret, proxy, window):
        proxies = ['squared return', 'realized', 'daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'return':
            t1 = ret.index + dt.timedelta(days=window-1)
            t1_adj = list(t1)
            for t in range(len(t1)):
                t_new = t1[t]
                while t_new not in ret.index:
                    t_new -= dt.timedelta(days=1)
                t1_adj[t] = t_new
            ret1 = ret.loc[t1_adj]
            ret1.index = ret.index
            remove_last = t1_adj.count(t1_adj[-1]) - 1
            if remove_last > 0:
                ret1 = ret1[:-remove_last]
                ret = ret[:-remove_last]
            return ret_fun(ret['Open'], ret1['Close'])
        elif proxy == 'realized':
            daily_ran_sq = np.square(np.log(ret['High'])-np.log(ret['Low']))/4*np.log(2)
            volvol = pd.Series(0, index=ret.index)
            for t in volvol.index:
                tt = t
                vv = 0
                N = 0
                past_date = t + dt.timedelta(days=window)
                while t < past_date:
                    if t in daily_ran_sq.index:
                        vv += daily_ran_sq.loc[t]
                        N += 1
                    t += dt.timedelta(days=1)
                volvol.loc[tt] = vv/N
            return volvol
        else:
            raise 'Proxy not introduced yet'
            
    ret_list = []
    vol_list = []
            
    if window:
        assert window > 0 and isinstance(window, int),'Incorrect window specified'
        for cc in range(len(comps)):
            ret = vol_proxy_window(ts[cc], 'return', window=window)
            ret_list.append(ret.to_frame())
            vol = vol_proxy_window(ts[cc], 'realized', window=window)
            vol_list.append(vol.to_frame())
    else:
        for cc in range(len(comps)):
            ret = vol_proxy(ts[cc], 'return')
            ret_list.append(ret.to_frame())
            vol = vol_proxy(ts[cc], 'daily-range')
            vol_list.append(vol.to_frame())
            
    df_ret = pd.concat(ret_list, axis=1)
    df_ret.columns = comps
    df_ret = df_ret.fillna(method='bfill')
    df_ret = df_ret.dropna()
    df_vol = pd.concat(vol_list, axis=1)
    df_vol.columns = comps
    df_vol = df_vol.fillna(method='bfill')
    df_vol = df_vol.dropna()
    

    return df_ret, df_vol
    
@time_log
def vol_reader_vectorised(comps, firms_dict, start_date, end_date, window=None, extra_end=False, extra_start=False, AR=None):
    def ret_fun(xt_1, xt):
        return np.log(xt / xt_1)  # Log returns
    
    # File path
    price_data_path = "../data/stock_price_daily.parquet"
    price_df = transform_parquet_to_yf_format(price_data_path)

    # Extend date range if needed
    start_dt, end_dt = dt.datetime.strptime(start_date, '%Y-%m-%d'), dt.datetime.strptime(end_date, '%Y-%m-%d')
    if extra_end:
        end_dt += dt.timedelta(days=(window + 3) if window else 1)
    if extra_start and window:
        start_dt -= dt.timedelta(days=(window * AR + 1) if AR else (window + 1))
    
    # Convert back to string format
    start_date, end_date = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')

    # Get the stock symbols for given companies
    stocks = [firms_dict[cc] for cc in comps if cc in firms_dict]
    
    # Efficient filtering: pre-filter stock prices by date and relevant symbols
    filtered_df = price_df.loc[
        (price_df['Symbol'].isin(stocks)) & (price_df.index >= start_date) & (price_df.index <= end_date)
    ]
    
    # Group by symbol for efficient lookup
    grouped = filtered_df.groupby('Symbol')

    # Store time series data
    ts_dict = {}
    empty = set()

    for cc in comps:
        stock = firms_dict.get(cc)
        if stock and stock in grouped.groups:
            ts_dict[cc] = grouped.get_group(stock)
        else:
            print(f'{stock} data is empty')
            empty.add(cc)

    # Remove empty companies from the list
    comps = list(set(comps) - empty)

    # Volatility proxy calculations
    def vol_proxy(ret, proxy):
        proxies = {'squared return', 'realized', 'daily-range', 'return'}
        assert proxy in proxies, f'Proxy should be one of {proxies}'
        
        if proxy == 'realized':
            raise NotImplementedError('Realized volatility proxy not yet implemented')
        elif proxy == 'daily-range':
            ran = np.log(ret['High']) - np.log(ret['Low'])
            adj_factor = 4 * np.log(2)
            return np.square(ran) / adj_factor
        elif proxy == 'return':
            return ret_fun(ret['Open'], ret['Close'])
        else:
            raise NotImplementedError('Squared return proxy not yet implemented')

    def vol_proxy_window(ret, proxy, window):
        proxies = {'squared return', 'realized', 'daily-range', 'return'}
        assert proxy in proxies, f'Proxy should be one of {proxies}'
        
        if proxy == 'return':
            t1 = ret.index + pd.DateOffset(days=window - 1)
            t1 = [ret.index[ret.index.get_indexer([t], method='pad')[0]] for t in t1]
            ret1 = ret.loc[t1]
            ret1.index = ret.index
            return ret_fun(ret['Open'], ret1['Close'])
        
        elif proxy == 'realized':
            daily_ran_sq = (np.log(ret['High']) - np.log(ret['Low']))**2 / (4 * np.log(2))
            return daily_ran_sq.rolling(window=window, min_periods=1).mean()
        
        else:
            raise NotImplementedError('Proxy not introduced yet')

    # Compute returns and volatilities
    ret_list, vol_list = [], []
    
    if window:
        assert isinstance(window, int) and window > 0, 'Incorrect window specified'
        for cc in comps:
            ret_list.append(vol_proxy_window(ts_dict[cc], 'return', window).to_frame())
            vol_list.append(vol_proxy_window(ts_dict[cc], 'realized', window).to_frame())
    else:
        for cc in comps:
            ret_list.append(vol_proxy(ts_dict[cc], 'return').to_frame())
            vol_list.append(vol_proxy(ts_dict[cc], 'daily-range').to_frame())

    # Concatenate and clean data
    df_ret = pd.concat(ret_list, axis=1, keys=comps).bfill().dropna()
    df_vol = pd.concat(vol_list, axis=1, keys=comps).bfill().dropna()

    return df_ret, df_vol


def price_reader(comps, firms_dict, start_date, end_date):
    price_data_path = "../data/stock_price_daily.parquet"
    price_df = transform_parquet_to_yf_format(price_data_path)
    ts = []
    empty = []
    for cc in comps:
        stock = firms_dict[cc]
        try:
            time_series = price_df[price_df['Symbol'] == stock]
            time_series = time_series.loc[start_date:end_date]
            if time_series.empty:
                print(f'{stock} data is empty')
                empty.append(cc)
                continue
            ts.append(time_series)
        except Exception as e:
            print(f'Error: {e}')
            print(f'Could not download {stock} data')
            pass
    comps = list(set(comps) - set(empty))
    ret_list = []
    for cc in range(len(comps)):
        ret = ts[cc]['Open']
        # ret = ret/ret[0] * 100
        ret_list.append(ret.to_frame())
    df_ret = pd.concat(ret_list, axis=1)
    df_ret.columns = comps
    df_ret = df_ret.fillna(method='bfill')
    df_ret = df_ret.dropna()
    
    return df_ret, comps

"""
#%% Aligning to future returns
start_date = '2012-01-01'
end_date = '2022-05-31'
comps = ['TOY','VOW','HYU','GM','FOR','NIS','HON','REN','SUZ','SAI']
first = True
for c in comps:
    x = vol_reader(c, start_date, end_date)
    x['_vol+1'] = x['_vol'].shift(-1)
    x['_ret+1'] = x['_ret'].shift(-1)
    x = x.drop(columns = ['_ret','_vol'])
    x['_stock'] = c
    if first:
        x_all = x
        first=False
    else:
        x_all = pd.concat([x_all,x],axis=0)
x_all = x_all.dropna()
new_ind1 = list(x_all['_stock'])
new_ind2 = list(x_all.index)
assert len(new_ind1) == len(new_ind2)
new_ind = []
for i in range(len(new_ind1)):
    new_ind.append(f'{new_ind1[i]}_{new_ind2[i]}')
x_all_new = x_all
x_all_new.index = new_ind

df2 = df_all
new_ind1 = list(df2['_stock'])
new_ind2 = list(df2.index)
assert len(new_ind1) == len(new_ind2)
new_ind = []
for i in range(len(new_ind1)):
    new_ind.append(f'{new_ind1[i]}_{new_ind2[i]}')
df2_new = df2
df2_new.index = new_ind

df3 = df2_new.join()


#%% Plotting ts for each company

import matplotlib.pyplot as plt
start_date = '2012-01-01'
end_date = '2022-05-31'
comps = ['TOY','VOW','HYU','GM','FOR','NIS','HON','REN','SUZ','SAI']
for c in comps:
    vv = vol_reader(c,start_date=start_date, end_date=end_date)
    fig,ax = plt.subplots()
    ax.plot(vv['_ret'])
    ax.set_ylabel('Daily Return')
    ax.set_xlabel('Date')
    fig.suptitle(c)
    plt.show()
    fig,ax = plt.subplots()
    ax.plot(vv['_vol'])
    ax.set_ylabel('Volatility')
    ax.set_xlabel('Date')
    fig.suptitle(c)
    plt.show()
"""


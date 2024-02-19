import csv
import pandas as pd
import os 
from collections import defaultdict
"""
Created on Sat Jan 6 2024

@author: Sean Sanggyu Choi
"""

"""
    Summary: This code builds up csv file of top 10 tech companies listed in QQQ ETF.
    
    ### message for me ###
    I can easilty expand to the whole tech companises by using QQQ_constituents.csv. Just change it and run. 
    
    Example
    -------
    Name         CIK        Date                                               Body
0    AAPL  0000320193  2006-12-29  Because of the following factors, as well as o...
1    AAPL  0000320193  2007-11-15  Because of the following factors, as well as o...
2    AAPL  0000320193  2008-11-05  Because of the following factors, as well as o...
3    AAPL  0000320193  2009-10-27  Because of the following factors, as well as o...
4    AAPL  0000320193  2010-10-27  Because of the following factors, as well as o...
..    ...         ...         ...                                                ...
125  TSLA  0001318605  2019-02-19   \n\n[heading]You should carefully consider th...
126  TSLA  0001318605  2020-02-13   \n\n[heading]You should carefully consider th...
127  TSLA  0001318605  2021-02-08   \n\n[heading]You should carefully consider th...
128  TSLA  0001318605  2022-02-07   \n\n[heading]You should carefully consider th...
129  TSLA  0001318605  2023-01-31   \n\n[heading]You should carefully consider th...    
    -------
    
"""
    
# Specify the path to your CSV file
top10_csv_file_path = "/Users/apple/Library/Mobile Documents/com~apple~CloudDocs/Final-Project/0Fintech/0hons-project/PROJECT/Code_4_10k/top10_QQQ_constituents.csv"
risk_factors_path = '/Users/apple/Library/Mobile Documents/com~apple~CloudDocs/Final-Project/0Fintech/0hons-project/PROJECT/Code_4_10k/risk_factors'

top10_df = pd.read_csv(top10_csv_file_path)
top10_df = top10_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
top10_df['CIK'] = top10_df['CIK'].apply(lambda x: str(x).zfill(10))
top10_dict = top10_df.set_index('Symbol')['CIK'].to_dict()

columns = ["Name", "CIK", "Date", "Body" ]
df = pd.DataFrame(columns = columns)


def import_file(file_dir):
    with open(file_dir, 'r', encoding='utf-8') as file:
        return file.read()

for top10_symbol, top10_cik in top10_dict.items():
    cik_path = os.path.join(risk_factors_path, top10_cik)
    if os.path.exists(cik_path):
        for filename in os.listdir(cik_path):
            date = filename.split('.')[0]  
            file_dir = os.path.join(cik_path, filename)
            if os.path.isfile(file_dir):
                # Initialize a dictionary with row data
                row_data = {
                    'Name': top10_symbol,
                    'CIK': top10_cik,
                    'Date': date,
                    'Body': import_file(file_dir)  
                }
                # Append the row to the DataFrame
                df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

df = df.groupby('Name', group_keys=False).apply(lambda group: group.sort_values(by='Date'))
df = df.reset_index(drop=True)
print(df)
# Save df to csv file
df.to_csv('top10_risk_factors.csv', index=False)
    




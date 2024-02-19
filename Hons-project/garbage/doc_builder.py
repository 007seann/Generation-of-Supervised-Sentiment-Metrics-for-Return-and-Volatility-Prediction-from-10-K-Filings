import csv
import pandas as pd
import os 
from collections import defaultdict
"""
Created on Sat Jan 6 2024

@author: Sean Sanggyu Choi
"""

    
# Specify the path to your CSV file
top10_csv_file_path = "/Users/apple/Library/Mobile Documents/com~apple~CloudDocs/Final-Project/0Fintech/0hons-project/PROJECT/Code_4_10k/top10_QQQ_constituents.csv"
risk_factors_path = '/Users/apple/Library/Mobile Documents/com~apple~CloudDocs/Final-Project/0Fintech/0hons-project/PROJECT/Code_4_10k/risk_factors'

top10_df = pd.read_csv(top10_csv_file_path)
top10_df = top10_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
top10_df['CIK'] = top10_df['CIK'].apply(lambda x: str(x).zfill(10))
top10_dict = top10_df.set_index('Symbol')['CIK'].to_dict()

columns = ["Name", "CIK", "Date", "Body" ]



def import_file(file_dir):
    with open(file_dir, 'r', encoding='utf-8') as file:
        return file.read()

for top10_symbol, top10_cik in top10_dict.items():
    cik_path = os.path.join(risk_factors_path, top10_cik)
    if os.path.exists(cik_path):
        for filename in os.listdir(cik_path):
            df = pd.DataFrame(columns = columns)
            date = filename.split('.')[0]  
            file_dir = os.path.join(cik_path, filename)
            save_to_csv_path = file_dir.rsplit('.', 1)[0] + '.csv'
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
                df.to_csv(save_to_csv_path, index=False)



    
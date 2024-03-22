import csv
import pandas as pd
import os 
from collections import defaultdict
"""
Created on Sat Jan 6 2024

@author: Sean Sanggyu Choi
"""
### Create A company's dataframe contained 10-K reports. Use this code.
    
# Specify the path to your CSV file
QQQfirms_csv_file_path = "/Users/apple/PROJECT/Code_4_10k/QQQ_constituents.csv"
files_path = '/Users/apple/PROJECT/Code_4_10k/fillings'

firms_df = pd.read_csv(QQQfirms_csv_file_path)
firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()

columns = ["Name", "CIK", "Date", "Body" ]



def import_file(file_dir):
    with open(file_dir, 'r', encoding='utf-8') as file:
        return file.read()

for symbol, cik in firms_dict.items():
    df = pd.DataFrame(columns = columns)
    cik_path = os.path.join(files_path, cik)
    if os.path.exists(cik_path):
        for filename in os.listdir(cik_path):
            date = filename.split('.')[0]  
            file_dir = os.path.join(cik_path, filename)
            # save_to_csv_path = file_dir.rsplit('.', 1)[0] + '.csv'
            if os.path.isfile(file_dir):
                # Initialize a dictionary with row data
                row_data = {
                    'Name': symbol,
                    'CIK': cik,
                    'Date': date,
                    'Body': import_file(file_dir)  
                }
                # Append the row to the DataFrame
                df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
        folder = 'company_df'        
        folder_path = os.path.join(files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Save df to csv file
        df = df.groupby('Name', group_keys=False).apply(lambda group: group.sort_values(by='Date'))
        df = df.reset_index(drop=True)

        df.to_csv(folder_path + "/" + f"{cik}.csv", index=False)



    
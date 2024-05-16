import os
from bs4 import BeautifulSoup
import re
from collections import Counter
import concurrent.futures
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.common_func import import_file
from common.sec_crawler import download_executor, test, download_fillings
from common.sec10k_extractor import process_fillings_for_cik
from common.sec10k_item1a_extractor import process_files_for_cik_with_italic
from packages.sec10k_dataset_construction import DicConstructor
import pandas as pd
import datetime
import subprocess

with DAG(
    dag_id="dags_firm_dict_builder",
    schedule="0 0 1 1,4,7,10 *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,

) as dag:

    csv_file_path = '/opt/airflow/data/nvidia_constituents.csv'
    data_raw_folder = '/opt/airflow/data/nvidia_data'
    sec10k_10k_extracted_folder = '/opt/airflow/data/nvidia_txt_data'
    sec10k_item1a_extracted_folder = '/opt/airflow/data/nvidia_item1a_data'
    columns = ["Name", "CIK", "Date", "Body" ]
    firms_df = pd.read_csv(csv_file_path)
    firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()

    seen = set()
    firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 

    
    start_date = '2006-01-01'
    end_date = '2023-12-31'

    
    @task(task_id='t1_test')
    def test(PATH):
    
        df = pd.read_csv(PATH, encoding = 'utf-8')
        QQQ_cik = df['CIK'].drop_duplicates().tolist() 
        
        return QQQ_cik
    
    @task(task_id='t2_download_executor')
    def download_executor(firm_list_path):
    # firm_list_path = '/Users/apple/PROJECT/Code_4_10k/top10_QQQ_constituents.csv'
        try:
            df = pd.read_csv(firm_list_path, encoding = 'utf-8')
            QQQ_cik = df['CIK'].drop_duplicates().tolist()
        except UnicodeDecodeError:
            df = pd.read_csv(firm_list_path, encoding = 'ISO-8859-1')
            QQQ_cik = df['CIK'].drop_duplicates().tolist()

        # root_folder = '/opt/airflow/data/top10_data'
        doc_type = '10-k'
        headers = {'User-Agent': 'UOE / 0.1'}
        start_date = '2006-01-01',
        end_date = datetime.datetime.now()
        if not os.path.exists(data_raw_folder):
            os.makedirs(data_raw_folder)
        download_fillings(QQQ_cik,data_raw_folder,doc_type,headers,end_date=end_date,start_date=start_date)  

    @task(task_id='t3_sec10k_extraction_executor')
    def sec10k_extraction_executor(data_folder, save_folder):
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for cik in os.listdir(data_folder):
                future = executor.submit(process_fillings_for_cik, cik, data_folder, save_folder)
                futures.append(future)
                
                
            # Wait for all tasks to complete
            for future in futures:
                future.result()
            
            # All tasks are completed, shutdown the executor
            executor.shutdown()

    @task(task_id='t4_10k_item1a_extraction_executor')
    def item1a_executor(data_folder, save_folder):
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for cik in os.listdir(data_folder):
                print('Processing CIK_executing risk factor process:', cik)
                future = executor.submit(process_files_for_cik_with_italic, cik, data_folder, save_folder)
                futures.append(future)
            
            # Wait for all tasks to complete
            for future in futures:
                future.result()
            
            # All tasks are completed, shutdown the executor
            executor.shutdown() 

    @task(task_id='t5_company_csv_builder')
    def csv_builder(save_folder, firm_dict, columns):
        for symbol, cik in firm_dict.items():
            df = pd.DataFrame(columns = columns)
            cik_path = os.path.join(save_folder, cik)
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
                folder_path = os.path.join(save_folder, folder)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)

                # Save df to csv file
                df = df.groupby('Name', group_keys=False).apply(lambda group: group.sort_values(by='Date'))
                df = df.reset_index(drop=True)

                df.to_csv(folder_path + "/" + f"{cik}.csv", index=False)
                

    @task(task_id='t6_sec10k_dataset_construction')
    def sec10k_dataset_construction():
        sec10k_constructor = DicConstructor(csv_file_path, sec10k_10k_extracted_folder, firms_ciks, firms_dict)
        sec10k_constructor.process_filings_for_cik()
        sec10k_constructor.concatenate_dataframes()
        
    @task(task_id='t6_1_sec10k_item1a_dataset_construction')
    def sec10k_item1a_dataset_construction():
        sec10k_constructor = DicConstructor(csv_file_path, sec10k_item1a_extracted_folder, firms_ciks, firms_dict)
        sec10k_constructor.process_filings_for_cik()
        sec10k_constructor.concatenate_dataframes()
    
        
        
                
    # @task(task_id='t6_sec10k_dataset_construction')
    # def sec10k_dataset_construction():
    #     subprocess.run(['python', '/opt/airflow/plugins/common/sec10k_dataset_construction.py'], check=True)

    # @task(task_id='t6_1_sec10k_item1a_dataset_construction')
    # def sec10k_item1a_dataset_construction():
    #     subprocess.run(['python', '/opt/airflow/plugins/common/sec10k_item1a_dataset_construction.py'], check=True)


    # t1_test = test(csv_file_path)
    t2_download_executor = download_executor(csv_file_path)
    t3_sec10k_extraction_executor = sec10k_extraction_executor(data_raw_folder, sec10k_10k_extracted_folder)
    t4_10k_item1a_extraction_executor = item1a_executor(data_raw_folder, sec10k_item1a_extracted_folder)
    t5_10k_company_csv_builder = csv_builder(sec10k_10k_extracted_folder, firms_dict, columns)
    t5_1_item1a_company_csv_builder = csv_builder(sec10k_item1a_extracted_folder, firms_dict, columns)
    t6_sec10k_dataset_construction = sec10k_dataset_construction()
    t6_1_sec10k_item1a_dataset_construction = sec10k_item1a_dataset_construction()
    # t6_sec10k_dataset_construction = sec10k_dataset_construction()
    # t6_1_sec10k_item1a_dataset_construction = sec10k_item1a_dataset_construction
    
    t2_download_executor >> t3_sec10k_extraction_executor >> t5_10k_company_csv_builder >> t6_sec10k_dataset_construction
    t2_download_executor >> t4_10k_item1a_extraction_executor >> t5_1_item1a_company_csv_builder >> t6_1_sec10k_item1a_dataset_construction
    
    
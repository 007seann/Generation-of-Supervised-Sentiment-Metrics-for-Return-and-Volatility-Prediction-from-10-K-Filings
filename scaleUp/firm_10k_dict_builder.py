
from bs4 import BeautifulSoup
import re
from collections import Counter

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# from common.common_func import import_file
# from common.sec_crawler import test, download_fillings
# from common.sec10k_extractor import process_fillings_for_cik
# from common.sec10k_item1a_extractor import process_files_for_cik_with_italic
# from packages.sec10k_dataset_construction import DicConstructor
# import concurrent.futures
import os
import pandas as pd
import datetime
import subprocess

with DAG(
    dag_id="firm_10k_dict_builder",
    schedule="0 0 1 6,12 *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,

) as dag:
    
    ############################### Configurations ################################
    level = 'firm'
    type = '10-K'
    start_date = '2023-01-01'
    end_date = datetime.datetime.now()
    
    # Save File Paths
    final_save_path = f'/opt/airflow/data/NASDAQ100/{level}_{type}_data'
    csv_file_path = '/opt/airflow/data/nvidia_constituents.csv'
    columns = ["Name", "CIK", "Date", "Body" ]
    firms_df = pd.read_csv(csv_file_path)
    firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()

    seen = set()
    firms_ciks = [cik for cik in firms_df['CIK'].tolist() if not (cik in seen or seen.add(cik))] 
    
    # if level == 'firm':        
    data_raw_folder = f'/opt/airflow/data/NASDAQ100/{firms_ciks[0]}_{type}_html'
    sec10k_10k_extracted_folder = f'/opt/airflow/data/NASDAQ100/{firms_ciks[0]}_{type}_all_txt'
    sec10k_item1a_extracted_folder = f'/opt/airflow/data/NASDAQ100/{firms_ciks[0]}_{type}_riskFactor_txt' 
    
    error_html_csv_path = 'opt/airflow/data/error_html_log.csv'
    error_txt_csv_path = 'opt/airflow/data/error_txt_log.csv'   
    if os.path.exists(error_html_csv_path):
        os.remove(error_html_csv_path)
    if os.path.exists(error_txt_csv_path):
        os.remove(error_txt_csv_path)         
    

    ###############################################################################
    @task(task_id='t1_test')
    def test(PATH):
    
        df = pd.read_csv(PATH, encoding = 'utf-8')
        QQQ_cik = df['CIK'].drop_duplicates().tolist() 
        
        return QQQ_cik
    
    @task(task_id='t2_download_executor')
    def download_executor(firm_list_path, type, start_date, end_date):
        from common.sec_crawler import test, download_fillings
        import os
        import pandas as pd
        
        try:
            df = pd.read_csv(firm_list_path, encoding = 'utf-8')
            QQQ_cik = df['CIK'].drop_duplicates().tolist()
            QQQ_ticker = df['Symbol'].tolist()
            QQQ_cik_ticker = dict(zip(QQQ_cik, QQQ_ticker))
        except UnicodeDecodeError:
            df = pd.read_csv(firm_list_path, encoding = 'ISO-8859-1')
            QQQ_cik = df['CIK'].drop_duplicates().tolist()
            QQQ_ticker = df['Symbol'].tolist()
            QQQ_cik_ticker = dict(zip(QQQ_cik, QQQ_ticker))
            
        # root_folder = '10k-html'
        doc_type = type
        headers = {'User-Agent': 'University of Edinburgh s2101367@ed.ac.uk'}

        if not os.path.exists(data_raw_folder):
            os.makedirs(data_raw_folder)
        download_fillings(QQQ_cik_ticker, data_raw_folder,doc_type,headers, end_date, start_date)
    
    
    @task(task_id='t3_extraction_executor')
    def sec10k_extraction_executor(data_folder, save_folder):
        from common.sec10k_extractor import process_fillings_for_cik
        import concurrent.futures
        import os
        
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

    @task(task_id='t4_item1a_extraction_executor')
    def item1a_executor(data_folder, save_folder):
        from common.sec10k_item1a_extractor import process_files_for_cik_with_italic
        import concurrent.futures
        import os
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for cik in os.listdir(data_folder):
                print('Processing CIK_executing risk factor process:', cik)
                future = executor.submit(process_files_for_cik_with_italic, cik, data_folder, save_folder, error_html_csv_path, error_txt_csv_path)
                futures.append(future)
            
            # Wait for all tasks to complete
            for future in futures:
                future.result()
            
            # All tasks are completed, shutdown the executor
            executor.shutdown() 

    @task(task_id='t5_company_csv_builder')
    def csv_builder(save_folder, firm_dict, columns):
        from common.common_func import import_file
        import os
        import pandas as pd
        
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
                
    @task(task_id='t5_1_item1a_company_csv_builder')
    def item1a_csv_builder(save_folder, firm_dict, columns):
        from common.common_func import import_file
        import os
        import pandas as pd
        
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

    @task(task_id='t6_dataset_construction')
    def sec10k_dataset_construction():
        from packages.sec10k_dataset_construction import DicConstructor
        
        sec10k_constructor = DicConstructor(csv_file_path, sec10k_10k_extracted_folder, firms_ciks, firms_dict)
        sec10k_constructor.process_filings_for_cik()
        sec10k_constructor.concatenate_dataframes(level = level, section = 'all', save_path = final_save_path)
        
    @task(task_id='t6_1_item1a_dataset_construction')
    def sec10k_item1a_dataset_construction():
        from packages.sec10k_dataset_construction import DicConstructor
        
        sec10k_constructor = DicConstructor(csv_file_path, sec10k_item1a_extracted_folder, firms_ciks, firms_dict)
        sec10k_constructor.process_filings_for_cik()
        sec10k_constructor.concatenate_dataframes(level = level, section = 'riskFactor', save_path = final_save_path)
    
        
    
    t2_download_executor = download_executor(csv_file_path, type = type, start_date=start_date, end_date=end_date)
    t3_extraction_executor = sec10k_extraction_executor(data_raw_folder, sec10k_10k_extracted_folder)
    t4_item1a_extraction_executor = item1a_executor(data_raw_folder, sec10k_item1a_extracted_folder)
    t5_company_csv_builder = csv_builder(sec10k_10k_extracted_folder, firms_dict, columns)
    t5_1_item1a_company_csv_builder = item1a_csv_builder(sec10k_item1a_extracted_folder, firms_dict, columns)
    t6_dataset_construction = sec10k_dataset_construction()
    t6_1_item1a_dataset_construction = sec10k_item1a_dataset_construction()

    
    t2_download_executor >> t3_extraction_executor >> t5_company_csv_builder >> t6_dataset_construction
    t2_download_executor >> t4_item1a_extraction_executor >> t5_1_item1a_company_csv_builder >> t6_1_item1a_dataset_construction
    
    
How to run the SEC Filing Extraction Model(SFEM)
10-K Report Scrapping.ipynb: This file extracts SEC fillings for a certain duration. (hyperapameter: 10-K)
Risk Factors Extraction.ipynb: This code extracts the risk factor section from an extracted file from the 10-K Report Scrapping code 
    
How to run the Sentiment Score Prediction Model(SSPM)

QQQ_constituents.csv(located at Code_4_10k): Offers a list of firm's stock information in QQQ ETF. Used for extracting a list of CIKs for the sector level analysis.
top10_constituents.csv(located at Code_4_10k): Offers a list of top 10 firm's stock information in QQQ ETF. Used for extracting a list of CIKs for the portfolio level analysis.
LM_dict.csv: Loughran_McDonald (LM) dictionary attained from https://sraf.nd.edu/loughranmcdonald-master-dictionary/, used to compute LM sentiments.
QQQ_weights.csv: Offers QQQ portfolio allocation proportion weights. Used for analysis.  

analysis.py: This code is the main code of the SSPM. Estimates the sentiment score(the p hat) for three different stakeholder levels. You should change configurations for each level analysis such as the cik lists, the document-term matrix, and the weights. 
annual_report_reader.py: This code contains the function for preprocessing
company_csv_builder.py: Creates a company's dataframe who contains 10-K reports contents with the columns:Name, CIK, Date, Body. The output of this code is used for constructing the document-term  matrix.
dataset_construction.py: Creates the document-term matrix, including return and volatility for a filing of a company at the publication date. 
lm_sent.py: Predicted LM sentiments for all filings on data set.
model.py: Several functions for training the model, making predictions, calculating loss/accuracy measures, and applying the Kalman filter.
vol_reader.py: Retrieves financial time series from Yahoo Finance and computes aggregated measures of volatility and returns for given company names.


How to run my local airflow server
The airflow datapipeline automates SEC filings extraction process on the SEC Filing Extraction Model, eventually creating the document-term matrix for our analysis with the integration of the SSPM.  
dags_firm_dict_builder.py: Creates an individual firm level document-term matrix.
dags_portfolio_dict_builder.py: Creates protfolio document-term matrix.
dags_sector_dict_builder.py: Creates sector document-term matrix.

Files in plugins are modified to equit the airflow server. The contents of all files come from the SFEM and SSPM.

1. My temporary local airflow server is installed on Docker. If you have not installed Docker, please install Docker first. 
2. If you install Docker, please follow the document to run airflow in Docker. Follow this document untill before ‘Accessing the enviroment’ section in the document. (the document: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
3. If you sucessfully install and initialise your airflow server on Docker, running the local airflow by executing $ docker compose up to check the loca server works well. 
4. If the airflow server runs well, comes to where the airflow is installed (usually on your root directory) and replace docker-compose.yaml with what I gave, which has the same name (i.e., docker-compose.yaml). the docker-compose.yaml file manages directory connection between my dag files and the local airflow server. Also, move ‘Docker file’, and ‘requirements.txt’ to the same directory where the docker-compose.yaml exists. 
5. Run again  by executing $ docker compose up. You can see my dags are connected to the local server and will run as I scheduled on the local airflow server. 

# Generation-of-Supervised-Sentiment-Metrics-for-Return-and-Volatility-Prediction-from-10-K-Filings
This paper introduces an automated system that generates sentiment metrics to support the prediction of stock returns and volatility.


## 1. Why did you start your project?

This project was initiated to explore the predictive power of 10-K regulatory filings, especially the risk factor section (Item 1A), on stock return and volatility. Given the increasing reliance on unstructured textual data in financial markets, the goal was to develop a supervised learning framework that generates sentiment scores from 10-K filings to support investment strategies across firm, portfolio, and sector levels. Prior research had shown significant market reaction to such filings, but few had explored return *and* volatility prediction using a scalable, automated sentiment analysis pipeline.

## 2. What issues did you find technically and in a domain context?

### Domain Issues:
- The risk factor disclosures in 10-Ks are known to be biased or overly generic due to managerial incentives.
- Textual data is high-dimensional and noisy, making it difficult to extract actionable insights directly.

### Technical Issues:
- HTML parsing of unstructured 10-K documents, especially identifying and extracting the "Item 1A Risk Factors" section accurately.
- Differentiating sentiment-charged vs. neutral words for supervised learning.
- Calibrating the sentiment model to reflect both return and volatility—two fundamentally different financial targets.
- Dealing with noise in time-series sentiment trends across the sector level.

## 3. What solutions did you consider?

- **Manual labeling vs. automatic supervised learning**: Manual labeling was ruled out due to cost and subjectivity. A supervised lexicon learning model based on returns/volatility was adopted .
- **Deep learning (BERT) vs. transparent models**: Chose a transparent, lightweight statistical model over black-box deep learning to retain interpretability and speed.
- **LM Dictionary vs. Model-Derived Lexicon**: While LM was used as a benchmark, the model derived its own sentiment lexicon directly from labeled return/volatility data.
- **Raw scores vs. Smoothed metrics**: Applied the Kalman Filter to smooth time-series sector and portfolio sentiment data for better signal stability.

## 4. What is your final decision among solutions?

The final system comprises an end-to-end pipeline with:

- **10-K Filing Extraction Model**: A scalable parser using regular expressions and HTML heuristics to extract both full text and Item 1A sections from EDGAR filings.
- **Sentiment Score Prediction Model**: A supervised lexicon learning framework that estimates sentiment scores using returns and volatility as labels.
- **Kalman Filter**: For smoothing sector- and portfolio-level sentiment trends.

This approach generated 12 different sentiment metrics (3 levels × 2 sections × 2 labels), which were quantitatively evaluated using Pearson correlation and qualitatively via the most influential words. The solution balances interpretability, scalability, and financial relevance—pushing the frontier of sentiment-driven market prediction from regulatory filings.



# Abstract
This paper introduces an automated system that generates sentiment metrics to support the prediction of stock returns and volatility. It focuses on three key stakeholder levels: sector, portfolio, and firm. Our system consists of two main components: the SEC Filing Extraction Model and the Supervised Lexicon Learning Model(or the Sentiment Score Prediction Model). The SEC Filing Extraction Model is responsible for preprocessing SEC filings, facilitating seamless integration with the subsequent Supervised Lexicon Learning Model. The lexicon model operates through a fourstage process: (i) identification of sentiment-charged words via predictive filtering, (ii) assignment of prediction weights to these tokens using topic modelling techniques, (iii) estimation of the most probable sentiment score by aggregating the weighted tokens through penalised likelihood, and (iv) application of the Kalman Filter for sector or portfolio sentiment trend analysis. In our empirical study, we study one of the most comprehensive and essential documents about a public firm - 10-K filling, and its Item1A risk factor section. At the sector level, our 10-K-centred model outperforms our risk-factor-centred model in extracting return/volatility-predictive signals in the context. At the portfolio level, both models excel in identifying return/volatility-predictive signals within the context. We recommend, at the company level, the risk model for trend and correlation analysis while advising both models for word analysis.

## Keywords
Sentiment Analysis, Fundamental Analysis, Data Orchestration, Machine Learning, Return, Volatility, 10-K fillings


# SEC Filing Extraction and Sentiment Score Prediction Models

## SEC Filing Extraction Model (SFEM)

### Files

- **10-K Report Scrapping.ipynb**: This file extracts SEC filings for a specified duration. (Hyperparameter: 10-K)
- **Risk Factors Extraction.ipynb**: Extracts the risk factor section from files generated by the 10-K Report Scrapping code.

## Sentiment Score Prediction Model (SSPM)

### Required Files

- **QQQ_constituents.csv** (located at `Code_4_10k`): List of firm's stock information in QQQ ETF. Used for extracting a list of CIKs for sector-level analysis.
- **top10_constituents.csv** (located at `Code_4_10k`): List of top 10 firm's stock information in QQQ ETF. Used for extracting a list of CIKs for portfolio-level analysis.
- **LM_dict.csv**: Loughran_McDonald (LM) dictionary from [LM Dictionary](https://sraf.nd.edu/loughranmcdonald-master-dictionary/), used to compute LM sentiments.
- **QQQ_weights.csv**: QQQ portfolio allocation proportion weights. Used for analysis.

### Code Files

- **analysis.py**: Main code of the SSPM. Estimates the sentiment score (the p hat) for three different stakeholder levels. Configurations for each level analysis (such as CIK lists, document-term matrix, and weights) need to be changed accordingly.
- **annual_report_reader.py**: Contains the function for preprocessing.
- **company_csv_builder.py**: Creates a company's dataframe containing 10-K reports contents with columns: Name, CIK, Date, Body. The output is used for constructing the document-term matrix.
- **dataset_construction.py**: Creates the document-term matrix, including return and volatility for a filing of a company at the publication date.
- **lm_sent.py**: Predicts LM sentiments for all filings in the dataset.
- **model.py**: Contains functions for training the model, making predictions, calculating loss/accuracy measures, and applying the Kalman filter.
- **vol_reader.py**: Retrieves financial time series from Yahoo Finance and computes aggregated measures of volatility and returns for given company names.

### Code Customization
Code is a bit crude as it is not refactored well for now. 
If you need more detail information, please contact me (sean.sanggyu@gmail.com). 

#### company_csv_builder.py

- **Input File**: `QQQ_constituents.csv` (`.../Code_4_10k/QQQ_constituents.csv`)
- **Modifications**:
  - Line 13: Change to your path setting for `QQQ_constituents.csv`.
  - Line 14: Change the output path to your preferred setting.
- **Output Path**: Defined on line 14.

#### dataset_construction.py

- **Input Files**:
  - `QQQ_constituents.csv` (`.../Code_4_10k/QQQ_constituents.csv`)
  - Firm’s data frame containing 10-K reports (outputs of `company_csv_builder.py`) (`.../Code_4_10k/company_df/fillings`)
- **Modifications**:
  - Line 27: Change the path for `QQQ_constituents.csv`.
  - Line 37: Change the path for files in the `company_df` folder.
  - Line 71: Change the path where firm’s data frames are saved.
  - Line 78: Change the path for saving the document-term matrix.

#### analysis.py

- **Generating Sentiment Matrix**: You can generate three different levels’ sentiment matrices by changing the configuration for each level. Follow the processes below:
- For your convenience, I separated configuration sections for each level, so you can uncomment one of them to run each. 
  - **Sector Level (QQQ ETF)**:
    - **Input Files**:
      - `QQQ_constituents.csv` (`.../Code_4_10k/QQQ_constituents.csv`)
      - `QQQ_weights.csv` (`.../Code_4_10k/QQQ_weights.csv`)
      - `df_all_QQQ_2.csv` (`.../Hons-project/data/df_all_QQQ_2.csv`) or `df_QQQ_2.csv` (`.../Hons-project/data/df_QQQ_2.csv`)
      - `lm_sent_all_QQQ_2.csv` (`.../Hons-project/data/lm_sent_all_QQQ_2.csv`) or `lm_sent_QQQ_2.csv` (`.../Hons-project/data/lm_sent_QQQ_2.csv`)
    - **Modifications**: Change paths to your relevant paths(on lines 32, 38, 47, 50, 55).
  - **Portfolio Level**:
    - **Input Files**:
      - `top10_QQQ_constituents.csv` (`.../Code_4_10k/top10_QQQ_constituents.csv`)
      - `df_all_top10_2.csv` (`.../Hons-project/data/df_all_top10_2.csv`) or `df_top10_2.csv` (`.../Hons-project/data/df_top10_2.csv`)
      - `lm_sent_all_top10_2.csv` (`.../Hons-project/data/lm_sent_all_top10_2.csv`) or `lm_sent_top10_2.csv` (`.../Hons-project/data/lm_sent_top10_2.csv`)
    - **Modifications**: Change paths to your relevant paths(on lines 60, 83, 86, 91).
  - **Firm Level**:
    - **Input Files**:
      - `df_all_{firm’s CIK numbers}_2.csv` (`.../Hons-project/data/df_all_{firm’s CIK numbers}_2.csv`) or `df_{firm’s CIK numbers}_2.csv` (`.../Hons-project/data/df_{firm’s CIK numbers}_2.csv`)
      - `lm_sent_all_{firm’s CIK numbers}_2.csv` (`.../Hons-project/data/lm_sent_all_{firm’s CIK numbers}_2.csv`) or `lm_sent_{firm’s CIK numbers}_2.csv` (`.../Hons-project/data/lm_sent_{firm’s CIK numbers}_2.csv`)
    - **Modifications**: Change paths to your relevant paths(on lines 115, 118, 123).

## Running Local Airflow Server

The Airflow data pipeline automates the SEC filings extraction process on the SEC Filing Extraction Model, creating the document-term matrix for analysis with the integration of the SSPM.

### DAG Files

- **dags_firm_dict_builder.py**: Creates an individual firm level document-term matrix.
- **dags_portfolio_dict_builder.py**: Creates portfolio document-term matrix.
- **dags_sector_dict_builder.py**: Creates sector document-term matrix.

### Setup

1. **Install Docker**: If Docker is not installed, install it first.
2. **Setup Airflow in Docker**: Follow the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) until the "Accessing the Environment" section.
3. **Run Local Airflow**: Initialize your Airflow server on Docker and check it works well by executing:
   ```sh
   $ docker compose up
   ```
4. **Configure Docker for Airflow**:
    - Replace `docker-compose.yaml` with the provided `docker-compose.yaml`.
    - Move `Dockerfile` and `requirements.txt` to the same directory where `docker-compose.yaml` exists.
5. **Run Airflow Server**: Execute:
   ```sh
   $ docker compose up
   ```
   Your DAGs should now be connected to the local server and will run as scheduled on the local Airflow server.

---

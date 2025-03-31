#%% PACKAGES
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 10:34:18 2023

@author: Sean Sanggyu Choi
"""


import os
os.chdir('/Users/apple/PROJECT/hons_project')

import pandas as pd
from datetime import datetime
import time
import pyarrow.parquet as pq
from model_spark import train_model_spark, predict_sent, loss, loss_perc, kalman_spark, predict_sent_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, to_timestamp, date_format
from pyspark.sql import functions as F
# Initialize Spark session

spark = SparkSession.builder \
    .appName("Sentiment Predictor with Spark") \
    .getOrCreate()
############################ #%% Configuration ############################

start_date = '2025-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')
trn_window = [start_date, end_date]
input_path = "/Users/apple/PROJECT/hons_project/data/SP500/nvidia/SEC/dtm_0001045810_2.parquet"


############################ LOAD DATA ############################
spark.conf.set("spark.sql.caseSensitive", "true")
df_all = spark.read.parquet(input_path)
df_all = df_all.withColumn("_ret", col("_ret") / 100).fillna(0.0)
df_all = df_all.drop("level_0")
df_all = df_all.withColumn("Date", to_timestamp("Date", "yyyy-MM-dd"))  # Convert to timestamp
df_all = df_all.withColumn("Date", date_format("Date", "yyyy-MM-dd"))  # Format as yyyy-MM-dd
df_all = df_all.orderBy(['Date', '_cik'])



############################ Helper Functions ############################
def adj_kappa(k, k_min=0.85):
    return 1 - (1 - k) / (1 - k_min)

############################ Model Training & In-Sample Predictions ############################
# Hyperparameters
kappa = adj_kappa(0.9) # 90% quantile of the count distribution
alpha_high = 100 # 50 words in both groups
alpha_low = alpha_high
llambda = 0.1 # Prenalty to shrink estimated sentiment towards 0.5 (i.e neutral)

# Filter training data
df_trn = df_all.filter((col("Date") <= trn_window[1]))

t0 = time.time()
for dep in ['_ret']:
    
    if dep == '_ret':
        # Train model for '_ret'
        mod = train_model_spark(df_trn, dep, kappa, alpha_high, pprint = False)
        S_pos_ret, S_neg_ret = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_ret = mod
    else:
        # Train model for '_vol'
        mod = train_model_spark(df_trn, dep, kappa, alpha_high, pprint = False, vol_q = None)
        S_pos_vol, S_neg_vol = mod[0][:alpha_high], mod[0][alpha_high:]
        mod_vol = mod
    
    # Make predictions on training data
    # Define metadata columns to exclude
    meta_cols = ['_cik', '_ret', '_vol', '_ret+1', '_vol+1']
    train_arts = df_trn.drop(*meta_cols)
    train_arts = train_arts.withColumn("Date", col("Date").cast("string"))  # Convert Date to string for Spark compatibility

    # Add the Date column to the predictions
    preds = predict_sent_spark(mod, train_arts, llambda)

    # preds is Spark DataFrame with p_hat and Date
    if dep == "_ret":
        mod_sent_ret = preds.select("Date", "p_hat")

    else:
        mod_sent_vol = preds.select("Date", "p_hat")
        
# Save the model        
# Apply the Kalman filter (assuming kalman_spark is compatible with Spark DataFrames)
mod_avg_ret = mod_sent_ret.groupBy("Date").agg(F.mean("p_hat").alias("p_hat"))
mod_kal_ret = kalman_spark(mod_avg_ret)
mod_kal_ret.write.parquet(f"mod_kal_ret_test.parquet", mode="overwrite")
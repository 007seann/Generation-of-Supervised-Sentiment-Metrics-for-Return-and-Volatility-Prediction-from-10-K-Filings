import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from airflow.hooks.postgres_hook import PostgresHook

n_forecast = 30

def plot_data_task():
    # Retrieve the data from the database
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, price FROM stock_price_predictions")
    results = cursor.fetchall()
    cursor.close()
    
    # Convert the data to a DataFrame
    predictions = pd.DataFrame(results, columns=['timestamp', 'price'])
    
    # Download historical stock data
    df = yf.download(tickers=['AAPL'], period='10y')

    # Organize the results in a DataFrame
    df_past = df[['Close']].reset_index()
    df_past.rename(columns={'Date': 'Date', 'Close': 'Actual'}, inplace=True)
    df_past['Forecast'] = np.nan
    df_past.loc[df_past.index[-1], 'Forecast'] = predictions['price'].iloc[0]

    df_future = pd.DataFrame({
        'Date': pd.date_range(start=df_past['Date'].iloc[-1] + pd.Timedelta(days=1), periods=n_forecast),
        'Forecast': predictions['price']
    })
    df_future['Actual'] = np.nan

    plt.figure(figsize=(16,6))
    plt.plot(df_past['Date'], df_past['Actual'], label='Actual')
    plt.plot(df_future['Date'], df_future['Forecast'], label='Forecast', color='red')

    plt.title('Apple Stock Price Prediction')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.xticks(rotation=45)

    # Save the plot
    filename = '/opt/airflow/dags/results/stock_price_predictions.jpg'
    plt.savefig(filename)
    plt.close()

import numpy as np
import yfinance as yf
from sklearn.preprocessing import MinMaxScaler
from keras.models import load_model
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

# Assuming these are constants you might use
n_lookback = 60
n_forecast = 30

def predict_and_store():
    df = yf.download(tickers=['AAPL'], period='10y')
    y = df['Close'].fillna(method='ffill')
    y = y.values.reshape(-1, 1)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler = scaler.fit(y)
    y = scaler.transform(y)

    # Load the saved LSTM model
    model = load_model('/opt/airflow/dags/models/lstm_model_nextmonth_apple_100_64.h5')

    
    # Make the prediction
    X = []
    Y = []
    for i in range(n_lookback, len(y) - n_forecast + 1):
        X.append(y[i - n_lookback: i])
        Y.append(y[i: i + n_forecast])
    X = np.array(X)
    Y = np.array(Y)
    X_ = y[- n_lookback:]
    X_ = X_.reshape(1, n_lookback, 1)

    Y_ = model.predict(X_).reshape(-1, 1)
    Y_ = scaler.inverse_transform(Y_)

    # Store the results in the PostgreSQL database
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM stock_price_predictions")
    for i in range(n_forecast):
        cursor.execute("INSERT INTO stock_price_predictions (timestamp, price) VALUES (%s, %s)", 
                       (datetime.now(), Y_[i].item()))
    conn.commit()
    cursor.close()

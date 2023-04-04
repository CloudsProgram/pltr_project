import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

# from prefect_gcp.bigquery import bigquery_load_cloud_storage
# from prefect_gcp.bigquery import bigquery_load_file







@task(log_prints=True, retries=2)
def extract(dataset_url: str) -> pd.DataFrame:
    """Read pltr historical stock data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> None:
    most_recent_stock_info_date  = df['Date'].iloc[-1]
    path = Path(f"pltr_stock_data/pltr_historic_till_{most_recent_stock_info_date}.parquet") #date will be last stock updated date 

    
    df['Date'] = pd.to_datetime(df['Date']) #convert str to datetime
    df["High"] = (df["High"].astype(float)).round(2) #convert str to float and only have 2 decimal places
    df["Low"] = (df["Low"].astype(float)).round(2) # same as above
    df["Close"] = (df["Close"].astype(float)).round(2) # same as above
    df["Adj Close"] = (df["Adj Close"].astype(float)).round(2) # same as above
    df["Volume"] = df["Volume"].astype('Int64') #conver str to int
    df = df.rename(columns={'Adj Close': 'Adj_Close'})
    df = df.rename(columns={'Date': 'Date_Recorded'})

    
    #making str into respective data
    return df, path

@task(log_prints=True, retries=2)
def write_gcs(df:pd.DataFrame, path:Path) -> None:
    path = Path(path).as_posix()
    gcs_block = GcsBucket.load("pltr-gcs")
    gcs_block.upload_from_dataframe(
        df=df,
        to_path=path,
        serialization_format='parquet'
    )

@flow(log_prints=True)
def web_to_gcs(dataset_url: str) -> None:
    df = extract(dataset_url)
    df_clean, path = transform(df)
    write_gcs(df_clean, path)



if __name__ == "__main__":
    dataset_url = "Put your copied link here"

    #from beg to 3/31/23
    dataset_url = "https://query1.finance.yahoo.com/v7/finance/download/PLTR?period1=1601510400&period2=1680307200&interval=1d&events=history&includeAdjustedClose=true"
    web_to_gcs(dataset_url)
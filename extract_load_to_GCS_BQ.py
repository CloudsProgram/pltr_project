import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def transform(cols):
    """only one row, so I can transform into desirable type with individual values?"""
    date = cols[0].text
    date = datetime.strptime(date, "%b %d, %Y").date() #convert to date time date
    open = float(cols[1].text)
    high = float(cols[2].text)
    low = float(cols[3].text)
    close = float(cols[4].text)
    adj_close = float(cols[5].text)
    volume = int(  (cols[6].text).replace(",", "")  )

    
    df = pd.DataFrame({'Date': [date], 'Open': [open], 
                       'High': [high], 'Low': [low], 
                       'Close':[close], 'Adj Close':[adj_close], 
                       'Volume':[volume]})
    
    df['Date'] = pd.to_datetime(df['Date']) #convert str to datetime
    df["High"] = (df["High"].astype(float)).round(2) #convert str to float and only have 2 decimal places
    df["Low"] = (df["Low"].astype(float)).round(2) # same as above
    df["Close"] = (df["Close"].astype(float)).round(2) # same as above
    df["Adj Close"] = (df["Adj Close"].astype(float)).round(2) # same as above
    df["Volume"] = df["Volume"].astype('Int64') #conver str to int
    df = df.rename(columns={'Adj Close': 'Adj_Close'})
    df = df.rename(columns={'Date': 'Date_Recorded'})
    
    return df, date

@task(retries=2)
def scrape_action():
    url =  "https://finance.yahoo.com/quote/PLTR/history?p=PLTR"

    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "cache-control":"max-age=0",
        "Accept-Language": "en-US,en;q=0.9", 
        'Referer': 'https://finance.yahoo.com/quote/PLTR/key-statistics?p=PLTR',
        'DNT': '1',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin', 
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57",   
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'lxml')
    #soup = BeautifulSoup(response.content, 'lxml')
    # #print(soup.prettify()) # Noticed that it doesn't return table

    table = soup.find_all('table')[0] #not quite sure why using [0] here? when I did [1] it shows out of range
    rows = table.find_all('tr')
    first_row = rows[1] # why is index 0 out of range ?
    cols = first_row.find_all('td')
    return cols


@task()
def workday_check(date, today_date):
    return date == today_date

# @task(log_prints=True)
# def write_local(df, date, today_date):
#     """Adjust {date} back to {today_date} when done with weekend development""" 
#     path = Path(f"pltr_stock_data/pltr_stock_{date}.parquet")
#     df.to_parquet(path, index=False, compression='gzip')
#     print("Scrape Job Done, csv saved to local folder")
#     return path

@flow(log_prints=True)
def scrape_stock_info(today_date):
    """
    The scrape gets saved if today's date matchs the most recent updated stock date.
    If the today's date doesn't match yahoo's most recent updated stock date, it means that the stock
    market is closed today.
    """
    columns = scrape_action()
    df, date = transform(columns)

    workday = workday_check(date, today_date)

    """delete the following line, this is used for weekend develoopment only"""
    #workday = True

    """Adjust {date} back to {today_date} when done with weekend development""" 
    if workday:
        path = Path(f"pltr_stock_data/pltr_stock_{date}.parquet")
        #path = write_local(df, date, today_date)
        return df, path
    else: #it's a holiday or a weekend
        print(f"Today's date is: {today_date}, Yahoo Finance stock historical data first row date is: {date}")
        print("Yahoo Finance stock record first row's date doesn't match today's date, might be a holiday")
        print("Not saving nor updating anythig, job is complete. ")
        return False       
        
@task(log_prints=True, retries=2)    
def write_gcs(df: pd.DataFrame, path: Path) -> None:
    path = Path(path).as_posix()
    gcs_block = GcsBucket.load("pltr-gcs")
    gcs_block.upload_from_dataframe(
        df=df,
        to_path=path,
        serialization_format='parquet'
    )
    #gcs_block.upload_from_path(from_path=path, to_path=path)
    pass


@task(log_prints=True, retries=2)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("pltr-gcp-creds")
    
    df.to_gbq(
        destination_table="pltr_stock_info.pltr_historical_data",
        project_id="de-project-pltr",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )

@flow()
def scrape_load_to_gcs_bq():
    """
    - Auotomation: This Script runs Mon ~ Fri 2pm PST, schedule via Prefect cron job


    - Action: 
        1) Scrapes stock info 
        2) Either a) save to local folder OR b) stops the job due to holiday
        if b) option occurs: job ends
        if a) option occurs:
            3) upload today's scraped and saved parquet onto GCS
            4) append that file's data onto BigQuery
            5) utlize DBT for data transformation & put it into BigQuery
            6) Looker studio update info
        

    (might change)Save csv to local folder (might want to change this without saving locally)
    upload file in to GCS
    get the info from GCS and append it to BigQuery
    How do I get it into dbt to do transformation?
    """

 
    today_date = date.today()

    # if scrape takes place, a path will be returned
    # if scrape didn't occur, False will be returned    
    df, path = scrape_stock_info(today_date)


    if path:
        write_gcs(df, path)
        write_bq(df)
   

    


if __name__ == "__main__":
    #scheduled task
    scrape_load_to_gcs_bq()


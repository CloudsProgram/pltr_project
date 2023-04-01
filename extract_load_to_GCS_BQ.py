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
    date = cols[0].text
    date = datetime.strptime(date, "%b %d, %Y").date() #convert to date time date
    open = float(cols[1].text)
    high = float(cols[2].text)
    low = float(cols[3].text)
    close = float(cols[4].text)
    adj_close = float(cols[5].text)
    volume = int(  (cols[6].text).replace(",", "")  )
    return date, open, high, low, close, adj_close, volume

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

@task(log_prints=True)
def write_local(date, open, high, low, close, adj_close, volume, today_date):
    """Not converting to parquet, b/c i'm only uploading one roll"""
    df = pd.DataFrame({'date': [date], 'open': [open], 
                       'high': [high], 'low': [low], 
                       'close':[close], 'adj_close':[adj_close], 
                       'volume':[volume]})

    """Adjust {date} back to {today_date} when done with weekend development""" 
    path = Path(f"pltr_stock_data/pltr_stock_{date}.csv")
    df.to_csv(path, index=False)
    print("Scrape Job Done, csv saved to local folder")
    return path

@flow(log_prints=True)
def scrape_stock_info(today_date):
    """
    The scrape gets saved if today's date matchs the most recent updated stock date.
    If the today's date doesn't match yahoo's most recent updated stock date, it means that the stock
    market is closed today.
    """
    columns = scrape_action()
    date, open, high, low, close, adj_close, volume = transform(columns)

    workday = workday_check(date, today_date)

    """delete the following line, this is used for weekend develoopment only"""
    workday = True

    if workday:
        path = write_local(date, open, high, low, close, adj_close, volume, today_date)
        return path
    else: #it's a holiday or a weekend
        print(f"Today's date is: {today_date}, Yahoo Finance stock historical data first row date is: {date}")
        print("Yahoo Finance stock record first row's date doesn't match today's date, might be a holiday")
        print("Not saving nor updating anythig, job is complete. ")
        return False       
        
@task(log_prints=True, retries=2)    
def write_gcs(path: Path) -> None:
    path = Path(path).as_posix()
    gcs_block = GcsBucket.load("pltr-gcs")
    # gcs_block.upload_from_dataframe(
    # )
    gcs_block.upload_from_path(from_path=path, to_path=path)
    pass



@flow()
def scrape_load_to_gcs_bq():
    """
    - Auotomation: This Script runs Mon ~ Fri 2pm PST, schedule via Prefect cron job


    - Action: 
        1) Scrapes stock info 
        2) Either a) save to local folder OR b) stops the job due to holiday
        if b) option occurs: job ends
        if a) option occurs:
            3) upload today's scraped and saved csv onto GCS
            4) append that file's data onto BigQuery
            5) utlize DBT for data transformation & put it into BigQuery
            6) Looker studio update info
        

    (might change)Save csv to local folder (might want to change this without saving locally)
    upload file in to GCS
    get the info from GCS and append it to BigQuery
    How do I get it into dbt to do transformation?
    """
    """Issue: not saving stuff to local, b/c I got today's date, which matches yahoo finance's date"""
 
    today_date = date.today()

    # if scrape takes place, a path will be returned
    # if scrape didn't occur, False will be returned    
    path = scrape_stock_info(today_date)


    if path:
        write_gcs(path)
        #write_bq(path)
   

    


if __name__ == "__main__":
    #scheduled task
    scrape_load_to_gcs_bq()


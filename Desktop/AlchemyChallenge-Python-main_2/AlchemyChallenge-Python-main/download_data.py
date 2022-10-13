import pathlib
import requests
from datetime import datetime
import logging
import pandas as pd
import csv

## Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('logs.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

## Download .csv data and return a dataframe
def download_data(category, url):
    url = url
    response = requests.get(url)

    ## current date
    now = datetime.now() 
    year_month = now.strftime("%Y-%b") # year-month    
    date = now.strftime("%m-%d-%Y") # month-day-year
    
    ## make directory
    pathlib.Path(f"data/{category}/{year_month}").mkdir(parents=True, exist_ok=True) 

    if(category == 'cines'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))

        ## csv --> dataframe
        df_cines = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_cines

    if(category == 'museos'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))

        ## csv --> dataframe
        df_museos = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_museos

    if(category == 'bibliotecas'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))
        
        ## csv --> dataframe
        df_bibliotecas = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_bibliotecas


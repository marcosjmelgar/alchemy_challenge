import pathlib
import requests
import csv
from datetime import datetime
import logging
import pandas as pd

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

    if(category == 'museos'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))

        ## csv --> dataframe
        df_museos = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_museos

    if(category == 'cines'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))

        ## csv --> dataframe
        df_cines = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_cines

    if(category == 'bibliotecas'):
        with open(f'data/{category}/{year_month}/{category}-{date}.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))
        
        ## csv --> dataframe
        df_bibliotecas = pd.read_csv(f'data/{category}/{year_month}/{category}-{date}.csv', on_bad_lines='skip')

        logger.info(f'Downloading data from "{category}"')

        return df_bibliotecas

"""
def create_df_table(df_museos, df_cines, df_bibliotecas):
    ## concat dataframes in 1 dataframe (table)
    df_cultural = pd.concat([df_museos, df_cines, df_bibliotecas])

    ## rename df columns
    df_cultural = df_cultural.drop(['Observaciones', 'subcategoria', 'piso', 
        'cod_area', 'Latitud', 'Longitud', 'TipoLatitudLongitud',
        'Info_adicional', 'fuente', 'jurisdiccion', 'año_inauguracion',
        'IDSInCA'], axis=1)

    df_cultural.columns = ['Cod_Loc', 'Id_Provincia', 'Id_Departamento', 'Categoria', 
        'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'Cod_Postal', 'Telefono', 'Mail', 'Web']
"""
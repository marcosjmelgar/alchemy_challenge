# db.py
# provides a connect() function that returns a SQLAlchemy connection to the database passed to config(); sample uses config() defaults. 

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from config import config
import logging

from download_data import download_data
from create_df import create_df_cultural, create_df_registros, create_df_cines

## Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('logs.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

file_handler = logging.FileHandler('logs.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

## Create table and db
def create_database():
    """ Connect to the PostgreSQL database server """

    try:
        # read connection params
        params = config()

        conn_string = f"postgresql://{params['username']}:{params['password']}@{params['hostname']}:{params['port']}/{params['database']}"

        # connect to PostgreSQL server
        logger.info('Connecting...')

        engine = create_engine(conn_string)
        
        # create dataframes
        df_museos = download_data('museos', 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv')
        df_cines = download_data('cines', 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv')
        df_bibliotecas = download_data('bibliotecas', 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv')
        
        # create df_cultural --> to_sql
        df_cultural = create_df_cultural(df_museos, df_cines, df_bibliotecas)
        df_cultural.to_sql('cultural', con=engine, if_exists='replace')
        #df_cultural.to_csv('cultural.csv')

        logger.info('Cultural table was created on PostgreSQL!')

        # create df_registros --> to_sql
        df_registros = create_df_registros(df_cultural)
        df_registros.to_sql('registros', con=engine, if_exists='replace')
        #df_registros.to_csv('registros.csv')

        logger.info('Registros table was created on PostgreSQL!')    
        
        # create df_cines_prov --> to_sql
        df_cines_prov = create_df_cines(df_cines)
        df_cines_prov.to_sql('cines', con=engine, if_exists='replace')
        #df_cines_prov.to_csv('cines.csv')

        logger.info('Cines table was created on PostgreSQL!')    

        connection = engine.connect()

        ## read sql 
        #cursor = connection
        #sql="""select * from cultural"""
        #query_results = cursor.execute(sql).fetchall()
        #df = pd.DataFrame(query_results)
        #print(df[:10])

        logger.info('Connected to the database!')

        #connection.close()
        #logger.info('Closed database.')
        
        return connection
    except:
        return logger.exception('Connection failed.')

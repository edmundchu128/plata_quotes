import duckdb
import pandas as pd
import datetime
import logging
import pytz
import os

logging.basicConfig(
    format = "%(asctime)s - %(levelname)-8s -%(message)s",
    level = logging.INFO,
    datefmt = "Y-%m-%d %H:%M:%S"
)

ROOT_PATH = r"C:/Users/edmun/OneDrive/Documents/GitHub/plata_assignment/tmp/"
FILE_NAME = "PlataAssignment_Data.xlsx"

DUCKDB_PATH = '../../duckdb/'

def read_excel(path:str, sheet_name = 0):
    """
    Args:
        path: String, path to file for ingestion
        sheet_name: String, default to 0 (first sheet of the file)

    This function is a helper function to read excel file and add metadata columns
    Returns dataframe
    """

    logging.info('Begin reading file at: ' + path)
    quotes_df = pd.read_excel(path, sheet_name=sheet_name)
    if len(quotes_df)>0:
        quotes_df['ingested_at'] = datetime.datetime.now(pytz.timezone('UTC'))
        quotes_df['file_name'] = path
        return quotes_df

def load_to_db():
    """
    Currently hardcoded to one path
    """

    quotes_df = read_excel(ROOT_PATH+FILE_NAME)
    
    conn = duckdb.connect(DUCKDB_PATH+'raw.db')
    
    try:
        logging.info('Begin loading file into table...')

        result = conn.execute("""INSERT INTO RAW.QUOTES.QUOTES SELECT * FROM quotes_df;""")      
        logging.info('Succesfully written to table RAW.QUOTES.QUOTES')

    
    except duckdb.CatalogException as e:
        logging.error(str(e))
        logging.info('Succesfully created table RAW.QUOTES.QUOTES')
        result = conn.execute("""CREATE TABLE RAW.QUOTES.QUOTES AS SELECT * FROM quotes_df;""")
        logging.info('Succesfully created table RAW.QUOTES.QUOTES')


def check_for_incremental():
    conn = duckdb.connect(DUCKDB_PATH+'raw.db')
    result = conn.execute('SELECT DISTINCT file_name from RAW.QUOTES.QUOTES;').df()

    list_files = [ROOT_PATH+ i for i in os.listdir(ROOT_PATH)]

    new_files = list(set(list_files)-set(result['file_name'].to_list()))
    return new_files

if __name__ == '__main__':
    
    new_files = check_for_incremental()

    if len(new_files)<=0:
        logging.info('No new files.')
    
    logging.info('Begin file ingestion...')
    load_to_db()
    logging.info('Pipeline finished.')
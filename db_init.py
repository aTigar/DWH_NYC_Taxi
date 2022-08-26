import os
import urllib

from dotenv import load_dotenv
from sqlalchemy import create_engine
from loguru import logger
import pandas as pd

load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USER')
password = os.getenv('PASSWORD')

META_TABLE_NAME = 'name'


params = urllib.parse.quote_plus("'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password")

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


def get_meta_info():
    table_df = pd.read_sql_table(
        META_TABLE_NAME,
        con=engine
    )
    return table_df


if __name__ == '__main__':
    logger.info('Connect to DB')
    print(get_meta_info())
    logger.success('done')

import os
import urllib
import glob

from dotenv import load_dotenv
from sqlalchemy import create_engine
from loguru import logger
import pandas as pd

import load, transform

load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USER')
password = os.getenv('PASSWORD')

META_TABLE_NAME = 'name'

params = urllib.parse.quote_plus(
    "'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password")

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


def get_meta_info():
    table_df = pd.read_sql_table(
        META_TABLE_NAME,
        con=engine
    )
    return table_df


def prepare_taxi_data():
    subdir = f'.{os.sep}data{os.sep}taxi{os.sep}'

    yellow_files = (glob.glob(f'{subdir}yellow*'), 'yellow')
    green_files = (glob.glob(f'{subdir}green*'), 'green')
    fhv_files = (glob.glob(f'{subdir}fhv*'), 'fhv')

    file_types = [yellow_files, green_files, fhv_files]

    # files = [files[-1], files[-2], files[-3]]  # TODO delete this row before deployment

    df_final = pd.DataFrame()
    for file_type in file_types:
        files = file_type[0]
        taxi_type = file_type[1]
        for file in files:
            df_raw = load.load_taxi_data(file)
            df_clean = transform.clean_taxi_data(df_raw, taxi_type)
            df_final = pd.concat([df_final, df_clean])
    return df_final

def prepare_covid_data():
    subdir = f'.{os.sep}data{os.sep}covid{os.sep}'
    file = subdir + 'rc75-m7u3.csv'

    df_final = pd.DataFrame()

    df_raw = load.load_csv_data(file)
    df_clean = transform.clean_covid_data(df_raw)
    df_final = pd.concat([df_final, df_clean])

    return df_final



def prepare_weather_data():
    subdir = f'.{os.sep}data{os.sep}weather{os.sep}'
    files = glob.glob(f'{subdir}2018*') + glob.glob(f'{subdir}2019*') + glob.glob(f'{subdir}2020*') + glob.glob(
        f'{subdir}2021*') + glob.glob(f'{subdir}2022*')

    df_final = pd.DataFrame()
    for file in files:
        df_raw = load.load_csv_data(file)
        df_clean = transform.clean_weather_data(df_raw)
        df_final = pd.concat([df_final, df_clean])

    return df_final


if __name__ == '__main__':
    # logger.info('Connect to DB')
    # print(get_meta_info())
    # logger.success('done')
    # df = prepare_taxi_data()
    # df.to_csv('taxi_test2.csv')
    # df = prepare_weather_data()
    df = prepare_covid_data()
    logger.success('u did it')

import glob
import os
import urllib

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine

import extract
import transform
import load
from config import *


def prepare_taxi_data(taxi_type: str) -> pd.DataFrame:
    """
    Prepares data from local storage for database.

    :param taxi_type: one of 'green', 'yellow', or 'fhv'
    :return: Cleaned dataframe for all available taxi data of one kind.
    """
    subdir = f'.{os.sep}data{os.sep}taxi{os.sep}'

    files = glob.glob(f'{subdir}{taxi_type}*')

    df_final = pd.DataFrame()

    for file in files:
        df_raw = extract.load_taxi_data(file)
        df_clean = transform.clean_taxi_data(df_raw)
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
    taxi_types = ['yellow', 'green', 'fhv']
    df = prepare_taxi_data(taxi_types[0])
    df.to_csv('taxi_test3.csv')
    # df = prepare_weather_data()
    # df = prepare_covid_data()
    logger.success('u did it')

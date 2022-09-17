import glob

import extract
import load
import transform
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

    df_raw = extract.load_csv_data(file)
    df_clean = transform.clean_covid_data(df_raw)
    df_final = pd.concat([df_final, df_clean])

    return df_final


def prepare_weather_data():
    subdir = f'.{os.sep}data{os.sep}weather{os.sep}'
    files = glob.glob(f'{subdir}2018*') + glob.glob(f'{subdir}2019*') + glob.glob(f'{subdir}2020*') + glob.glob(
        f'{subdir}2021*') + glob.glob(f'{subdir}2022*')

    df_final = pd.DataFrame()
    for file in files:
        df_raw = extract.load_csv_data(file)
        df_clean = transform.clean_weather_data(df_raw)
        df_final = pd.concat([df_final, df_clean])

    return df_final


if __name__ == '__main__':
    engine, ins = load.connect_sqlalchemy(USER, PASSWORD, SERVER, DATABASE)

    df = prepare_covid_data()
    df.to_csv('covid.csv')
    load.load_dataframe_to_database(df, 'covid', engine)

    df = prepare_weather_data()
    df.to_csv('weather.csv')
    load.load_dataframe_to_database(df, 'weather', engine)

    for taxi_type in TAXI_TYPES:
        df = prepare_taxi_data(taxi_type)
        df.to_csv('covid.csv')
        load.load_dataframe_to_database(df, f'taxi_{taxi_type}', engine)

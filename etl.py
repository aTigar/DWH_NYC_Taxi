import datetime
import glob

import pandas as pd

import extract
import load
import transform
from config import *


def sort_features(_df: pd.DataFrame):
    cols = list(_df.columns)
    c_ints = []
    c_str = []
    for c in cols:
        try:
            c_ints.append(int(c))
        except ValueError:
            c_str.append(c)
    c_ints.sort()
    c_str.extend([str(i) for i in c_ints])

    df_return = _df[c_str]
    return df_return


def prepare_taxi_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepares data from local storage for database.

    :param taxi_type: one of 'green', 'yellow', or 'fhv'
    :return: Cleaned dataframe for all available taxi data of one kind.
    """
    taxi_types = ['green', 'yellow', 'fhv']
    actions = ['pickup', 'dropoff']

    subdir = f'.{os.sep}data{os.sep}taxi{os.sep}'

    df_final = pd.DataFrame()

    for taxi_type in taxi_types:
        # select parquet files starting with taxi_type
        files = glob.glob(f'{subdir}{taxi_type}*.parquet')

        for file in files:
            df_raw = extract.load_taxi_data(file)
            df1 = transform.clean_taxi_data(df_raw, taxi_type)
            df_final = pd.concat([df_final, df1])

        # sort features
        df_final = sort_features(df_final)

    return df_final


def prepare_covid_data():
    subdir = f'.{os.sep}data{os.sep}covid{os.sep}'
    file = subdir + 'rc75-m7u3.csv'

    df_final = pd.DataFrame()

    df_raw = extract.load_csv_data(file)
    df_clean = transform.clean_covid_data(df_raw)
    df_final = pd.concat([df_final, df_clean])

    df_final = df_final.reset_index().rename(columns={'index': 'CID'})
    df_final['dateID'] = df_final['date_of_interest'].apply(pd.Timestamp.toordinal).astype(int)

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

    df_final = df_final.reset_index().rename(columns={'index': 'WID'})
    df_final['dateID'] = df_final['DATE'].apply(pd.Timestamp.toordinal).astype(int)

    return df_final


def prepare_calender_data(_t0: str, _t1: str) -> pd.DataFrame:
    df = pd.DataFrame()
    df['date'] = pd.date_range(start=_t0, end=_t1)
    df['dateID'] = df['date'].apply(pd.Timestamp.toordinal).astype(int)
    df['year'] = df['dateID'].apply(lambda x: x.year)
    df['month'] = df['dateID'].apply(lambda x: x.month)
    df['day'] = df['dateID'].apply(lambda x: x.day)
    df['dayofweek'] = df['dateID'].dt.dayofweek
    df['day_name'] = df['dateID'].dt.day_name()
    return df


if __name__ == '__main__':
    logger.info('Starting...')

    # init
    engine, ins = load.connect_sqlalchemy(USER, PASSWORD, SERVER, DATABASE)

    # covid data
    df_covid = prepare_covid_data()
    df_covid.to_csv('covid.csv')
    load.load_dataframe_to_database(df_covid, 'covid', engine)

    # weather data
    df_weather = prepare_weather_data()
    df_weather.to_csv('weather.csv')
    load.load_dataframe_to_database(df_weather, 'weather', engine)

    # taxi data
    df_facts = prepare_taxi_data()
    df_facts = df_facts.merge(df_weather[['WID', 'dateID']])
    df_facts = df_facts.merge(df_covid[['CID', 'dateID']])

    df_facts.to_csv(f'df_taxi_facts.csv')

    # load.load_dataframe_to_database(df_pickup, f'taxi_pickup', engine)
    # load.load_dataframe_to_database(df_dropoff, f'taxi_dropoff', engine)
    # load.load_dataframe_to_database(df_dist, f'taxi_distances', engine)

    # taxi meta data
    df = pd.read_csv('data/taxi_zone_lookup_enhanced.csv')
    load.load_dataframe_to_database(df, 'taxi_lookup', engine)

    # calender data
    t0 = '2018-01-01'
    t1 = '2022-12-31'
    df_calender = prepare_calender_data(t0, t1)
    load.load_dataframe_to_database(df_calender, f'calender', engine)

    logger.success('Done!')


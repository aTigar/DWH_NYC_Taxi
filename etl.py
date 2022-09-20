import glob
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

    df_pickup_final = pd.DataFrame()
    df_dropoff_final = pd.DataFrame()
    df_distance_final = pd.DataFrame()

    for taxi_type in taxi_types:
        # select parquet files starting with taxi_type
        files = glob.glob(f'{subdir}{taxi_type}*.parquet')

        for file in files:
            df_raw = extract.load_taxi_data(file)
            df1, df2, df3 = transform.clean_taxi_data(df_raw, taxi_type)
            df_pickup_final = pd.concat([df_pickup_final, df1])
            df_dropoff_final = pd.concat([df_dropoff_final, df2])
            df_distance_final = pd.concat([df_distance_final, df3])

        # sort features
        df_pickup_final = sort_features(df_pickup_final)
        df_dropoff_final = sort_features(df_dropoff_final)

    return df_pickup_final, df_dropoff_final, df_distance_final


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
    logger.info('Starting...')

    # init
    # engine, ins = load.connect_sqlalchemy(USER, PASSWORD, SERVER, DATABASE)
    #
    # # covid data
    # df = prepare_covid_data()
    # df.to_csv('covid.csv')
    # load.load_dataframe_to_database(df, 'covid', engine)
    #
    # # weather data
    # df = prepare_weather_data()
    # df.to_csv('weather.csv')
    # load.load_dataframe_to_database(df, 'weather', engine)

    # taxi data

    df_pickup, df_dropoff, df_dist = prepare_taxi_data()

    df_pickup.to_csv(f'df_pickup.csv')
    df_dropoff.to_csv(f'df_dropoff.csv')
    df_dist.to_csv(f'df_distances.csv')

    # load.load_dataframe_to_database(df_pickup, f'taxi_pickup', engine)
    # load.load_dataframe_to_database(df_dropoff, f'taxi_dropoff', engine)
    # load.load_dataframe_to_database(df_date, f'taxi_distances', engine)

    # # taxi meta data
    # df = pd.read_csv('data/taxi_zone_lookup_enhanced.csv')
    # load.load_dataframe_to_database(df, 'taxi_lookup', engine)

    logger.success('Done!')


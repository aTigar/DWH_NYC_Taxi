from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd

from config import *

column_mapper = {'tpep_pickup_datetime': 'pickup_datetime',
                 'tpep_dropoff_datetime': 'dropoff_datetime',
                 'lpep_pickup_datetime': 'pickup_datetime',
                 'lpep_dropoff_datetime': 'dropoff_datetime',
                 'PULocationID': 'PUlocationID',
                 'DOLocationID': 'DOlocationID',
                 }
weather_column_mapper = {
    'WT01': 'FOG',  # fog, ice fog or freezing fog (may include heavy fog) / value nan or 1.0
    'WT02': 'HFOG',  # heavy fog or heaving freezing fog (not always distinguished from fog) / value nan or 1.0
    'WT03': 'THUNDER',  # thunder / value nan or 1.0
    'WT04': 'ICEP',  # ice pellets, sleet, snow pellets or small hail / value nan or 1.0
    'WT05': 'HAIL',  # hail (may include small hail) / value nan or 1.0
    'WT06': 'GLAZE',  # glaze or rime / value nan or 1.0
    'WT07': 'DUST',  # dust, volcanic ash, blowing dust, blowing sand or blowing obstruction / value nan or 1.0
    'WT08': 'SMOKE',  # smoke or haze / value nan or 1.0
    'WT09': 'BLOW',  # blowing or drifting snow / value nan or 1.0
    'WT11': 'HWIND',  # high wind or damaging wind / value nan or 1.0
}

taxi_zone_lookup = pd.read_csv('data/taxi_zone_lookup.csv')
taxi_zone_lookup['temp'] = ' '
taxi_zone_lookup['location'] = taxi_zone_lookup['Borough'] + taxi_zone_lookup['temp'] + taxi_zone_lookup['Zone']
taxi_zone_lookup = taxi_zone_lookup[['LocationID', 'location']]
taxi_zone_lookup = taxi_zone_lookup.set_index('LocationID').to_dict()['location']


def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.

    Source: http://stackoverflow.com/a/29546836/2901002
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km


def agg_resolve_dates(df_group, feature):
    ret_df = pd.DataFrame()
    for group in df_group.groups:
        sub_df = pd.DataFrame(index=[group], data=[{'year': group.year, 'month': group.month, 'day': group.day}])
        ret_df = pd.concat([ret_df, sub_df], axis=0)
    return ret_df


def agg_get_value_counts_as_columns(df_group, feature):
    """
    pivots the summary for each location as columns

    :param df_group: the key for the groups
    :param feature: the key of the feature to extract
    :return: dataframe with grouped summaries' ad columns
    """
    ret_df = pd.DataFrame()
    for group in df_group.groups:
        sub_df = df_group.get_group(group)
        sub_df = sub_df[sub_df[feature].notna()]
        sub_df[feature] = sub_df[feature].astype('int')
        sub_df = sub_df[feature].value_counts().reset_index()
        sub_df['count'] = sub_df[feature]
        sub_df[feature] = sub_df['index']
        # sub_df = sub_df.swapaxes(0, 1)
        # sub_df.columns = list(sub_df.iloc[0])
        # sub_df = sub_df.drop('index')
        sub_df = sub_df.assign(index=group).set_index('index')
        ret_df = pd.concat([ret_df, sub_df], axis=0)
    return ret_df


def extract_features(df, group_key, feature_aggregations):
    """
    extracts features from df by aggregation
    :param df:
    :param group_key:
    :param feature_aggregations:
    :return:
    """
    df_new = pd.DataFrame()
    df_group = df.groupby(group_key)
    df_aggregated = pd.DataFrame()
    logger.info(f'Extract features {list(feature_aggregations.keys())}...')
    for feature in feature_aggregations:
        if feature in list(df.columns):
            aggregate = feature_aggregations[feature]
            if isinstance(aggregate, str):
                df_new = df_group[feature]
                df_new = getattr(df_new, aggregate)()
                df_new.columns = [feature]
            elif callable(aggregate):
                df_new = aggregate(df_group, feature)
            else:
                logger.error(f"Can't aggregate with {aggregate} ({type(aggregate)}).")

            df_aggregated = pd.concat([df_aggregated, df_new], axis=1)
        else:
            logger.warning(f'{feature} not found.')

    return df_aggregated


def clean_taxi_data(df: pd.DataFrame, taxi_type: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    clean pd.DataFrame
    :return:
    """
    # fix naming consensus
    df = df.rename(columns=column_mapper)

    # transform and reduce timestamp to date
    logger.info('Cleaning datetime ...')
    df['date'] = pd.to_datetime(df['pickup_datetime'], unit='us').dt.date

    # get year and month from data appearance
    temp_df = pd.DataFrame()
    temp_df['dates'] = list(set(df['date']))
    temp_df['years'] = temp_df['dates'].apply(lambda x: x.year)
    temp_df['months'] = temp_df['dates'].apply(lambda x: x.month)
    temp_df = temp_df.value_counts(subset=['years', 'months'])
    year, month = temp_df.index[0]
    start_date = date(year, month, 1)
    end_date = date(year, month, (start_date.replace(month=start_date.month % 12 + 1, day=1) - timedelta(days=1)).day)

    # remove data rows with time outliers
    df = df[df['date'].between(start_date, end_date)].copy()

    logger.success('Datetime cleaned.')

    # calculate missing features
    if "trip_distance" not in list(df.columns):
        logger.info('trip_distance not available: calculating...')

        # delete empty location IDs
        df = df.dropna(subset=['PUlocationID', 'DOlocationID'])

        # map ID to name
        df['PUlocation_lat'] = df['PUlocationID'].astype(int).map(TAXI_ZONE_LOOKUP['lat'])
        df['PUlocation_long'] = df['PUlocationID'].astype(int).map(TAXI_ZONE_LOOKUP['long'])
        df['DOlocation_lat'] = df['DOlocationID'].astype(int).map(TAXI_ZONE_LOOKUP['lat'])
        df['DOlocation_long'] = df['DOlocationID'].astype(int).map(TAXI_ZONE_LOOKUP['long'])

        df = df.dropna(subset=['PUlocation_long', 'DOlocation_long'])

        df['trip_distance'] = haversine_np(df['PUlocation_long'], df['PUlocation_lat'],
                                           df['DOlocation_long'], df['DOlocation_lat'])

        logger.success('trip_distance calculated.')

    # aggregation - reduce smallest timeunit to a day
    feature_aggregations = {
        'trip_distance': 'mean',
        'PUlocationID': agg_get_value_counts_as_columns,
        'DOlocationID': agg_get_value_counts_as_columns,
    }

    df_pickups = extract_features(df, 'date',  {'PUlocationID': agg_get_value_counts_as_columns})
    df_dropoffs = extract_features(df, 'date', {'DOlocationID': agg_get_value_counts_as_columns})

    df_distances = extract_features(df, 'date', {'trip_distance': 'mean'})

    # set taxi_type
    df_dropoffs = df_dropoffs.assign(taxi_type=taxi_type)
    df_pickups = df_pickups.assign(taxi_type=taxi_type)
    df_distances = df_distances.assign(taxi_type=taxi_type)

    logger.success('Features extracted.')

    return df_pickups, df_dropoffs, df_distances


def clean_covid_data(df: pd.DataFrame):
    """
    clean pd.DataFrame
    :return:
    """
    logger.info('Cleaning covid data ...')

    # convert timestamp to yyyy-mm-dd
    df['date_of_interest'] = pd.to_datetime(df['date_of_interest']).dt.date

    return df


def clean_weather_data(df: pd.DataFrame):
    """
    clean pd.DataFrame
    :return:
    """
    logger.info('Cleaning weather data ...')
    # convert timestamp to yyyy-mm-dd
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    # remove unnecessary columns
    # df = df.drop(['NAME', 'STATION'],axis=1)
    df = df[df['STATION'] == 'USW00094789']
    # rename columns for better identification
    df = df.rename(columns=weather_column_mapper)

    logger.success('Cleaning weather done.')

    return df


def run():
    pass


if __name__ == '__main__':
    run()

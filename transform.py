import pandas as pd
from loguru import logger
from geopy.geocoders import Nominatim
import geopy.distance

column_mapper = {'tpep_pickup_datetime': 'pickup_datetime',
                 'tpep_dropoff_datetime': 'dropoff_datetime',
                 'lpep_pickup_datetime': 'pickup_datetime',
                 'lpep_dropoff_datetime': 'dropoff_datetime',
                 'PULocationID': 'PUlocationID',
                 'DOLocationID': 'DOlocationID',
                 }
weather_column_mapper = {
    'WT01' : 'FOG',     # fog, ice fog or freezing fog (may include heavy fog) / value nan or 1.0
    'WT02' : 'HFOG',    # heavy fog or heaving freezing fog (not always distinguished from fog) / value nan or 1.0
    'WT03' : 'THUNDER', # thunder / value nan or 1.0
    'WT04' : 'ICEP',    # ice pellets, sleet, snow pellets or small hail / value nan or 1.0
    'WT05' : 'HAIL',    # hail (may include small hail) / value nan or 1.0
    'WT06' : 'GLAZE',   # glaze or rime / value nan or 1.0
    'WT07' : 'DUST',    # dust, volcanic ash, blowing dust, blowing sand or blowing obstruction / value nan or 1.0
    'WT08' : 'SMOKE',   # smoke or haze / value nan or 1.0
    'WT09' : 'BLOW',    # blowing or drifting snow / value nan or 1.0
    'WT11' : 'HWIND',   # high wind or damaging wind / value nan or 1.0
}

taxi_zone_lookup = pd.read_csv('data/taxi_zone_lookup.csv')
taxi_zone_lookup['temp'] = ' '
taxi_zone_lookup['location'] = taxi_zone_lookup['Borough'] + taxi_zone_lookup['temp'] + taxi_zone_lookup['Zone']
taxi_zone_lookup = taxi_zone_lookup[['LocationID', 'location']]
taxi_zone_lookup = taxi_zone_lookup.set_index('LocationID').to_dict()['location']

def clean_taxi_data(df: pd.DataFrame):
    """
    clean pd.DataFrame
    :return:
    """

    def get_value_counts_as_columns(df_group, feature):
        ret_df = pd.DataFrame()
        for group in df_group.groups:
            sub_df = df_group.get_group(group)  # .groupby('date')#
            sub_df = sub_df[sub_df[feature].notna()]
            sub_df = sub_df[feature].value_counts().reset_index()
            sub_df['temp'] = feature + '_'
            sub_df['index'] = sub_df['index'].astype('str')
            sub_df['index'] = sub_df['temp'] + sub_df['index']
            sub_df = sub_df.drop(columns=['temp'])
            # sub_df['index'] = sub_df['index'].apply(lambda x: f'{feature}_{int(x)}')
            sub_df = sub_df.swapaxes(0, 1)
            sub_df.columns = list(sub_df.iloc[0])
            sub_df = sub_df.drop('index')
            sub_df.index = [group]
            ret_df = pd.concat([ret_df, sub_df], axis=0)
        return ret_df

    # transform and reduce timestamp to date
    # if 'tpep_pickup_datetime' in list(df.columns):
    df = df.rename(columns=column_mapper)

    # if 'lpep_pickup_datetime' in list(df.columns):
    #    df = df.rename(columns={'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime'})

    logger.info('cleaning datetime ...')
    # df['date'] = df['pickup_datetime'].apply(lambda x: pd.to_datetime(x, unit='us').strftime('%Y-%m-%d'))
    df['date'] = pd.to_datetime(df['pickup_datetime'], unit='us').dt.date
    # df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    # df['date'] =) df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    logger.success('datetime cleaned.')

    meta_data = {
        'PUlocationID': 'value_count',
        'DOlocationID': 'value_count',
        'trip_distance': 'mean'
    }

    if 'trip_distance' not in list(df.columns):
        # calculate distance via locations
        print('')

    df_group = df.groupby('date')
    df_daily = pd.DataFrame()
    logger.info('extract features ...')
    for feature in meta_data:
        if feature in list(df.columns):
            combine_type = meta_data[feature]
            if combine_type == 'value_count':
                df_new = get_value_counts_as_columns(df_group, feature)
            else:
                df_new = df_group[feature]
                df_new = getattr(df_new, combine_type)()
                df_new.columns = [feature]
            df_daily = pd.concat([df_daily, df_new], axis=1)
        else:
            logger.warning(f'{feature} not found.')
    logger.success('features extracted.')

    return df_daily


def get_distance_from_location(loc1: int, loc2: int):
    """
    calculates the estimated distance for two given locations.
    Query: Borough + Zone
    :param loc1:
    :param loc2:
    :return: distance from center of locations
    """

    loc1 = taxi_zone_lookup[loc1]
    loc2 = taxi_zone_lookup[loc2]

    if '/' in list(loc1):
        loc1.replace('/', ' ')

    if '/' in list(loc2):
        loc2.replace('/', ' ')

    logger.info(f"Get distance from {loc1} and {loc2}")
    geolocator = Nominatim(user_agent="DWH_NYC")

    location1 = geolocator.geocode(loc1)
    location2 = geolocator.geocode(loc2)

    coord1 = (location1.latitude, location1.longitude)
    coord2 = (location2.latitude, location2.longitude)

    distance = geopy.distance.geodesic(coord1, coord2).km

    return distance


def clean_covid_data(df: pd.DataFrame):
    """
    clean pd.DataFrame
    :return:
    """
    logger.info('cleaning covid data ...')

    # convert timestamp to yyyy-mm-dd
    df['date_of_interest'] = pd.to_datetime(df['date_of_interest']).dt.date

    return df


def clean_weather_data(df: pd.DataFrame):
    """
    clean pd.DataFrame
    :return:
    """
    logger.info('cleaning weather data ...')
    # convert timestamp to yyyy-mm-dd
    df['DATE'] = pd.to_datetime(df['DATE']).dt.date
    # remove unnecessary columns
    df = df.drop(['NAME', 'STATION'],axis=1)
    # rename columns for better identification
    df = df.rename(columns=weather_column_mapper)

    logger.success('cleaning weather done.')

    return df


def run():
    pass


if __name__ == '__main__':
    run()

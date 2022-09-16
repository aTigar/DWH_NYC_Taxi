import os
import pandas as pd
from loguru import logger
import os.path
from geopy.geocoders import Bing
import geopy.distance
from geopy.extra.rate_limiter import RateLimiter
from dotenv import load_dotenv

load_dotenv('.env')

lat_range = [40.486150, 40.989166]
long_range = [-74.408936, -73.679730]

# geopy setup
api_key = os.environ['BING_API_KEY']
geolocator = Bing(api_key=api_key)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # convenient function to delay between geocoding calls

if not os.path.isfile('data/taxi_zone_lookup_enhanced.csv'):
    logger.warning('No enhanced taxi lookup table found. Fallback to raw table.')
    taxi_lookup_path = 'data/taxi_zone_lookup.csv'
    taxi_zone_lookup = pd.read_csv(taxi_lookup_path)
    # combine full location name
    taxi_zone_lookup['location'] = taxi_zone_lookup['Borough'] + ' ' + taxi_zone_lookup['Zone']

    taxi_zone_lookup = taxi_zone_lookup[['LocationID', 'location']]

    logger.info('Get geocodes from names ...')
    taxi_zone_lookup['geocode'] = taxi_zone_lookup['location'].apply(geocode).copy()
    logger.success('Fetched geocodes.')

    taxi_zone_lookup['lat'] = taxi_zone_lookup['geocode'].apply(lambda x: x.latitude)
    taxi_zone_lookup['long'] = taxi_zone_lookup['geocode'].apply(lambda x: x.longitude)

    # fix outliers
    taxi_zone_lookup = taxi_zone_lookup[taxi_zone_lookup['lat'].between(lat_range[0], lat_range[1])]
    taxi_zone_lookup = taxi_zone_lookup[taxi_zone_lookup['long'].between(long_range[0], long_range[1])]

    taxi_zone_lookup = taxi_zone_lookup.reset_index(drop=True)

    taxi_zone_lookup.to_csv('data/taxi_zone_lookup_enhanced.csv', index=False)
else:
    logger.info('Using enhanced taxi lookup table.')
    taxi_lookup_path = 'data/taxi_zone_lookup_enhanced.csv'
    taxi_zone_lookup = pd.read_csv(taxi_lookup_path)


TAXI_ZONE_LOOKUP = taxi_zone_lookup[['LocationID', 'lat', 'long']].set_index('LocationID').to_dict()



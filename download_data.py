import glob
import os
from time import sleep

from loguru import logger
import pandas as pd
import requests
import wget

# Years of data to download (currently 2018 - present)
years = ['2018', '2019', '2020', '2021', '2022']
# Months of data to download
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

types = ['yellow', 'green', 'fhv', 'fhvhv']
url_add = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

"""
It downloads the parquet file, converts it to csv (with compression: 800MB -> 100MB) and then saves it to data/.
Then it deletes the downloaded parquet file. 
"""

if not os.path.exists('data'):
    os.mkdir('./data')

# Set the download directory
os.chdir('data/')


def requests_data():
    # Get list of already downloaded files
    files = glob.glob('yellow*') + glob.glob('green*') + glob.glob('fhv*')

    for x in types:
        for y in years:
            for z in months:
                url_tmp = url_add + x + '_tripdata_' + y + '-' + z + '.parquet'
                file_tmp = x + '_' + z + '_' + y + '.parquet'

                if file_tmp not in files:
                    downloaded = False
                    while not downloaded:
                        logger.info(f'try and get {file_tmp}')
                        r = requests.get(url_tmp)
                        if r.status_code == 403:
                            logger.warning('Error 403: waiting a bit..')
                            sleep(10)
                            continue
                        if r.status_code == 404:
                            logger.info('Error 404: try next.')
                            break
                        with open(file_tmp, 'wb') as f:
                            f.write(r.content)
                            logger.success(f'Saved {file_tmp}.')
                            downloaded = True


if __name__ == "__main__":
    requests_data()

import glob
import os
from time import sleep

from loguru import logger
import pandas as pd
import requests


def requests_data():
    """function to download specific taxi parquet files
    """
    # Years of data to download (currently 2018 - present)
    years = ['2018', '2019', '2020', '2021', '2022']
    # Months of data to download
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    types = ['yellow', 'green', 'fhv', 'fhvhv']
    url_add = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    if not os.path.exists('data'):
        os.mkdir('./data')

    # Set the download directory
    os.chdir('data/')
    # Get list of already downloaded files
    files = glob.glob('yellow*') + glob.glob('green*') + glob.glob('fhv*')

    try:
        for x in types:
            for y in years:
                for z in months:
                    url_tmp = url_add + x + '_tripdata_' + y + '-' + z + '.parquet'
                    file_tmp = x + '_' + z + '_' + y + '.parquet'

                    if file_tmp not in files:
                        downloaded = False
                        while not downloaded:
                            logger.info(f'Request {file_tmp}...')
                            r = requests.get(url_tmp)
                            if r.status_code == 403:
                                logger.warning('Error 403: To many requests. Waiting a bit..')
                                sleep(300)  # cooldown 5 minutes
                                continue
                            if r.status_code == 404:
                                logger.warning('Error 404: file not available - try next.')
                                break
                            if r.status_code == 200:
                                with open(file_tmp, 'wb') as f:
                                    f.write(r.content)
                                    logger.success(f'Saved {file_tmp}.')
                                    downloaded = True
                            else:
                                logger.error(f'Some unexpected error code occurred: {r.status_code}')
    except KeyboardInterrupt as e:
        logger.info('Keyboard interrupt. Clean up directory..')
        temp_files = glob.glob('*temp')
        for temp_file in temp_files:
            os.remove(temp_file)


if __name__ == "__main__":
    requests_data()

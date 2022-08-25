import glob
import os

import requests
from loguru import logger

workdir = os.getcwd()


def requests_taxi_data():
    """function to download specific taxi parquet files
    """
    logger.info("Downloading taxi-data")
    # Years of data to download (currently 2018 - present)
    years = ['2018', '2019', '2020', '2021', '2022']
    # Months of data to download
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    # vehicle types
    types = ['yellow', 'green', 'fhv']

    url_add = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    if not os.path.exists('data/taxi/'):
        os.mkdir('./data/taxi/')
    # Set the download directory
    os.chdir('data/taxi/')
    # Get list of already downloaded files
    files = glob.glob('yellow*') + glob.glob('green*') + glob.glob('fhv*')

    try:
        for x in types:
            for y in years:
                for z in months:
                    url_tmp = url_add + x + '_tripdata_' + y + '-' + z + '.parquet'
                    file_tmp = x + '_' + z + '_' + y + '.parquet'

                    if file_tmp not in files:
                        logger.info(f'Request {file_tmp}...')
                        r = requests.get(url_tmp)
                        if r.status_code == 403:
                            logger.warning('Error 403: Access denied. - try next.')
                            break
                        if r.status_code == 404:
                            logger.warning('Error 404: file not available - try next.')
                            break
                        if r.status_code == 200:
                            with open(file_tmp, 'wb') as f:
                                f.write(r.content)
                                logger.success(f'Saved {file_tmp}.')
                        else:
                            logger.error(f'Some unexpected error code occurred: {r.status_code}')
    except KeyboardInterrupt as e:
        logger.info('Keyboard interrupt. Clean up directory..')
        temp_files = glob.glob('*temp')
        for temp_file in temp_files:
            os.remove(temp_file)

    os.chdir(workdir)


def requests_covid_data():
    """function to download covid data
    """
    logger.info("Downloading COVID-data")

    urls = [
        'https://data.cityofnewyork.us/resource/rc75-m7u3.csv',
        # https://data.cityofnewyork.us/Health/COVID-19-Daily-Counts-of-Cases-Hospitalizations-an/rc75-m7u3
        'https://health.data.ny.gov/resource/jw46-jpb7.csv',
        # https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Hospitalizations/jw46-jpb7
        'https://health.data.ny.gov/resource/qutr-irdf.csv'
        # https://health.data.ny.gov/Health/New-York-Forward-COVID-19-Daily-Hospitalization-Su/qutr-irdf
    ]

    if not os.path.exists('data/covid/'):
        os.mkdir('./data/covid/')
    # Set the download directory
    os.chdir('data/covid/')
    # Get list of already downloaded files
    files = glob.glob(urls[0].split('/')[-1]) + glob.glob(urls[1].split('/')[-1]) + glob.glob(urls[2].split('/')[-1])

    try:
        for url in urls:
            if url.split('/')[-1] not in files:
                logger.info(f'Request {url}...')
                r = requests.get(url)
                if r.status_code == 200:
                    with open(url.split('/')[-1], 'wb') as f:
                        f.write(r.content)
                else:
                    logger.error(f'Error code: {r.status_code}')
    except KeyboardInterrupt as e:
        logger.info('Keyboard interrupt. Clean up directory..')
        temp_files = glob.glob('*temp')
        for temp_file in temp_files:
            os.remove(temp_file)

    os.chdir(workdir)


if __name__ == "__main__":
    requests_taxi_data()
    requests_covid_data()

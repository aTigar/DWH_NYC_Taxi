import glob
import os
import pandas as pd
import requests

# Years of data to download (currently 2018 - present)
years = ['2018', '2019', '2020', '2021', '2022']
# Months of data to download
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

types = ['yellow'] #, 'green', 'fhv', 'fhvhv'
url_add = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'  # https://s3.amazonaws.com/nyc-tlc/trip+data/
# Set the download directory
os.chdir('data/')
# Get list of already downloaded files
files = glob.glob('yellow*') + glob.glob('green*') + glob.glob('fhv*')

"""
It downloads the parquet file, converts it to csv (with compression: 800MB -> 100MB) and then saves it to data/.
Then it deletes the downloaded parquet file. 
"""
for x in types:
    for y in years:
        for z in months:
            url_tmp = url_add + x + '_tripdata_' + y + '-' + z + '.parquet'
            file_tmp = x + '_' + z + '_' + y + '.gz'
            if (file_tmp not in files) and (requests.head(url_tmp).status_code != 404):
                # Determine number of columns (from data dictionaries)
                # col_list = list(range(0, 20)) if 'green' in file_tmp else list(range(0, 19))
                tmp_data = pd.read_parquet(url_tmp)
                tmp_data.to_csv(file_tmp, compression='gzip')
                del tmp_data
            print(file_tmp)

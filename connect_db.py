import os
from typing import Optional

from dotenv import load_dotenv
import pyodbc

load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USER')
password = os.getenv('PASSWORD')

cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password + ';Trusted_Connection=yes;')
cursor = cnxn.cursor()


def update_table(df: pd.Dataframe, db_table: str, force: Optional[bool] = False):
    """ TODO implement
    Sends cleaned pd.Dataframe to SQL DB.
    :param df:
    :param db_table:
    :param force: set DB table to new df. If false, DB updated from df
    :return: success or not
    """
    pass


if __name__ == '__main__':
    pass

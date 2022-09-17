import os
from typing import Optional
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
from dotenv import load_dotenv
from loguru import logger


def load_environment():
    logger.info('Load environment variables...')

    load_dotenv()
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')

    logger.success('Environment variables loaded.')

    return username, password, server, database


def connect_pyodbc(db_env: tuple):
    # this alternative is only if sqlalchemy does not work
    logger.info('Connect to DB with pyodbc...')
    try:
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + db_env[2] + ';DATABASE=' + db_env[3] + ';UID=' + db_env[
                0] + ';PWD=' + db_env[1] + ';Trusted_Connection=yes;' + ';TrustServerCertificate=yes;')
        cursor = cnxn.cursor()
        # test connection
        for row in cursor.tables():
            print(row.table_name)
        logger.success('Connection to DB was successful')
    except Exception as e:
        logger.error('Connection to DB with pyodbc failed')


def connect_sqlalchemy(username: str, password: str, host: str, database: str):
    logger.info('Connect to DB with SQLAlchemy...')
    try:
        connection_url = URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=host,
            port=1433,
            database=database,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "Trusted_Connection": "yes",
                "TrustServerCertificate": "yes",
                # "authentication": "ActiveDirectoryIntegrated",
            },
        )
        engine = create_engine(connection_url)
        # test connection
        print(engine.table_names())
        logger.success('Connection to DB was successful')
    except Exception as e:
        logger.error('Connection to DB with SQLAlchemy failed')


# def update_table(df: pd.Dataframe, db_table: str, force: Optional[bool] = False):
#     """ TODO implement
#     Sends cleaned pd.Dataframe to SQL DB.
#     :param df:
#     :param db_table:
#     :param force: set DB table to new df. If false, DB updated from df
#     :return: success or not
#     """
#     pass


if __name__ == '__main__':
    # connect_pyodbc(load_environment())
    connect_sqlalchemy(load_environment())

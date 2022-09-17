from typing import Optional

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from config import *


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
        ins = sqlalchemy.inspect(engine)
        ins.get_table_names()
        logger.success('Connection to DB was successful')
    except Exception as e:
        logger.error('Connection to DB with SQLAlchemy failed')
        raise

    return engine, ins


def load_dataframe_to_database(df_job: pd.DataFrame, table_name: str, engine: sqlalchemy.engine,
                               dtypes: Optional[dict] = None):
    logger.info(f'Sending data to database table {table_name}...')

    sql_kwargs = {'name': table_name,
                  'con': engine,
                  'if_exists': 'replace',
                  'index': False,
                  'chunksize': 500
                  }
    if dtypes:
        sql_kwargs.update({'dtype': dtypes})

    try:
        df_job.to_sql(**sql_kwargs)
        logger.success(f'{table_name} send')
    except Exception as e:
        logger.error('Sending failed')
        raise


if __name__ == '__main__':
    # connect_pyodbc(load_environment())
    pass

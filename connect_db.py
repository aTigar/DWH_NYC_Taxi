


# def connect_pyodbc(db_env: tuple):
#     # this alternative is only if sqlalchemy does not work
#     logger.info('Connect to DB with pyodbc...')
#     try:
#         cnxn = pyodbc.connect(
#             'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + db_env[2] + ';DATABASE=' + db_env[3] + ';UID=' + db_env[
#                 0] + ';PWD=' + db_env[1] + ';Trusted_Connection=yes;' + ';TrustServerCertificate=yes;')
#         cursor = cnxn.cursor()
#         # test connection
#         for row in cursor.tables():
#             print(row.table_name)
#         logger.success('Connection to DB was successful')
#     except Exception as e:
#         logger.error('Connection to DB with pyodbc failed')




# def update_table(df: pd.Dataframe, db_table: str, force: Optional[bool] = False):
#     """ TODO implement
#     Sends cleaned pd.Dataframe to SQL DB.
#     :param df:
#     :param db_table:
#     :param force: set DB table to new df. If false, DB updated from df
#     :return: success or not
#     """
#     pass





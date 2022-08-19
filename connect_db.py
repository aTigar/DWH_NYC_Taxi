import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USER')
password = os.getenv('PASSWORD')

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';Trusted_Connection=yes;')
cursor = cnxn.cursor()

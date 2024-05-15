import os
import pymysql
import pymysql.cursors
from pymysqlpool.pool import Pool
from fastapi import Depends
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration from environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT")),
    "cursorclass": pymysql.cursors.DictCursor
}

# Initialize and configure the connection pool
pool = Pool(**db_config)
pool.init()

#Dependency to get a database connection from the pool
def get_db_connection():
    connection = pool.get_conn()
    return connection

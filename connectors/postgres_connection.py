import os
import psycopg2
from pandas import DataFrame
from connectors.postgres_db_config import HR_DATABASE_CONFIG
from sqlalchemy import create_engine

USER = os.environ["USER"]
PASSWORD = os.environ["PASSWORD"]
HOST = os.environ["HOST"]

class PostgresHRConnector:
    def __init__(self):
        self.user = USER
        self.password = PASSWORD
        self.host = HOST
    
    def connect(self):
        self.conn = psycopg2.connect(
            database=HR_DATABASE_CONFIG['database_name'],
            user=self.user,
            password=self.password,
            host=self.host,
            port=HR_DATABASE_CONFIG['port']
        )

    def disconnect(self):
        try:
            self.conn.close()
        finally:
            print("Connection closed")
    
    def fetch_all(self, query: str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    
    def run(self, query: str):
        cursor = self.conn.cursor()
        cursor.execute(query)

    def insert_df_in_postgres(self, table: str, df: DataFrame):
        db_name = HR_DATABASE_CONFIG['database_name']
        port = HR_DATABASE_CONFIG['port']
        conn_string = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{port}/{db_name}'
        engine = create_engine(conn_string)
        conn_pg = engine.connect()
        df.to_sql(table, conn_pg, if_exists='append', index=False)
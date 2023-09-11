import boto3
import math
import os
import pandas as pd
from pandas import DataFrame
from io import StringIO
from connectors.postgres_connection import PostgresHRConnector
from connectors.postgres_db_config import HR_DATABASE_CONFIG

AWS_ACCESS_KEY=os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY=os.environ["AWS_SECRET_KEY"]

MIGRATION_BUCKET = os.environ["MIGRATION_BUCKET"]


def get_s3_to_df(
    bucket: str,
    file_name: str,
    column_names: list
):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="us-east-1"
    )

    s3 = session.client('s3')
    s3_object = s3.get_object(Bucket=bucket, Key=file_name)
    s3_data = s3_object['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(s3_data), header=None,)
    df.columns = column_names
    return df

def split_and_insert_df(
    table_name: str,
    df: DataFrame
):
    try:
        postgres_conn = PostgresHRConnector()
        num_of_partitions = math.ceil(len(df)/1000)
        postgres_conn.connect()
        for partition in range(1, num_of_partitions+1):
            start = (partition-1)*1000
            end = (partition*1000)-1
            if partition == num_of_partitions:
                end = len(df)
            print(f"Inserting batch {start}:{end}")
            if partition > 1:
                start=start-1
            df_partition = df.iloc[start:end]
            postgres_conn.insert_df_in_postgres(table_name, df_partition)
    except Exception as e:
        raise e
    finally:
        postgres_conn.disconnect()

def insert_departments_in_postgres(file: str):
    file_url = HR_DATABASE_CONFIG["folder_migration"]+file
    try:
        columns = ["id", "department"]
        df = get_s3_to_df(MIGRATION_BUCKET, file_url, columns)
        print(df)
        split_and_insert_df(HR_DATABASE_CONFIG["departments_table"], df)
    except Exception as e:
        raise e

def insert_jobs_in_postgres(file: str):
    file_url = HR_DATABASE_CONFIG["folder_migration"]+file
    try:
        columns = ["id", "job"]
        df = get_s3_to_df(MIGRATION_BUCKET, file_url, columns)
        split_and_insert_df(HR_DATABASE_CONFIG["jobs_table"], df)
    except Exception as e:
        raise e

def insert_hired_employees_in_postgres(file: str):
    file_url = HR_DATABASE_CONFIG["folder_migration"]+file
    try:
        columns = ["id", "name", "datetime", "department_id", "job_id"]
        df = get_s3_to_df(MIGRATION_BUCKET, file_url, columns)
        split_and_insert_df(HR_DATABASE_CONFIG["hired_employees_tables"], df)
    except Exception as e:
        raise e

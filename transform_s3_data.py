import pandas as pd
import boto3, dotenv, os
from datetime import datetime
from io import StringIO
import io
from pathlib import Path


# Instantiate an s3 client object
dotenv.load_dotenv('./.env')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


def read_csv_from_s3() -> pd.DataFrame:
    """
    The function reads a CSV file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key_name (str): The key name of the S3 object.

    Returns:
        pd.DataFrame: The DataFrame containing the CSV file data.
    """
    bucket_name = 'jobsearch-data'
    raw_folder = 'raw_job_data'
    current_date = f"{datetime.now().strftime('%Y-%m-%d')}.csv"
    raw_key_name = f'{raw_folder}/{current_date}'
    object_list = s3_client.list_objects(Bucket=bucket_name, Prefix=raw_key_name)
    file = object_list.get('Contents')[0]
    key = file.get('Key')
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:   

    df['job_posted_at_timestamp'] = pd.to_datetime(df['job_posted_at_timestamp'], unit='s')
    df['job_posted_at_timestamp'] = df.job_posted_at_timestamp.sort_values(ascending=True)
    return df


def write_transformed_to_s3(trans_df: pd.DataFrame) -> Path:
    bucket_name = 'jobsearch-data'
    current_date = f"{datetime.now().strftime('%Y-%m-%d')}.csv"
    trans_folder = 'transformed_job_data'
    trans_key_name = f'{trans_folder}/{current_date}'
    trans_df.to_csv(trans_key_name, index=False)
    s3_client.upload_file(trans_key_name, bucket_name, trans_key_name)
    print('successful')
    return trans_key_name



def main():
    df = read_csv_from_s3()
    trans_df = transform(df)
    write_transformed_to_s3(trans_df)
    

if __name__ == '__main__':
    main()
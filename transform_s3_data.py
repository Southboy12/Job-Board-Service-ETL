import pandas as pd
import boto3, dotenv, os
from datetime import datetime
from io import StringIO
import io


# Instantiate an s3 client object
dotenv.load_dotenv('./.env')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


def read_csv_from_s3(bucket_name: str, key_name: str) -> pd.DataFrame:
    """
    The function reads a CSV file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key_name (str): The key name of the S3 object.

    Returns:
        pd.DataFrame: The DataFrame containing the CSV file data.
    """
    # response = s3_client.get_object(
    #     Bucket=bucket_name,
    #     Key=key_name
    # )

    # data = response['Body'].read().decode('utf-8')
    # df = pd.read_csv(data)

    object_list = s3_client.list_objects(Bucket=bucket_name, Prefix=key_name)
    file = object_list.get('Contents')[0]
    key = file.get('Key')
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df


def transform(df):   

    df['job_posted_at_timestamp'] = pd.to_datetime(df.job_posted_at_timestamp)
    print(df.info())



def main():
    bucket_name = 'jobsearch-data'
    current_date = f"{datetime.now().strftime('%Y-%m-%d')}.csv"
    folder = 'job_data'
    key_name = f'{folder}/{current_date}'
    df = read_csv_from_s3(bucket_name, key_name)
    transform(df)


if __name__ == '__main__':
    main()
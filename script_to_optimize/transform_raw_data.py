import pandas as pd
import boto3
from script_to_optimize.extract_raw_to_s3 import extract_data
from datetime import datetime
from io import StringIO
import io
import json



def read_raw_data_from_s3():
    
    bucket_name = 'jobsearch-data'

    # Get the current date and time
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Define the folder where the data will be saved
    folder = 'raw_data_test'

    # Create the path to the file in S3
    aws_path = f'{folder}/{current_date}'

    s3_client = boto3.client('s3')

    object_list = s3_client.list_objects(Bucket=bucket_name, Prefix=aws_path) 

    key = object_list.get('Contents')[0].get('Key')

    obj = s3_client.get_object(Bucket=bucket_name, Key=key)

    data = json.loads(obj['Body'].read())
    print(type(data))
    return data

def extract_records(data):
    print(type(data))
    

        

    # values = []
    # for key, value in dictionary.items():
    #     values.append(value)
    # print(list(values))


    #all_jobs = []
    # Iterate over the job results
    #for item in list(data())[3]:

    #     # Get the job id
    #     job_id = item['job_id']
    #     # Get the employer name
    #     employer_names = item['employer_name']
    #     # Get the employer website
    #     employer_website = item['employer_website'] 
    #     # Get the job employment type
    #     job_employment_type = item['job_employment_type']
    #     # Get the job title
    #     job_title = item['job_title'] 
    #     # Get the job country
    #     job_country = item['job_country'] 
    #     # Get the job apply link
    #     job_apply_link = item['job_apply_link']
    #     # Get the job city
    #     job_city = item['job_city'] 
    #     # Get the job posted at timestamp
    #     job_posted_at_timestamp = item['job_posted_at_timestamp']
    #     # Get the employer company type
    #     employer_company_type = item['employer_company_type']
    #     # Get the job description
    #     job_description = item['job_description'] 
    #     # Append the job data to the list
    #     all_jobs.append([job_id, employer_names, employer_website, job_employment_type, job_title, job_city, job_country, job_apply_link, employer_company_type, job_description, job_posted_at_timestamp])

    # print(all_jobs)


def main():
    data = read_raw_data_from_s3()
    extract_records(data)


if __name__ == '__main__':
    main()
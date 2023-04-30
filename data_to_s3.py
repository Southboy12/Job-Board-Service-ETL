import pandas as pd
import requests
import boto3, os, dotenv
from datetime import datetime
from io import StringIO
from pathlib import Path
from prefect import flow, task




# Instantiate an s3 client object
dotenv.load_dotenv('./.env')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

@task(log_prints=True, retries=3)
def get_job_data() -> pd.DataFrame:
    """
    The function sends request, checks if the response is ok, 
    and rectrieves information about jobs queried

    Args:
        None

    Returns:
        pd.DataFrame: A DataFrrame containing the job data.
    """

    # Define the URL for the job search API
    url = "https://jsearch.p.rapidapi.com/search"

    # Define the query parameters for the job search API
    query_list = [
            {"query":"Data Engineer in USA ", "page":"1", "num_pages":"1", "date_posted":"today"},
            {"query":"Data Engineer in UK ", "page":"1", "num_pages":"1", "date_posted":"today"}]
            # {"query":"Data Engineer in Canada ", "page":"1", "num_pages":"1", "date_posted":"today"},
            # {"query":"Data Analyst in USA ", "page":"1", "num_pages":"1", "date_posted":"today"},
            # {"query":"Data Analyst in UK ", "page":"1", "num_pages":"1", "date_posted":"today"},
            # {"query":"Data Analyst in Canada ", "page":"1", "num_pages":"1", "date_posted":"today"}]

    # Define the headers for the job search API
    headers = {
	"content-type": "application/octet-stream",
	"X-RapidAPI-Key": os.getenv('X-RAPIDAPI-KEY'),
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    all_jobs = []
    for querystring in query_list:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code != 200:
            print(f'Can not retrieve data at the moment. Error: {response.status_code}')
        data = response.json()

        # Iterate over the job results
        for item in list(data.values())[3]:
            # Get the job id
            job_id = item['job_id']
            # Get the employer name
            employer_names = item['employer_name']
            # Get the employer website
            employer_website = item['employer_website'] 
            # Get the job employment type
            job_employment_type = item['job_employment_type']
            # Get the job title
            job_title = item['job_title'] 
            # Get the job country
            job_country = item['job_country'] 
            # Get the job apply link
            job_apply_link = item['job_apply_link']
            # Get the job city
            job_city = item['job_city'] 
            # Get the job posted at timestamp
            job_posted_at_timestamp = item['job_posted_at_timestamp']
            # Get the employer company type
            employer_company_type = item['employer_company_type']
            # Get the job description
            job_description = item['job_description'] 
            
            # Append the job data to the list
            all_jobs.append([job_id, employer_names, employer_website, job_employment_type, job_title, job_city, job_country, job_apply_link, employer_company_type, job_description, job_posted_at_timestamp])
           
    columns = ['job_id', 'employer_names', 'employer_website', 'job_employment_type', 'job_title', 'job_city', 'job_country', 'job_apply_link', 'employer_company_type', 'job_description', 'job_posted_at_timestamp']
    # Create a DataFrame from the job data
    df = pd.DataFrame(all_jobs, columns=columns)
    print(df.job_posted_at_timestamp[2])
    # Print a success message
    print('Successfully written to a DataFrame')
    return df


@task(log_prints=True)
def write_to_s3(df: pd.DataFrame) -> None:
    bucket_name = 'jobsearch-data'
    path = 'raw_job_data'
    file_name = f"{datetime.now().strftime('%Y-%m-%d')}.csv"
    raw_file_name = f'{path}/{file_name}'
    df.to_csv(raw_file_name, index=False)
    s3_client.upload_file(raw_file_name, bucket_name, raw_file_name)
    print('successful')

@flow(log_prints=True)
def main():
    df = get_job_data()
    write_to_s3(df)

if __name__ == '__main__':
    main()
import pandas as pd
import requests
import boto3
from datetime import datetime
from io import StringIO
from pathlib import Path



# Instantiate an s3 client object
s3_client = boto3.client('s3',
                         aws_access_key_id='AKIAZOGVM4TFYEYR2M4I',
                         aws_secret_access_key='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')


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
            {"query":"Data Engineer in UK ", "page":"1", "num_pages":"1", "date_posted":"today"},
            {"query":"Data Engineer in Canada ", "page":"1", "num_pages":"1", "date_posted":"today"},
            {"query":"Data Analyst in USA ", "page":"1", "num_pages":"1", "date_posted":"today"},
            {"query":"Data Analyst in UK ", "page":"1", "num_pages":"1", "date_posted":"today"},
            {"query":"Data Analyst in Canada ", "page":"1", "num_pages":"1", "date_posted":"today"}]

    # Define the headers for the job search API
    headers = {
	"content-type": "application/octet-stream",
	"X-RapidAPI-Key": "4fce829339msh43b77f9802b5118p1e6aa8jsnbe60e04ba4e0",
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

    # Create an empty list to store the job data
    all_jobs = []

    # Iterate over the query list
    for querystring in query_list:
        # Make a request to the job search API
        
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code !=200:
            print('Could not retrieve data from the endpoint')
        else:
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

        # Print a success message
        print('Successfully written to a DataFrame')
        return df


def write_to_s3(df: pd.DataFrame, bucket_name, path: Path) -> None:
    file_name = f"{datetime.now().strftime('%Y-%m-%d')}"
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_str = csv_buffer.getvalue()
    print(csv_str)
    s3_client.put_object(Bucket=bucket_name, Key=f'{path}/{file_name}', Body=csv_str)
    print('Successfully written file to s3 bucket')


def main():
    bucket_name = 'jobsearch-data'
    path = 'job_data'
    df = get_job_data()
    write_to_s3(df, bucket_name, path)



if __name__ == '__main__':
    main()

import pandas as pd
import requests
import boto3



# Instantiate an s3 client object
s3_client = boto3.client('s3')


def get_job_data():
    """
    The function sends request, checks if the response is ok, 
    and rectrieves information about jobs queried
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
	"X-RapidAPI-Key": "5aef64f41amshd5ab87b91e8267ep12adcbjsnb9e7bab34d37",
	"X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    # Create an empty list to store the job data
    all_jobs = []

    # Iterate over the query list
    for querystring in query_list:
        # Make a request to the job search API
        data = requests.get(url, headers=headers, params=querystring).json()

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

    # Print the first few rows of the DataFrame
    print(df.head())

    # Print the shape of the DataFrame
    print(df.shape)

def main():
    get_job_data()



if __name__ == '__main__':
    main()

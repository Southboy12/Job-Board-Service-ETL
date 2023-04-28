# Import the necessary libraries
import requests
import os
import boto3, dotenv, json
from datetime import datetime
from pathlib import Path


# Load the environment variables
dotenv.load_dotenv('./.env')

# Create an S3 client object
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


# Define a function to extract data from the JSearch API
def extract_data(folder: str) -> Path:
    """
     Extracts job search data from an external API and saves it to a local and S3 bucket file.

    Args:
        folder (str): The name of the folder in which to store the local file.

    Returns:
        Path.

    Raises:
        None.

    This function queries an external API for job search data, with different parameters for job roles and locations, and saves the responses to temporary files on disk. It then merges the temporary files into a single JSON-formatted string and writes it to a local file named "merged-data.json" in the specified folder. Finally, the merged data is uploaded to an S3 bucket named "jobsearch-data", with a key composed of the specified folder name and the current date in the format "folder/YYYY-MM-DD.json".

    The function requires the following environment variables to be set in a local .env file:
    - AWS_ACCESS_KEY_ID: The AWS access key ID for the user with S3 write permissions.
    - AWS_SECRET_ACCESS_KEY: The AWS secret access key for the user with S3 write permissions.
    """



    # Define the API endpoint and parameters
    url = "https://jsearch.p.rapidapi.com/search"

    # Create a list of queries
    query_list = [
        {"query": "Data Engineer in USA", "page": "1", "num_pages": "1", "date_posted": "today"},
        {"query": "Data Engineer in UK", "page": "1", "num_pages": "1", "date_posted": "today"},
        {"query": "Data Engineer in Canada", "page": "1", "num_pages": "1", "date_posted": "today"},
        {"query": "Data Analyst in USA", "page": "1", "num_pages": "1", "date_posted": "today"},
        {"query": "Data Analyst in UK", "page": "1", "num_pages": "1", "date_posted": "today"},
        {"query": "Data Analyst in Canada", "page": "1", "num_pages": "1", "date_posted": "today"}
    ]

    # Define the headers for the requests
    headers = {
        "content-type": "application/octet-stream",
        "X-RapidAPI-Key": "4fce829339msh43b77f9802b5118p1e6aa8jsnbe60e04ba4e0",
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    # Make requests to the API and save responses to temporary files
    temp_files = []
    i = 1
    for querystring in query_list:
        # Make a request to the API
        response = requests.get(url, headers=headers, params=querystring)

        # Save the response to a temporary file
        temp_file = f'test{i}.json'
        with open(temp_file, 'w') as f:
            f.write(response.text)

        # Add the temporary file to the list of temporary files
        temp_files.append(temp_file)

        i += 1

    # Merge the temporary files into a single file
    merged_data = ''
    for file in temp_files:
        # Read the contents of the file
        with open(file, 'r') as f:
            merged_data += f.read()

        # Remove the file
        os.remove(file)

    # Write the merged data to a local file
    with open('merged-data.json', 'w') as f:
        f.write(merged_data)

    # Get the current date and time
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Create the path to the file in S3
    aws_path = f'{folder}/{current_date}'

    # Upload the file to S3
    s3_client.put_object(Bucket='jobsearch-data',
                            Key=aws_path,
                            Body=json.dumps(merged_data))

    print('Success')
    return aws_path


# Define the main function
def main():
    # Define the folder where the data will be saved
    folder = 'raw_data'

    # Extract the data from the JSearch API
    path = extract_data(folder)


# If this is the main script, call the main function
if __name__ == '__main__':
    main()

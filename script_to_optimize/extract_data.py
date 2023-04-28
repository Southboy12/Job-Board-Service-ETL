import requests, boto3
import json, dotenv, os
from datetime import datetime
from pathlib import Path
from dotenv import dotenv_values

# Instantiate an s3 client object
dotenv.load_dotenv('./.env')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


def extract_data(url, folder: Path) -> Path:
    # Define the URL for the job search API

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

    # Iterate over the query list

    all_data_dict = []
    for querystring in query_list:
        # Make a request to the job search API
        response = requests.get(url, headers=headers, params=querystring)
        data = json.loads(response.text)
        all_data_dict.append(data)


    # all_data_dict = {}
    # for dict in all_data:
    #     all_data_dict.update(dict)

    file_name = f"{datetime.now().strftime('%Y-%m-%d')}"

    with open(f'{folder}/{file_name}', 'w') as f:
        json.dump(all_data_dict, f, indent=4)

    aws_path =f'{folder}/{file_name}'
    
    s3_client.put_object(Bucket='jobsearch-data',
                         Key=aws_path,
                         Body=json.dumps(all_data_dict))
    print('Success')
    return  aws_path
    
    
#def transform():


def main():
    url = "https://jsearch.p.rapidapi.com/search"
    folder = 'raw_data'
    extract_data(url, folder)


if __name__ == '__main__':
    main()
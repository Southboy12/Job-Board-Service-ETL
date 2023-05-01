# **Job Search Scraper to Amazon Redshift Pipeline**

## **Introduction**

<p>This project scrapes job search results from RapidAPI and stores the data in an S3 bucket. It consists of two main parts: the scraper that searches for job listings, and the S3 pipeline that stores the scraped data.</p>

## **Technologies**

<p>This project uses the following technologies:</p>

* Python (version 3.7+)
* Pandas
* Requests
* Boto3
* Dotenv
* Prefect


## **Pipeline Architecture**


                      +---------------+		|
                      |               |		|
                      |    Source     |		|
                      |  (RapidAPI)   |        	|
                      |               |		|
                      +-------+-------+		|
                              |			|
                     +--------v--------+	|		
                     |                 |	|
                     |      Local      |	|
                     |     Storage     |	|
                     +--------+--------+	|
                              |			|
                     +--------v--------+	|
                     |                 |	|Prefect Orchestraction Workflow
                     |    S3 Bucket    |	|
                     |   (Raw Data)    |	|
                     +--------+--------+	|
                              |			|
                     +--------v--------+	|
                     |     S3 Bucket   |	|
                     |   Transformed   |    	|
                     |      Data       |	|
                     +--------+--------+	|
                              |			|
                     +--------v--------+	|
                     |                 |	|
                     |       AWS       |	|
                     |     Redshift    |	|
                     |                 |	|
                     +--------+--------+	V

## **Setup**
To run this project, you need to have Python installed on your machine. You can download the latest version of Python from the official Python website. You also need to have an AWS account and access to an S3 bucket abd Redshift.

1. Clone this repository to your local machine:

```python copyable
$ https://github.com/Southboy12/Job-Board-Service-ETL.git
```

2. Navigate to the project directory:

```python copyable
$ cd Job-Board-Service-ETL
```

3. Create the project dependencies:

```python copyable
$ pip install -r requirements.txt
```

4. Create a .env file in the root directory of the project and add your AWS access key ID, AWS secret access key, and RapidAPI key:

```python copyable
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
X-RAPIDAPI-KEY=your_rapidapi_key
```
5. Start the prefect server

```python copyable
prefect orion start
```

6. Run the python script

7. Deploy the workflow
```python copyable
prefect deployment build <script_name>:<main_flow_name> -n "<name_the_deployment>"
```

8. The above will create a YAML file. Apply the YAML file using the following code:
```python copyable
prefect deployment apply <yaml_file_name>
```

8. Create a work queue if it does not exist in the GUI.
```python copyable
localhost:4200
```
9. Create an agent using the following code:
```python copyable
prefect agent start --work-queue "<work-queue_name>"
```

## **Using Airflow**

To test each dag task
1. Run astro dev start
2. Get <container id> for the websever by running <docker ps>
2. Run docker exec -it <container id> bash inside a terminal
3. In the bash prompt, type 
```python copyable
airflow tasks test <dag name> <task id> <backward date:2023-03-01>
```


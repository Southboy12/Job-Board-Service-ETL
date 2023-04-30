from data_to_s3 import get_job_data, write_to_s3
from transform_s3_data import write_transformed_to_s3, transform, read_csv_from_s3
import dotenv, os
from dotenv import dotenv_values
import psycopg2
import pandas as pd
from datetime import datetime
from prefect import flow, task



config = dotenv_values('.env')
iam_role = config.get('IAM_ROLE')

#@task(log_prints=True)
def get_redshift_connection():
    #iam_role = config.get('IAM_ROLE')
    user=config.get('USER')
    password= config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn

#@task(log_prints=True)
def execute_sql(sql_query, conn):
    conn = get_redshift_connection()
    cur = conn.cursor() # Creating a cursor object for executing SQL query
    cur.execute(sql_query)
    conn.commit()
    print('Table created successfully')
    cur.close() # Close cursor
    conn.close() # Close connection
    print('Connection to database closed')

@task(log_prints=True)
def generate_schema(df: pd.DataFrame) -> str:
    table_name = 'job_data'
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''
    
    types_checker = {
        'INT':pd.api.types.is_integer_dtype,
        'VARCHAR':pd.api.types.is_string_dtype,
        'FLOAT':pd.api.types.is_float_dtype,
        'TIMESTAMP':pd.api.types.is_datetime64_any_dtype,
        'OBJECT':pd.api.types.is_dict_like,
        'ARRAY':pd.api.types.is_list_like,
    }
    for column in df: # Iterate through all the columns in the dataframe
        last_column = list(df.columns)[-1] # Get the name of the last column
        for type_ in types_checker: 
            mapped = False
            if types_checker[type_](df[column]): # Check each column against data types in the type_checker dictionary
                mapped = True # A variable to store True of False if there's type is found. Will be used to raise an exception if type not found
                if column != last_column: # Check if the column we're checking its type is the last comlumn
                    column_type_query += f'{column} {type_},\n' # 
                else:
                    column_type_query += f'{column} {type_}\n'
                break
        if not mapped:
            raise ('Type not found')
    column_type_query += ');'
    output_query = create_table_statement + column_type_query
    return output_query

@task(log_prints=True)
def load_to_redshift():
    table_name = 'job_data'
    trans_folder = 'transformed_job_data'
    current_date = f"{datetime.now().strftime('%Y-%m-%d')}.csv"
    trans_key_name = f'{trans_folder}/{current_date}'
    s3_path = f"s3://jobsearch-data/{trans_key_name}" # Replace this with your file path (bucket name, folder & file name)
    conn = get_redshift_connection()
    # A copy query to copy csv files from S3 bucket to Redshift.
    copy_query = f"""
    copy {table_name}
    from '{s3_path}'
    IAM_ROLE '{iam_role}'
    csv
    IGNOREHEADER 1;
    """
    execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')

@flow(log_prints=True)
def etl_parent_flow():
    df = get_job_data()
    write_to_s3(df)
    s3_df = read_csv_from_s3()
    df_transformed = transform(s3_df)
    write_transformed_to_s3(df_transformed)
    conn = get_redshift_connection()
    sql_query = generate_schema(df_transformed)
    execute_sql(sql_query, conn)
    load_to_redshift()


if __name__ == '__main__':
    etl_parent_flow()
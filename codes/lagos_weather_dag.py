# Import necessary packages
from airflow import DAG		# for creating and authoring DAGs
import requests
import json		# for handling JSON response from REST API endpoint
import s3fs
import snowflake.connector   # instead of SnowflakeOperator
import pandas as pd	# for handling dataframes and creating CSV files
from datetime import timedelta, datetime	# for handling date/time data

from airflow.providers.http.sensors.http import HttpSensor	# for creating the DAG task that probes the REST API endpoint for data
from airflow.operators.python import PythonOperator	# for creating any Custom Python Function (CPF), in this case, the DAG that task would call the Python-based data transform/load function logic

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor		# For creating the DAG task that would allow Airflow probe a specified S3 bucket for data arrival
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator	# For creating the DAG task that would let Airflow move data into Snowflake DW

from airflow.operators.email import EmailOperator	# For authoring the DAG task that would send out email notification


### DEFINE CUSTOM PYTHON FUCNTIONS (CPFs), IMPORTANT VARIABLES, AND OTHERS

# Define important AWS credentials for inter-services integration
aws_credentials = {"key": "AJLIKSGCYUNMNSGTCXRP", 
        "secret": "i9TREV7LKYzaun5nvdOcQXxTw8iPrFjk+1/30BLg", 
        }	# The AWS account credential goes here


# The temperature conversion Custom Python Function
def kelvin_to_celsius(temp):
        return round(temp - 273.15, 2)


# The data transform/load Custom Python Function to read API JSON file and transform it
def transform_load_data(task_instance):
        data = task_instance.xcom_pull(task_ids="extract_weather_data")
        weather_list = []
        # The transformation block 1 - transform each of the 17 fields from JSON into variables
        for entry in data["list"]:
                date_of_record = entry["dt_txt"].split(" ")[0]  # Extract only the date
                time_of_record = entry["dt_txt"].split(" ")[1]  # Extract only the time
                temp_celsius = kelvin_to_celsius(entry["main"]["temp"])
                feels_like_celsius = kelvin_to_celsius(entry["main"]["feels_like"])
                min_temp_celsius = kelvin_to_celsius(entry["main"]["temp_min"])
                max_temp_celsius = kelvin_to_celsius(entry["main"]["temp_max"])
                pressure = entry["main"]["pressure"]
                sea_level_pressure = entry["main"].get("sea_level", "N/A")
                ground_level_pressure = entry["main"].get("grnd_level", "N/A")
                humidity = entry["main"]["humidity"]
                weather_description = entry["weather"][0]["description"]
                wind_speed = entry["wind"]["speed"]
                cloud_cover = entry["clouds"]["all"]
                visibility = entry.get("visibility", "N/A")
                precipitation_probability = entry.get("pop", "N/A")
                wind_gust = entry["wind"].get("gust", "N/A")
                part_of_day = "Daytime" if entry["sys"]["pod"] == "d" else "Nighttime"       
                
                # The transformation block 2 - providing each of the 17 fields of data with a proper field name
                weather_list.append({
                        "Date Of Record": date_of_record,
                        "Time Of Record": time_of_record,
                        "Temperature (C)": temp_celsius,
                        "Feels Like (C)": feels_like_celsius,
                        "Minimum Temperature (C)": min_temp_celsius,
                        "Maximum Temperature (C)": max_temp_celsius,
                        "Pressure (hPa)": pressure,
                        "Sea Level Pressure (hPa)": sea_level_pressure,
                        "Ground Level Pressure (hPa)": ground_level_pressure,
                        "Humidity (%)": humidity,
                        "Weather Description": weather_description,
                        "Wind Speed (m/s)": wind_speed,
                        "Cloud Cover (%)": cloud_cover,
                        "Visibility (m)": visibility,
                        "Precipitation Probability (%)": precipitation_probability,
                        "Wind Gust (m/s)": wind_gust,
                        "Part of Day": part_of_day
                        })
                
                # The data load phase i.e. load data into S3 bucket data lake
        df_data = pd.DataFrame(weather_list)
        dt_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Leverage s3fs to interact with the S3 bucket
        df_data.to_csv(f"s3://lagos-weather-airflow-s3-snow-email-notif-bucket/lagos_weather/{dt_string}.csv", 
        index=False, storage_options=aws_credentials,
        )	# The S3 bucket goes here


# The function to pull data from the API endpoint
def fetch_data():
        url = "https://api.openweathermap.org/data/2.5/forecast?q=lagos&appid=59250d7y8k082p9023ij683t478rnvxt"
        response = requests.get(url)
        response.raise_for_status()  # Handle errors
        return response.json()


# The credentials of the destination Snowflake DW 
SNOWFLAKE_CONN = {
        "user": "vaxdata22",
        "password": "dnhWYOdhp245&&",
        "account": "eijvkgo-qf39648",
        "warehouse": "lagos_weather_warehouse",
        "database": "lagos_weather_database",
        "schema": "lagos_weather_schema",    
        }


# The SQL command to create table in the destination Snowflake DW
sql_create_table = '''CREATE TABLE IF NOT EXISTS lagos_weather (
                        Date_Of_Record DATE NOT NULL,
                        Time_Of_Record TIME NOT NULL,
                        Typical_Temperature_C DECIMAL(4,2) NOT NULL,
                        Temperature_Feel_C DECIMAL(4,2) NOT NULL,
                        Maximum_Temperature_C DECIMAL(4,2) NOT NULL,
                        Minimum_Temperature_C DECIMAL(4,2) NOT NULL,
                        Typical_Pressure_hPa INTEGER NOT NULL,
                        Sea_Level_hPa INTEGER NOT NULL,
                        Ground_Level_hPa INTEGER NOT NULL,
                        Humidity_percent INTEGER NOT NULL,
                        Weather_Description VARCHAR(20) NOT NULL,
                        Wind_Speed_ms DECIMAL(4,2) NOT NULL,
                        Cloud_Cover_percent INTEGER NOT NULL,
                        Visibility_m INTEGER NOT NULL,
                        Precipitation_prob DECIMAL(4,2) NOT NULL,
                        Wind_Gust_ms DECIMAL(4,2) NOT NULL,
                        Part_Of_Day VARCHAR(20) NOT NULL                  
                        );
                        '''


# The function to run the SQL command to create table in the destination Snowflake DW
def create_snowflake_table():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cursor = conn.cursor()
        # Step 1: Drop table (execute separately)
        cursor.execute("DROP TABLE IF EXISTS lagos_weather")
        # Step 2: Create table (execute separately)
        cursor.execute(sql_create_table)  # Example query
        cursor.close()
        conn.close()


# The SQL command to copy data from the Staging Area into the destination table in Snowflake DW
sql_copy_data = '''COPY INTO lagos_weather_database.lagos_weather_schema.lagos_weather FROM @lagos_weather_database.lagos_weather_schema.lagos_weather_stage_area FILE_FORMAT = csv_format
        '''


# The function to run the SQL command to copy data from the Staging Area into the destination table in Snowflake DW
def copy_data_to_snowflake_table():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cursor = conn.cursor()
        cursor.execute(sql_copy_data)  # Example query
        cursor.close()
        conn.close()


# Define the S3 bucket and file details
s3_bucket_name = 'lagos-weather-airflow-s3-snow-email-notif-bucket'
s3_key = 'lagos_weather/*.csv'


### CREATING THE DAG LOGIC AND TASKS FOR THE ETL DATA PIPELINE ORCHESTRATION

# Define the default main arguments for orchestration
default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 3, 4),		# Enter today's date here
        'email': 'donatus.enebuse@gmail.com',		# Put the sender's (mail from) email here
        'email_on_failure': True,		# This would send out email when the entire DAG run encountered an error and ultimately failed
        'email_on_retry': True,		# This would send out email when the entire DAG run encountered an error and is now retrying
        'retries': 2,
        'retry_delay': timedelta(seconds=3),		# can be set to minutes=2
        }


# Define the seven (7) DAG tasks and their dependencies information (Operators/Sensors)
with DAG(dag_id = 'lagos_weather_airflow_s3_snow_email_notif_dag', default_args=default_args, schedule_interval = '@daily', catchup=False,) as dag:
        
        # Task 1 - This DAG task checks if the API is ready to communicate
        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/forecast?q=lagos&appid=59250d7y8k082p9023ij683t478rnvxt',		# Put your API key here as well as the City you want e.g Lagos, etc
        )

	# Task 2 - This DAG task would extract JSON data from the API endpoint
        extract_weather_data = PythonOperator(
        task_id = 'extract_weather_data',
        python_callable=fetch_data,       # call the function (defined earlier on) that would pull data from the API endpoint
        )
	
	# Task 3 - This DAG task would trigger the data transformation/Load process
        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data,	 # this calls the Data Transform/Load Function defined much earlier on
        )

	# Task 4 - This DAG task would probe the S3 bucket to check if a CSV file has arrived
        is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key=s3_key,
        bucket_name=s3_bucket_name,
        aws_conn_id='aws_s3_conn',
        wildcard_match=True,     # Set this to True if you want to use wildcards in the prefix
        # timeout=60,       # Optional: Timeout for the sensor (in seconds)
        poke_interval=5,      # Optional: Time interval between S3 checks (in seconds)
        )

	# Task 5 - This DAG task would create a destination table with 17 columns in the Snowflake DW
        create_table = PythonOperator(
        task_id="create_snowflake_table",
        python_callable=create_snowflake_table,    # this calls the function that would run the SQL command to create table in the destination Snowflake DW
        )

	# Task 6 - This DAG task would copy data from the CSV file in the S3 bucket into the destination table in Snowflake DW via a staging area
        copy_csv_into_snowflake_table = PythonOperator(
        task_id="tsk_copy_csv_into_snowflake_table",
        python_callable=copy_data_to_snowflake_table,    # this calls the function that would run the SQL command to copy data from the Staging Area into the destination table in Snowflake DW
        )

	# Task 7 - This DAG task would send out notification immediately the entire DAG run is completed successfully
        notification_by_email = EmailOperator(
        task_id="tsk_notification_by_email",
        to="donatus.enebuse@gmail.com",
        subject="Successful Orchestration of the lagos_weather_airflow_s3_snow_email_notif ETL Process",
        html_content="This is to notify you of the SUCCESS of the lagos_weather_airflow_s3_snow_email_notif ETL DAG orchestrated by Airflow. Check on the Snowflake DW for your anticipated data. Enjoy!",
        )


# Define the DAG sequence (chronological order of the tasks)
        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> is_file_in_s3_available >> create_table >> copy_csv_into_snowflake_table >> notification_by_email

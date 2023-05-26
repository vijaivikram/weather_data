import datetime
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2
from datetime import date
import time


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['vijaivikramiyyappan@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_details_dag',
    default_args=default_args,
    description='weather_details_dag',
    schedule_interval='0 8 * * *',
)

def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius

def extract():
    base_url = 'https://api.openweathermap.org/data/2.5/weather?'
    api_key = '978a967e766b550f9adb562c0f29750d'
    city = 'mumbai'
    url = base_url + 'appid=' + api_key + '&q=' + city
    response = requests.get(url).json()
    
    return response

def transform(task_instance):

    response = task_instance.xcom_pull(task_ids = ['extraction_task'])

    response = response[0]    
    
    result = []
    
    today_date = date.today()
    result.append(today_date)
    
    city = 'Mumbai'
    result.append(city)
    
    temp_kelvin = response['main']['temp']
    temp_celsius = "{:.2f}".format(kelvin_to_celsius(temp_kelvin))
    result.append(temp_celsius)
    
    feels_like_kelvin = response['main']['feels_like']
    feels_like_celsius = "{:.2f}".format(kelvin_to_celsius(feels_like_kelvin))
    result.append(feels_like_celsius)
    
    min_temp_kelvin = response['main']['temp_min']
    min_temp_celsius = "{:.2f}".format(kelvin_to_celsius(min_temp_kelvin))
    result.append(min_temp_celsius)
    
    max_temp_kelvin = response['main']['temp_max']
    max_temp_celsius = "{:.2f}".format(kelvin_to_celsius(max_temp_kelvin))
    result.append(max_temp_celsius)
    
    windspeed = response['wind']['speed']
    result.append(windspeed)
    
    humidity = response['main']['humidity']
    result.append(humidity)
    
    description = response['weather'][0]['description']
    result.append(description)
    
    return result


def load(task_instance):

    result = task_instance.xcom_pull(task_ids = ['transformation_task'])
    
    result = result[0]
    
    dates = result[0]
    city = result[1]
    temperature = result[2]
    feels_like = result[3]
    minimum_temperature = result[4]
    maximum_temperature = result[5]
    windspeed = result[6]
    humidity = result[7]
    description = result[8]
    
    conn = psycopg2.connect(host='weather_db', dbname='weather_data', user='vijai', password='vijaivikram', port=5432)

    cur = conn.cursor()
    
    cur.execute(f'CREATE TABLE IF NOT EXISTS weather_data (today DATE,city VARCHAR(255),temperature FLOAT,feels_like FLOAT,minimum_temperature FLOAT,maximum_temperature FLOAT,windspeed FLOAT,humidity INT,description TEXT);')
    
    cur.execute('INSERT INTO weather_data (today,city,temperature,feels_like,minimum_temperature,maximum_temperature,windspeed,humidity,description) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',(dates,city,temperature,feels_like,minimum_temperature,maximum_temperature,windspeed,humidity,description) )                      
    
    
    cur.execute("""SELECT * FROM weather_data;""")
    print(cur.fetchall())
    conn.commit()
    cur.close()
    conn.close()
    

extraction_task = PythonOperator(task_id='extraction_task', python_callable=extract, dag=dag)

transformaation_task = PythonOperator(task_id='transformation_task', python_callable=transform, dag=dag)

load_task = PythonOperator(task_id='load_task', python_callable=load, dag=dag)

extraction_task >> transformaation_task >> load_task

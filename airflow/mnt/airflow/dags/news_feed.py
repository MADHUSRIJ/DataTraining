from datetime import datetime, timedelta
import csv
import requests
from airflow import DAG
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator


default_args = {
    'start_date': datetime(2023, 7, 18, 23, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['srimadhu.j@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

def extract_feed(**kwargs):
    rss_url = 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms'
    response = requests.get(rss_url)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f'raw_rss_feed_{timestamp}.xml'
    with open(filename, 'w') as file:
        file.write(response.text)
    kwargs['ti'].xcom_push(key='raw_xml_filename', value=filename)

def transform_feed(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(key='raw_xml_filename', task_ids='extract')

    tree = ET.parse(filename)
    root = tree.getroot()

    items = []
    for item in root.findall('.//item'):
        title = item.find('title').text
        link = item.find('link').text
        pub_date = item.find('pubDate').text
        items.append((title, link, pub_date))

    curated_filename = f'curated_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    with open(curated_filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Link', 'Pub Date'])
        writer.writerows(items)

    ti.xcom_push(key='curated_filename', value=curated_filename)

def load_feed(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(key='curated_filename', task_ids='transform')

    postgres_user = 'airflow'
    postgres_password = 'airflow'
    postgres_db = 'airflow_db'
    postgres_host = 'postgres'

    conn_str = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_db}"

    engine = create_engine(conn_str)

    df = pd.read_csv(filename)

    df.to_sql('RssNewsFeed', con=engine, if_exists='append', index=False)

with DAG('RssFeed',
         default_args=default_args,
         start_date=datetime(2023, 7, 18, 23, 0),
         schedule_interval='0 23 * * *',) as dag:

    download_task = PythonOperator(task_id='extract',
                                   python_callable=extract_feed)

    parse_task = PythonOperator(task_id='transform',
                                python_callable=transform_feed)

    load_task = PythonOperator(task_id='load',
                               python_callable=load_feed)
    
    
    email_task = EmailOperator(task_id='send_email',
                               to='srimadhu.j@gmail.com',
                               subject='RssFeed DAG Alert',
                               html_content='The RssFeed DAG has completed successfully.',
                               )

    download_task >> parse_task >> load_task >> email_task
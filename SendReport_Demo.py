from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import  requests, json, lxml.etree, time

default_args = {
    'start_date': days_ago(1), 
    'owner': 'Airflow',
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG (
     dag_id = 'send_report_demo',
     default_args = default_args,
     description = 'A demo to send excel report of a company''s employee info',
     schedule_interval = '00 01 * * *'
)

def executeJob(**kwargs):

    query_status_url = 'http://172.16.111.35:8080/kettle/executeJob/?job=/app/kettle_repo/Generate.kjb&level=debug'

    basicAuthCredentials = ('cluster', 'cluster')

    headers = {'content-type': 'application/x-www-form-urlencoded'}

    r = requests.post(query_status_url, auth=basicAuthCredentials, headers=headers)

    root = lxml.etree.fromstring(r.text.encode('utf-8'))

    if r.status_code == 200: 
    
        id = root.xpath('/webresult/id')[0].text

        return id
    else:
        raise Exception(root.xpath('/webresult/message')[0].text)

execute_kettle_job = PythonOperator(
    task_id='execute_kettle_job',
    provide_context=True,
    python_callable=executeJob,
    dag=dag
)

def getJobStatus(**kwargs):

    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='execute_kettle_job')

    query_status_url = 'http://172.16.111.35:8080/kettle/jobStatus/?name=Generate&id=' + ls + '&xml=Y'

    basicAuthCredentials = ('cluster', 'cluster')

    headers = {'content-type': 'application/x-www-form-urlencoded'}

    while True:
        r = requests.post(query_status_url, auth=basicAuthCredentials, headers=headers)

        root = lxml.etree.fromstring(r.text.encode('utf-8'))

        hasError = len(root.xpath('/webresult')) > 0

        if hasError: 
            raise Exception(root.xpath('/webresult/message')[0].text)

        else:
            root = lxml.etree.fromstring(r.text.encode('utf-8'))

            status = root.xpath('/jobstatus/status_desc')[0].text

            if status == "Finished":
                break

            time.sleep(2)

check_kettle_status = PythonOperator(
    task_id='check_kettle_status',
    provide_context=True,
    python_callable=getJobStatus,
    dag=dag
)

execute_kettle_job >> check_kettle_status
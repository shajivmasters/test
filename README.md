from airflow import DAG
from datetime import datetime
from servicenow_client import ServiceNowClient

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 6),
    'retries': 1,
}

dag = DAG(
    'service_now_workflow_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def close_incidents():
    sn_client = ServiceNowClient(connection_id="your_connection_id")
    query_conditions = [('state', '=', 'active')]
    incidents = sn_client.query('incident', query_conditions)
    
    for incident in incidents.all():
        sn_client.close_incident(incident['sys_id'], 'Closed via Airflow')

close_incidents_task = PythonOperator(
    task_id='close_incidents',
    python_callable=close_incidents,
    dag=dag,
)

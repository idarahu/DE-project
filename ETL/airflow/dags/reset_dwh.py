"""
This DAG resets the DWH by dropping all the generated tables from the database,
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# DAG

default_args = {
    'owner': 'Ihar',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='reset_dwh',
    default_args=default_args,
    description='Reset the DWH',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['dev'],
)

# Flow

drop_tables = PostgresOperator(
    task_id='drop_tables',
    dag=dag,
    postgres_conn_id='citus_warehouse',
    sql='TRUNCATE warehouse.authors, warehouse.institution, warehouse.publication_author, '
        'warehouse.publication_domain, warehouse.publication_institution, '
        'warehouse.publication_time, warehouse.publication_venues, warehouse.publications, '
        'warehouse.scientific_domain CASCADE;',
    autocommit=True
)

EmptyOperator(task_id='start') >> drop_tables >> EmptyOperator(task_id='end')

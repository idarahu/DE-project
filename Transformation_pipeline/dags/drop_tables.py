"""
This DAG drops all the generated tables from the database for development purposes.
"""

from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'de-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='drop_tables',
    default_args=default_args,
    description='Drop all the tables from the database',
    schedule_interval=None,
    start_date=days_ago(2),
)

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql="""
        DROP TABLE IF EXISTS affiliations;
        DROP TABLE IF EXISTS affiliation2publication;
        DROP TABLE IF EXISTS publication2arxiv;
        DROP TABLE IF EXISTS affiliations_temp;
        DROP TABLE IF EXISTS authors_temp;
        DROP TABLE IF EXISTS publications;
        DROP TABLE IF EXISTS arxiv_categories;
        DROP TABLE IF EXISTS author2affiliation;
        DROP TABLE IF EXISTS authors;
        DROP TABLE IF EXISTS publication2arxiv;
        DROP TABLE IF EXISTS venues;
        
        """,
    autocommit=True
)

drop_tables

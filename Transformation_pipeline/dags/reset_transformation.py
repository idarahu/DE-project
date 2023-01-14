"""
This DAG resets the tranformation pipeline by dropping all the generated tables from the database,
resetting split_no.txt and publication_ID.txt, and deleting the files in the data folder.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id='reset_transformation',
    default_args=default_args,
    description='Reset the transformation pipeline',
    schedule_interval=None,
    start_date=days_ago(2),
)

reset_split_no = BashOperator(
    task_id='reset_split_no',
    dag=dag,
    bash_command='echo 0 > /tmp/data/setups/split_no.txt'
)

reset_publication_ID = BashOperator(
    task_id='reset_publication_ID',
    dag=dag,
    bash_command='echo 0 > /tmp/data/setups/publication_ID.txt'
)

delete_final_data = BashOperator(
    task_id='delete_data',
    dag=dag,
    bash_command='rm -f /tmp/data/final_data/affiliation*; '
                 'rm -f /tmp/data/final_data/author*; '
                 'rm -f /tmp/data/final_data/publication*; '
                 'rm -f /tmp/data/final_data/venues*; '
                 'rm -f /tmp/data/final_data/citing_pub*'
)

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql="""DROP TABLE IF EXISTS affiliations CASCADE;
DROP TABLE IF EXISTS affiliation2publication CASCADE;
DROP TABLE IF EXISTS publication2arxiv CASCADE;
DROP TABLE IF EXISTS affiliations_temp CASCADE;
DROP TABLE IF EXISTS authors_temp CASCADE;
DROP TABLE IF EXISTS publications CASCADE;
DROP TABLE IF EXISTS arxiv_categories CASCADE;
DROP TABLE IF EXISTS author2affiliation CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS venues CASCADE;
DROP TABLE IF EXISTS author2publication CASCADE;
DROP TABLE IF EXISTS updated_publications CASCADE;
""",
    autocommit=True
)

create_tables_trigger = TriggerDagRunOperator(
    task_id='create_tables',
    trigger_dag_id='create_DB_tables_and_SQL_statements',
    dag=dag,
)

# Flow

EmptyOperator(task_id='start') >> [
    reset_split_no,
    reset_publication_ID,
    delete_final_data,
    drop_tables
] >> create_tables_trigger

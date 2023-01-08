"""
This DAG loads the data into neo4j graph database.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.utils.dates import days_ago

neo4j_conn_id = 'neo4j'


# Lib

def load_venues():
    global neo4j_conn_id

    hook = Neo4jHook(conn_id=neo4j_conn_id)
    hook.run('MATCH (n:Venue) DELETE n')
    hook.run("""LOAD CSV FROM "file:///venues.csv" AS row
CALL {
  WITH row
  CREATE (:Venue {full_name: row[1], venue_id: row[0]})
} IN TRANSACTIONS OF 100 ROWS
""")


def load_authors():
    global neo4j_conn_id

    hook = Neo4jHook(conn_id=neo4j_conn_id)
    hook.run('MATCH (n:Author) DELETE n')
    hook.run("""LOAD CSV FROM "file:///authors.csv" AS row
CALL {
  WITH row
  CREATE (:Author {author_id: row[0], full_name: row[1], h_index_calculated: toInteger(row[2])})
} IN TRANSACTIONS OF 100 ROWS
""")


# DAG


default_args = {
    'owner': 'Ihar',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='load_into_graph_db',
    default_args=default_args,
    description='Load the data into neo4j graph database',
    schedule_interval=None,
    start_date=days_ago(2),
)

load_venues = PythonOperator(
    task_id='load_venues',
    dag=dag,
    python_callable=load_venues,
)

load_authors = PythonOperator(
    task_id='load_authors',
    dag=dag,
    python_callable=load_authors,
)

# Flow

EmptyOperator(task_id='start') >> [
    load_venues,
    load_authors,
]

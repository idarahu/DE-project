"""
This DAG pulls the data of the publications with DOI from the up-to-date database (DB). 
Then, it makes the API calls to check if the number of citations has been changed and updates
the values if needed (in the table of publications in the DB). 
Finally, it refreshes the authors' and venues' views in the DB and generates CSV files 
based on the updated data needed for populating the DWH and graph DB with data.
"""

import datetime
import io
from datetime import datetime, timedelta, timezone

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'Ida',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DATA2DB_FOLDER = '/tmp/data/data2db'
SQL_FOLDER = '/tmp/data/sql'
FINAL_DATA_FOLDER = '/tmp/data/final_data'

update_articles_in_DB_dag = DAG(
    dag_id='update_articles_in_DB', 
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022,10,12,14,0,0),
    schedule_interval=None,
    #schedule_interval=@monthly,
    catchup=False,
    template_searchpath=[SQL_FOLDER]
)

def do_opencitation_call(DOI):
    import opencitingpy
    client = opencitingpy.client.Client()
    try:
        no_citations = client.get_citation_count(DOI)
        return no_citations
    except:
        return -1

def update_publications():
    import pandas as pd

    conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    publications_old_df = pd.read_sql('SELECT publication_id, doi, number_of_citations FROM publications WHERE doi IS NOT NULL', conn)
    publications_old_df['number_of_citations_new'] = publications_old_df.apply(lambda x: do_opencitation_call(x.doi), axis=1)
    publications_new_df = publications_old_df[publications_old_df.number_of_citations_new.gt(publications_old_df.number_of_citations)]
    publications_new_df = publications_new_df.drop(['doi', 'number_of_citations'], axis=1)
    publications_new_df = publications_new_df.rename(columns={'number_of_citations_new': 'number_of_citations'})
    publications_new_df['updated_at'] = datetime.now(timezone.utc)
    publications_new_df.to_csv(f'{DATA2DB_FOLDER}/updated_publications_df.tsv', sep="\t", index=False)

update_publications_data = PythonOperator(
    task_id='update_publications_data',
    dag=update_articles_in_DB_dag,
    python_callable=update_publications
)

truncate_updated_publications_table = PostgresOperator(
	task_id='truncate_updated_publications_table',
    dag=update_articles_in_DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE updated_publications"
)

update_publications_data >> truncate_updated_publications_table    

def tsv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    with open(f'{DATA2DB_FOLDER}/{file_name}', 'r') as f:
        next(f)
        curr.copy_from(f, DB_table, sep='\t')
        get_postgres_conn.commit()

load_updated_publications_data = PythonOperator(
    task_id='load_updated_publications_data',
    dag=update_articles_in_DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'updated_publications_df.tsv',
        'DB_table': 'updated_publications'
    }
)

truncate_updated_publications_table >> load_updated_publications_data

update_no_citations = PostgresOperator(
    task_id='update_no_citations',
    dag=update_articles_in_DB_dag,
    postgres_conn_id='airflow_pg',
    sql='update_no_citations.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

load_updated_publications_data >> update_no_citations

create_updated_authors_view = PostgresOperator(
    task_id='create_updated_authors_view',
    dag=update_articles_in_DB_dag,
    postgres_conn_id='airflow_pg',
    sql='authors_view.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

create_updated_venues_view = PostgresOperator(
    task_id='create_updated_venues_view',
    dag=update_articles_in_DB_dag,
    postgres_conn_id='airflow_pg',
    sql='venues_view.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def copy_data_from_DB(output_folder, SQL_statement, data_type):
    import pandas as pd
    conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    df = pd.read_sql(SQL_statement, conn)
    file_name = data_type + '_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'    
    df.to_csv(f'{output_folder}/{file_name}', index=False)

copy_updated_publications = PythonOperator(
    task_id='copy_updated_publications',
    dag=update_articles_in_DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM publications',
        'data_type': 'publications'
    }
)

copy_updated_authors = PythonOperator(
    task_id='copy_updated_authors',
    dag=update_articles_in_DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM authors_view',
        'data_type': 'authors'
    }
)

copy_updated_venues = PythonOperator(
    task_id='copy_updated_venues',
    dag=update_articles_in_DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM venues_view',
        'data_type': 'venues'
    }
)

update_no_citations >> create_updated_authors_view >> copy_updated_authors
update_no_citations >> create_updated_venues_view >> copy_updated_venues
update_no_citations >> copy_updated_publications

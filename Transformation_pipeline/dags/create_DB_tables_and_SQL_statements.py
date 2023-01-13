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

SQL_FOLDER = '/tmp/data/sql'
DATA2DB_FOLDER = '/tmp/data/data2db'

create_DB_tables_and_SQL_statements_dag = DAG(
    dag_id='create_DB_tables_and_SQL_statements', 
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022,10,12,14,0,0),
    schedule_interval=None,
    #schedule_interval=timedelta(minutes=2),
    catchup=False,
    template_searchpath=[SQL_FOLDER]
)

# Creating venues table
def create_venues_table(output_folder):
    with open(f'{output_folder}/venues.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS venues (\n'
            'venue_ID INT PRIMARY KEY,\n'
            'full_name VARCHAR(500),\n'
            'abbreviation VARCHAR(100) UNIQUE,\n'
            'print_issn VARCHAR(50),\n'
            'electronic_issn VARCHAR(50));\n'
        )

prepare_venues_sql = PythonOperator(
    task_id='prepare_venues_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_venues_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_venues_sql = PostgresOperator(
    task_id='create_venues_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='venues.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating the publications table
def create_publications_table(output_folder):
    with open(f'{output_folder}/publications.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS publications (\n'
            'publication_ID INT PRIMARY KEY,\n'
            'venue_ID INT,\n'
            'DOI VARCHAR(255),\n'
            'title TEXT,\n'
            'date DATE,\n'
            'submitter VARCHAR(500),\n'
            'type VARCHAR(50),\n'
            'language VARCHAR(50),\n'
            'page_numbers VARCHAR(16),\n'
            'volume INT,\n'
            'issue INT,\n'
            'number_of_references INT,\n'
            'number_of_citations INT,\n'
            'no_versions_arXiv INT,\n'
            'date_of_first_version DATE,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_publications_sql = PythonOperator(
    task_id='prepare_publications_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_publications_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_publications_sql = PostgresOperator(
    task_id='create_publications_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='publications.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating the publications table
def create_publications_temp_table(output_folder):
    with open(f'{output_folder}/publications_temp.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS publications_temp (\n'
            'publication_ID INT PRIMARY KEY,\n'
            'venue_ID INT,\n'
            'DOI VARCHAR(255),\n'
            'title TEXT,\n'
            'date DATE,\n'
            'submitter VARCHAR(500),\n'
            'type VARCHAR(50),\n'
            'language VARCHAR(50),\n'
            'page_numbers VARCHAR(16),\n'
            'volume INT,\n'
            'issue INT,\n'
            'number_of_references INT,\n'
            'number_of_citations INT,\n'
            'no_versions_arXiv INT,\n'
            'date_of_first_version DATE,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_publications_temp_sql = PythonOperator(
    task_id='prepare_publications_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_publications_temp_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_publications_temp_sql = PostgresOperator(
    task_id='create_publications_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='publications_temp.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating authors table and also a temporary authors table for bulk inserts to check the duplicates
def create_authors_table(output_folder):
    with open(f'{output_folder}/authors.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS authors (\n'
            'author_ID serial PRIMARY KEY,\n'
            'last_name VARCHAR(1000) NOT NULL,\n'
            'first_name VARCHAR(1000),\n'
            'first_name_abbr VARCHAR(100) NOT NULL,\n'
            'extra VARCHAR(100),\n'
            'position TEXT,\n'
            'h_index_real INT,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_authors_sql = PythonOperator(
    task_id='prepare_authors_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_authors_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_authors_sql = PostgresOperator(
    task_id='create_authors_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def create_authors_temp_table(output_folder):
    with open(f'{output_folder}/authors_temp.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS authors_temp (\n'
            'publication_ID INT,\n'
            'last_name VARCHAR(1000) NOT NULL,\n'
            'first_name VARCHAR(1000),\n'
            'first_name_abbr VARCHAR(100) NOT NULL,\n'
            'extra VARCHAR(100),\n'
            'position TEXT,\n'
            'h_index_real INT,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_authors_temp_sql = PythonOperator(
    task_id='prepare_authors_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_authors_temp_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_authors_temp_sql = PostgresOperator(
    task_id='create_authors_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='authors_temp.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating connection table between authors and publications
def create_author2publication_table(output_folder):
    with open(f'{output_folder}/author2publication.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS author2publication (\n'
            'author2pub_ID serial PRIMARY KEY,\n'
            'author_ID INT NOT NULL,\n'
            'publication_ID INT NOT NULL,\n'
            'UNIQUE (author_ID, publication_ID));\n'
        )

prepare_author2publication_sql = PythonOperator(
    task_id='prepare_author2publication_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_author2publication_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_author2publication_sql = PostgresOperator(
    task_id='create_author2publication_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='author2publication.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating affiliations table and also a temporary affiliations table for bulk inserts to check the duplicates
def create_affiliations_table(output_folder):
    with open(f'{output_folder}/affiliations.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS affiliations (\n'
            'affiliation_ID serial PRIMARY KEY,\n'
            'institution_name VARCHAR(255),\n'
            'institution_place VARCHAR(255),\n'
            'UNIQUE (institution_name, institution_place));\n'
        )

prepare_affiliations_sql = PythonOperator(
    task_id='prepare_affiliations_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliations_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_affiliations_sql = PostgresOperator(
    task_id='create_affiliations_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliations.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def create_affiliations_temp_table(output_folder):
    with open(f'{output_folder}/affiliations_temp.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS affiliations_temp (\n'
            'publication_ID INT,\n'
            'institution_name VARCHAR(255),\n'
            'institution_place VARCHAR(255),\n'
            'author_last_name VARCHAR(100) NOT NULL,\n'
            'author_first_name_abbr VARCHAR(25) NOT NULL);\n'
        )

prepare_affiliations_temp_sql = PythonOperator(
    task_id='prepare_affiliations_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliations_temp_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_affiliations_temp_sql = PostgresOperator(
    task_id='create_affiliations_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliations_temp.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating connection tables between affiliations and publications together with temporary table (for checking duplicates) and authors and affiliations
def create_affiliation2publication_table(output_folder):
    with open(f'{output_folder}/affiliation2publication.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS affiliation2publication (\n'
            'affiliation2pub_ID serial PRIMARY KEY,\n'
            'affiliation_ID INT NOT NULL,\n'
            'publication_ID INT NOT NULL,\n'
            'UNIQUE (affiliation_ID, publication_ID));\n'
        )

prepare_affiliation2publication_sql = PythonOperator(
    task_id='prepare_affiliation2publication_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliation2publication_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_affiliation2publication_sql = PostgresOperator(
    task_id='create_affiliation2publication_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliation2publication.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def create_affiliation2publication_temp_table(output_folder):
    with open(f'{output_folder}/affiliation2publication_temp.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS affiliation2publication_temp (\n'
            'affiliation2pub_ID serial PRIMARY KEY,\n'
            'affiliation_ID INT NOT NULL,\n'
            'publication_ID INT NOT NULL);\n'
        )

prepare_affiliation2publication_temp_sql = PythonOperator(
    task_id='prepare_affiliation2publication_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliation2publication_temp_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_affiliation2publication_temp_sql = PostgresOperator(
    task_id='create_affiliation2publication_temp_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliation2publication_temp.sql',
    trigger_rule='none_failed',
    autocommit=True,
)


def create_author2affiliation_table(output_folder):
    with open(f'{output_folder}/author2affiliation.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS author2affiliation (\n'
            'author2affiliation_ID serial PRIMARY KEY,\n'
            'author_ID INT NOT NULL,\n'
            'affiliation_ID INT NOT NULL,\n'
            'UNIQUE (author_ID, affiliation_ID));\n'
        )

prepare_author2affiliation_sql = PythonOperator(
    task_id='prepare_author2affiliation_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_author2affiliation_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_author2affiliation_sql = PostgresOperator(
    task_id='create_author2affiliation_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='author2affiliation.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating arXiv categories table and cennection table between publication and arXiv category
def create_arxiv_categories_table(output_folder):
    with open(f'{output_folder}/arxiv_categories.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS arxiv_categories (\n'
            'arxiv_category_ID INT PRIMARY KEY,\n'
            'arxiv_category VARCHAR(50) NOT NULL UNIQUE);\n'
        )

prepare_arxiv_categories_sql = PythonOperator(
    task_id='prepare_arxiv_categories_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_arxiv_categories_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_arxiv_categories_sql = PostgresOperator(
    task_id='create_arxiv_categories_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='arxiv_categories.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def create_publication2arxiv_table(output_folder):
    with open(f'{output_folder}/publication2arxiv.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS publication2arxiv (\n'
            'publication_ID INT NOT NULL,\n'
            'arxiv_category_ID INT NOT NULL);\n'
        )

prepare_publication2arxiv_sql = PythonOperator(
    task_id='prepare_publication2arxiv_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_publication2arxiv_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_publication2arxiv_sql = PostgresOperator(
    task_id='create_publication2arxiv_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='publication2arxiv.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def csv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    with open(f'{DATA2DB_FOLDER}/{file_name}', 'r') as f:
        next(f)
        curr.copy_from(f, DB_table, sep=',')
        get_postgres_conn.commit()

truncate_arxiv_table = PostgresOperator(
	task_id='truncate_arxiv_table',
    dag=create_DB_tables_and_SQL_statements_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE arxiv_categories"
)

load_arxiv_category = PythonOperator(
    task_id='load_arxiv_category',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=csv_to_db,
    op_kwargs={
        'file_name': 'arxiv_categories.csv',
        'DB_table': 'arxiv_categories'
    }
)

truncate_arxiv_table >> load_arxiv_category

step1 = EmptyOperator(task_id='step1')
step2 = EmptyOperator(task_id='step2') 

step1 >> [prepare_venues_sql, prepare_publications_sql, prepare_publications_temp_sql, prepare_authors_sql,
          prepare_authors_temp_sql, prepare_author2publication_sql, prepare_affiliations_sql, prepare_affiliations_temp_sql, 
          prepare_affiliation2publication_sql, prepare_affiliation2publication_temp_sql, prepare_author2affiliation_sql, 
          prepare_arxiv_categories_sql, prepare_publication2arxiv_sql] >> step2

step3 = EmptyOperator(task_id='step3')      
step2 >> [create_authors_sql, create_authors_temp_sql, create_publications_temp_sql,
          create_author2publication_sql, create_affiliations_sql, create_affiliations_temp_sql, 
          create_affiliation2publication_sql, create_affiliation2publication_temp_sql, 
          create_author2affiliation_sql, create_publication2arxiv_sql] >> step3 
step2 >> create_venues_sql >> create_publications_sql >> step3
step2 >> create_arxiv_categories_sql >> truncate_arxiv_table >> load_arxiv_category >> step3

# Creating publications_temp2publications.sql to populate publications table with only new publications
def create_publications_temp2publications_sql(output_folder):
    with open(f'{output_folder}/publications_temp2publications.sql', 'w') as f:
        f.write(
            'INSERT INTO publications (publication_id, venue_id, doi, title, date, submitter, type, language, page_numbers, volume, issue, number_of_references, number_of_citations, no_versions_arxiv, date_of_first_version, updated_at)\n'
            'SELECT DISTINCT publication_id, venue_id, doi, title, date, submitter, type, language, page_numbers, volume, issue, number_of_references, number_of_citations, no_versions_arxiv, date_of_first_version, updated_at\n'
            'FROM publications_temp\n'
            'WHERE NOT EXISTS (\n'
            'SELECT * FROM publications\n'
            'WHERE\n'
            'publications.title = publications_temp.title);\n'
        )

prepare_publications_temp2publications_sql = PythonOperator(
    task_id='prepare_publications_temp2publications_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_publications_temp2publications_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Creating affiliation2publication_temp2affiliation2publication.sql to populate affiliation2publication table with only new data
def create_affiliation2publication_temp2affiliation2publication_sql(output_folder):
    with open(f'{output_folder}/affiliation2publication_temp2affiliation2publication.sql', 'w') as f:
        f.write(
            'INSERT INTO affiliation2publication (affiliation_id, publication_id)\n'
            'SELECT DISTINCT affiliation_id, publication_id\n'
            'FROM affiliation2publication_temp\n'
            'WHERE NOT EXISTS (\n'
            'SELECT * FROM affiliation2publication\n'
            'WHERE\n'
            'affiliation2publication.affiliation_id = affiliation2publication_temp.affiliation_id\n'
            'AND affiliation2publication.publication_id = affiliation2publication_temp.publication_id);\n'
        )

prepare_affiliation2publication_temp2affiliation2publication_sql = PythonOperator(
    task_id='prepare_affiliation2publication_temp2affiliation2publication_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliation2publication_temp2affiliation2publication_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Creating authors_temp2authors.sql to populate authors table with only new authors that appeared in this batch of data
def create_authors_temp2authors_sql(output_folder):
    with open(f'{output_folder}/authors_temp2authors.sql', 'w') as f:
        f.write(
            'INSERT INTO authors (last_name, first_name, first_name_abbr, extra, position, h_index_real, updated_at)\n'
            'SELECT DISTINCT last_name, first_name, first_name_abbr, extra, position, h_index_real, updated_at\n'
            'FROM authors_temp\n'
            'WHERE NOT EXISTS (\n'
            'SELECT * FROM authors\n'
            'WHERE\n'
            'authors.last_name = authors_temp.last_name\n'
            'AND authors.first_name_abbr = authors_temp.first_name_abbr);\n'
        )

prepare_authors_temp2authors_sql = PythonOperator(
    task_id='prepare_authors_temp2authors_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_authors_temp2authors_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Creating affiliations_temp2affiliations.sql to populate affiliations table with only new affiliations that appeared in this batch of data
def create_affiliations_temp2affiliations_sql(output_folder):
    with open(f'{output_folder}/affiliations_temp2affiliations.sql', 'w') as f:
        f.write(
            'INSERT INTO affiliations (institution_name, institution_place)\n'
            'SELECT DISTINCT institution_name, institution_place\n'
            'FROM affiliations_temp\n'
            'WHERE NOT EXISTS (\n'
            'SELECT * FROM affiliations\n'
            'WHERE\n'
            'affiliations.institution_name = affiliations_temp.institution_name\n'
            'AND affiliations.institution_place = affiliations_temp.institution_place);\n'
        )

prepare_affiliations_temp2affiliations_sql = PythonOperator(
    task_id='prepare_affiliations_temp2affiliations_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_affiliations_temp2affiliations_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Connecting all the tables that need to be connected in DB
# Authors with publications
def connect_author2pub_sql(output_folder):
    with open(f'{output_folder}/connect_author2pub.sql', 'w') as f:
        f.write(
            'INSERT INTO author2publication (author_id, publication_id)\n'
            'SELECT DISTINCT t2.author_id, t1.publication_id\n'
            'FROM authors_temp t1\n'
            'JOIN authors t2 ON t1.last_name = t2.last_name AND t1.first_name_abbr = t2.first_name_abbr;\n'
        )

prepare_connect_author2pub_sql = PythonOperator(
    task_id='prepare_connect_author2pub_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=connect_author2pub_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Affiliations with publications
def connect_aff2pub_sql(output_folder):
    with open(f'{output_folder}/connect_aff2pub.sql', 'w') as f:
        f.write(
            'INSERT INTO affiliation2publication_temp (affiliation_id, publication_id)\n'
            'SELECT DISTINCT t2.affiliation_id, t1.publication_id\n'
            'FROM affiliations_temp t1\n'
            'JOIN affiliations t2 ON t1.institution_name = t2.institution_name AND t1.institution_place = t2.institution_place;\n'
        )

prepare_connect_aff2pub_sql = PythonOperator(
    task_id='prepare_connect_aff2pub_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=connect_aff2pub_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

# Authors with affiliations
def connect_author2aff_sql(output_folder):
    with open(f'{output_folder}/connect_author2aff.sql', 'w') as f:
        f.write(
            'INSERT INTO author2affiliation (author_id, affiliation_id)\n'
            'SELECT DISTINCT t2.author_id, t3.affiliation_id\n'
            'FROM affiliations_temp t1\n'
            'JOIN authors t2 ON t1.author_last_name = t2.last_name AND t1.author_first_name_abbr = t2.first_name_abbr\n'
            'JOIN affiliations t3 ON t1.institution_name = t3.institution_name AND t1.institution_place = t3.institution_place\n'
            'WHERE NOT EXISTS (\n'
            'SELECT * FROM author2affiliation\n'
            'WHERE\n'
            'author2affiliation.author_id = t2.author_id\n'
            'AND author2affiliation.affiliation_id = t3.affiliation_id);\n'
        )

prepare_connect_author2aff_sql = PythonOperator(
    task_id='prepare_connect_author2aff_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=connect_author2aff_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

def create_authors_view_sql(output_folder):
    with open(f'{output_folder}/authors_view.sql', 'w') as f:
        f.write(
            'CREATE or REPLACE VIEW authors_view AS\n'
            'SELECT DISTINCT authors.author_id, last_name, first_name, first_name_abbr,\n'
            "first_name_abbr||' '||last_name as full_name,\n"
            'position, h_index_real, COALESCE(h_index_calculated, -1) AS h_index_calculated\n'
            'FROM\n'
            '(SELECT r.author_id, MAX(ranking) AS h_index_calculated\n'
            'FROM\n'
            '(SELECT a.author_ID, p.number_of_citations, ROW_NUMBER() OVER (PARTITION BY a.author_ID ORDER BY p.number_of_citations DESC) AS ranking\n'
            'FROM\n'
            'author2publication ap JOIN authors a ON a.author_ID = ap.author_id JOIN publications p ON p.publication_id = ap.publication_id) AS r\n'
            'WHERE r.number_of_citations >= r.ranking\n'
            'GROUP BY r.author_id) AS h RIGHT JOIN authors ON h.author_id = authors.author_id;\n'
        )

prepare_authors_view_sql = PythonOperator(
    task_id='prepare_authors_view_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_authors_view_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

def create_venues_view_sql(output_folder):
    with open(f'{output_folder}/venues_view.sql', 'w') as f:
        f.write(
            'CREATE or REPLACE VIEW venues_view AS\n'
            'SELECT DISTINCT venues.venue_id, full_name, abbreviation,\n'
            'print_issn, electronic_issn,\n'
            'COALESCE(h_index_calculated, -1) AS h_index_calculated\n'
            'FROM\n'
            '(SELECT venue_id, MAX(ranking) AS h_index_calculated\n'
            'FROM\n'
            '(SELECT p.venue_ID, p.number_of_citations, ROW_NUMBER() OVER (PARTITION BY p.venue_ID ORDER BY p.number_of_citations DESC) AS ranking\n'
            'FROM publications p) AS r\n'
            'WHERE r.number_of_citations >= r.ranking\n'
            'GROUP BY r.venue_id) AS h RIGHT JOIN venues ON h.venue_id = venues.venue_id;\n'
        )

prepare_venues_view_sql = PythonOperator(
    task_id='prepare_venues_view_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_venues_view_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

step4 = EmptyOperator(task_id='step4') 
step3 >> [prepare_publications_temp2publications_sql, prepare_authors_temp2authors_sql, prepare_affiliations_temp2affiliations_sql, 
          prepare_connect_author2pub_sql, prepare_affiliation2publication_temp2affiliation2publication_sql,
          prepare_connect_aff2pub_sql, prepare_connect_author2aff_sql, prepare_authors_view_sql, prepare_venues_view_sql] >> step4

def create_updated_publications_table(output_folder):
    with open(f'{output_folder}/updated_publications.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS updated_publications (\n'
            'publication_ID INT NOT NULL,\n'
            'number_of_citations INT,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_updated_publications_sql = PythonOperator(
    task_id='prepare_updated_publications_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_updated_publications_table,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

create_updated_publications_sql = PostgresOperator(
    task_id='create_updated_publications_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    postgres_conn_id='airflow_pg',
    sql='updated_publications.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

def create_update_no_citations_sql(output_folder):
    with open(f'{output_folder}/update_no_citations.sql', 'w') as f:
        f.write(
            'UPDATE publications\n'
            'SET (number_of_citations, updated_at) = (updated_publications.number_of_citations, updated_publications.updated_at)\n'
            'FROM updated_publications\n'
            'WHERE publications.publication_id = updated_publications.publication_id;\n'
        )

prepare_update_no_citations_sql = PythonOperator(
    task_id='prepare_update_no_citations_sql',
    dag=create_DB_tables_and_SQL_statements_dag,
    python_callable=create_update_no_citations_sql,
    op_kwargs={
        'output_folder': SQL_FOLDER
    }
)

step4 >> prepare_updated_publications_sql >> create_updated_publications_sql
step4 >> prepare_update_no_citations_sql

from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DATA2DB_FOLDER = '/tmp/data/data2db'
SQL_FOLDER = '/tmp/data/sql'
FINAL_DATA_FOLDER = '/tmp/data/final_dat'

DEFAULT_ARGS = {
    'owner': 'Timofei',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='transform_for_batch_injection',
    default_args=DEFAULT_ARGS,
    description='Prepare the graph injection CSVs',
    schedule_interval=None,
    start_date=days_ago(2),
)

input_base_dir = Path('/tmp/data/')
final_data_dir = input_base_dir / 'final_data'


def get_latest_filename(folder_path: Path, prefix: str) -> Optional[Path]:
    files = sorted(folder_path.glob(f'{prefix}*'))
    print(files)
    return files[-1] if files else None


def infer_separator(file_path: Path) -> str:
    return '\t' if file_path.suffix == '.tsv' else ','


def get_time_id(date: str) -> int:
    params = {date: date}
    query = open('/tmp/data/wh_sql/upsert_publication_time.sql', 'r')
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    publication_time_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    query.close()
    return int(publication_time_id)


def get_venue_id(full_name: str, abbreviation: str, h_index_calculated: str) -> int:
    params = {full_name: full_name, abbreviation: abbreviation, h_index_calculated: h_index_calculated}
    query = open('/tmp/data/wh_sql/insert_venue.sql', 'r')
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    venue_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    query.close()
    return int(venue_id)


def get_publication_id(params) -> int:
    query = open('/tmp/data/wh_sql/upsert_publication.sql', 'r')
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    publication_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    query.close()
    return int(publication_id)


def get_author_id(params) -> int:
    query = open('/tmp/data/wh_sql/upsert_author.sql', 'r')
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    publication_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    query.close()
    return int(publication_id)


def insert_author_publication(params) -> None:
    query = open('/tmp/data/wh_sql/insert_publication_author.sql', 'r')
    cursor = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn().cursor()
    cursor.execute(query.read().format(**params))


def update_warehouse(venues_path: Path, publications_path: Path, authors_path: Path, author_to_publications_path: Path,
                     affiliations_path: Path, publication_to_affiliations_path: Path,
                     publication_to_domains_path: Path):
    venues_df = pd.read_csv(venues_path, sep=infer_separator(venues_path))
    publications_df = pd.read_csv(publications_path, sep=infer_separator(publications_path))
    authors_df = pd.read_csv(authors_path, sep=infer_separator(authors_path))
    author_to_publications_df = pd.read_csv(author_to_publications_path, sep=infer_separator(authors_path))
    # affiliations_df = pd.read_csv(affiliations_path, sep=infer_separator(affiliations_path))
    # publication_to_affiliations_df = pd.read_csv(publication_to_affiliations_path,
    #                                              sep=infer_separator(publication_to_affiliations_path))
    # publication_to_domains_df = pd.read_csv(publication_to_domains_path,
    #                                         sep=infer_separator(publication_to_domains_path))
    for venue in venues_df.iterrows():
        venue.db_id = get_venue_id(venue['full_name'], venue['abbreviation'], venue['h_index_calculated'])

    for publication in publications_df.iterrows():
        venue_id = publication['venue_id']
        venue_db_id = venues_df.query('venue_id = @venue_id').iloc[0, 'venue_db_id']
        time_db_id = get_time_id(publication['date'])
        parameters = {}
        parameters['doi'] = publication['doi']
        parameters['title'] = publication['titile']
        parameters['submitter'] = publication['submitter']
        parameters['lang'] = publication['lang']
        parameters['venue_id'] = venue_db_id
        parameters['time_id'] = time_db_id
        parameters['volume'] = publication['volume']
        parameters['issue'] = publication['issue']
        parameters['page_numbers'] = publication['page_numbers']
        parameters['number_of_references'] = publication['number_of_references']
        parameters['no_ver_arxiv'] = publication['no_ver_arxiv']
        parameters['date_of_first_version'] = publication['date_of_first_version']
        parameters['number_of_citations'] = publication['number_of_citations']
        publication.db_id = get_publication_id(parameters)

    for author in authors_df.iterrows():
        params = {}
        params['first_name'] = author['first_name']
        params['last_name'] = author['last_name']
        params['first_name_abbr'] = author['first_name_abbr']
        params['full_name'] = author['full_name']
        params['h_index_real'] = author['h_index_real']
        author.db_id = get_author_id(params)

        # author id, publication_id
    for publication_author in author_to_publications_df.iterrows():
        author_id = publication_author['author_id']
        author_db_id = authors_df.query('author_id = @author_id').iloc[0, 'author_db_id']
        publication_id = publication_author['publication_id']
        publication_db_id = publications_df.query('publication_id = @publication_id').iloc[0, 'publication_db_id']
        params = {}
        params['author_id'] = author_db_id
        params['publication_id'] = publication_db_id
        insert_author_publication(params)


update_warehouse_db = PythonOperator(
    task_id='prepare_author_of_relationships',
    python_callable=update_warehouse,
    op_kwargs={
        'venues_path': get_latest_filename(final_data_dir, 'venues_'),
        'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        'authors_path': get_latest_filename(final_data_dir, 'authors_'),
        'author_to_publications_path': get_latest_filename(final_data_dir, 'author2publication_'),
        'affiliations_path': get_latest_filename(final_data_dir, 'affiliations_'),
        'publication_to_affiliations_path': get_latest_filename(final_data_dir, 'affiliation2publication_'),
        'publication_to_domains_path': get_latest_filename(final_data_dir, 'publication2domain_'),
    },
    dag=dag
)

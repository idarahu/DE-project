from datetime import timedelta
from pathlib import Path
from typing import Optional
import swifter
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DATA2DB_DIR = Path('/tmp/data/data2db')
SQL_DIR = Path('/tmp/data/sql')
FINAL_DATA_DIR = Path('/tmp/data/final_data')
PREPARED_DATA_FILE = Path('/tmp/data/wh-data/prepared_publications.csv')

DEFAULT_ARGS = {
    'owner': 'Timofei',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='update_warehouse_db',
    default_args=DEFAULT_ARGS,
    description='Update warehouse DB with latest data',
    schedule_interval=None,
    start_date=days_ago(2),
)


def get_latest_filename(folder_path: Path, prefix: str) -> Optional[Path]:
    files = sorted(folder_path.glob(f'{prefix}*'))
    return files[-1] if files else None


def infer_separator(file_path: Path) -> str:
    return '\t' if file_path.suffix == '.tsv' else ','


def prepare_string_value(value) -> str:
    return str(value).replace("'", "").replace('nan', '')


def get_time_id(date: str, connection) -> int:
    params = {}
    params['pdate'] = date
    query = open('/tmp/data/wh_sql/upsert_publication_time.sql', 'r')
    publication_time_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    connection.commit()
    query.close()
    return int(publication_time_id)


def get_venue_id(venue, connection) -> int:
    params = {}
    params['full_name'] = venue['full_name']
    params['abbreviation'] = venue['abbreviation']
    query = open('/tmp/data/wh_sql/insert_venue.sql', 'r')
    prepared_query = query.read().format(**params)
    cursor = connection.cursor()
    cursor.execute(prepared_query)
    queried_venue_id = cursor.fetchone()[0]
    connection.commit()
    query.close()
    return int(queried_venue_id)


def get_publication_id(publication, venues_df, connection) -> int:
    venue_db_id = venues_df.query('venue_ID == {}'.format(publication['venue_id'])).iloc[0]['db_id'] if (
                publication['venue_id'] not in [0, 1]) else 'null'
    params = {}
    params['doi'] = prepare_string_value(publication.get('doi', default=''))
    params['title'] = prepare_string_value(publication.get('title'))
    params['submitter'] = prepare_string_value(publication.get('submitter', default=''))
    params['lang'] = prepare_string_value(publication.get('lang', default=''))
    params['venue_id'] = 'null' if venue_db_id == 'nan' else venue_db_id
    params['time_id'] = get_time_id(publication.get('date'), connection)
    params['volume'] = publication.get('volume')
    params['issue'] = publication['issue']
    params['page_numbers'] = prepare_string_value(publication.get('page_numbers', default=''))
    params['number_of_references'] = publication['number_of_references']
    params['no_ver_arxiv'] = publication.get('no_versions_arxiv')
    params['date_of_first_version'] = publication['date_of_first_version']
    params['number_of_citations'] = publication['number_of_citations']
    query = open('/tmp/data/wh_sql/upsert_publication.sql', 'r')
    prepared_query = query.read().format(**params)
    cursor = connection.cursor()
    cursor.execute(prepared_query)
    publication_id = cursor.fetchone()[0]
    connection.commit()
    query.close()
    return int(publication_id)


def update_warehouse(venues_path: Path, publications_path: Path, output_prepared_publication_path: Path):
    venues_df = pd.read_csv(venues_path, sep=infer_separator(venues_path))
    publications_df = pd.read_csv(publications_path, sep=infer_separator(publications_path))
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    venues_df['db_id'] = venues_df.swifter.apply(lambda venue: get_venue_id(venue, connection), axis=1)
    publications_df['db_id'] = publications_df.swifter.apply(
        lambda publication: get_publication_id(publication, venues_df, connection),
        axis=1)
    output_prepared_publication_path.parent.mkdir(parents=True, exist_ok=True)
    publications_df.to_csv(output_prepared_publication_path, index=False, encoding='utf-8')


update_publication = PythonOperator(
    task_id='update_warehouse_data',
    python_callable=update_warehouse,
    op_kwargs={
        'venues_path': get_latest_filename(DATA2DB_DIR, 'venues_'),
        'publications_path': get_latest_filename(FINAL_DATA_DIR, 'publications_'),
        'output_prepared_publication_path': PREPARED_DATA_FILE
    },
    dag=dag
)


def get_affiliation_id(affiliation, connection):
    params = {}
    params['name'] = prepare_string_value(affiliation.get('institution_name', ''))
    params['address'] = prepare_string_value(affiliation.get('institution_place', ''))
    query = open('/tmp/data/wh_sql/upsert_institution.sql', 'r')
    affiliation_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    connection.commit()
    query.close()
    return int(affiliation_id)


def insert_affiliation_publication(publication_affiliation, affiliations_df, publications_df, connection) -> None:
    affiliation_id = int(publication_affiliation['affiliation_id'])
    affiliation_db_id = int(affiliations_df.query('affiliation_id == {}'.format(affiliation_id)).iloc[0]['db_id'])
    publication_id = int(publication_affiliation['publication_id'])
    publication_db_id = int(publications_df.query('publication_id == {}'.format(publication_id)).iloc[0]['db_id'])
    params = {}
    params['institution_id'] = affiliation_db_id
    params['publication_id'] = publication_db_id
    query = open('/tmp/data/wh_sql/insert_publication_affiliation.sql', 'r')
    connection.cursor().execute(query.read().format(**params))
    connection.commit()


def update_warehouse_affiliations(prepared_publication_path: Path, affiliations_path: Path,
                                  publication_to_affiliations_path: Path):
    publication_df = pd.read_csv(prepared_publication_path, sep=infer_separator(prepared_publication_path))
    affiliations_df = pd.read_csv(affiliations_path, sep=infer_separator(affiliations_path))
    publication_to_affiliations_df = pd.read_csv(publication_to_affiliations_path,
                                                 sep=infer_separator(publication_to_affiliations_path))
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    affiliations_df['db_id'] = affiliations_df.swifter.apply(
        lambda affiliation: get_affiliation_id(affiliation, connection), axis=1)
    publication_to_affiliations_df.swifter.apply(
        lambda publication_affiliation: insert_affiliation_publication(publication_affiliation, affiliations_df,
                                                                       publication_df, connection), axis=1)


update_affiliations_data = PythonOperator(
    task_id='update_affiliations_data',
    python_callable=update_warehouse_affiliations,
    op_kwargs={
        'prepared_publication_path': PREPARED_DATA_FILE,
        'affiliations_path': get_latest_filename(FINAL_DATA_DIR, 'affiliations_'),
        'publication_to_affiliations_path': get_latest_filename(FINAL_DATA_DIR, 'affiliation2publication_'),
    },
    dag=dag
)


def get_author_id(author, connection) -> int:
    params = {}
    params['first_name'] = prepare_string_value(author.get('first_name', ''))
    params['last_name'] = prepare_string_value(author.get('last_name', ''))
    params['first_name_abbr'] = prepare_string_value(author.get('first_name_abbr', ''))
    params['full_name'] = prepare_string_value(author.get('full_name', ''))
    params['h_index_real'] = author['h_index_real']
    query = open('/tmp/data/wh_sql/upsert_author.sql', 'r')
    publication_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    connection.commit()
    query.close()
    return int(publication_id)


def insert_author_publication(publication_author, authors_df, publications_df, connection) -> None:
    author_id = int(publication_author['author_id'])
    author_db_id = int(authors_df.query('author_id == {}'.format(author_id)).iloc[0]['db_id'])
    publication_id = int(publication_author['publication_id'])
    publication_db_id = int(publications_df.query('publication_id == {}'.format(publication_id)).iloc[0]['db_id'])
    params = {}
    params['author_id'] = author_db_id
    params['publication_id'] = publication_db_id
    query = open('/tmp/data/wh_sql/insert_publication_author.sql', 'r')
    connection.cursor().execute(query.read().format(**params))
    connection.commit()
    query.close()


def update_warehouse_authors(prepared_publication_path: Path, authors_path: Path, author_to_publications_path: Path):
    publication_df = pd.read_csv(prepared_publication_path, sep=infer_separator(prepared_publication_path))
    authors_df = pd.read_csv(authors_path, sep=infer_separator(authors_path))
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    author_to_publications_df = pd.read_csv(author_to_publications_path,
                                            sep=infer_separator(author_to_publications_path))
    authors_df['db_id'] = authors_df.apply(lambda author: get_author_id(author, connection), axis=1)
    author_to_publications_df.swifter.apply(
        lambda publication_author: insert_author_publication(publication_author, authors_df, publication_df,
                                                             connection), axis=1)


update_authors_data = PythonOperator(
    task_id='update_authors_data',
    python_callable=update_warehouse_authors,
    op_kwargs={
        'prepared_publication_path': PREPARED_DATA_FILE,
        'authors_path': get_latest_filename(FINAL_DATA_DIR, 'authors_'),
        'author_to_publications_path': get_latest_filename(FINAL_DATA_DIR, 'author2publication_'),
    },
    dag=dag
)


def get_domain_id(domain, connection) -> int:
    params = {}
    params['major_field'] = prepare_string_value(domain.get('major_field', ''))
    params['sub_category'] = prepare_string_value(domain.get('sub_category', ''))
    params['exact_category'] = prepare_string_value(domain.get('exact_category', ''))
    params['arxiv_category'] = prepare_string_value(domain.get('arxiv_category', ''))
    query = open('/tmp/data/wh_sql/upsert_scientific_domain.sql', 'r')
    domain_id = pd.read_sql_query(query.read().format(**params), connection)['id']
    connection.commit()
    query.close()
    return int(domain_id)


def insert_publication_domain(publication_domain, publications_df, connection) -> None:
    domain_db_id = int(publication_domain['domain_db_id'])
    publication_id = int(publication_domain['publication_id'])
    publication_db_id = int(publications_df.query('publication_id == {}'.format(publication_id)).iloc[0]['db_id'])
    params = {}
    params['domain_id'] = domain_db_id
    params['publication_id'] = publication_db_id
    query = open('/tmp/data/wh_sql/insert_publication_domain.sql', 'r')
    connection.cursor().execute(query.read().format(**params))
    connection.commit()
    query.close()


def update_warehouse_domains(prepared_publication_path: Path, publication_domains_path: Path):
    publication_df = pd.read_csv(prepared_publication_path, sep=infer_separator(prepared_publication_path))
    publication_domains_df = pd.read_csv(publication_domains_path, sep=infer_separator(publication_domains_path))
    connection = PostgresHook(postgres_conn_id='citus-warehouse', schema='warehouse').get_conn()
    publication_domains_df['domain_db_id'] = publication_domains_df.swifter.apply(
        lambda domain: get_domain_id(domain, connection), axis=1)
    publication_domains_df.swifter.apply(
        lambda publication_author: insert_publication_domain(publication_author, publication_df, connection), axis=1)


update_domain_data = PythonOperator(
    task_id='update_domain_data',
    python_callable=update_warehouse_domains,
    op_kwargs={
        'prepared_publication_path': PREPARED_DATA_FILE,
        'publication_domains_path': get_latest_filename(FINAL_DATA_DIR, 'publication2domain_')
    },
    dag=dag
)

update_authors_h_index_calculated = PostgresOperator(
    task_id='update_authors_h_index_calculated',
    sql='update warehouse.authors a set h_index_calculated ='
        ' (select count(1) from warehouse.publication_author pa, warehouse.publications p'
        '  where pa.author_id = a.id'
        '  and pa.publication_id = p.id'
        '  and p.snapshot_valid_to is null'
        '  and p.number_of_citations > 0) '
        'where a.valid_to is null;',
    postgres_conn_id='citus-warehouse',
    trigger_rule='none_failed',
    autocommit=True,
    dag=dag
)

update_venues_h_index_calculated = PostgresOperator(
    task_id='update_venues_h_index_calculated',
    sql='update warehouse.publication_venues pv set h_index_calculated = '
        ' (select count(1) from warehouse.publications p'
        '  where p.venue_id = pv.id and p.snapshot_valid_to is null and p.number_of_citations > 0)'
        'where pv.valid_to is null;',
    postgres_conn_id='citus-warehouse',
    trigger_rule='none_failed',
    autocommit=True,
    dag=dag
)


commit_task = EmptyOperator(
        task_id='commit',
        dag=dag
    )


publication_affiliations = [update_authors_data, update_affiliations_data, update_domain_data]
update_indexes = [update_authors_h_index_calculated, update_venues_h_index_calculated]

update_publication >> publication_affiliations >> commit_task >> update_indexes

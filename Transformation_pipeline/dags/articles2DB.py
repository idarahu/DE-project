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

DATA_FOLDER = '/tmp/data'

articles2DB_dag = DAG(
    dag_id='articles2DB', 
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022,10,12,14,0,0),
    schedule_interval=None,
    #schedule_interval=timedelta(minutes=2),
    catchup=False,
    template_searchpath=[DATA_FOLDER]
)

task_start = BashOperator(
    task_id='start',
    bash_command='date'
)

# Creating venues table
def create_venues_table(output_folder):
    with open(f'{output_folder}/venues.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS venues (\n'
            'venue_ID INT PRIMARY KEY,\n'
            'full_name VARCHAR(255),\n'
            'abbreviation VARCHAR(50) UNIQUE,\n'
            'print_issn VARCHAR(50),\n'
            'electronic_issn VARCHAR(50));\n'
        )

prepare_venues_sql = PythonOperator(
    task_id='prepare_venues_sql',
    dag=articles2DB_dag,
    python_callable=create_venues_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_venues_sql = PostgresOperator(
    task_id='create_venues_sql',
    dag=articles2DB_dag,
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
            'title TEXT UNIQUE NOT NULL,\n'
            'date DATE,\n'
            'submitter VARCHAR(255),\n'
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
    dag=articles2DB_dag,
    python_callable=create_publications_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_publications_sql = PostgresOperator(
    task_id='create_publications_sql',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='publications.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating authors table and also a temporary authors table for bulk inserts to check the duplicates
def create_authors_table(output_folder):
    with open(f'{output_folder}/authors.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS authors (\n'
            'author_ID serial PRIMARY KEY,\n'
            'last_name VARCHAR(100) NOT NULL,\n'
            'first_name VARCHAR(100),\n'
            'first_name_abbr VARCHAR(10) NOT NULL,\n'
            'extra VARCHAR(100),\n'
            'position TEXT,\n'
            'h_index_real INT,\n'
            'updated_at TIMESTAMP WITH TIME ZONE,\n'
            'UNIQUE (last_name, first_name_abbr));\n'
        )

prepare_authors_sql = PythonOperator(
    task_id='prepare_authors_sql',
    dag=articles2DB_dag,
    python_callable=create_authors_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_authors_sql = PostgresOperator(
    task_id='create_authors_sql',
    dag=articles2DB_dag,
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
            'last_name VARCHAR(100) NOT NULL,\n'
            'first_name VARCHAR(100),\n'
            'first_name_abbr VARCHAR(10) NOT NULL,\n'
            'extra VARCHAR(100),\n'
            'position TEXT,\n'
            'h_index_real INT,\n'
            'updated_at TIMESTAMP WITH TIME ZONE);\n'
        )

prepare_authors_temp_sql = PythonOperator(
    task_id='prepare_authors_temp_sql',
    dag=articles2DB_dag,
    python_callable=create_authors_temp_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_authors_temp_sql = PostgresOperator(
    task_id='create_authors_temp_sql',
    dag=articles2DB_dag,
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
    dag=articles2DB_dag,
    python_callable=create_author2publication_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_author2publication_sql = PostgresOperator(
    task_id='create_author2publication_sql',
    dag=articles2DB_dag,
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
            'institution_name VARCHAR(255) UNIQUE,\n'
            'institution_place VARCHAR(255));\n'
        )

prepare_affiliations_sql = PythonOperator(
    task_id='prepare_affiliations_sql',
    dag=articles2DB_dag,
    python_callable=create_affiliations_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_affiliations_sql = PostgresOperator(
    task_id='create_affiliations_sql',
    dag=articles2DB_dag,
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
            'author_first_name_abbr VARCHAR(10) NOT NULL);\n'
        )

prepare_affiliations_temp_sql = PythonOperator(
    task_id='prepare_affiliations_temp_sql',
    dag=articles2DB_dag,
    python_callable=create_affiliations_temp_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_affiliations_temp_sql = PostgresOperator(
    task_id='create_affiliations_temp_sql',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliations_temp.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Creating connection tables between affiliations and publications and authors and affiliations
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
    dag=articles2DB_dag,
    python_callable=create_affiliation2publication_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_affiliation2publication_sql = PostgresOperator(
    task_id='create_affiliation2publication_sql',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliation2publication.sql',
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
    dag=articles2DB_dag,
    python_callable=create_author2affiliation_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_author2affiliation_sql = PostgresOperator(
    task_id='create_author2affiliation_sql',
    dag=articles2DB_dag,
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
    dag=articles2DB_dag,
    python_callable=create_arxiv_categories_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_arxiv_categories_sql = PostgresOperator(
    task_id='create_arxiv_categories_sql',
    dag=articles2DB_dag,
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
    dag=articles2DB_dag,
    python_callable=create_publication2arxiv_table,
    op_kwargs={
        'output_folder': DATA_FOLDER
    }
)

create_publication2arxiv_sql = PostgresOperator(
    task_id='create_publication2arxiv_sql',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='publication2arxiv.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

step1 = EmptyOperator(task_id='step1')
step2 = EmptyOperator(task_id='step2') 

step1 >> [prepare_venues_sql, prepare_publications_sql, prepare_authors_sql, prepare_authors_temp_sql, 
       prepare_author2publication_sql, prepare_affiliations_sql, prepare_affiliations_temp_sql, 
       prepare_affiliation2publication_sql, prepare_author2affiliation_sql, prepare_arxiv_categories_sql,
       prepare_publication2arxiv_sql] >> step2

step3 = EmptyOperator(task_id='step3')      
step2 >> [create_authors_sql, create_authors_temp_sql, 
       create_author2publication_sql, create_affiliations_sql, create_affiliations_temp_sql, 
       create_affiliation2publication_sql, create_author2affiliation_sql,
       create_publication2arxiv_sql] >> step3 
step2 >> create_venues_sql >> create_publications_sql >> step3

def csv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    with open(f'{DATA_FOLDER}/{file_name}', 'r') as f:
        next(f)
        curr.copy_from(f, DB_table, sep=',')
        get_postgres_conn.commit()

load_arxiv_category = PythonOperator(
    task_id='load_arxiv_category',
    dag=articles2DB_dag,
    python_callable=csv_to_db,
    op_kwargs={
        'file_name': 'arxiv_categories.csv',
        'DB_table': 'arxiv_categories'
    }
)

truncate_arxiv_table = PostgresOperator(
	task_id='truncate_arxiv_table',
    dag=articles2DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE arxiv_categories"
)

step2 >> create_arxiv_categories_sql >> truncate_arxiv_table >> load_arxiv_category >> step3 

# Choosing the suitable new data (50K batch) - simulating the arrival of new data
def get_file_name():
    with open(f'{DATA_FOLDER}/split_no.txt', 'r') as f:
            old_split_no = f.read()
            new_split_no = int(old_split_no) + 1
            if new_split_no > 44:
                file_name = ''
            else: 
                file_name = 'original_data_split' + str(new_split_no) + '.json'
    with open(f'{DATA_FOLDER}/split_no.txt', 'w') as f2:
            f2.write(str(new_split_no))
    return file_name

def get_metadata():
    import pandas as pd
    import json

    metadata_df = pd.DataFrame(columns=['submitter', 'authors', 'title', 'journal_ref', 'doi', 'date',
                                        'categories', 'no_versions_arxiv', 'date_of_first_version'])
    
    # For testing use data from original_data_split1.json because it contains the DOIs that are needed for enrichment
    file_name = 'original_data_split1.json'
    # Otherwise use the following command
    #file_name = get_file_name()
    
    # For testing to limit the number of publications
    counter = 0

    with open(f'{DATA_FOLDER}/{file_name}', 'r') as f:
        for line in f:
            if counter == 50:
                return metadata_df
            parsed_line = json.loads(line)
            # Only selecting publications that have enough data: DOI or(and) authors + title
            if parsed_line['doi'] != None or (parsed_line['authors_parsed'] != None and parsed_line['title'] != None):
                metadata_df.loc[len(metadata_df.index)] = [parsed_line['submitter'], 
                                                           parsed_line['authors_parsed'], 
                                                           parsed_line['title'],
                                                           parsed_line['journal-ref'],
                                                           parsed_line['doi'],
                                                           datetime.strptime(parsed_line['versions'][-1]['created'], '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d'),
                                                           parsed_line['categories'].split(' '),
                                                           len(parsed_line['versions']),
                                                           datetime.strptime(parsed_line['versions'][0]['created'], '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d')]
            counter += 1
    return metadata_df

# Holding the ID of last publication that was inserted to DB in memory
def get_previous_publication_ID():
    with open(f'{DATA_FOLDER}/publication_ID.txt', 'r') as f:
            old_ID = f.read()
            new_ID = int(old_ID) + 50000
    with open(f'{DATA_FOLDER}/publication_ID.txt', 'w') as f2:
            f2.write(str(new_ID))
    return int(old_ID)

def data_by_author(author):
    from scholarly import scholarly
    from scholarly import ProxyGenerator
    # Setting up a ProxyGenerator object to use free proxies
    # This needs to be done only once per session
    pg = ProxyGenerator()
    pg.FreeProxies()
    scholarly.use_proxy(pg)

    search_query = scholarly.search_author(author)
    author_data = next(search_query)
    full_data = scholarly.fill(author_data, sections=['basics', 'indices'])
    full_name = full_data['name']
    first_name = ' '.join(re.split(' ', full_name)[:-1])
    try:
        h_index_real = int(full_data['hindex'])
    except:
        h_index_real = -1
    affiliation = full_data['affiliation'].split(', ')
    author_position = None   
    institution_place = ''
    institution_name = None
    for elem in affiliation:
        parsed_aff = scholarly.search_org(elem)
        if len(parsed_aff) == 0:
            author_position = elem
        else:
            institution_data = parsed_aff[0]['Organization'].split(', ')
            for index in range(len(institution_data)):
                if index == 0:
                    institution_name = institution_data[index]
                elif index == 1:
                    institution_place = institution_data[index]
                else:
                    institution_place = institution_place + ', ' + institution_data[index]
    
    return first_name, h_index_real, author_position, institution_name, institution_place

def transform_and_enrich_the_data():
    import crossref_commons.retrieval
    import opencitingpy
    from scholarly import scholarly
    from scholarly import ProxyGenerator
    import numpy as np
    from numpy import loadtxt
    import pandas as pd
    import re
    
    # Loading the preselected DOIs that belong to the publications which will be enriched
    # Selection was made by checking that all the domains (fields major_domain + sub_category in DWH) would be covered    
    DOIs_for_enrichment = loadtxt(f'{DATA_FOLDER}/DOIs_for_enrichment.csv', dtype='str', delimiter=',')
    
    arxiv_categories = pd.read_csv(f'{DATA_FOLDER}/arxiv_categories.csv')
    #venues_and_IDs = pd.read_csv(f'{DATA_FOLDER}/venues_and_IDs.csv')

    # Setting up a ProxyGenerator object to use free proxies
    # This needs to be done only once per session
    pg = ProxyGenerator()
    pg.FreeProxies()
    scholarly.use_proxy(pg)

    client = opencitingpy.client.Client()
    
    old_pub_id = get_previous_publication_ID()
    publication_ID =  old_pub_id + 1
    venue_ID = 0

    #venues_df = pd.DataFrame(columns=['venue_ID', 'full_name', 'abbreviation', 'print_issn', 'electronic_issn'])
    venues_df = pd.read_table(f'{DATA_FOLDER}/venues_df.tsv')
    print(venues_df)
    publications_df = pd.DataFrame(columns=['publication_ID', 'venue_ID', 'DOI', 'title', 'date',
                                            'submitter', 'type', 'language', 'page_numbers', 'volume', 'issue', 
                                            'number_of_references', 'number_of_citations', 'no_versions_arxiv',
                                            'date_of_first_version', 'updated_at'])

    authors_df = pd.DataFrame(columns=['publication_ID', 'last_name', 'first_name', 'first_name_abbr',
                                       'extra', 'position', 'h_index_real', 'updated_at'])

    affiliations_df = pd.DataFrame(columns=['publication_ID', 'institution_name', 'institution_place',
                                            'author_last_name', 'author_first_name_abbr'])

    publication2arxiv_df = pd.DataFrame(columns=['publication_ID', 'arxiv_category_ID'])
    
    citing_pub_df = pd.DataFrame(columns=['publication_ID', 'citing_publication_DOI'])

    metadata_df = get_metadata()
    
    for i in range(len(metadata_df)):
        article_DOI = metadata_df.iloc[i]['doi']
        article_title = metadata_df.iloc[i]['title'].replace('\n','')
        language = None
        page_numbers = None
        volume = -1
        issue = -1
        no_references = -1
        no_citations = -1
        venue_name = None
        venue_abbr = None
        print_issn = None
        electronic_issn = None
        institution_name = None
        institution_place = None
        type = None
        h_index_real = -1

        if metadata_df.iloc[i]['journal_ref'] != None:
            try:
                venue_abbr = re.split(r'(^[^\d]+)', metadata_df.iloc[i]['journal_ref'])[1:][0].replace(',', '').rstrip()
            except:
                venue_abbr = None

        authors_temp_df = pd.DataFrame(columns=['publication_ID', 'last_name', 'first_name', 'first_name_abbr',
                                                'extra', 'position', 'h_index_real', 'updated_at'])
        affiliations_temp_df = pd.DataFrame(columns=['publication_ID', 'institution_name', 'institution_place',
                                                     'author_last_name', 'author_first_name_abbr'])
        authors = metadata_df.iloc[i]['authors']
        for author in authors:
            for k in range(len(author)):
                elem = author[k]
                if k == 0:
                    last_name = elem
                if k == 1:
                    first_name_raw = elem
                if k == 2:
                    if elem == '':
                        extra = None
                    elif elem in ['Jr', 'Jr.', 'jr', 'jr.', 'I', 'II', 'III', 'IV', 'V']:
                        extra = elem
                    else:
                        extra = None
                        institution_name = elem
                if k > 2:
                    if len(re.findall('\d+', elem)) == 0:
                        institution_name = elem
            if '.' in first_name_raw:
                if len(first_name_raw) > 10:
                    first_name = first_name_raw
                    if ' ' in first_name_raw:
                        splitted_first = [word[0] for word in first_name_raw.split(' ')]
                        first_name_abbr = '. '.join(splitted_first) + '.'
                else:
                    first_name = None
                    first_name_abbr = first_name_raw
            else:
                first_name = first_name_raw
                if '-' in first_name_raw:
                    splitted_first = [word[0] for word in first_name_raw.split('-')]
                    first_name_abbr = '.-'.join(splitted_first) + '.'
                elif ' ' in first_name_raw:
                    splitted_first = [word[0] for word in first_name_raw.split(' ')]
                    first_name_abbr = '. '.join(splitted_first) + '.'
                else:
                    first_name = None
                    first_name_abbr = first_name_raw
            authors_temp_df.loc[len(authors_temp_df.index)] = [publication_ID, last_name, first_name, 
                                                               first_name_abbr, extra, None, -1, None]
            affiliations_temp_df.loc[len(affiliations_temp_df.index)] = [publication_ID, institution_name, institution_place,
                                                                         last_name, first_name_abbr]                        

        categories = metadata_df.iloc[i]['categories']
        for category in categories:
            category_idx_arxiv = arxiv_categories.index[arxiv_categories['arxiv_category'] == category]
            arxiv_category_ID = arxiv_categories.iloc[category_idx_arxiv[0]]['arxiv_category_ID']
            publication2arxiv_df.loc[len(publication2arxiv_df.index)] = [publication_ID, arxiv_category_ID]

        if article_DOI in DOIs_for_enrichment:        
            crossref_results = crossref_commons.retrieval.get_publication_as_json(article_DOI)
            try:
                type = crossref_results['type']
            except:
                type = None
            try:
                issue = int(crossref_results['issue'])
            except:
                issue = -1
            try:
                volume = int(crossref_results['volume'])
            except:
                volume = -1
            try:
                no_references = int(crossref_results['reference-count'])
            except:
                no_references = -1
            try:
                venue_name = crossref_results['container-title'][0]
            except:
                venue_name = None
            try:
                venue_abbr_new = crossref_results['short-container-title'][0].replace(',', '').rstrip()
            except:
                venue_abbr_new = None
            if venue_abbr_new != None:
                venue_abbr = venue_abbr_new
            try:
                issn_numbers = crossref_results['issn-type']
                
                for issn in issn_numbers:
                    if issn['type'] == 'print':
                        print_issn = issn['value']
                    if issn['type'] == 'electronic':
                        electronic_issn = issn['value']
            except:
                print_issn = None
                electronic_issn = None

            try:
                language = crossref_results['language']
            except:
                language = None
            
            opencitingpy_meta = client.get_metadata(article_DOI)
            authors_openc = opencitingpy_meta[0].author
            if venue_name == None:
                venue_name = opencitingpy_meta[0].source_title
            if volume == -1:
                try:
                    volume = int(opencitingpy_meta[0].volume)
                except:
                    volume = -1
            if issue == -1:
                try:
                    issue = int(opencitingpy_meta[0].issue)
                except:
                    issue = -1
            page_numbers = opencitingpy_meta[0].page
            no_citations = client.get_citation_count(article_DOI)

            cititing_articles = opencitingpy_meta[0].citation
            citing_pub_df.loc[len(citing_pub_df.index)] = [1, cititing_articles]

            for author in authors_openc:
                # Scholarly has limited times for receiving the data
                #try:
                #    first_name, h_index_real, author_position, institution_name, institution_place = data_by_author(author)
                #except:
                #    h_index_real = -1
                #    author_position = None
                h_index_real = -1
                author_position = None

                name_splitted = author.split(', ')
                last_name_control = name_splitted[0]
                first_name_control = name_splitted[1]
                if '-' in first_name_control:
                    splitted_first_control = [word[0] for word in first_name_control.split('-')]
                    first_name_abbr_control = '.-'.join(splitted_first_control) + '.'
                elif ' ' in first_name_control:
                    splitted_first_control = [word[0] for word in first_name_control.split(' ')]
                    first_name_abbr_control = '. '.join(splitted_first_control) + '.'
                else:
                    first_name_abbr_control = first_name_control

                author_control_index = authors_temp_df.loc[(authors_temp_df['last_name'].str.lower() == last_name_control.lower()) & (authors_temp_df['first_name_abbr'].str.lower() == first_name_abbr_control.lower())].index
                if len(author_control_index) > 0:
                    author_index = author_control_index[0]
                    if authors_temp_df.iloc[author_index]['first_name'] == None:
                        authors_temp_df.loc[author_index, 'first_name'] = first_name
                    if authors_temp_df.iloc[author_index]['h_index_real'] == -1:
                        authors_temp_df.loc[author_index, 'h_index_real'] = h_index_real
                    if authors_temp_df.iloc[author_index]['position'] == None:
                        authors_temp_df.loc[author_index, 'position'] = author_position
                else:
                    authors_temp_df.loc[len(authors_temp_df.index)] = [publication_ID, last_name, first_name, first_name_abbr,
                                                                       None, author_position, h_index_real, None]
                try:
                    affiliation_control_index = affiliations_temp_df.loc[(affiliations_temp_df['institution_name'].str.lower() == institution_name.lower())].index
                except:
                    affiliation_control_index = []
                if len(affiliation_control_index) > 0:
                    affiliation_index = affiliation_control_index[0]
                    if affiliations_temp_df.iloc[affiliation_index]['institution_place'] == None:
                        affiliations_temp_df.loc[affiliation_index, 'institution_place'] = institution_place
                else:
                    affiliations_temp_df.loc[len(affiliations_temp_df.index)] = [publication_ID, institution_name, institution_place,
                                                                                 last_name, first_name_abbr]       

        if venue_abbr != None:
            venue_control_index = venues_df.loc[(venues_df['abbreviation'].str.lower() == venue_abbr.lower())].index
            if len(venue_control_index) > 0:
                venue_index = venue_control_index[0]
                if venues_df.iloc[venue_index]['full_name'] == None:
                    venues_df.loc[venue_index, 'full_name'] = venue_name
                if venues_df.iloc[venue_index]['print_issn'] == None:
                    venues_df.loc[venue_index, 'print_issn'] = print_issn
                if venues_df.iloc[venue_index]['electronic_issn'] == None:
                    venues_df.loc[venue_index, 'electronic_issn'] = electronic_issn
            else:
                venue_ID = len(venues_df.index) + 1
                venues_df.loc[len(venues_df.index)] = [int(venue_ID), venue_name, venue_abbr, print_issn, electronic_issn]
        
        
        publications_df.loc[len(publications_df.index)] = [publication_ID, int(venue_ID), article_DOI, article_title, 
                                                           metadata_df.iloc[i]['date'], metadata_df.iloc[i]['submitter'],
                                                           type, language, page_numbers, volume, issue, no_references, 
                                                           no_citations, metadata_df.iloc[i]['no_versions_arxiv'],
                                                           metadata_df.iloc[i]['date_of_first_version'], None]
        
        authors_df = pd.concat([authors_df, authors_temp_df], ignore_index=True)
        affiliations_df = pd.concat([affiliations_df, affiliations_temp_df], ignore_index=True)

        publication_ID += 1
    
    publications_df['updated_at'] = datetime.now(timezone.utc)
    authors_df['updated_at'] = datetime.now(timezone.utc)
    
    authors_df.applymap(lambda x: None if x == ' ' else x)
    authors_df.applymap(lambda x: None if x == '' else x)

    venues_df.applymap(lambda x: None if x == ' ' else x)
    venues_df.applymap(lambda x: None if x == '' else x)

    affiliations_df.applymap(lambda x: None if x == ' ' else x)
    affiliations_df.applymap(lambda x: None if x == '' else x)

    publication2arxiv_df.applymap(lambda x: None if x == ' ' else x)
    publication2arxiv_df.applymap(lambda x: None if x == '' else x)

    publications_df.applymap(lambda x: None if x == ' ' else x)
    publications_df.applymap(lambda x: None if x == '' else x)

    venues_df.to_csv(f'{DATA_FOLDER}/venues_df.tsv', sep="\t", index=False)
    publications_df.to_csv(f'{DATA_FOLDER}/publications_df.tsv', sep="\t", index=False)
    authors_df.to_csv(f'{DATA_FOLDER}/authors_df.tsv', sep="\t", index=False)
    affiliations_df.to_csv(f'{DATA_FOLDER}/affiliations_df.tsv', sep="\t", index=False)
    publication2arxiv_df.to_csv(f'{DATA_FOLDER}/publication2arxiv_df.tsv', sep="\t", index=False)
    citing_pub_df.to_csv(f'{DATA_FOLDER}/citing_pub_df{old_pub_id}.tsv', sep="\t", index=False)
    #venues_and_IDs.to_csv(f'{DATA_FOLDER}/venues_and_IDs.csv', index=False)


transform_the_data = PythonOperator(
    task_id='transform_the_data',
    dag=articles2DB_dag,
    python_callable=transform_and_enrich_the_data
)

step3 >> transform_the_data

def tsv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    with open(f'{DATA_FOLDER}/{file_name}', 'r') as f:
        next(f)
        curr.copy_from(f, DB_table, sep='\t')
        get_postgres_conn.commit()

load_venues_data = PythonOperator(
    task_id='load_venues_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'venues_df.tsv',
        'DB_table': 'venues'
    }
)

load_publications_data = PythonOperator(
    task_id='load_publications_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'publications_df.tsv',
        'DB_table': 'publications'
    }
)

load_authors_data = PythonOperator(
    task_id='load_authors_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'authors_df.tsv',
        'DB_table': 'authors_temp'
    }
)


load_affiliations_data = PythonOperator(
    task_id='load_affiliations_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'affiliations_df.tsv',
        'DB_table': 'affiliations_temp'
    }
)

load_publication2arxiv_data = PythonOperator(
    task_id='load_publication2arxiv_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'publication2arxiv_df.tsv',
        'DB_table': 'publication2arxiv'
    }
)

truncate_venues_table = PostgresOperator(
	task_id='truncate_venues_table',
    dag=articles2DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE venues"
)

truncate_authors_temp_table = PostgresOperator(
	task_id='truncate_authors_temp_table',
    dag=articles2DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE authors_temp"
)

truncate_affiliations_temp_table = PostgresOperator(
	task_id='truncate_affiliations_temp_table',
    dag=articles2DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE affiliations_temp"
)

authors_temp2authors = PostgresOperator(
    task_id='authors_temp2authors',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='authors_temp2authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

affiliations_temp2affiliations = PostgresOperator(
    task_id='affiliations_temp2affiliations',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliations_temp2affiliations.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

step4 = EmptyOperator(task_id='step4') 

transform_the_data >>  load_publication2arxiv_data >> step4
transform_the_data >> truncate_venues_table >> load_venues_data >> load_publications_data >> step4
transform_the_data >> truncate_authors_temp_table >> load_authors_data >> step4
transform_the_data >> truncate_affiliations_temp_table >> load_affiliations_data >> step4 

step4 >> authors_temp2authors
step4 >> affiliations_temp2affiliations
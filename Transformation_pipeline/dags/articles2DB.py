import datetime
import io
import os
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

INPUT_FOLDER = '/tmp/data/inputs'
SETUP_FOLDER = '/tmp/data/setups'
LOOKUP_DATA_FOLDER = '/tmp/data/lookup_tables'
DATA2DB_FOLDER = '/tmp/data/data2db'
SQL_FOLDER = '/tmp/data/sql'
FINAL_DATA_FOLDER = '/tmp/data/final_data'

articles2DB_dag = DAG(
    dag_id='articles2DB', 
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022,10,12,14,0,0),
    schedule_interval=None,
    #schedule_interval=timedelta(minutes=2),
    catchup=False,
    template_searchpath=[SQL_FOLDER]
)

# Choosing the suitable new data (50K batch) - simulating the arrival of new data
def get_file_name():
    with open(f'{SETUP_FOLDER}/split_no.txt', 'r') as f:
            old_split_no = f.read()
            new_split_no = int(old_split_no) + 1
            if new_split_no > 44:
                file_name = ''
            else: 
                file_name = 'original_data_split' + str(new_split_no) + '.json'
    with open(f'{SETUP_FOLDER}/split_no.txt', 'w') as f2:
            f2.write(str(new_split_no))
    return file_name

def get_metadata():
    import pandas as pd
    import json

    metadata_df = pd.DataFrame(columns=['submitter', 'authors', 'title', 'journal_ref', 'doi', 'date',
                                        'categories', 'no_versions_arxiv', 'date_of_first_version'])
    
    # For testing use data from original_data_split1.json because it contains the DOIs that are needed for enrichment
    #file_name = 'original_data_split1.json'
    # Otherwise use the following command
    file_name = get_file_name()
    
    # For testing to limit the number of publications
    counter = 0

    with open(f'{INPUT_FOLDER}/{file_name}', 'r') as f:
        for line in f:
            if counter == 100:
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
    with open(f'{SETUP_FOLDER}/publication_ID.txt', 'r') as f:
            old_ID = f.read()
            new_ID = int(old_ID) + 50000
    with open(f'{SETUP_FOLDER}/publication_ID.txt', 'w') as f2:
            f2.write(str(new_ID))
    return int(old_ID)

def find_venue(venue_data_raw):
    import pandas as pd
    import re

    venues_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/venues_lookup.tsv')
    venue_data_check = re.sub(r'\W', '', venue_data_raw).lower()
    venue_abbr = None
    venue_name = None
    found_venue = venues_lookup.loc[(venues_lookup['abbrev_check'] == venue_data_check) | (venues_lookup['full_check'] == venue_data_check)]
    if len(found_venue) > 0:
        venue_abbr = found_venue.iloc[0]['abbrev.dots']
        venue_name = found_venue.iloc[0]['full']
    
    return venue_abbr, venue_name 

def check_first_name_raw(first_name_raw_to_check):
    first_name_raw = first_name_raw_to_check.strip()
    if first_name_raw.endswith('-'):
        new_first_name_raw = list(filter(None, re.split('[. ]', first_name_raw))) 
        real_first_char = new_first_name_raw[-1] 
        if real_first_char == '-':
            first_first_name = new_first_name_raw[-2] + real_first_char
            last_first_name = ' '.join(new_first_name_raw[:-2])
            first_name_raw = first_first_name + last_first_name
        else:
            first_first_name = real_first_char
            last_first_name = ' '.join(new_first_name_raw[:-1])
            first_name_raw = first_first_name + last_first_name
    return first_name_raw

def parse_first_name(first_name_raw_to_parse):
    import re
    first_name_raw = check_first_name_raw(first_name_raw_to_parse)
    if '-' in first_name_raw:
        splitted_first = [word[0] for word in list(filter(None, first_name_raw.split('-')))]
        control_for_full_name = max([len(word) for word in re.split('[. -]', first_name_raw)])
        if control_for_full_name > 1:
            first_name = first_name_raw
        else:
            first_name = None    
        first_name_abbr = ('.-'.join(splitted_first) + '.').upper()
    elif first_name_raw in ['', ' ', None]:
        first_name_abbr = None
        first_name = None
    else:
        splitted_first = [word[0] for word in list(filter(None, first_name_raw.split(' ')))]
        control_for_full_name = max([len(word) for word in re.split('[. -]', first_name_raw)])
        if control_for_full_name > 1:
            first_name = first_name_raw
        else:
            first_name = None    
        first_name_abbr = ('. '.join(splitted_first) + '.').upper()
    return first_name, first_name_abbr

def find_institution_information(institution_name_raw):
    import pandas as pd
    import re

    universities_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/universities_lookup.tsv')
    cities_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/cities_lookup.tsv')
    splitted_institution_name = re.sub(r'\s', '', institution_name_raw).split(',')
    institution_name = None
    institution_place = None
    memory = None
    for split in splitted_institution_name:
        if universities_lookup['institution'].str.lower().str.contains(split.lower()).any():
            memory = split.lower()

    if memory != None:
        for idx, institution in enumerate(universities_lookup['institution']):
            institution_splitted = re.split('[-,]', institution)[0]
            if memory in institution_splitted.lower():
                institution_name = institution_splitted
                institution_place = universities_lookup.iloc[idx]['country']
            if memory in institution.lower() and memory not in institution_splitted.lower():
                institution_name = institution
                institution_place = universities_lookup.iloc[idx]['country']

    if institution_place == None:
        memory2 = None
        for split in splitted_institution_name:
            if cities_lookup['city'].str.lower().str.contains(split.lower()).any() or cities_lookup['city_ascii'].str.lower().str.contains(split.lower()).any() or cities_lookup['country'].str.lower().str.contains(split.lower()).any() :
                memory2 = split.lower()

        if memory2 != None:
            for i in range(len(cities_lookup)):
                if memory2 in cities_lookup.iloc[i]['city'].lower() or memory2 in cities_lookup.iloc[i]['city_ascii'].lower() or memory2 in cities_lookup.iloc[i]['country'].lower():
                    institution_place = cities_lookup.iloc[i]['country']
    return institution_name, institution_place

def data_by_author(author):
    from scholarly import scholarly
    from scholarly import ProxyGenerator
    # Setting up a ProxyGenerator object to use free proxies
    # This needs to be done only once per session
    pg = ProxyGenerator()
    pg.FreeProxies()
    scholarly.use_proxy(pg)
    try:
        search_query = scholarly.search_author(author)
        author_data = next(search_query)
        full_data = scholarly.fill(author_data, sections=['basics', 'indices'])
        full_name = full_data['name']
        first_name, first_name_abbr = parse_first_name(' '.join(re.split(' ', full_name)[:-1]))
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
    except:
        return None, -1, None, None, None

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
    DOIs_for_enrichment = loadtxt(f'{SETUP_FOLDER}/DOIs_for_enrichment.csv', dtype='str', delimiter=',')
    
    arxiv_categories = pd.read_csv(f'{DATA2DB_FOLDER}/arxiv_categories.csv')

    # Setting up a ProxyGenerator object to use free proxies
    # This needs to be done only once per session
    pg = ProxyGenerator()
    pg.FreeProxies()
    scholarly.use_proxy(pg)

    client = opencitingpy.client.Client()
    
    old_pub_id = get_previous_publication_ID()
    publication_ID =  old_pub_id + 1

    venues_df = pd.read_table(f'{DATA2DB_FOLDER}/venues_df.tsv')

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
        venue_data_raw = None
        venue_abbr_new = None
        print_issn = None
        electronic_issn = None
        institution_name = None
        institution_place = None
        type = None
        h_index_real = -1
        venue_ID = 0
        
        if metadata_df.iloc[i]['journal_ref'] != None:
            try:
                venue_data_raw = re.split(r'(^[^\d]+)', metadata_df.iloc[i]['journal_ref'])[1:][0].replace(',', '').rstrip()
            except:
                venue_data_raw = None
        if venue_data_raw != None:
            venue_abbr, venue_name = find_venue(venue_data_raw)

        authors_temp_df = pd.DataFrame(columns=['publication_ID', 'last_name', 'first_name', 'first_name_abbr',
                                                'extra', 'position', 'h_index_real', 'updated_at'])
        affiliations_temp_df = pd.DataFrame(columns=['publication_ID', 'institution_name', 'institution_place',
                                                     'author_last_name', 'author_first_name_abbr'])
        authors = metadata_df.iloc[i]['authors']
        for author in authors:
            first_name = None
            last_name = None
            first_name_abbr = None
            extra = None
            institution_name_raw = None
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
                        if elem != '':
                            institution_name_raw = elem
                if k > 2:
                    if len(re.findall('\d+', elem)) == 0:
                        if elem != '':
                            institution_name_raw = elem
            if institution_name_raw != None:
                try:
                    institution_name, institution_place = find_institution_information(institution_name_raw)
                except:
                    institution_name = None
                    institution_place = None
            try:
                first_name, first_name_abbr = parse_first_name(first_name_raw)
            except:
                first_name = None
                first_name_abbr = None
            authors_temp_df.loc[len(authors_temp_df.index)] = [int(publication_ID), last_name, first_name, 
                                                               first_name_abbr, extra, None, -1, None]
            affiliations_temp_df.loc[len(affiliations_temp_df.index)] = [int(publication_ID), institution_name, institution_place,
                                                                         last_name, first_name_abbr]                        

        categories = metadata_df.iloc[i]['categories']
        for category in categories:
            category_idx_arxiv = arxiv_categories.index[arxiv_categories['arxiv_category'] == category]
            arxiv_category_ID = arxiv_categories.iloc[category_idx_arxiv[0]]['arxiv_category_ID']
            publication2arxiv_df.loc[len(publication2arxiv_df.index)] = [int(publication_ID), int(arxiv_category_ID)]

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
            venue_name_new = opencitingpy_meta[0].source_title
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
            citing_pub_df.loc[len(citing_pub_df.index)] = [int(publication_ID), cititing_articles]

            for author in authors_openc:
                first_name = None
                last_name = None
                first_name_abbr = None
                institution_name = None
                institution_place = None
                # Scholarly has limited times for receiving the data
                #try:
                #    first_name, h_index_real, author_position, institution_name, institution_place = data_by_author(author)
                #except:
                #    h_index_real = -1
                #    author_position = None
                h_index_real = -1
                author_position = None

                name_splitted = author.split(', ')
                if len(name_splitted) == 2:
                    last_name_control = name_splitted[0]
                    first_name_control, first_name_abbr_control = parse_first_name(name_splitted[1])
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
                        if first_name != None:
                            authors_temp_df.loc[len(authors_temp_df.index)] = [int(publication_ID), last_name_control, first_name, 
                                                                            first_name_abbr_control, None, author_position, 
                                                                            int(h_index_real), None]
                        else:
                            authors_temp_df.loc[len(authors_temp_df.index)] = [int(publication_ID), last_name_control, first_name_control, 
                                                                            first_name_abbr_control, None, author_position, 
                                                                            int(h_index_real), None]

                try:
                    affiliation_control_index = affiliations_temp_df.loc[(affiliations_temp_df['institution_name'].str.lower() == institution_name.lower())].index
                except:
                    affiliation_control_index = []
                if len(affiliation_control_index) > 0:
                    affiliation_index = affiliation_control_index[0]
                    if affiliations_temp_df.iloc[affiliation_index]['institution_place'] == None:
                        affiliations_temp_df.loc[affiliation_index, 'institution_place'] = institution_place
                else:
                    affiliations_temp_df.loc[len(affiliations_temp_df.index)] = [int(publication_ID), institution_name, institution_place,
                                                                                 last_name, first_name_abbr]       
        
        if venue_abbr == None and venue_abbr_new != None:
            venue_abbr_try, venue_name_try = find_venue(venue_abbr_new)
            if venue_abbr_try == None:
                venue_abbr = venue_abbr_new
                venue_name = venue_name_new
            else:
                venue_abbr = venue_abbr_try
                venue_name = venue_name_try

        if venue_abbr != None:
            venue_control_index = venues_df.loc[(venues_df['abbreviation'].str.lower() == venue_abbr.lower())].index
            if len(venue_control_index) > 0:
                venue_index = venue_control_index[0]
                venue_ID = venues_df.iloc[venue_index]['venue_ID'] 
                if venues_df.iloc[venue_index]['full_name'] == None:
                    venues_df.loc[venue_index, 'full_name'] = venue_name
                if venues_df.iloc[venue_index]['print_issn'] == None:
                    venues_df.loc[venue_index, 'print_issn'] = print_issn
                if venues_df.iloc[venue_index]['electronic_issn'] == None:
                    venues_df.loc[venue_index, 'electronic_issn'] = electronic_issn
            else:
                venue_ID = len(venues_df.index) + 1
                venues_df.loc[len(venues_df.index)] = [int(venue_ID), venue_name, venue_abbr, print_issn, electronic_issn]
        
        
        publications_df.loc[len(publications_df.index)] = [int(publication_ID), int(venue_ID), article_DOI, article_title, 
                                                           metadata_df.iloc[i]['date'], metadata_df.iloc[i]['submitter'],
                                                           type, language, page_numbers, int(volume), int(issue), int(no_references), 
                                                           int(no_citations), metadata_df.iloc[i]['no_versions_arxiv'],
                                                           metadata_df.iloc[i]['date_of_first_version'], None]
        
        authors_df = pd.concat([authors_df, authors_temp_df], ignore_index=True)
        affiliations_df = pd.concat([affiliations_df, affiliations_temp_df], ignore_index=True)

        publication_ID += 1
    
    publications_df['updated_at'] = datetime.now(timezone.utc)
    authors_df['updated_at'] = datetime.now(timezone.utc)
    
    authors_df.applymap(lambda x: None if x == ' ' else x)
    authors_df.applymap(lambda x: None if x == '' else x)
    authors_df[['publication_ID', 'h_index_real']] = authors_df[['publication_ID', 'h_index_real']].applymap(np.int64)

    venues_df.applymap(lambda x: None if x == ' ' else x)
    venues_df.applymap(lambda x: None if x == '' else x)
    venues_df[['venue_ID']] = venues_df[['venue_ID']].applymap(np.int64)

    affiliations_df.applymap(lambda x: None if x == ' ' else x)
    affiliations_df.applymap(lambda x: None if x == '' else x)
    affiliations_df[['publication_ID']] = affiliations_df[['publication_ID']].applymap(np.int64)

    publication2arxiv_df.applymap(lambda x: None if x == ' ' else x)
    publication2arxiv_df.applymap(lambda x: None if x == '' else x)
    publication2arxiv_df[['publication_ID', 'arxiv_category_ID']] = publication2arxiv_df[['publication_ID', 'arxiv_category_ID']].applymap(np.int64)

    publications_df.applymap(lambda x: None if x == ' ' else x)
    publications_df.applymap(lambda x: None if x == '' else x)
    columns = ['publication_ID', 'venue_ID', 'volume', 'issue', 'number_of_references', 'number_of_citations', 'no_versions_arxiv']
    publications_df[columns] = publications_df[columns].applymap(np.int64)

    venues_df.to_csv(f'{DATA2DB_FOLDER}/venues_df.tsv', sep="\t", index=False)
    publications_df.to_csv(f'{DATA2DB_FOLDER}/publications_df.tsv', sep="\t", index=False)
    authors_df.to_csv(f'{DATA2DB_FOLDER}/authors_df.tsv', sep="\t", index=False)
    affiliations_df.to_csv(f'{DATA2DB_FOLDER}/affiliations_df.tsv', sep="\t", index=False)
    publication2arxiv_df.to_csv(f'{DATA2DB_FOLDER}/publication2arxiv_df.tsv', sep="\t", index=False)
    
    if os.path.exists(f'{FINAL_DATA_FOLDER}/citing_pub.tsv') == True:
        citing_pub_df.to_csv(f'{FINAL_DATA_FOLDER}/citing_pub.tsv', sep="\t", index=False, header=False, mode="a")
    else:
        citing_pub_df.to_csv(f'{FINAL_DATA_FOLDER}/citing_pub.tsv', sep="\t", index=False)

transform_the_data = PythonOperator(
    task_id='transform_the_data',
    dag=articles2DB_dag,
    python_callable=transform_and_enrich_the_data
)

def tsv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    with open(f'{DATA2DB_FOLDER}/{file_name}', 'r') as f:
        next(f)
        curr.copy_from(f, DB_table, sep='\t')
        get_postgres_conn.commit()

# Deleting data from venues, temporary authors table and temporary affiliations table - these tables are always populated with new data
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

# Populating the DB tables with new data
# Venues data to venues table
load_venues_data = PythonOperator(
    task_id='load_venues_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'venues_df.tsv',
        'DB_table': 'venues'
    }
)

# Publications data to publications table
load_publications_data = PythonOperator(
    task_id='load_publications_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'publications_df.tsv',
        'DB_table': 'publications'
    }
)
# Authors data to temporary authors table - for checking the duplicates
load_authors_data = PythonOperator(
    task_id='load_authors_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'authors_df.tsv',
        'DB_table': 'authors_temp'
    }
)

# Affiliations data to temporary affiliations table - for checking the duplicates
load_affiliations_data = PythonOperator(
    task_id='load_affiliations_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'affiliations_df.tsv',
        'DB_table': 'affiliations_temp'
    }
)

# Data about publications arXiv categories to publication2arxiv table
load_publication2arxiv_data = PythonOperator(
    task_id='load_publication2arxiv_data',
    dag=articles2DB_dag,
    python_callable=tsv_to_db,
    op_kwargs={
        'file_name': 'publication2arxiv_df.tsv',
        'DB_table': 'publication2arxiv'
    }
)

# Populating the authors table only with new data
authors_temp2authors = PostgresOperator(
    task_id='authors_temp2authors',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='authors_temp2authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Populating the affiliations table only with new data
affiliations_temp2affiliations = PostgresOperator(
    task_id='affiliations_temp2affiliations',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliations_temp2affiliations.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Connecting all the tables that need to be connected in DB
# Authors with publications
connect_author2pub = PostgresOperator(
    task_id='connect_author2pub',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='connect_author2pub.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Affiliations with publications
connect_aff2pub = PostgresOperator(
    task_id='connect_aff2pub',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='connect_aff2pub.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

# Authors with affiliations
connect_author2aff = PostgresOperator(
    task_id='connect_author2aff',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='connect_author2aff.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

connector1 = EmptyOperator(task_id='connector1') 
transform_the_data >>  load_publication2arxiv_data >> connector1
transform_the_data >> truncate_venues_table >> load_venues_data >> load_publications_data >> connector1
transform_the_data >> truncate_authors_temp_table >> load_authors_data >> authors_temp2authors >> connector1
transform_the_data >> truncate_affiliations_temp_table >> load_affiliations_data >> affiliations_temp2affiliations >> connector1

[load_publications_data, authors_temp2authors] >> connect_author2pub >> connector1
[load_publications_data, affiliations_temp2affiliations] >> connect_aff2pub >> connector1
[authors_temp2authors, affiliations_temp2affiliations] >> connect_author2aff >> connector1

create_authors_view = PostgresOperator(
    task_id='create_authors_view',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='authors_view.sql',
    trigger_rule='none_failed',
    autocommit=True,
)        

create_venues_view = PostgresOperator(
    task_id='create_venues_view',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='venues_view.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

connector2 = EmptyOperator(task_id='connector2') 
connector1 >> [create_authors_view, create_venues_view] >> connector2 

def copy_data_from_DB(output_folder, SQL_statement, data_type):
    import pandas as pd
    conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    df = pd.read_sql(SQL_statement, conn)
    if data_type == 'publication2arxiv':
        file_name = 'publication2domain_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'
        arxiv_categories = pd.read_csv(f'{DATA2DB_FOLDER}/arxiv_categories.csv')
        domains_lookup = pd.read_csv(f'{LOOKUP_DATA_FOLDER}/lookup_table_domains.csv')
        connected_domains_data = pd.merge(left=arxiv_categories, how='outer', left_on='arxiv_category', right=domains_lookup, right_on='arxiv_category')
        final_df = pd.merge(left=connected_domains_data, how='outer', left_on='arxiv_category_ID', right=df, right_on='arxiv_category_id')
        final_df_print = final_df[['publication_id', 'domain_id', 'major_field', 'sub_category', 'exact_category', 'arxiv_category']]
        final_df_print.to_csv(f'{output_folder}/{file_name}', index=False)
    else:
        file_name = data_type + '_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'    
        df.to_csv(f'{output_folder}/{file_name}', index=False)

copy_affiliations = PythonOperator(
    task_id='copy_affiliations',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM affiliations',
        'data_type': 'affiliations'
    }
)

copy_authors = PythonOperator(
    task_id='copy_authors',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM authors_view',
        'data_type': 'authors'
    }
)

copy_affiliation2publication = PythonOperator(
    task_id='copy_affiliation2publication',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM affiliation2publication',
        'data_type': 'affiliation2publication'
    }
)

copy_author2affiliation = PythonOperator(
    task_id='copy_author2affiliation',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM author2affiliation',
        'data_type': 'author2affiliation'
    }
)

copy_author2publication = PythonOperator(
    task_id='copy_author2publication',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM author2publication',
        'data_type': 'author2publication'
    }
)

copy_publications = PythonOperator(
    task_id='copy_publications',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM publications',
        'data_type': 'publications'
    }
)

copy_publication2arxiv = PythonOperator(
    task_id='copy_publication2arxiv',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM publication2arxiv',
        'data_type': 'publication2arxiv'
    }
)

connector3 = EmptyOperator(task_id='connector3') 
connector2 >> [copy_affiliations, copy_authors, copy_affiliation2publication, copy_author2affiliation, 
          copy_author2publication, copy_publications, copy_publication2arxiv] >> connector3
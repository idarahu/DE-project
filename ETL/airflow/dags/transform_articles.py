"""
This DAG pulls the raw data (reads in the raw JSON file with 50K publications) and then transforms and enriches it.
After that, it loads all the data into the up-to-date database (DB) and generates the needed views for querying.
Finally, the DAG generates the CSV files (based on the data in the DB) needed for populating the DWH and graph DB with data.
"""

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
METADATA_FOLDER = '/tmp/data/metadata'
LOOKUP_DATA_FOLDER = '/tmp/data/lookup_tables'
DATA2DB_FOLDER = '/tmp/data/data2db'
SQL_FOLDER = '/tmp/data/sql'
FINAL_DATA_FOLDER = '/tmp/data/final_data'

articles2DB_dag = DAG(
    dag_id='transform_articles',
    description='Transforming the raw data into the up-to-date DB with enriched fields',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2022,10,12,14,0,0),
    schedule_interval=None,
    #schedule_interval=timedelta(minutes=2),
    catchup=False,
    template_searchpath=[SQL_FOLDER],
    tags=['transform']
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

def get_first_and_last_date_from_versions(versions) -> (datetime, datetime):
    long_format = '%a, %d %b %Y %H:%M:%S %Z'
    short_format = '%Y-%m-%d'

    dates = list(
        sorted(
            map(lambda y: datetime.strptime(y['created'], long_format),
                versions)))

    if len(dates) > 1:
        return dates[0].strftime(short_format), dates[-1].strftime(short_format)
    else:
        return dates[0].strftime(short_format), dates[0].strftime(short_format)

# Holding the ID of last publication that was inserted to DB in memory
def get_previous_publication_ID():
    with open(f'{SETUP_FOLDER}/publication_ID.txt', 'r') as f:
            old_ID = f.read()
            new_ID = int(old_ID) + 50000
    with open(f'{SETUP_FOLDER}/publication_ID.txt', 'w') as f2:
            f2.write(str(new_ID))
    return int(old_ID)


def get_metadata():
    import pandas as pd

    file_name = get_file_name()
    df = pd.read_json(f'{INPUT_FOLDER}/{file_name}', lines=True)
    df = df[['submitter', 'authors_parsed', 'title', 'journal-ref', 'doi', 'versions', 'categories']]
    df = df.rename(columns={
        'authors_parsed': 'authors',
        'journal-ref': 'journal_ref',
    })

    df['categories'] = df['categories'].str.split(' ')
    df['versions'] = df['versions'].apply(get_first_and_last_date_from_versions)
    df['no_versions_arxiv'] = df['versions'].apply(lambda x: len(x))
    df['doi'] = df['doi'].apply(lambda x: x.split(' ')[0] if x is not None else '')
    df['date_of_first_version'] = df['versions'].apply(lambda x: x[0])
    df['date'] = df['versions'].apply(lambda x: x[1])
    df = df.drop(columns=['versions'])
    df = df.reset_index()
    df = df.rename(columns={"index":"publication_ID"})
    df = df.reset_index(drop=True)
    df['publication_ID'] = df.index + get_previous_publication_ID() + 1
    df.to_csv(f'{METADATA_FOLDER}/metadata_df.tsv', sep="\t", index=False)

convert_metadata_for_ingestion = PythonOperator(
    task_id='convert_metadata_for_ingestion',
    dag=articles2DB_dag,
    python_callable=get_metadata
)

def parse_venue(x):
    import re
    try:
        return re.split(r'(^[^\d]+)', x)[1:][0].replace(',', '').rstrip()
    except:
        return None

def find_venue(venue_data_raw, venues_lookup):
    import pandas as pd
    import re
    import numpy as np
    try:
        venue_data_check = re.sub(r'\W', '', venue_data_raw).lower()
        found_venue1 = venues_lookup[['abbrev.dots', 'full']].to_numpy()[venues_lookup['abbrev_check'].to_numpy() == venue_data_check].tolist()
        found_venue2 = venues_lookup[['abbrev.dots', 'full']].to_numpy()[venues_lookup['full_check'].to_numpy() == venue_data_check].tolist()
        if len(found_venue1) > 0:
            return [found_venue1[0][0], found_venue1[0][1]]
        elif len(found_venue2) > 0:
            return [found_venue2[0][0], found_venue2[0][1]]
        else:
            return [None, None]
    except:
        return [None, None]

def do_crossref_call(DOI, DOIs_for_enrichment):
    import crossref_commons.retrieval
    if DOI in DOIs_for_enrichment:
        type, language, issue, volume, no_references, venue_name, venue_abbr_new, print_issn, electronic_issn = None, None, -1, -1, -1, None, None, None, None 
        crossref_results = crossref_commons.retrieval.get_publication_as_json(DOI)
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
        return [type, language, issue, volume, no_references, venue_name, venue_abbr_new, print_issn, electronic_issn]
    else:
        return [None, None, -1, -1, -1, None, None, None, None]

def do_opencitation_call(DOI, DOIs_for_enrichment):
    import opencitingpy
    if DOI in DOIs_for_enrichment:
        client = opencitingpy.client.Client()
        opencitingpy_meta = client.get_metadata(DOI)
        venue_name_open = opencitingpy_meta[0].source_title
        try:
            volume_open = int(opencitingpy_meta[0].volume)
        except:
            volume_open = -1
        try:
            issue_open = int(opencitingpy_meta[0].issue)
        except:
            issue_open = -1
        page_numbers = opencitingpy_meta[0].page
        no_citations = client.get_citation_count(DOI)
        cititing_articles = opencitingpy_meta[0].citation
        return [venue_name_open, volume_open, issue_open, page_numbers, no_citations, cititing_articles]
    else:
        return [None, -1, -1, None, -1, None]

def check_API_venues(venue_name_cross, venue_abbr_cross, venue_name_open, venues_lookup):
    final_abbr = None
    final_name = None
    if venue_abbr_cross != None:
        list_abbr = find_venue(venue_abbr_cross, venues_lookup)
        if any(elem is None for elem in list_abbr) == False:
            return list_abbr
        else:
            final_abbr = venue_abbr_cross
    if venue_name_cross != None:
        list_name_cross = find_venue(venue_name_cross, venues_lookup)
        if any(elem is None for elem in list_name_cross) == False:
            return list_name_cross
        else:
            final_name = venue_name_cross
    if final_name == None and venue_name_open != None:
        list_name_open = find_venue(venue_name_open, venues_lookup)
        if any(elem is None for elem in list_name_open) == False:
            return list_name_open
    return [final_abbr, final_name]
    

def get_venues_and_publications_data():
    import numpy as np
    from numpy import loadtxt
    import pandas as pd
    
    # Loading the preselected DOIs that belong to the publications which will be enriched
    # Selection was made by checking that all the domains (fields major_domain + sub_category in DWH) would be covered    
    DOIs_for_enrichment = loadtxt(f'{SETUP_FOLDER}/DOIs_for_enrichment.csv', dtype='str', delimiter=',')
    venues_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/venues_lookup.tsv')

    venues_df = pd.read_table(f'{DATA2DB_FOLDER}/venues_df.tsv')
    if len(venues_df) == 0:
        max_venue_ID = 0
    else:
        max_venue_ID = int(max(venues_df['venue_ID']))

    metadata_df = pd.read_table(f'{METADATA_FOLDER}/metadata_df.tsv')
    metadata_df['title'] = metadata_df['title'].apply(lambda x: x.replace('\n','').strip())
    metadata_df['venue_data_raw'] = metadata_df['journal_ref'].apply(lambda x: parse_venue(x))
    metadata_df[['venue_abbr', 'venue_name']] = metadata_df.apply(lambda x: find_venue(x.venue_data_raw, venues_lookup) if x.venue_data_raw is not None else [None, None], axis = 1, result_type='expand')
    crossref_columns = ['type', 'language', 'issue_cross', 'volume_cross', 'number_of_references', 'venue_name_cross', 'venue_abbr_cross', 'print_issn_pub', 'electronic_issn_pub']
    metadata_df[crossref_columns] = metadata_df.apply(lambda x: do_crossref_call(x.doi, DOIs_for_enrichment), axis = 1, result_type='expand') 
    opencitation_columns = ['venue_name_open', 'volume_open', 'issue_open', 'page_numbers', 'number_of_citations', 'citing_publication_DOI']
    metadata_df[opencitation_columns] = metadata_df.apply(lambda x: do_opencitation_call(x.doi, DOIs_for_enrichment), axis = 1, result_type='expand')
    metadata_df['issue'] = metadata_df.apply(lambda x: x.issue_cross if x.issue_cross != -1 else x.issue_open, axis=1)
    metadata_df['volume'] = metadata_df.apply(lambda x: x.volume_cross if x.volume_cross != -1 else x.volume_open, axis=1)
    metadata_df[['venue_abbr', 'venue_name']] = metadata_df.apply(lambda x: check_API_venues(x.venue_name_cross, x.venue_abbr_cross, x.venue_name_open, venues_lookup) if x.venue_abbr is None else [x.venue_abbr, x.venue_name], axis = 1, result_type='expand')
    merged_df = pd.merge(left=metadata_df, left_on=['venue_abbr', 'venue_name'], right=venues_df, right_on=['abbreviation', 'full_name'], how='outer')
    merged_df.abbreviation.fillna(merged_df.venue_abbr, inplace=True)
    merged_df.full_name.fillna(merged_df.venue_name, inplace=True)
    merged_df.print_issn.fillna(merged_df.print_issn_pub, inplace=True)
    merged_df.electronic_issn.fillna(merged_df.electronic_issn_pub, inplace=True)
    merged_df.venue_ID.fillna(0, inplace=True)
    merged_df = merged_df.sort_values(['abbreviation', 'full_name']).reset_index(drop=True)
    
    previous_name = ''
    previous_abbr = ''
    new_venue_ID = max_venue_ID + 1
    for i in range(len(merged_df)):
        name = merged_df.iloc[i]['full_name']
        abbr = merged_df.iloc[i]['abbreviation']
        if merged_df.iloc[i]['venue_ID'] == 0 and abbr != None:
            if previous_abbr == abbr:
                if previous_name == name:
                    merged_df.loc[i, 'venue_ID'] = new_venue_ID
                    previous_name = previous_name
                    previous_abbr= previous_abbr
                if previous_name != name:
                    new_venue_ID += 1
                    merged_df.loc[i, 'venue_ID'] = new_venue_ID
                    previous_name = name
                    previous_abbr = previous_abbr    
            if previous_abbr != abbr:
                new_venue_ID += 1
                merged_df.loc[i,'venue_ID'] = new_venue_ID
                previous_abbr = abbr
                previous_name = name
    
    mask = merged_df['full_name'].isnull() & merged_df['abbreviation'].isnull()
    indexes_mask = merged_df[mask].index
    merged_df.loc[indexes_mask, 'venue_ID'] = 0

    publications_df = merged_df[['publication_ID', 'venue_ID', 'doi', 'title', 'date', 'submitter', 'type', 'language', 'page_numbers', 'volume', 'issue',
                                 'number_of_references', 'number_of_citations', 'no_versions_arxiv', 'date_of_first_version']]
    publications_df.dropna(axis=0, how='all',inplace=True)
    publications_df['updated_at'] = datetime.now(timezone.utc)
    publications_df.applymap(lambda x: None if x == ' ' else x)
    publications_df.applymap(lambda x: None if x == '' else x)
    columns = ['publication_ID', 'venue_ID', 'volume', 'issue', 'number_of_references', 'number_of_citations', 'no_versions_arxiv']
    publications_df[columns] = publications_df[columns].fillna(-1)
    publications_df = publications_df[publications_df.publication_ID != -1]
    publications_df[columns] = publications_df[columns].applymap(np.int64)
    publications_df = publications_df.drop_duplicates()

    venues_df = merged_df[['venue_ID', 'full_name', 'abbreviation', 'print_issn', 'electronic_issn']]
    venues_df.dropna(axis=0, how='all',inplace=True)
    venues_df = venues_df[venues_df.venue_ID != 0]
    venues_df.applymap(lambda x: None if x == ' ' else x)
    venues_df.applymap(lambda x: None if x == '' else x)
    venues_df[['venue_ID']] = venues_df[['venue_ID']].applymap(np.int64)
    venues_df = venues_df.drop_duplicates()
    venues_df = venues_df[venues_df.full_name.notnull() | venues_df.abbreviation.notnull()]
    venues_df = venues_df.sort_values(['venue_ID', 'print_issn', 'electronic_issn']).reset_index(drop=True)
    m = (venues_df['print_issn'].isnull() & venues_df['electronic_issn'].isnull()) & venues_df['venue_ID'].duplicated()
    indexes_drop = venues_df[m].index
    venues_df = venues_df.drop(indexes_drop, axis=0).reset_index(drop=True)
    venues_df = venues_df.drop_duplicates(subset='venue_ID', keep='first').reset_index(drop=True)

    venues_df.to_csv(f'{DATA2DB_FOLDER}/venues_df.tsv', sep="\t", index=False)
    publications_df.to_csv(f'{DATA2DB_FOLDER}/publications_df.tsv', sep="\t", index=False)

    citing_pub_df = merged_df[['publication_ID', 'citing_publication_DOI']]
    citing_pub_df.dropna(axis=0, how='all',inplace=True)
    citing_pub_df[['publication_ID']] = citing_pub_df[['publication_ID']].applymap(np.int64)
    if os.path.exists(f'{FINAL_DATA_FOLDER}/citing_pub.tsv') == True:
        citing_pub_df.to_csv(f'{FINAL_DATA_FOLDER}/citing_pub.tsv', sep="\t", index=False, header=False, mode="a")
    else:
        citing_pub_df.to_csv(f'{FINAL_DATA_FOLDER}/citing_pub.tsv', sep="\t", index=False)

transform_venues_and_publications_data = PythonOperator(
    task_id='transform_venues_and_publications_data',
    dag=articles2DB_dag,
    python_callable=get_venues_and_publications_data
)

convert_metadata_for_ingestion >> transform_venues_and_publications_data

def get_arxiv_category_data():
    import numpy as np
    import pandas as pd
    from ast import literal_eval

    arxiv_categories = pd.read_csv(f'{DATA2DB_FOLDER}/arxiv_categories.csv')
    metadata_df = pd.read_table(f'{METADATA_FOLDER}/metadata_df.tsv')
    metadata_df.drop(['submitter', 'authors', 'title', 'journal_ref', 'doi', 'no_versions_arxiv', 'date_of_first_version','date'], axis=1, inplace=True)
    metadata_df['categories'] = metadata_df['categories'].apply(literal_eval)
    metadata_df = metadata_df.explode('categories')
    merged_df = pd.merge(left=metadata_df, left_on='categories', right=arxiv_categories, right_on='arxiv_category', how='left')
    publication2arxiv_df = merged_df[['publication_ID', 'arxiv_category_ID']]
    publication2arxiv_df.applymap(lambda x: None if x == ' ' else x)
    publication2arxiv_df.applymap(lambda x: None if x == '' else x)
    publication2arxiv_df[['publication_ID', 'arxiv_category_ID']] = publication2arxiv_df[['publication_ID', 'arxiv_category_ID']].applymap(np.int64)
    publication2arxiv_df.to_csv(f'{DATA2DB_FOLDER}/publication2arxiv_df.tsv', sep="\t", index=False)

transform_arxiv_data = PythonOperator(
    task_id='transform_arxiv_data',
    dag=articles2DB_dag,
    python_callable=get_arxiv_category_data
)

convert_metadata_for_ingestion >> transform_arxiv_data

def check_first_name_raw(first_name_raw_to_check):
    import re
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

def extra_or_affiliation(value1, value2):
    if value1.upper() in ['JR', 'JR.', 'I', 'II', 'III', 'IV', 'V']:
        return [value1, value2]
    else:
        return [None, value1]

def find_institution_information(institution_name_raw, universities_lookup, cities_lookup):
    import pandas as pd
    import re
    try:
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
        return [institution_name, institution_place]
    except:
        return [None, None]

def do_scholarly_call(author):
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
        try:
            h_index_real = int(full_data['hindex'])
        except:
            h_index_real = -1
        affiliation = full_data['affiliation'].split(', ')
        author_position = None   
        for elem in affiliation:
            parsed_aff = scholarly.search_org(elem)
            if len(parsed_aff) == 0:
                author_position = elem
        
        return [h_index_real, author_position]
    except:
        return [-1, None]

def get_authors_and_affiliations_data():
    import numpy as np
    import pandas as pd
    from ast import literal_eval

    universities_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/universities_lookup.tsv')
    cities_lookup = pd.read_table(f'{LOOKUP_DATA_FOLDER}/cities_lookup.tsv')

    metadata_df = pd.read_table(f'{METADATA_FOLDER}/metadata_df.tsv')
    metadata_df.drop(['submitter', 'title', 'journal_ref', 'doi', 'no_versions_arxiv', 'date_of_first_version','date', 'categories'], axis=1, inplace=True)
    metadata_df['authors'] = metadata_df['authors'].apply(literal_eval)
    metadata_df = metadata_df.explode('authors')
    metadata_df['authors'] = metadata_df['authors'].apply(lambda x: ','.join(x))
    metadata_df = pd.concat([metadata_df['publication_ID'], metadata_df['authors'].str.split(',', expand=True)], axis=1)
    metadata_df = metadata_df.iloc[:, 0:5]
    metadata_df = metadata_df.rename(columns={0: 'last_name', 1: 'first_name_raw', 2: 'raw1', 3: 'raw2'})
    metadata_df[['first_name', 'first_name_abbr']] = metadata_df.apply(lambda x: parse_first_name(x.first_name_raw) if x.first_name_raw is not None else [None, None], axis=1, result_type='expand')
    metadata_df[['extra', 'institution_name_raw']] = metadata_df.apply(lambda x: extra_or_affiliation(x.raw1, x.raw2) if x.raw1 != '' else [None, None], axis = 1, result_type='expand')
    metadata_df[['institution_name', 'institution_place']] = metadata_df.apply(lambda x: find_institution_information(x.institution_name_raw, universities_lookup, cities_lookup) if x.institution_name_raw is not None else [None, None], axis=1, result_type='expand')
    # Scholarly has limited times for receiving the data - therefore the following code line is commented in and the line after that is used.
    # Also this step is very time-consuming and out of this project's scope.
    # However, if one wants to use Scholarly, the following line shoud be commented out and the line after commented in.
    #metadata_df[['h_index_real', 'position']] = metadata_df.apply(lambda x: do_scholarly_call(x.last_name + ', ' + x.first_name_abbr), axis=1, result_type='expand')
    metadata_df[['h_index_real', 'position']] = [-1, None]
    metadata_df[['publication_ID', 'h_index_real']] = metadata_df[['publication_ID', 'h_index_real']].fillna(-1)
    metadata_df = metadata_df[metadata_df.publication_ID != -1]
    authors_df = metadata_df[['publication_ID', 'last_name', 'first_name', 'first_name_abbr', 'extra', 'position', 'h_index_real']]
    authors_df.dropna(axis=0, how='all',inplace=True)
    authors_df['updated_at'] = datetime.now(timezone.utc)
    authors_df = authors_df[authors_df['first_name_abbr'].isnull() == False]
    authors_df.applymap(lambda x: None if x == ' ' else x)
    authors_df.applymap(lambda x: None if x == '' else x)
    authors_df[['publication_ID', 'h_index_real']] = authors_df[['publication_ID', 'h_index_real']].applymap(np.int64)
    authors_df = authors_df.drop_duplicates()
    
    affiliations_df = metadata_df[['publication_ID', 'institution_name', 'institution_place', 'last_name', 'first_name_abbr']]
    affiliations_df = affiliations_df.rename(columns={'last_name': 'author_last_name', 'first_name_abbr': 'author_first_name_abbr'})
    affiliations_df = affiliations_df[(affiliations_df.institution_name.notnull()) | (affiliations_df.institution_place.notnull())]
    affiliations_df.applymap(lambda x: None if x == ' ' else x)
    affiliations_df.applymap(lambda x: None if x == '' else x)
    affiliations_df[['publication_ID']] = affiliations_df[['publication_ID']].applymap(np.int64)
    affiliations_df.drop_duplicates()

    authors_df.to_csv(f'{DATA2DB_FOLDER}/authors_df.tsv', sep="\t", index=False)
    affiliations_df.to_csv(f'{DATA2DB_FOLDER}/affiliations_df.tsv', sep="\t", index=False)

transform_authors_and_affiliations_data = PythonOperator(
    task_id='transform_authors_and_affiliations_data',
    dag=articles2DB_dag,
    python_callable=get_authors_and_affiliations_data
)

convert_metadata_for_ingestion >> transform_authors_and_affiliations_data
connector1 = EmptyOperator(task_id='connector1')
transform_venues_and_publications_data >> connector1
transform_arxiv_data >> connector1
transform_authors_and_affiliations_data >> connector1

def tsv_to_db(file_name, DB_table):
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    if DB_table == 'publications_temp':
        with open(f'{DATA2DB_FOLDER}/{file_name}', 'r') as f:
            next(f)
            curr.copy_expert("COPY publications_temp FROM STDIN CSV DELIMITER '\t' ESCAPE '\r';", f)
            get_postgres_conn.commit()
    elif DB_table == 'authors_temp':
        with open(f'{DATA2DB_FOLDER}/{file_name}', 'r') as f:
            next(f)
            curr.copy_expert("COPY authors_temp FROM STDIN CSV DELIMITER '\t';", f)
            get_postgres_conn.commit()
    else:
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

truncate_publications_temp_table = PostgresOperator(
	task_id='truncate_publications_temp_table',
    dag=articles2DB_dag,
	postgres_conn_id='airflow_pg',
	sql="TRUNCATE publications_temp"
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

truncate_affiliation2publication_temp_table = PostgresOperator(
	task_id='truncate_affiliation2publication_temp_table',
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
        'DB_table': 'publications_temp'
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

# Populating the publications table only with new data
publications_temp2publications = PostgresOperator(
    task_id='publications_temp2publications',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='publications_temp2publications.sql',
    trigger_rule='none_failed',
    autocommit=True,
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
# Populating the affilitation2publication table only with unique values
affiliation2publication_temp2affiliation2publication = PostgresOperator(
    task_id='affiliation2publication_temp2affiliation2publication',
    dag=articles2DB_dag,
    postgres_conn_id='airflow_pg',
    sql='affiliation2publication_temp2affiliation2publication.sql',
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

connector2 = EmptyOperator(task_id='connector2') 
connector1 >>  load_publication2arxiv_data >> connector2
connector1>> truncate_venues_table >> load_venues_data >> load_publications_data >> connector2
connector1 >> truncate_publications_temp_table >> load_publications_data >> publications_temp2publications >> connector2
connector1 >> truncate_authors_temp_table >> load_authors_data >> authors_temp2authors >> connector2
connector1 >> truncate_affiliations_temp_table >> load_affiliations_data >> affiliations_temp2affiliations >> connector2

[load_publications_data, authors_temp2authors] >> connect_author2pub >> connector2
[load_publications_data, affiliations_temp2affiliations] >> connect_aff2pub
connect_aff2pub >> truncate_affiliation2publication_temp_table >> affiliation2publication_temp2affiliation2publication >> connector2
[authors_temp2authors, affiliations_temp2affiliations] >> connect_author2aff >> connector2

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

connector3 = EmptyOperator(task_id='connector3') 
connector2 >> [create_authors_view, create_venues_view] >> connector3 

def copy_data_from_DB(output_folder, SQL_statement, data_type):
    import pandas as pd
    import numpy as np
    conn = PostgresHook(postgres_conn_id='airflow_pg').get_conn()
    df = pd.read_sql(SQL_statement, conn)
    if data_type == 'publication2arxiv':
        file_name = 'publication2domain_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'
        arxiv_categories = pd.read_csv(f'{DATA2DB_FOLDER}/arxiv_categories.csv')
        domains_lookup = pd.read_csv(f'{LOOKUP_DATA_FOLDER}/lookup_table_domains.csv')
        connected_domains_data = pd.merge(left=arxiv_categories, how='outer', left_on='arxiv_category', right=domains_lookup, right_on='arxiv_category')
        final_df = pd.merge(left=connected_domains_data, how='right', left_on='arxiv_category_ID', right=df, right_on='arxiv_category_id')
        final_df_print = final_df[['publication_id', 'domain_id', 'major_field', 'sub_category', 'exact_category', 'arxiv_category']]
        final_df_print[['publication_id', 'domain_id']] = final_df_print[['publication_id', 'domain_id']].applymap(np.int64)
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

copy_venues = PythonOperator(
    task_id='copy_venues',
    dag=articles2DB_dag,
    python_callable=copy_data_from_DB,
    op_kwargs={
        'output_folder': FINAL_DATA_FOLDER,
        'SQL_statement': 'SELECT * FROM venues_view',
        'data_type': 'venues'
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

connector4 = EmptyOperator(task_id='connector4') 
connector3 >> [copy_affiliations, copy_authors, copy_venues, copy_affiliation2publication, 
               copy_author2affiliation, copy_author2publication, copy_publications, 
               copy_publication2arxiv] >> connector4

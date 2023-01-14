"""
This DAG prepares header CSVs and content CSVs for neo4j graph injection.
"""

import itertools
from ast import literal_eval
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Lib

def get_latest_filename(folder_path: Path, prefix: str) -> Optional[Path]:
    files = sorted(folder_path.glob(f'{prefix}*'))
    print(files)
    return files[-1] if files else None


def save_str_to_file(data: str, file_path: Union[Path, str]) -> None:
    with open(file_path, 'w') as f:
        f.write(data)


def save_df_to_file(
        df: pd.DataFrame,
        file_path: Union[Path, str],
        header: bool = False,
        columns: Optional[Iterable] = None,
) -> None:
    df.to_csv(file_path, index=False, header=header, columns=columns if columns else df.columns)


def infer_separator(file_path: Path) -> str:
    return '\t' if file_path.suffix == '.tsv' else ','


def convert_column_to_type(df: pd.DataFrame, column: str, type_):
    df[column] = df[column].astype(type_)
    return df


def convert_columns_to_type(df: pd.DataFrame, columns: dict):
    for column, type_ in columns.items():
        df[column] = df[column].astype(type_)
    return df


def process_venue_entities(venues_path: Path, output_dir: Path) -> None:
    df = pd.read_csv(venues_path, sep=infer_separator(venues_path))

    df = df[['venue_ID', 'full_name']]

    df['venue_ID'] = df['venue_ID'].astype(int)

    df.rename(columns={
        'venue_ID': 'venue_id:ID(Venue-ID)',
    }, inplace=True)

    df[':LABEL'] = 'Venue'

    header_path = output_dir / 'venues_header.csv'
    content_path = output_dir / 'venues.csv'

    header = 'venue_id:ID(Venue-ID),full_name,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'venue_id:ID(Venue-ID)',
        'full_name',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_entities(authors_path: Path, output_dir: Path):
    df = pd.read_csv(authors_path, sep=infer_separator(authors_path))

    df = df[['author_id', 'full_name', 'h_index_calculated']]

    df['author_id'] = df['author_id'].astype(int)
    df['h_index_calculated'] = df['h_index_calculated'].astype(int)

    df.rename(columns={
        'author_id': 'author_id:ID(Author-ID)',
        'h_index_calculated': 'h_index_calculated:int',
    }, inplace=True)

    df[':LABEL'] = 'Author'

    header_path = output_dir / 'authors_header.csv'
    content_path = output_dir / 'authors.csv'

    header = 'author_id:ID(Author-ID),full_name,h_index_calculated:int,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'author_id:ID(Author-ID)',
        'full_name',
        'h_index_calculated:int',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_entities(affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(affiliations_path, sep=infer_separator(affiliations_path))

    df = df[['affiliation_id', 'institution_name', 'institution_place']]

    df['affiliation_id'] = df['affiliation_id'].astype(int)

    df.rename(columns={
        'affiliation_id': 'affiliation_id:ID(Affiliation-ID)',
        'institution_name': 'name',
        'institution_place': 'place',
    }, inplace=True)

    df[':LABEL'] = 'Affiliation'

    header_path = output_dir / 'affiliations_header.csv'
    content_path = output_dir / 'affiliations.csv'

    header = 'affiliation_id:ID(Affiliation-ID),name,place,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'affiliation_id:ID(Affiliation-ID)',
        'name',
        'place',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_entities(publications_path: Path, venues_path: Path, output_dir: Path):
    df = pd.read_csv(publications_path, sep=infer_separator(publications_path))

    df = df[['publication_id', 'venue_id', 'title', 'doi', 'date']]
    df = convert_columns_to_type(df, {'publication_id': int, 'venue_id': int})

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['date'] = df['date'].dt.strftime('%Y')

    venues_df = pd.read_csv(venues_path, sep=infer_separator(venues_path))[['venue_ID', 'full_name']]
    venues_df = convert_columns_to_type(venues_df, {'venue_ID': int})

    df = df.merge(venues_df, left_on='venue_id', right_on='venue_ID', how='left')

    df = df.drop_duplicates(subset=['publication_id'])

    df.rename(columns={
        'publication_id': 'publication_id:ID(Publication-ID)',
        'full_name': 'venue',
        'date': 'year:int',
    }, inplace=True)

    df[':LABEL'] = 'Publication'

    header_path = output_dir / 'publications_header.csv'
    content_path = output_dir / 'publications.csv'

    header = 'publication_id:ID(Publication-ID),title,doi,year:int,venue,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'publication_id:ID(Publication-ID)',
        'title',
        'doi',
        'year:int',
        'venue',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_scientific_domain_entities(domains_path: Path, output_dir: Path):
    df = pd.read_csv(domains_path, sep=infer_separator(domains_path))

    df = df[['arxiv_category', 'major_field', 'sub_category', 'exact_category']]

    df.rename(columns={
        'arxiv_category': 'arxiv_category:ID(Arxiv-Category-ID)',
    }, inplace=True)

    df[':LABEL'] = 'ScientificDomain'

    # TODO: we may want to reconsider dropping duplicates and create multiple references instead
    df = df.drop_duplicates(subset=['arxiv_category:ID(Arxiv-Category-ID)'])

    header_path = output_dir / 'domains_header.csv'
    content_path = output_dir / 'domains.csv'

    header = 'arxiv_category:ID(Arxiv-Category-ID),major_field,sub_category,exact_category,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'arxiv_category:ID(Arxiv-Category-ID)',
        'major_field',
        'sub_category',
        'exact_category',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_of_relationships(author_to_publications_path: Path, output_dir: Path):
    df = pd.read_csv(author_to_publications_path, sep=infer_separator(author_to_publications_path))

    df = df[['author_id', 'publication_id']]

    df = df.rename(columns={
        'author_id': ':START_ID(Author-ID)',
        'publication_id': ':END_ID(Publication-ID)',
    })

    df[':TYPE'] = 'AUTHOR_OF'

    header_path = output_dir / 'author_of_header.csv'
    content_path = output_dir / 'author_of.csv'

    header = ':START_ID(Author-ID),:END_ID(Publication-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Publication-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_collaborates_with_relationships(author_to_publications_path: Path, output_dir: Path):
    df = pd.read_csv(author_to_publications_path, sep=infer_separator(author_to_publications_path))

    df = df[['author_id', 'publication_id']]

    records = []

    for publication_id, publication_df in df.groupby('publication_id'):
        author_ids = publication_df['author_id'].unique()
        if len(author_ids) < 2:
            continue
        for author_id_1, author_id_2 in itertools.combinations(author_ids, 2):
            records.append({
                'author_id_1': author_id_1,
                'author_id_2': author_id_2,
            })

    result = pd.DataFrame.from_records(records)

    result = result.rename(columns={
        'author_id_1': ':START_ID(Author-ID)',
        'author_id_2': ':END_ID(Author-ID)',
    })

    result[':TYPE'] = 'COLLABORATES_WITH'

    header_path = output_dir / 'author_collaborates_with_header.csv'
    content_path = output_dir / 'author_collaborates_with.csv'

    header = ':START_ID(Author-ID),:END_ID(Author-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Author-ID)',
        ':TYPE',
    ]
    save_df_to_file(result, content_path, columns=columns)


def process_author_works_in_relationships(author_to_affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(author_to_affiliations_path, sep=infer_separator(author_to_affiliations_path))

    df = df[['author_id', 'affiliation_id']]

    df = df.rename(columns={
        'author_id': ':START_ID(Author-ID)',
        'affiliation_id': ':END_ID(Affiliation-ID)',
    })

    df[':TYPE'] = 'WORKS_IN'

    header_path = output_dir / 'works_at_header.csv'
    content_path = output_dir / 'works_at.csv'

    header = ':START_ID(Author-ID),:END_ID(Affiliation-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Affiliation-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_published_in_relationships(publications_path: Path, output_dir: Path):
    df = pd.read_csv(publications_path, sep=infer_separator(publications_path))

    df = df[['publication_id', 'venue_id']]

    df = df[df['venue_id'] != 0]

    df = df.rename(columns={
        'publication_id': ':START_ID(Publication-ID)',
        'venue_id': ':END_ID(Venue-ID)',
    })

    df[':TYPE'] = 'PUBLISHED_IN'

    header_path = output_dir / 'published_in_header.csv'
    content_path = output_dir / 'published_in.csv'

    header = ':START_ID(Publication-ID),:END_ID(Venue-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Venue-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_belongs_to_domain_relationships(
        publications_to_domains_path: Path,
        # publications_path: Path,
        # arxiv_categories_path: Path,
        # domains_path: Path,
        output_dir: Path
):
    # publication_ID, arxiv_category_ID
    pub_to_domains_df = pd.read_csv(publications_to_domains_path, sep=infer_separator(publications_to_domains_path))

    # arxiv_category_ID, arxiv_category
    # arxiv_categories_df = pd.read_csv(arxiv_categories_path, sep=infer_separator(arxiv_categories_path))
    # domain_id, major_field, sub_category, exact_category, arxiv_category
    # domains_df = pd.read_csv(domains_path, sep=infer_separator(domains_path))
    # publication_id
    # pub_df = pd.read_csv(publications_path, sep=infer_separator(publications_path))[['publication_id']]
    # pub_df = convert_column_to_type(pub_df, 'publication_id', int)

    # pub_to_domains_df = pub_to_domains_df.merge(arxiv_categories_df, on='arxiv_category_ID')
    # pub_to_domains_df = pub_to_domains_df.merge(domains_df, on='arxiv_category')

    # pub_to_domains_df = pub_to_domains_df.rename(columns={'publication_ID': 'publication_id'})
    pub_to_domains_df = pub_to_domains_df[['publication_id', 'arxiv_category']]
    pub_to_domains_df = pub_to_domains_df.dropna(subset=['publication_id'])

    pub_to_domains_df = convert_column_to_type(pub_to_domains_df, 'publication_id', int)

    # Removing publications with IDs that are not in the publications.csv file, i.e., not in the database
    # pub_to_domains_df = pub_to_domains_df.merge(pub_df, on='publication_id')

    pub_to_domains_df = pub_to_domains_df.drop_duplicates(subset=['publication_id'])

    pub_to_domains_df = pub_to_domains_df.rename(columns={
        'publication_id': ':START_ID(Publication-ID)',
        'arxiv_category': ':END_ID(Arxiv-Category-ID)',
    })

    pub_to_domains_df[':TYPE'] = 'BELONGS_TO'

    header_path = output_dir / 'belongs_to_header.csv'
    content_path = output_dir / 'belongs_to.csv'

    header = ':START_ID(Publication-ID),:END_ID(Arxiv-Category-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Arxiv-Category-ID)',
        ':TYPE',
    ]
    save_df_to_file(pub_to_domains_df, content_path, columns=columns)


def process_publication_cited_by_relationships(citations_path: Path, publications_path: Path, output_dir: Path):
    # publication_ID, citing_publication_DOI (array)
    df = pd.read_csv(citations_path, sep=infer_separator(citations_path))

    df = df.dropna(subset=['citing_publication_DOI'])

    df['citing_publication_DOI'] = df['citing_publication_DOI'].apply(literal_eval)
    df = df.explode('citing_publication_DOI')

    pub_df = pd.read_csv(publications_path, sep=infer_separator(publications_path))[['publication_id', 'doi']]

    # Removing publications that are not in the publications.csv file
    df = df.merge(pub_df, left_on='citing_publication_DOI', right_on='doi')

    df = df[['publication_ID', 'publication_id']]

    df = df.rename(columns={
        'publication_ID': ':START_ID(Publication-ID)',
        'publication_id': ':END_ID(Publication-ID)',
    })

    df[':TYPE'] = 'CITED_BY'

    header_path = output_dir / 'cited_by_header.csv'
    content_path = output_dir / 'cited_by.csv'

    header = ':START_ID(Publication-ID),:END_ID(Publication-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Publication-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_covers_scientific_domain_relationships(
        publications_to_affiliations_path: Path,
        publications_to_domains_path: Path,
        output_dir: Path
):
    # publication_id, affiliation_id
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path))
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_id', 'affiliation_id']]
    publications_to_affiliations_df = convert_columns_to_type(
        publications_to_affiliations_df, {'publication_id': int, 'affiliation_id': int})

    # publication_ID, arxiv_category_ID
    publications_to_domains_df = pd.read_csv(
        publications_to_domains_path, sep=infer_separator(publications_to_domains_path))
    publications_to_domains_df = publications_to_domains_df.dropna(subset=['publication_id'])
    publications_to_domains_df = convert_columns_to_type(publications_to_domains_df, {'publication_id': int})

    publications_to_affiliations_df = publications_to_affiliations_df.merge(
        publications_to_domains_df, left_on='publication_id', right_on='publication_id')

    df = publications_to_affiliations_df[['affiliation_id', 'arxiv_category']]
    df = df.drop_duplicates(['affiliation_id', 'arxiv_category'])

    df = df.rename(columns={
        'affiliation_id': ':START_ID(Affiliation-ID)',
        'arxiv_category': ':END_ID(Arxiv-Category-ID)',
    })

    df[':TYPE'] = 'COVERS'

    header_path = output_dir / 'covers_header.csv'
    content_path = output_dir / 'covers.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Arxiv-Category-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Arxiv-Category-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_collaborates_with_relationships(
        author_to_publications_path: Path,
        publications_to_affiliations_path: Path,
        output_dir: Path
):
    # author_id, publication_id
    author_to_publications_df = pd.read_csv(
        author_to_publications_path, sep=infer_separator(author_to_publications_path))
    author_to_publications_df = author_to_publications_df[['author_id', 'publication_id']]

    author_to_publications_df = convert_columns_to_type(
        author_to_publications_df, {'author_id': int, 'publication_id': int})

    # publication_id, affiliation_id
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path))
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_id', 'affiliation_id']]

    publications_to_affiliations_df = convert_columns_to_type(
        publications_to_affiliations_df, {'publication_id': int, 'affiliation_id': int})

    # author_id, publication_id, affiliation_id
    df = author_to_publications_df.merge(publications_to_affiliations_df, on='publication_id')

    collaboration_df = pd.DataFrame(columns=[':START_ID(Affiliation-ID)', ':END_ID(Affiliation-ID)', ':TYPE'])

    for _, group in df.groupby('publication_id'):
        collaborators = group['affiliation_id'].unique()
        ids_permutations = list(itertools.permutations(collaborators, 2))
        rows = [[start, end, 'COLLABORATES_WITH'] for start, end in ids_permutations]
        collaboration_df = pd.concat([collaboration_df, pd.DataFrame(rows, columns=collaboration_df.columns)])

    collaboration_df = collaboration_df.drop_duplicates()

    header_path = output_dir / 'affiliation_collaborates_with_header.csv'
    content_path = output_dir / 'affiliation_collaborates_with.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Affiliation-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Affiliation-ID)',
        ':TYPE',
    ]
    save_df_to_file(collaboration_df, content_path, columns=columns)


def process_affiliation_publishes_in_relationships(
        publications_to_affiliations_path: Path,
        publications_path: Path,
        output_dir: Path
):
    # publication_id, affiliation_id
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path))
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_id', 'affiliation_id']]
    publications_to_affiliations_df = convert_columns_to_type(
        publications_to_affiliations_df, {'publication_id': int, 'affiliation_id': int})

    # publication_id, venue_id
    publications_df = pd.read_csv(publications_path, sep=infer_separator(publications_path))
    publications_df = publications_df[['publication_id', 'venue_id']]
    publications_df = convert_columns_to_type(publications_df, {'publication_id': int, 'venue_id': int})

    # publication_id, affiliation_id, venue_id
    df = publications_to_affiliations_df.merge(publications_df, on='publication_id')

    # affiliation_id, venue_id
    df = df[['affiliation_id', 'venue_id']]
    df = df.drop_duplicates(['affiliation_id', 'venue_id'])

    df = df[df['venue_id'] != 0]

    df = df.rename(columns={
        'affiliation_id': ':START_ID(Affiliation-ID)',
        'venue_id': ':END_ID(Venue-ID)',
    })

    df[':TYPE'] = 'PUBLISHES_IN'

    header_path = output_dir / 'affiliation_publishes_in_header.csv'
    content_path = output_dir / 'affiliation_publishes_in.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Venue-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Venue-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


# DAG

input_base_dir = Path('/tmp/data/')
db_data_dir = input_base_dir / 'data2db'
final_data_dir = input_base_dir / 'final_data'
lookup_data_dir = input_base_dir / 'lookup_tables'

output_dir = Path('/tmp/neo4j_import/')
output_dir.mkdir(exist_ok=True, parents=True)

default_args = {
    'owner': 'Ihar',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='transform_for_graph_injection',
    default_args=default_args,
    description='Prepare the graph injection CSVs',
    schedule_interval=None,
    start_date=days_ago(2),
)

prepare_venue_entities = PythonOperator(
    task_id='prepare_venue_entities',
    python_callable=process_venue_entities,
    op_kwargs={
        'venues_path': db_data_dir / 'venues_df.tsv',
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_author_entities = PythonOperator(
    task_id='prepare_author_entities',
    python_callable=process_author_entities,
    op_kwargs={
        'authors_path': get_latest_filename(final_data_dir, 'authors_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_affiliation_entities = PythonOperator(
    task_id='prepare_affiliation_entities',
    python_callable=process_affiliation_entities,
    op_kwargs={
        'affiliations_path': get_latest_filename(final_data_dir, 'affiliations_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_publication_entities = PythonOperator(
    task_id='prepare_publication_entities',
    python_callable=process_publication_entities,
    op_kwargs={
        'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        'venues_path': db_data_dir / 'venues_df.tsv',
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_scientific_domain_entities = PythonOperator(
    task_id='prepare_scientific_domain_entities',
    python_callable=process_scientific_domain_entities,
    op_kwargs={
        'domains_path': lookup_data_dir / 'lookup_table_domains.csv',
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_author_of_relationships = PythonOperator(
    task_id='prepare_author_of_relationships',
    python_callable=process_author_of_relationships,
    op_kwargs={
        'author_to_publications_path': get_latest_filename(final_data_dir, 'author2publication_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_author_collaborates_with_relationships = PythonOperator(
    task_id='prepare_author_collaborates_with_relationships',
    python_callable=process_author_collaborates_with_relationships,
    op_kwargs={
        'author_to_publications_path': get_latest_filename(final_data_dir, 'author2publication_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_author_works_in_relationships = PythonOperator(
    task_id='prepare_author_works_in_relationships',
    python_callable=process_author_works_in_relationships,
    op_kwargs={
        'author_to_affiliations_path': get_latest_filename(final_data_dir, 'author2affiliation_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_publication_published_in_relationships = PythonOperator(
    task_id='prepare_publication_published_in_relationships',
    python_callable=process_publication_published_in_relationships,
    op_kwargs={
        'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_publication_belongs_to_domain_relationships = PythonOperator(
    task_id='prepare_publication_belongs_to_domain_relationships',
    python_callable=process_publication_belongs_to_domain_relationships,
    op_kwargs={
        'publications_to_domains_path': get_latest_filename(final_data_dir, 'publication2domain_'),
        # 'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        # 'arxiv_categories_path': db_data_dir / 'arxiv_categories.csv',
        # 'domains_path': lookup_data_dir / 'lookup_table_domains.csv',
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_publication_cited_by_relationships = PythonOperator(
    task_id='prepare_publication_cited_by_relationships',
    python_callable=process_publication_cited_by_relationships,
    op_kwargs={
        'citations_path': final_data_dir / 'citing_pub.tsv',
        'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_affiliation_covers_domain_relationships = PythonOperator(
    task_id='prepare_affiliation_covers_domain_relationships',
    python_callable=process_affiliation_covers_scientific_domain_relationships,
    op_kwargs={
        'publications_to_affiliations_path': get_latest_filename(final_data_dir, 'affiliation2publication_'),
        'publications_to_domains_path': get_latest_filename(final_data_dir, 'publication2domain_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_affiliation_collaborates_with_relationships = PythonOperator(
    task_id='prepare_affiliation_collaborates_with_relationships',
    python_callable=process_affiliation_collaborates_with_relationships,
    op_kwargs={
        'author_to_publications_path': get_latest_filename(final_data_dir, 'author2publication_'),
        'publications_to_affiliations_path': get_latest_filename(final_data_dir, 'affiliation2publication_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

prepare_affiliation_publishes_in_relationships = PythonOperator(
    task_id='prepare_affiliation_publishes_in_relationships',
    python_callable=process_affiliation_publishes_in_relationships,
    op_kwargs={
        'publications_to_affiliations_path': get_latest_filename(final_data_dir, 'affiliation2publication_'),
        'publications_path': get_latest_filename(final_data_dir, 'publications_'),
        'output_dir': output_dir,
    },
    dag=dag,
)

# Flow

EmptyOperator(task_id='start') >> [
    prepare_venue_entities,
    prepare_author_entities,
    prepare_affiliation_entities,
    prepare_publication_entities,
    prepare_scientific_domain_entities,
    prepare_author_of_relationships,
    prepare_author_collaborates_with_relationships,
    prepare_author_works_in_relationships,
    prepare_publication_published_in_relationships,
    prepare_publication_belongs_to_domain_relationships,
    prepare_publication_cited_by_relationships,
    prepare_affiliation_covers_domain_relationships,
    prepare_affiliation_collaborates_with_relationships,
    prepare_affiliation_publishes_in_relationships,
]

# Localhost Testing
#
# if __name__ == '__main__':
#     process_author_collaborates_with_relationships(
#         Path('/Users/ihar/Studies/Data Engineering/Project/DE-project/Transformation_pipeline/data/final_data/author2publication_20230114173416.csv'),
#         Path('/Users/ihar/Studies/Data Engineering/Project/DE-project/Transformation_pipeline/neo4j/import')
#     )
#
#     process_publication_cited_by_relationships(
#         Path('/Users/ihar/Studies/Data Engineering/Project/DE-project/Transformation_pipeline/data/final_data/citing_pub.tsv'),
#         Path('/Users/ihar/Studies/Data Engineering/Project/DE-project/Transformation_pipeline/data/final_data/publications_20230114173416.csv'),
#         Path('/Users/ihar/Studies/Data Engineering/Project/DE-project/Transformation_pipeline/neo4j/import')
#     )

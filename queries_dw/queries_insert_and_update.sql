-- INSERTS

-- Publications
INSERT INTO warehouse.publications(
    doi,
    title,
    number_of_authors,
    submitter,
    lang,
    volume,
    issue,
    page_numbers,
    number_of_pages,
    number_of_references,
    no_ver_arxiv,
    date_of_first_version,
    number_of_citations,
    snapshot_valid_from,
    snapshot_valid_to
)
VALUES ('doi', 'title', 1, 'submitter', 'lang', 1, 1, 'page_numbers', 1, 1, 
	   1, 'date', 1, 'snapshot_valid_from', 'snapshot_valid_to');
	   
-- Publications (required)
INSERT INTO warehouse.publications(
    doi,
    title,
    number_of_authors,
    submitter,
    lang,
)
VALUES ('doi', 'title', 1, 'submitter', 'lang');

-- Authors
INSERT INTO warehouse.authors(
    first_name,
    last_name,
    full_name,
    h_index_real,
    h_index_calculated,
    valid_from,
    valid_to
)
VALUES ('first_name', 'last_name', 'full_name', 1, 1, 'valid_from', 'valid_to');

-- Authors (required)
INSERT INTO warehouse.authors(
    first_name,
    last_name,
    full_name
)
VALUES ('first_name', 'last_name', 'full_name');
	
-- Institutions (all fields required)
INSERT INTO warehouse.institution(
    name,
    address
)
VALUES ('name', 'address');
	
-- Scientific domain (all fields required)
INSERT INTO warehouse.scientific_domain(
    major_field,
    sub_category,
    exact_category,
    arxivx_category
)
VALUES ('major_field', 'sub_category', 'exact_category', 'arxivx_category');

-- Bridge: publication X author
INSERT INTO warehouse.publication_author(
    publication_id,
    author_id,
    valid_from,
    valid_to,
)
VALUES (1, 1, 'valid_from', 'valid_to');

-- Bridge: publication X author (required)
INSERT INTO warehouse.publication_author(
    publication_id,
    author_id,
    valid_from
)
VALUES (1, 1, 'valid_from');
	
-- Bridge: publication X scientific domain (all fields required)
INSERT INTO warehouse.publication_domain(
    publication_id,
    domain_id,
)
VALUES (1, 1);
	
-- Bridge: publication X institution (all fields required)
INSERT INTO warehouse.publication_domain(
    publication_id,
    institution_id,
)
VALUES (1, 1);
	
-- Venues
INSERT INTO warehouse.authors(
    publication_id,
    full_name,
    abbreviation,
    type,
    h_index_calculated,
    valid_from,
    valid_to
)
VALUES ('publication_id', 'full_name', 'abbreviation', 'type', 
		'h_index_calculated', 'valid_from', 'valid_to');

-- Venues (required)
INSERT INTO warehouse.authors(
    publication_id,
    full_name,
    abbreviation,
    type,
    h_index_calculated,
    valid_from
)
VALUES ('publication_id', 'full_name', 'abbreviation', 'type', 
		'h_index_calculated', 'valid_from');
	
-- Time (all fields required)
INSERT INTO warehouse.publication_domain(
	date,
    publication_id
)
VALUES ('date', 1);
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
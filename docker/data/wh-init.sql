create database warehouse encoding 'utf8';
\connect warehouse;
create schema warehouse;
create sequence wh;

create table if not exists warehouse.publication_venues
(
    id                 bigint    not null default nextval('wh') primary key,
    full_name          text      not null,
    abbreviation       text      not null,
    type               text      not null,
    h_index_calculated text      not null,
    valid_from         timestamp not null,
    valid_to           timestamp,
    constraint publications_venues_check_date check (valid_from < valid_to)
);

create table if not exists warehouse.publication_time
(
    id    bigint not null default nextval('wh') primary key,
    date  date   not null,
    year  int             default extract(year from date),
    month int             default extract(month from date),
    day   int             default extract(day from date)
);

create table if not exists warehouse.publications
(
    id                    bigint not null default nextval('wh') primary key,
    doi                   text   not null,
    title                 text   not null,
    number_of_authors     int    not null,
    submitter             text   not null,
    lang                  text   not null,
    venue_id              bigint,
    time_id               bigint,
    volume                int,
    issue                 int,
    page_numbers          text,
    number_of_pages       int,
    number_of_references  int,
    no_ver_arxiv          int,
    date_of_first_version date,
    number_of_citations   int,
    snapshot_valid_from   timestamp,
    snapshot_valid_to     timestamp,
    CONSTRAINT fk_author_publication_authors FOREIGN KEY (venue_id) REFERENCES warehouse.publication_venues (id),
    CONSTRAINT fk_author_publication_authors FOREIGN KEY (time_id) REFERENCES warehouse.publication_time (id)
);

create table if not exists warehouse.authors
(
    id                 bigint not null default nextval('wh') primary key,
    first_name         text   not null,
    last_name          text   not null,
    full_name          text   not null,
    h_index_real       int,
    h_index_calculated int,
    valid_from         timestamp,
    valid_to           timestamp,
    constraint authors_check_date check ( valid_from < authors.valid_to )
);

create table if not exists warehouse.publication_author
(
    id             bigint not null default nextval('wh') primary key,
    publication_id bigint not null,
    author_id      bigint not null,
    valid_from     bigint not null,
    valid_to       bigint,
    CONSTRAINT fk_author_publication_publications FOREIGN KEY (publication_id) REFERENCES warehouse.publications (id),
    CONSTRAINT fk_author_publication_authors FOREIGN KEY (author_id) REFERENCES warehouse.authors (id),
    constraint publication_author_check_date check (valid_from < valid_to)
);

create table if not exists warehouse.institution
(
    id      bigint not null default nextval('wh') primary key,
    name    text   not null,
    address text   not null
);

create table if not exists warehouse.publication_institution
(
    id             bigint not null default nextval('wh') primary key,
    publication_id bigint not null,
    institution_id bigint not null,
    CONSTRAINT fk_publication_institution_publications FOREIGN KEY (publication_id) REFERENCES warehouse.publications (id),
    CONSTRAINT fk_publication_institution_institution FOREIGN KEY (institution_id) REFERENCES warehouse.institution (id)
);

create table if not exists warehouse.scientific_domain
(
    id              bigint not null default nextval('wh') primary key,
    major_field     text   not null,
    sub_category    text   not null,
    exact_category  text   not null,
    arxivx_category text   not null
);

create table if not exists warehouse.publication_domain
(
    id             bigint not null default nextval('wh') primary key,
    publication_id bigint not null,
    domain_id      bigint not null,
    CONSTRAINT fk_publication_domain_publications FOREIGN KEY (publication_id) REFERENCES warehouse.publications (id),
    CONSTRAINT fk_publication_domain_domain FOREIGN KEY (domain_id) REFERENCES warehouse.scientific_domain (id)
);



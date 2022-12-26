create database warehouse encoding 'utf8';
\connect warehouse;
create schema warehouse;
create sequence wh;

create table if not exists warehouse.publications (
    id bigint not null default nextval('wh') primary key,
    doi text not null,
    title text not null,
    number_of_authors int not null,
    submitter text not null,
    lang text not null,
    volume int,
    issue int,
    page_numbers text,
    number_of_pages int,
    number_of_references int,
    no_ver_arxiv int,
    date_of_first_version date,
    number_of_citations int,
    snapshot_valid_from timestamp,
    snapshot_valid_to timestamp
);

create table if not exists warehouse.authors (
    id bigint not null default nextval('wh'),
    first_name text not null,
    last_name text not null,
    full_name text not null,
    h_index_real int,
    h_index_calculated int,
    valid_from timestamp,
    valid_to timestamp
);

create table if not exists warehouse.institution (
    id bigint not null default nextval('wh'),
    name text not null,
    address text not null
);

create table if not exists warehouse.scientific_domain (
    id bigint not null default nextval('wh'),
    major_field text not null,
    sub_category text not null,
    exact_category text not null,
    arxivx_category text not null
);


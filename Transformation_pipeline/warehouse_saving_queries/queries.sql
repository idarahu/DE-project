-- PUBLICATION TIME

with saved as (select id
               from warehouse.publication_time pt
               where exists(select 1
                            from warehouse.publications p
                            where p.venue_id = pt.id
                              and p.doi = 'test-doi'
                              and p.title = 'test title')),
     inserted as (
         insert into warehouse.publication_time (date, year, month, day)
             select TO_TIMESTAMP('2002-09-17', 'YYYY-MM-DD'),
                    extract(year from TO_TIMESTAMP('2002-09-17', 'YYYY-MM-DD')),
                    extract(month from TO_TIMESTAMP('2002-09-17', 'YYYY-MM-DD')),
                    extract(day from TO_TIMESTAMP('2002-09-17', 'YYYY-MM-DD'))
             where not exists(select 1
                              from warehouse.publications p
                              where p.doi = 'test-doi'
                                and p.title = 'test title')
             returning id)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;

-- VENUES

update warehouse.publication_venues pv
set valid_to = now()
where pv.full_name = 'test full name'
  and pv.abbreviation = 'test abbr'
  and pv.h_index_calculated != 1
  and pv.valid_to is null;

with inserted as (
    insert into warehouse.publication_venues (full_name, abbreviation, h_index_calculated, valid_from)
        select 'test full name', 'test abbr', 1, now()
        where not exists(select 1
                         from warehouse.publication_venues pv
                         where pv.full_name = 'test full name'
                           and pv.abbreviation = 'test abbr'
                           and pv.h_index_calculated = 1
                           and pv.valid_to is null
            )
        returning id),
     saved as (select pv.id
               from warehouse.publication_venues pv
               where pv.full_name = 'test full name'
                 and pv.abbreviation = 'test abbr'
                 and pv.h_index_calculated = 1
                 and pv.valid_to is null
               order by valid_from desc
               limit 1)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;


-- PUBLICATIONS
update warehouse.publications p
set snapshot_valid_to = now()
where p.doi = 'test doi'
  and p.title = 'test title'
  and (p.number_of_references != 4
    or p.number_of_citations != 128)
  and p.snapshot_valid_to is null;

with inserted as (
    insert into warehouse.publications (doi, title, submitter, lang, venue_id, time_id, volume, issue, page_numbers,
                                        number_of_references, no_ver_arxiv, date_of_first_version, number_of_citations,
                                        snapshot_valid_from)
        select 'test doi',
               'test title',
               'submitter name',
               '',
               2,
               1,
               -1,
               -1,
               10,
               4,
               2,
               TO_TIMESTAMP('2002-09-17', 'YYYY-MM-DD'),
               128,
               now()
        where not exists(select 1
                         from warehouse.publications p
                         where p.doi = 'test doi'
                           and p.title = 'test title'
                           and p.number_of_references = 4
                           and p.number_of_citations = 128
                           and p.venue_id = 55)
        returning id),
     saved as (select p.id
               from warehouse.publications p
               where p.doi = 'test doi'
                 and p.title = 'test title'
                 and p.number_of_references = 4
                 and p.number_of_citations = 128
                 and p.venue_id = 55
               order by p.snapshot_valid_from desc
               limit 1)
SELECT COALESCE(
               (select id from inserted),
               (select id from saved)
           ) as id;

-- AUTHORS
-- update author if author h_index_real is changed

update warehouse.authors a
set valid_to = now()
where a.first_name = 'name'
  and a.last_name = 'last'
  and a.full_name = 'fullname'
  and a.h_index_real != 4
  and a.valid_to is null;

with inserted as (
    insert into warehouse.authors (first_name, last_name, first_name_abbr, full_name, h_index_real, valid_from)
        select 'name', 'last', 'n.', 'fullname', 4, now()
        where not exists(select 1
                         from warehouse.authors a
                         where a.first_name = 'name'
                           and a.last_name = 'last'
                           and a.full_name = 'fullname'
                           and a.h_index_real = 4
                           and a.valid_to is null
            )
        returning id),
     saved as (select id
               from warehouse.authors a
               where a.first_name = 'name'
                 and a.last_name = 'last'
                 and a.full_name = 'fullname'
                 and a.h_index_real = 4
                 and a.valid_to is null
               order by a.valid_from desc
               limit 1)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;

-- PUBLICATION_AUTHOR

insert into warehouse.publication_author (publication_id, author_id)
values (3, 4)
on conflict on constraint publication_author_unique_idx do nothing;

-- INSTITUTION (AFFILIATION)
with inserted as (
    insert into warehouse.institution (name, address)
        select 'test institution', 'tartus'
        where not exists(select 1
                         from warehouse.institution i
                         where i.name = 'test institution'
                           and i.address = 'tartus')
        on conflict do nothing
        returning id),
     saved as (select i.id from warehouse.institution i where i.name = 'test institution' and i.address = 'tartus')
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;

-- PUBLICATION INSTITUTION
insert into warehouse.publication_institution (publication_id, institution_id)
values (3, 16)
on conflict on constraint publication_institution_unique_idx do nothing;

-- SCIENTIFIC DOMAIN
with inserted as (
    insert into warehouse.scientific_domain (major_field, sub_category, exact_category, arxivx_category)
        select 'test major field1', 'test sub category', 'exact category', 'arxivx category'
        where not exists(select 1
                         from warehouse.scientific_domain sd
                         where sd.major_field = 'test major field1'
                           and sd.sub_category = 'test sub category'
                           and sd.exact_category = 'exact category'
                           and sd.arxivx_category = 'arxivx category')
        on conflict do nothing returning id),
     saved as (select sd.id
               from warehouse.scientific_domain sd
               where sd.major_field = 'test major field1'
                 and sd.sub_category = 'test sub category'
                 and sd.exact_category = 'exact category'
                 and sd.arxivx_category = 'arxivx category')
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;

-- PUBLICATION PUBLICATION_DOMAIN
insert into warehouse.publication_domain (publication_id, domain_id)
values (3, 23)
on conflict on constraint publication_domain_unique_idx do nothing;



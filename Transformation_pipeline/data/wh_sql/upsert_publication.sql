update warehouse.publications p
set snapshot_valid_to = now()
where p.doi = '{doi}'
  and p.title = '{title}'
  and (p.number_of_references != {number_of_references}
    or p.number_of_citations != {number_of_citations})
  and p.snapshot_valid_to is null;

with inserted as (
    insert into warehouse.publications (doi, title, submitter, lang, venue_id, time_id, volume, issue, page_numbers,
                                        number_of_references, no_ver_arxiv, date_of_first_version, number_of_citations,
                                        snapshot_valid_from)
        select '{doi}'
                   '{title}',
               '{submitter}',
               '{lang}',
        {venue_id},
        {time_id},
        {volume},
        {issue},
        '{page_numbers}',
        {number_of_references},
        {no_ver_arxiv},
        TO_TIMESTAMP('{date_of_first_version}', 'YYYY-MM-DD'),
        {number_of_citations},
        now()
        where not exists (select 1
        from warehouse.publications p
        where p.doi = '{doi}'
        and p.title = '{title}'
        and p.number_of_references = {number_of_references}
        and p.number_of_citations = {number_of_citations}
        and p.venue_id = {venue_id})
        returning id),
     saved as (select p.id
               from warehouse.publications p
               where p.doi = '{doi}'
                 and p.title = '{title}'
                 and p.number_of_references = {number_of_references}
                 and p.number_of_citations = {number_of_citations}
                 and p.venue_id = {venue_id}
               order by p.snapshot_valid_from desc
               limit 1)
SELECT COALESCE(
               (select id from inserted),
               (select id from saved)
           ) as id;
with inserted as (
    insert into warehouse.publication_venues (full_name, abbreviation, valid_from)
        select '{full_name}', '{abbreviation}', now()
        on conflict on constraint publication_venues_unique_idx do nothing
        returning id),
     saved as (select pv.id
               from warehouse.publication_venues pv
               where pv.full_name = '{full_name}'
                 and pv.abbreviation = '{abbreviation}'
                 and pv.valid_to is null
               order by valid_from desc
               limit 1)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
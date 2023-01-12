update warehouse.publication_venues pv
set valid_to = now()
where pv.full_name = '{full_name}'
  and pv.abbreviation = '{abbreviation}'
  and pv.h_index_calculated != {h_index_calculated}
  and pv.valid_to is null;

with inserted as (
    insert into warehouse.publication_venues (full_name, abbreviation, h_index_calculated, valid_from)
        select '{full_name}', 'test abbr', {h_index_calculated}, now()
        where not exists(select 1
                         from warehouse.publication_venues pv
                         where pv.full_name = '{full_name}'
                           and pv.abbreviation = '{abbreviation}'
                           and pv.h_index_calculated = {h_index_calculated}
                           and pv.valid_to is null
            )
        returning id),
     saved as (select pv.id
               from warehouse.publication_venues pv
               where pv.full_name = '{full_name}'
                 and pv.abbreviation = '{abbreviation}'
                 and pv.h_index_calculated = {h_index_calculated}
                 and pv.valid_to is null
               order by valid_from desc
               limit 1)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
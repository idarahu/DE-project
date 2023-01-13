update warehouse.authors a
set valid_to = now()
where a.first_name = '{first_name}'
  and a.last_name = '{last_name}'
  and a.full_name = '{full_name}'
  and a.h_index_real != {h_index_real}
  and a.valid_to is null;

with inserted as (
    insert into warehouse.authors (first_name, last_name, first_name_abbr, full_name, h_index_real, valid_from)
        select '{first_name}',
               '{last_name}',
               '{first_name_abbr}',
               '{full_name}', {h_index_real}, now()
        where not exists (select 1
        from warehouse.authors a
        where a.first_name = '{first_name}'
        and a.last_name = '{last_name}'
        and a.full_name = '{full_name}'
        and a.h_index_real = {h_index_real}
        and a.valid_to is null
        )
        returning id),
     saved as (select id
               from warehouse.authors a
               where a.first_name = '{first_name}'
                 and a.last_name = '{last_name}'
                 and a.full_name = '{full_name}'
                 and a.h_index_real = {h_index_real}
                 and a.valid_to is null
               order by a.valid_from desc
               limit 1)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
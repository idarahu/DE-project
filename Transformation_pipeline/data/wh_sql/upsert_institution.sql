with inserted as (
    insert into warehouse.institution (name, address)
        select '{name}', '{address}'
        where not exists(select 1
                         from warehouse.institution i
                         where i.name = '{name}'
                           and i.address = '{address}')
        on conflict do nothing
        returning id),
     saved as (select i.id from warehouse.institution i where i.name = '{name}' and i.address = '{address}')
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
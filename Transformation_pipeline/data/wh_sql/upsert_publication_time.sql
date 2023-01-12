with saved as (select id
               from warehouse.publication_time pt
               where pt.date = TO_TIMESTAMP('{date}', 'YYYY-MM-DD')),
     inserted as (
         insert into warehouse.publication_time (date, year, month, day)
             select TO_TIMESTAMP('{date}', 'YYYY-MM-DD'),
                    extract(year from TO_TIMESTAMP('{date}', 'YYYY-MM-DD')),
                    extract(month from TO_TIMESTAMP('{date}', 'YYYY-MM-DD')),
                    extract(day from TO_TIMESTAMP('{date}', 'YYYY-MM-DD'))
             on conflict on constraint publication_time_unique_idx do nothing
             returning id)
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
with inserted as (
    insert into warehouse.scientific_domain (major_field, sub_category, exact_category, arxivx_category)
        select '{major_field}', '{sub_category}', '{exact_category}', '{arxiv_category}'
        where not exists(select 1
                         from warehouse.scientific_domain sd
                         where sd.major_field = '{major_field}'
                           and sd.sub_category = '{sub_category}'
                           and sd.exact_category = '{exact_category}'
                           and sd.arxivx_category = '{arxiv_category}')
        on conflict do nothing returning id),
     saved as (select sd.id
               from warehouse.scientific_domain sd
               where sd.major_field = '{major_field}'
                 and sd.sub_category = '{sub_category}'
                 and sd.exact_category = '{exact_category}'
                 and sd.arxivx_category = '{arxiv_category}')
select coalesce(
               (select id from inserted),
               (select id from saved)
           ) as id;
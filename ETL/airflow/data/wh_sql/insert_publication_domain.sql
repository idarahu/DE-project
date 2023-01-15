insert into warehouse.publication_domain (publication_id, domain_id)
values ({publication_id}, {domain_id})
on conflict on constraint publication_domain_unique_idx do nothing;
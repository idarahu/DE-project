insert into warehouse.publication_institution (publication_id, institution_id)
values ({publication_id}, {institution_id})
on conflict on constraint publication_institution_unique_idx do nothing;
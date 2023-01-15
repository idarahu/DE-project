insert into warehouse.publication_author (publication_id, author_id)
values ({publication_id}, {author_id})
on conflict on constraint publication_author_unique_idx do nothing;
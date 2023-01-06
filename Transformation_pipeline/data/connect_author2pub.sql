INSERT INTO author2publication (author_id, publication_id)
SELECT DISTINCT t2.author_id, t1.publication_id
FROM authors_temp t1
JOIN authors t2 ON t1.last_name = t2.last_name AND t1.first_name_abbr = t2.first_name_abbr;

INSERT INTO authors (last_name, first_name, first_name_abbr, extra, position, h_index_real, updated_at)
SELECT DISTINCT last_name, first_name, first_name_abbr, extra, position, h_index_real, updated_at
FROM authors_temp
WHERE NOT EXISTS (
SELECT * FROM authors
WHERE
authors.last_name = authors_temp.last_name
AND authors.first_name_abbr = authors_temp.first_name_abbr);

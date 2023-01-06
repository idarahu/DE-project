CREATE or REPLACE VIEW authors_view AS
SELECT DISTINCT authors.author_id, last_name, first_name, first_name_abbr,
first_name_abbr||' '||last_name as full_name,
position, h_index_real, COALESCE(h_index_calculated, -1) AS h_index_calculated
FROM
(SELECT r.author_id, MAX(ranking) AS h_index_calculated
FROM
(SELECT a.author_ID, p.number_of_citations, ROW_NUMBER() OVER (PARTITION BY a.author_ID ORDER BY p.number_of_citations DESC) AS ranking
FROM
author2publication ap JOIN authors a ON a.author_ID = ap.author_id JOIN publications p ON p.publication_id = ap.publication_id) AS r
WHERE r.number_of_citations >= r.ranking
GROUP BY r.author_id) AS h RIGHT JOIN authors ON h.author_id = authors.author_id;

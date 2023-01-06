CREATE or REPLACE VIEW venues_view AS
SELECT DISTINCT venues.venue_id, full_name, abbreviation,
print_issn, electronic_issn,
COALESCE(h_index_calculated, -1) AS h_index_calculated
FROM
(SELECT venue_id, MAX(ranking) AS h_index_calculated
FROM
(SELECT p.venue_ID, p.number_of_citations, ROW_NUMBER() OVER (PARTITION BY p.venue_ID ORDER BY p.number_of_citations DESC) AS ranking
FROM publications p) AS r
WHERE r.number_of_citations >= r.ranking
GROUP BY r.venue_id) AS h RIGHT JOIN venues ON h.venue_id = venues.venue_id;

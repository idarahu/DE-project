-- Ranking authors with the highest h-index (calculated) in a given time period
SELECT au.full_name, au.first_name, au.h_index_calculated FROM (
	SELECT DISTINCT ON (aut.full_name)
		aut.full_name AS full_name,
		aut.first_name AS first_name,
		aut.h_index_calculated AS h_index_calculated
	FROM warehouse.authors aut
	WHERE
	    (aut.valid_from, aut.valid_to) OVERLAPS ('2012-01-01'::DATE, '2012-04-12'::DATE)
	ORDER BY aut.full_name, aut.valid_to DESC
) au
ORDER BY au.h_index_calculated DESC;
-- Ranking venues/journals with the highest h-index (calculated) in a given time period
SELECT pub_venues.full_name, pub_venues.h_index_calculated FROM (
	SELECT DISTINCT ON (venue.full_name)
		venue.full_name AS full_name,
		venue.h_index_calculated AS h_index_calculated
	FROM warehouse.publication_venues venue
	WHERE
	    (venue.valid_from, venue.valid_to) OVERLAPS ('2012-01-01'::DATE, '2012-04-12'::DATE)
	ORDER BY venue.full_name, venue.valid_to DESC
) pub_venues
ORDER BY pub_venues.h_index_calculated DESC;











	

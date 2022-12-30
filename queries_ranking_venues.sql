-- Ranking venues/journals with the highest h-index in a given time period
SELECT pub_venues.full_name, pub_venues.h_index FROM (
	SELECT 
		DISTINCT ON (venue.id)
		venue.full_name AS full_name,
		venue.h_index_calculated AS h_index
	FROM warehouse.publication_venues venue
	WHERE 
		('date2' BETWEEN venue.valid_from AND venue.valid_to)
		OR
		('date1' BETWEEN venue.valid_from AND venue.valid_to)
		OR
		('date1' <= venue.valid_from AND 'date2' >= venue.valid_to)
	ORDER BY venue.valid_to DESC
) pub_venues
ORDER BY pub_venues.h_index DESC;











	
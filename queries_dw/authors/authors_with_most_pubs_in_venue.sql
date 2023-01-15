-- Ranking authors with the most publications in publication venue
SELECT
	aut.full_name,
	aut.first_name,
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
WHERE pub_venues.full_name = ''
GROUP BY aut.id
ORDER BY num_of_publications DESC;
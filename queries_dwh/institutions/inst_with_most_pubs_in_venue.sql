-- Ranking institutions with the most publications in publication venue
SELECT
	inst.name,
	COUNT(DISTINCT pub.doi) AS num_of_publications
FROM warehouse.publications pub
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY inst.id
ORDER BY num_of_publications DESC;
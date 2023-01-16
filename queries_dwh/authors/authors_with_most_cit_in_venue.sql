-- Ranking authors with the most citations in publication venue
SELECT
	aut.full_name,
	aut.first_name,
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id
		FROM warehouse.publications pu
		WHERE pu.snapshot_valid_to is NULL
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY aut.id
ORDER BY number_of_citations DESC;
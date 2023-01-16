-- Ranking institutions with the most citations in a given publication venue
SELECT
	inst.name,
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
-- join institutions with publications
JOIN warehouse.publication_institution pub_inst
	ON pub_inst.publication_id = pub.pub_id
JOIN warehouse.institution inst
	ON pub_inst.institution_id = inst.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY inst.id
ORDER BY number_of_citations DESC;
-- What are the year's hottest topics (categories of scientific disciplines)?
-- HOTTEST = most publications
-- by venue
-- in major field
SELECT
	scientific_domain.major_field,
	COUNT(DISTINCT pub.pub_doi) AS num_of_publications
FROM
	-- use latest DOIs
	(
		SELECT
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id,
			pu.title AS pub_title,
			pu.doi AS pub_doi
		FROM warehouse.publications pu
		WHERE pu.snapshot_valid_to is NULL
		LIMIT 1
	) pub
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
WHERE
	pub_venues.full_name = ''
GROUP BY scientific_domain.major_field
ORDER BY num_of_publications DESC;
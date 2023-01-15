-- How does the number of publications on a given topic change during a given time frame
SELECT
	pub_time.year,
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
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	-- could do the filtering by major field, sub cat, or id, or exact cat if desired
	-- but in project preparation we mostly talked about major_field
	-- additional queries could easily be added in few clicks
	scientific_domain.major_field = 'natural sciences'
GROUP BY pub_time.year
ORDER BY num_of_publications DESC;
-- Ranking authors with the most citations in a given year, scientific domain and/or publication venue
SELECT
	aut.full_name,
	SUM(pub.pub_num_of_citations) AS number_of_citations
FROM
	-- use latest DOIs
	(
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.time_id AS pub_time_id,
			pu.number_of_citations AS pub_num_of_citations,
			pu.venue_id AS pub_venue_id
		FROM warehouse.publications pu
		ORDER BY pu.doi, pu.snapshot_valid_to DESC
	) pub
-- join authors with publications
JOIN warehouse.publication_author pub_auth
	ON pub_auth.publication_id = pub.pub_id
JOIN warehouse.authors aut
	ON pub_auth.author_id = aut.id
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.pub_id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.pub_venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.pub_time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND scientific_domain.id = 1
	AND pub_venues.full_name = ''
GROUP BY aut.id
ORDER BY number_of_citations DESC;
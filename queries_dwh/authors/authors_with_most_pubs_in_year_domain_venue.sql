-- Ranking authors with the most publications in a given year, scientific domain and/or publication venue
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
-- join scientific domains with publications
JOIN warehouse.publication_domain pub_domain
	ON pub_domain.publication_id = pub.id
JOIN warehouse.scientific_domain scientific_domain
	ON pub_domain.domain_id = scientific_domain.id
-- join venues with publications
JOIN warehouse.publication_venues pub_venues
	ON pub_venues.id = pub.venue_id
-- join time with publications
JOIN warehouse.publication_time pub_time
	ON pub.time_id = pub_time.id
WHERE
	pub_time.year = '2023'
	AND scientific_domain.id = 1
	AND pub_venues.full_name = ''
GROUP BY aut.id
ORDER BY num_of_publications DESC;
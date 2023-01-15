-- What are the year's hottest topics (categories of scientific disciplines)?
-- Hottest = most publications
-- By venue, by institution

SELECT pub_venues.full_name, pub_venues.h_index FROM (
	SELECT 
		DISTINCT ON (venue.full_name)
		venue.full_name AS full_name,
		venue.h_index_calculated AS h_index
	FROM warehouse.publication_venues venue
	WHERE
	    (venue.valid_from, venue.valid_to) OVERLAPS ('2012-01-01'::DATE, '2012-04-12'::DATE)
	ORDER BY venue.valid_to DESC
) pub_venues
ORDER BY pub_venues.h_index DESC;


SELECT
    COUNT()
	scientific_domain.major_field,
	scientific_domain.sub_category,
	SUM(pub.number_of_citations) AS number_of_citations
FROM
	(
		-- Only look at the latest DOI (multiple rows can have same DOI).
		-- Otherwise citations of same paper are counted multiple times.
		SELECT
			DISTINCT ON (pu.doi)
			pu.id AS pub_id,
			pu.number_of_citations AS number_of_citations
		FROM warehouse.publications pu
		ORDER BY pu.snapshot_valid_to DESC
	) pub
	JOIN warehouse.publication_time pub_time
	JOIN warehouse.publication_domain pub_domain
	JOIN warehouse.scientific_domain scientific_domain
	JOIN warehouse.publication_venues pub_venues
	AND pub.publication_time = pub_time.id
	AND pub_domain.domain_id = scientific_domain.id
	AND pub_venues.publication_id = pub.id
WHERE
	pub_time.year = '2023'
	AND scientific_domain.id = 'scientific_domain_id'
	AND pub_venues.id = 'pub_venues_id'
GROUP BY pub.doi
ORDER BY number_of_citations DESC;








	

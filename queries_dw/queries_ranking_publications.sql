-- Ranking publications based on nr of citations in a given year
SELECT 
	pub.title,
	pub.doi,
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
	AND pub.publication_time = pub_time.id
WHERE 
	pub_time.year = '2023'
GROUP BY pub.doi
ORDER BY number_of_citations DESC;

-- Ranking publications based on nr of citations in a given year, scientific domain and/or publication venue
SELECT 
	pub.title,
	pub.doi,
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
























	